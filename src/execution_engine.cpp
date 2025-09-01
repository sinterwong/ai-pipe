/**
 * @file execution_engine.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "execution_engine.hpp"
#include <logger.hpp>
#include <memory>
#include <thread_pool.hpp>
#include <thread_safe_queue.hpp>

namespace ai_pipe {

ExecutionEngine::ExecutionEngine()
    : graph_(nullptr), threadPool_(nullptr),
      pipelineState_(PipelineState::IDLE), activeTasks_(0), stopFlag_(false) {}

ExecutionEngine::~ExecutionEngine() {
  // Ensure graceful shutdown if not already stopped
  if (pipelineState_ == PipelineState::RUNNING ||
      pipelineState_ == PipelineState::STOPPING) {
    stopExecutionSync();
  }
}

ExecutionEngine::ExecutionEngine(ExecutionEngine &&other) {
  if (pipelineState_ == PipelineState::RUNNING ||
      pipelineState_ == PipelineState::STOPPING) {
    stopExecutionSync();
  }

  std::lock_guard<std::mutex> lock(other.engineMutex_);
  std::lock_guard<std::mutex> self_lock(engineMutex_, std::adopt_lock);
  std::lock_guard<std::mutex> other_lock(other.engineMutex_, std::adopt_lock);

  graph_ = other.graph_;
  threadPool_ = std::move(other.threadPool_);
  pipelineState_.store(other.pipelineState_.load(), std::memory_order_relaxed);
  nodeStates_ = std::move(other.nodeStates_);
  nodeInputQueues_ = std::move(other.nodeInputQueues_);
  nodeMutexes_ = std::move(other.nodeMutexes_);
  activeTasks_.store(other.activeTasks_.load(), std::memory_order_relaxed);
  stopFlag_.store(other.stopFlag_.load(), std::memory_order_relaxed);

  other.graph_ = nullptr;
  other.threadPool_.reset();
  other.pipelineState_ = PipelineState::STOPPED;
  other.nodeStates_.clear();
  other.nodeInputQueues_.clear();
  other.nodeMutexes_.clear();
  other.activeTasks_ = 0;
  other.stopFlag_ = true;
}

ExecutionEngine &ExecutionEngine::operator=(ExecutionEngine &&other) {
  if (this != &other) {
    if (pipelineState_ == PipelineState::RUNNING ||
        pipelineState_ == PipelineState::STOPPING) {
      stopExecutionSync();
    }
    std::lock(engineMutex_, other.engineMutex_);
    std::lock_guard<std::mutex> self_lock(engineMutex_, std::adopt_lock);
    std::lock_guard<std::mutex> other_lock(other.engineMutex_, std::adopt_lock);

    graph_ = other.graph_;
    threadPool_ = std::move(other.threadPool_);
    pipelineState_.store(other.pipelineState_.load(),
                         std::memory_order_relaxed);
    nodeStates_ = std::move(other.nodeStates_);
    nodeInputQueues_ = std::move(other.nodeInputQueues_);
    nodeMutexes_ = std::move(other.nodeMutexes_);
    activeTasks_.store(other.activeTasks_.load(), std::memory_order_relaxed);
    stopFlag_.store(other.stopFlag_.load(), std::memory_order_relaxed);

    other.graph_ = nullptr;
    other.threadPool_.reset();
    other.pipelineState_ = PipelineState::STOPPED;
    other.nodeStates_.clear();
    other.nodeInputQueues_.clear();
    other.nodeMutexes_.clear();
    other.activeTasks_ = 0;
    other.stopFlag_ = true;

    return *this;
  }
  return *this;
}

bool ExecutionEngine::initialize(Graph *graph, uint8_t numWorkers) {
  if (!graph) {
    LOG_ERRORS << "ExecutionEngine: Invalid graph pointer.";
    return false;
  }

  std::lock_guard<std::mutex> lock(engineMutex_);
  graph_ = graph;
  threadPool_ = std::make_unique<ThreadPool>(numWorkers);

  sinkNodes_.clear();
  nodeStates_.clear();
  nodeInputQueues_.clear();
  nodeMutexes_.clear();

  for (const auto &node : graph_->getNodes()) {
    nodeStates_[node] = std::make_unique<std::atomic<NodeExecutionState>>(
        NodeExecutionState::WAITING);
    nodeMutexes_[node] = std::make_unique<std::mutex>();

    PortInputQueues portQueues;
    for (const auto &portName : node->getExpectedInputPorts()) {
      portQueues[portName] = std::make_shared<ThreadSafeQueue<PortDataPtr>>();
    }
    nodeInputQueues_[node] = std::move(portQueues);
  }

  activeTasks_ = 0;
  stopFlag_ = false;
  pipelineState_ = PipelineState::IDLE;

  // Identify sink nodes
  for (const auto &node : graph_->getNodes()) {
    if (graph_->getOutDegree(node) == 0) {
      sinkNodes_.push_back(node);
      LOG_INFOS << "ExecutionEngine: Identified sink node: " << node->getName();
    }
  }
  LOG_INFOS << "ExecutionEngine: Initialized.";
  return true;
}

bool ExecutionEngine::execute(const PortDataMap &initialInputs,
                              bool waitForCompletion,
                              std::shared_ptr<PipelineContext> context) {
  curContext_ = context;
  std::unique_lock<std::mutex> lock(engineMutex_);
  if (pipelineState_ == PipelineState::RUNNING) {
    LOG_ERRORS
        << "ExecutionEngine: Already running. Cannot start new execution.";
    curContext_ = nullptr;
    return false;
  }

  if (pipelineState_ == PipelineState::STOPPING) {
    LOG_ERRORS
        << "ExecutionEngine: Currently stopping. Cannot start new execution.";
    curContext_ = nullptr;
    return false;
  }
  if (!graph_ || !threadPool_) {
    LOG_ERRORS << "ExecutionEngine: Not initialized.";
    curContext_ = nullptr;
    return false;
  }

  LOG_INFOS << "ExecutionEngine: Starting execution.";

  pipelineState_ = PipelineState::RUNNING;
  stopFlag_ = false;
  activeTasks_ = 0;

  { // Scope for finalResultsMutex_ lock
    std::lock_guard<std::mutex> final_results_lock(finalResultsMutex_);
    accumulatedFinalResults_.clear();
  }

  for (const auto &node : graph_->getNodes()) {
    nodeStates_[node]->store(NodeExecutionState::WAITING,
                             std::memory_order_relaxed);
    for (const auto &pair : nodeInputQueues_[node]) {
      pair.second->clear();
    }
  }
  // release engine lock before distributing data and scheduling
  lock.unlock();

  if (!distributeInitialInputs(initialInputs)) {
    std::lock_guard<std::mutex> endLock(engineMutex_);
    pipelineState_ = PipelineState::ERROR;
    LOG_ERRORS << "ExecutionEngine: Failed to distribute initial inputs.";
    curContext_ = nullptr;
    return false;
  }

  if (waitForCompletion) {
    std::unique_lock<std::mutex> completionLock(engineMutex_);
    completionCondition_.wait(completionLock, [this] {
      return activeTasks_ == 0 || stopFlag_.load(std::memory_order_acquire);
    });
    completionLock.unlock();

    std::lock_guard<std::mutex> finalStateLock(engineMutex_);
    if (stopFlag_.load(std::memory_order_acquire) &&
        pipelineState_ != PipelineState::STOPPED) {
      pipelineState_ = PipelineState::STOPPED;
      LOG_ERRORS << "ExecutionEngine: Execution was stopped.";
    } else if (activeTasks_ == 0 && pipelineState_ == PipelineState::RUNNING) {
      pipelineState_ = PipelineState::IDLE; // Successful completion
      LOG_INFOS << "ExecutionEngine: Execution completed successfully.";
    } else if (pipelineState_ != PipelineState::ERROR &&
               pipelineState_ != PipelineState::STOPPED) {
      if (pipelineState_ != PipelineState::ERROR)
        pipelineState_ = PipelineState::ERROR; // Default to error if stuck
      LOG_ERRORS << "ExecutionEngine: Execution finished with activeTasks="
                 << activeTasks_
                 << " and state=" << static_cast<int>(pipelineState_.load());
    }
    bool result = pipelineState_ == PipelineState::IDLE;
    curContext_ = nullptr;
    return result;
  }
  curContext_ = nullptr;
  return true;
}

void ExecutionEngine::stopExecutionAsync() {
  LOG_INFOS << "ExecutionEngine: stopExecutionAsync called.";
  bool expected = false;
  if (stopFlag_.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(engineMutex_);
    if (pipelineState_ == PipelineState::RUNNING) {
      pipelineState_ = PipelineState::STOPPING;
    }
    completionCondition_.notify_all();
  }
}

void ExecutionEngine::stopExecutionSync() {
  LOG_INFOS << "ExecutionEngine: stopExecutionSync called.";
  stopExecutionAsync();
  std::unique_lock<std::mutex> lock(engineMutex_);
  if (pipelineState_ == PipelineState::STOPPING ||
      pipelineState_ == PipelineState::RUNNING) {
    completionCondition_.wait(lock, [this] {
      return activeTasks_ == 0 || pipelineState_ == PipelineState::STOPPED ||
             pipelineState_ == PipelineState::ERROR;
    });
  }
  // ensure final state is STOPPED if it was stopping.
  if (pipelineState_ == PipelineState::STOPPING) {
    pipelineState_ = PipelineState::STOPPED;
  }
  LOG_INFOS << "ExecutionEngine: Execution fully stopped. Active tasks: "
            << activeTasks_;
}

void ExecutionEngine::reset() {
  LOG_INFOS << "ExecutionEngine: Resetting.";

  // ensure any ongoing execution is stopped
  stopExecutionSync();

  std::lock_guard<std::mutex> lock(engineMutex_);
  for (const auto &node : graph_->getNodes()) {
    if (nodeStates_.count(node)) {
      nodeStates_[node]->store(NodeExecutionState::WAITING,
                               std::memory_order_relaxed);
    }
    if (nodeInputQueues_.count(node)) {
      for (const auto &pair : nodeInputQueues_.at(node)) {
        pair.second->clear();
      }
    }
  }
  activeTasks_ = 0;
  stopFlag_ = false;
  pipelineState_ = PipelineState::IDLE;
  LOG_INFOS << "ExecutionEngine: Reset complete.";
}

PipelineState ExecutionEngine::getState() const {
  return pipelineState_.load(std::memory_order_acquire);
}

std::unordered_map<std::string, NodeExecutionState>
ExecutionEngine::getNodeStates() const {
  std::unordered_map<std::string, NodeExecutionState> result;
  // std::lock_guard<std::mutex> lock(engineMutex_);
  for (const auto &[nodePtr, stateAtomicPtr] : nodeStates_) {
    if (nodePtr && stateAtomicPtr) {
      result[nodePtr->getName()] =
          stateAtomicPtr->load(std::memory_order_acquire);
    }
  }
  return result;
};

bool ExecutionEngine::distributeInitialInputs(
    const PortDataMap &initialInputs) {
  bool hasScheduledSomething = false;
  for (const auto &node : graph_->getNodes()) {
    if (graph_->getInDegree(node) == 0) {
      // Check if this source node needs one of the initial inputs
      if (initialInputs.count(node->getName())) {
        const auto dataPacket = initialInputs.at(node->getName());
        auto expectedPorts = node->getExpectedInputPorts();
        if (!expectedPorts.empty()) {
          // FIXME: Feed to first port
          const std::string &targetPortName = expectedPorts[0];
          if (nodeInputQueues_[node].count(targetPortName)) {
            nodeInputQueues_[node][targetPortName]->push(dataPacket);
            LOG_INFOS << "ExecutionEngine: Distributed initial input to "
                      << node->getName() << ":" << targetPortName;
            hasScheduledSomething = true;
            tryScheduleNode(node);
          } else {
            LOG_ERRORS << "ExecutionEngine: Initial input for "
                       << node->getName() << " - port " << targetPortName
                       << " queue not found.";
          }
        } else {
          // Node takes no named inputs but is a source, maybe it just starts
          LOG_INFOS << "ExecutionEngine: Source node " << node->getName()
                    << " has no input ports, attempting to schedule.";
          hasScheduledSomething = true;
          tryScheduleNode(node); // It might be ready if it expects no inputs
        }
      } else if (node->getExpectedInputPorts().empty()) {
        // Source node that doesn't take external data, e.g., a generator
        LOG_INFOS << "ExecutionEngine: Auto-scheduling source node "
                  << node->getName() << " (no inputs expected).";
        hasScheduledSomething = true;
        tryScheduleNode(node);
      }
    }
  }
  if (!hasScheduledSomething && !initialInputs.empty()) {
    LOG_ERRORS << "ExecutionEngine: Initial inputs provided, but no source "
                  "nodes consumed them or were scheduled.";
    throw std::runtime_error(
        "ExecutionEngine: Initial inputs provided, but no "
        "source nodes consumed them or were scheduled. This might be an "
        "error "
        "depending on graph structure.");
  }
  // No inputs, no auto-start source nodes
  if (initialInputs.empty() && !hasScheduledSomething) {
    bool foundAnySourceNode = false;
    for (const auto &node : graph_->getNodes()) {
      if (graph_->getInDegree(node) == 0) {
        foundAnySourceNode = true;
        break;
      }
    }
    if (foundAnySourceNode) {
      LOG_ERRORS << "ExecutionEngine: No initial inputs and no auto-starting "
                    "source nodes were scheduled.";
      throw std::runtime_error("There are source nodes but none started.");
    }
  }
  return true; // distribution itself didn't fail, even if nothing was
               // scheduled
}

void ExecutionEngine::tryScheduleNode(const std::shared_ptr<NodeBase> &node) {
  if (stopFlag_.load(std::memory_order_acquire))
    return;

  NodeExecutionState currentState =
      nodeStates_[node]->load(std::memory_order_acquire);
  if (currentState != NodeExecutionState::WAITING) {
    return;
  }

  // Lock this specific node's mutex for the check-and-schedule logic
  std::lock_guard<std::mutex> nodeLock(*(nodeMutexes_[node]));

  // Re-check state after acquiring lock, in case it changed
  currentState = nodeStates_[node]->load(std::memory_order_relaxed);
  if (currentState != NodeExecutionState::WAITING) {
    return;
  }

  // Check if all input port queues have data
  bool allInputsReady = true;
  auto expectedPorts = node->getExpectedInputPorts();
  if (expectedPorts.empty() && graph_->getInDegree(node) > 0) {
    // This node expects no named inputs, but has graph predecessors.
    // This specific scenario needs careful handling of how data flows from
    // unnamed ports. For now, assume if getExpectedInputPorts is empty, it's
    // ready if it's a source, or if data flow is handled differently (e.g.
    // control dependency). If it has in-degree > 0 and no named input ports,
    // it's ambiguous how it gets data. Let's assume for now it's only ready
    // if in-degree is 0.
    if (graph_->getInDegree(node) > 0) {
      // It has parents but no way to receive data via named ports
      LOG_INFOS << "Debug: Node " << node->getName()
                << " has in-degree but no expected input ports. Cannot "
                   "determine readiness.";
      allInputsReady = false;
    }
  } else {
    for (const auto &portName : expectedPorts) {
      if (!nodeInputQueues_[node].count(portName) ||
          nodeInputQueues_[node][portName]->empty()) {
        allInputsReady = false;
        break;
      }
    }
  }

  if (allInputsReady) {
    if (nodeStates_[node]->compare_exchange_strong(
            currentState, // currentState is WAITING here
            NodeExecutionState::READY, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      activeTasks_++;
      LOG_INFOS << "ExecutionEngine: Node " << node->getName()
                << " is READY. Active tasks: " << activeTasks_;
      threadPool_->submit(&ExecutionEngine::executeNodeTask, this, node,
                          curContext_);
    }
  }
}

void ExecutionEngine::executeNodeTask(
    std::shared_ptr<NodeBase> node, std::shared_ptr<PipelineContext> context) {
  if (stopFlag_.load(std::memory_order_acquire)) {
    nodeStates_[node]->store(NodeExecutionState::WAITING,
                             std::memory_order_release); // Or a CANCELLED state
    activeTasks_--;
    checkCompletionAndNotify();
    LOG_INFOS << "ExecutionEngine: Node " << node->getName()
              << " execution cancelled due to stop flag. Active tasks: "
              << activeTasks_;
    return;
  }

  NodeExecutionState expectedReady = NodeExecutionState::READY;
  if (!nodeStates_[node]->compare_exchange_strong(
          expectedReady, NodeExecutionState::EXECUTING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    // Another thread might have tried to cancel it, or it wasn't READY
    // This case should be rare if scheduling logic is correct
    LOG_INFOS << "ExecutionEngine: Node " << node->getName()
              << " was not READY for execution. State: "
              << static_cast<int>(expectedReady) << ". Aborting task.";
    // activeTasks_ was incremented when set to READY. If it's not executed,
    // it should be decremented. However, if it's already EXECUTING by another
    // thread (shouldn't happen with nodeMutex), or COMPLETED/FAILED, then
    // activeTasks_ would be handled by that path. This situation implies a
    // logic flaw or a race not fully covered. For safety, if it wasn't set to
    // EXECUTING by this call, we might not own the activeTasks_ decrement
    // here. The original submitter that set it to READY is responsible. But
    // since we are here, it means this task was submitted. The CAS failed,
    // meaning the state changed from READY. If it changed to EXECUTING by
    // this very thread, fine. If it changed by something else (e.g. reset,
    // stop), that's different. The current logic: READY -> submit -> (here)
    // READY to EXECUTING. If CAS fails, it means it's no longer READY. It
    // could be STOPPED, WAITING (if reset). We only decrement activeTasks if
    // we are sure this task won't proceed.
    activeTasks_--; // It was marked READY, activeTasks incremented, but won't
                    // execute now.
    checkCompletionAndNotify();
    return;
  }
  LOG_INFOS << "ExecutionEngine: Node " << node->getName() << " is EXECUTING.";

  PortDataMap inputs;
  PortDataMap outputs;
  bool success = true;

  try {
    // Prepare inputs (pop from queues) - this part needs the node's mutex
    {
      std::lock_guard<std::mutex> node_lock(*(nodeMutexes_[node]));
      for (const auto &port_name : node->getExpectedInputPorts()) {
        // Assume queue is not empty because tryScheduleNode checked
        // However, a pop could fail if queue becomes empty due to external
        // clear (e.g. reset) For simplicity, we assume try_pop succeeds. A
        // robust version would handle failure.
        auto dataItem = nodeInputQueues_[node][port_name]->try_pop();
        if (dataItem.has_value()) {
          inputs[port_name] = dataItem.value();
        } else {
          // This should not happen if readiness check was correct and no
          // external clear
          LOG_ERRORS << "ExecutionEngine: CRITICAL - Input queue for "
                     << node->getName() << ":" << port_name
                     << " was empty during input prep!";
          success = false; // Cannot proceed without input
          break;
        }
      }
    } // Release node mutex before calling process

    if (success) { // Only process if inputs were successfully gathered
      node->process(inputs, outputs, context); // The actual work
    }

  } catch (const std::exception &e) {
    LOG_ERRORS << "ExecutionEngine: Node " << node->getName()
               << " execution failed with exception: " << e.what();
    if (onErrorCallback_) {
      onErrorCallback_(e.what(), node->getName());
    }
    success = false;
  } catch (...) {
    LOG_ERRORS << "ExecutionEngine: Node " << node->getName()
               << " execution failed with unknown exception.";
    if (onErrorCallback_) {
      onErrorCallback_("Unknown exception during node processing",
                       node->getName());
    }
    success = false;
  }

  if (stopFlag_.load(std::memory_order_acquire)) {
    nodeStates_[node]->store(NodeExecutionState::WAITING,
                             std::memory_order_release); // Or CANCELLED
    LOG_INFOS << "ExecutionEngine: Node " << node->getName()
              << " processing interrupted by stop flag.";
  } else if (success) {
    nodeStates_[node]->store(NodeExecutionState::COMPLETED,
                             std::memory_order_release);
    LOG_INFOS << "ExecutionEngine: Node " << node->getName() << " COMPLETED.";
    // Check if this node is a sink node
    bool isSinkNode = false;
    for (const auto &sinkNodePtr : sinkNodes_) {
      if (sinkNodePtr == node) {
        isSinkNode = true;
        break;
      }
    }
    if (isSinkNode) {
      LOG_INFOS << "ExecutionEngine: Node " << node->getName()
                << " is a sink node. Collecting results.";
      std::lock_guard<std::mutex> finalResultsLock(finalResultsMutex_);
      for (const auto &pair : outputs) {
        std::string resultKey = node->getName() + ":" + pair.first;
        accumulatedFinalResults_[resultKey] = pair.second;
        LOG_INFOS << "ExecutionEngine: Added final result with key: "
                  << resultKey;
      }
    }
    propagateOutputAndScheduleDownstream(node, outputs);
  } else {
    nodeStates_[node]->store(NodeExecutionState::FAILED,
                             std::memory_order_release);
    LOG_ERRORS << "ExecutionEngine: Node " << node->getName() << " FAILED.";
    // set global stopFlag_ on first failure:
    stopExecutionAsync();
  }

  activeTasks_--;
  LOG_INFOS << "ExecutionEngine: Node " << node->getName()
            << " task finished. Active tasks: " << activeTasks_;
  checkCompletionAndNotify();
}

void ExecutionEngine::propagateOutputAndScheduleDownstream(
    const std::shared_ptr<NodeBase> &sourceNode, const PortDataMap &outputs) {
  if (stopFlag_.load(std::memory_order_acquire))
    return;

  auto outgoingEdges = graph_->getOutgoingEdges(sourceNode);
  for (const auto &edge : outgoingEdges) {
    if (outputs.count(edge.sourcePort)) {
      // This is a shared_ptr
      const auto dataToPropagate = outputs.at(edge.sourcePort);
      auto destNode = edge.destNode;
      auto destPort = edge.destPort;

      if (nodeInputQueues_.count(destNode) &&
          nodeInputQueues_[destNode].count(destPort)) {
        nodeInputQueues_[destNode][destPort]->push(dataToPropagate);
        LOG_INFOS << "ExecutionEngine: Propagated output from "
                  << sourceNode->getName() << ":" << edge.sourcePort << " to "
                  << destNode->getName() << ":" << destPort;
        tryScheduleNode(destNode);
      } else {
        LOG_ERRORS << "ExecutionEngine: ERROR - Downstream queue not found for "
                   << destNode->getName() << ":" << destPort;
      }
    }
  }
}

void ExecutionEngine::checkCompletionAndNotify() {
  if (activeTasks_ == 0) { // Could also check stopFlag_ here
    LOG_INFOS << "ExecutionEngine: All active tasks seem to be completed or "
                 "pipeline is stopping.";
    // If pipelineState_ is RUNNING and activeTasks_ becomes 0, it means
    // successful completion of the current workload. If pipelineState_ is
    // STOPPING, this signals that all tasks have indeed finished.
    PipelineState currentPipelineState =
        pipelineState_.load(std::memory_order_acquire);

    // Check if it's a successful completion and the callback is set
    if (currentPipelineState == PipelineState::RUNNING &&
        !stopFlag_.load(std::memory_order_acquire) && onResultCallback_) {

      PortDataMap resultsToSend;
      { // Scope for finalResultsMutex_ lock
        std::lock_guard<std::mutex> final_results_lock(finalResultsMutex_);
        resultsToSend = accumulatedFinalResults_;
      }
      LOG_INFOS << "ExecutionEngine: Invoking onResultCallback_ with "
                << resultsToSend.size() << " final results.";
      onResultCallback_(resultsToSend);
    }

    // Notify any waiting threads (e.g., in execute or stopExecutionSync)
    if (currentPipelineState == PipelineState::RUNNING ||
        currentPipelineState == PipelineState::STOPPING) {
      // Note: We only notify. The `execute` or `stopExecutionSync` methods
      // waiting on completionCondition_ will re-check conditions and update
      // pipelineState_ properly under engineMutex_.
      completionCondition_.notify_all();
    }
  }
}

void ExecutionEngine::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  onResultCallback_ = std::move(callback);
}

void ExecutionEngine::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {
  onErrorCallback_ = std::move(callback);
}
} // namespace ai_pipe
