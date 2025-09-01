/**
 * @file default_execution_engine.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "default_execution_engine.hpp"
#include <logger.hpp>
#include <memory>
#include <thread_pool.hpp>
#include <thread_safe_queue.hpp>

namespace ai_pipe {

DefaultExecutionEngine::DefaultExecutionEngine()
    : mGraph(nullptr), mThreadPool(nullptr), mEngineState(EngineState::IDLE),
      mActiveTasks(0), mStopFlag(false) {}

DefaultExecutionEngine::~DefaultExecutionEngine() {
  // Ensure graceful shutdown if not already stopped
  if (mEngineState == EngineState::RUNNING) {
    stopExecutionSync();
  }
}

DefaultExecutionEngine::DefaultExecutionEngine(DefaultExecutionEngine &&other) {
  if (mEngineState == EngineState::RUNNING) {
    stopExecutionSync();
  }

  std::lock_guard<std::mutex> lock(other.mEngineMutex);
  std::lock_guard<std::mutex> self_lock(mEngineMutex, std::adopt_lock);
  std::lock_guard<std::mutex> other_lock(other.mEngineMutex, std::adopt_lock);

  mGraph = other.mGraph;
  mThreadPool = std::move(other.mThreadPool);
  mEngineState.store(other.mEngineState.load(), std::memory_order_relaxed);
  mNodeStates = std::move(other.mNodeStates);
  mNodeInputQueues = std::move(other.mNodeInputQueues);
  mNodeMutexes = std::move(other.mNodeMutexes);
  mActiveTasks.store(other.mActiveTasks.load(), std::memory_order_relaxed);
  mStopFlag.store(other.mStopFlag.load(), std::memory_order_relaxed);

  other.mGraph = nullptr;
  other.mThreadPool.reset();
  other.mEngineState = EngineState::STOPPED;
  other.mNodeStates.clear();
  other.mNodeInputQueues.clear();
  other.mNodeMutexes.clear();
  other.mActiveTasks = 0;
  other.mStopFlag = true;
}

DefaultExecutionEngine &
DefaultExecutionEngine::operator=(DefaultExecutionEngine &&other) {
  if (this != &other) {
    if (mEngineState == EngineState::RUNNING) {
      stopExecutionSync();
    }
    std::lock(mEngineMutex, other.mEngineMutex);
    std::lock_guard<std::mutex> self_lock(mEngineMutex, std::adopt_lock);
    std::lock_guard<std::mutex> other_lock(other.mEngineMutex, std::adopt_lock);

    mGraph = other.mGraph;
    mThreadPool = std::move(other.mThreadPool);
    mEngineState.store(other.mEngineState.load(), std::memory_order_relaxed);
    mNodeStates = std::move(other.mNodeStates);
    mNodeInputQueues = std::move(other.mNodeInputQueues);
    mNodeMutexes = std::move(other.mNodeMutexes);
    mActiveTasks.store(other.mActiveTasks.load(), std::memory_order_relaxed);
    mStopFlag.store(other.mStopFlag.load(), std::memory_order_relaxed);

    other.mGraph = nullptr;
    other.mThreadPool.reset();
    other.mEngineState = EngineState::STOPPED;
    other.mNodeStates.clear();
    other.mNodeInputQueues.clear();
    other.mNodeMutexes.clear();
    other.mActiveTasks = 0;
    other.mStopFlag = true;

    return *this;
  }
  return *this;
}

bool DefaultExecutionEngine::initialize(Graph *graph, uint8_t numWorkers) {
  if (!graph) {
    LOG_ERRORS << "DefaultExecutionEngine: Invalid graph pointer.";
    return false;
  }

  std::lock_guard<std::mutex> lock(mEngineMutex);
  mGraph = graph;
  mThreadPool = std::make_unique<ThreadPool>(numWorkers);

  mSinkNodes.clear();
  mNodeStates.clear();
  mNodeInputQueues.clear();
  mNodeMutexes.clear();

  for (const auto &node : mGraph->getNodes()) {
    mNodeStates[node] = std::make_unique<std::atomic<NodeExecutionState>>(
        NodeExecutionState::WAITING);
    mNodeMutexes[node] = std::make_unique<std::mutex>();

    PortInputQueues portQueues;
    for (const auto &portName : node->getExpectedInputPorts()) {
      portQueues[portName] = std::make_shared<ThreadSafeQueue<PortDataPtr>>();
    }
    mNodeInputQueues[node] = std::move(portQueues);
  }

  mActiveTasks = 0;
  mStopFlag = false;
  mEngineState = EngineState::IDLE;

  // Identify sink nodes
  for (const auto &node : mGraph->getNodes()) {
    if (mGraph->getOutDegree(node) == 0) {
      mSinkNodes.push_back(node);
      LOG_INFOS << "DefaultExecutionEngine: Identified sink node: "
                << node->getName();
    }
  }
  LOG_INFOS << "DefaultExecutionEngine: Initialized.";
  return true;
}

bool DefaultExecutionEngine::execute(const PortDataMap &initialInputs,
                                     bool waitForCompletion,
                                     std::shared_ptr<PipelineContext> context) {
  mCurContext = context;
  std::unique_lock<std::mutex> lock(mEngineMutex);
  if (mEngineState == EngineState::RUNNING) {
    LOG_ERRORS << "DefaultExecutionEngine: Already running. Cannot start new "
                  "execution.";
    mCurContext = nullptr;
    return false;
  }

  if (!mGraph || !mThreadPool) {
    LOG_ERRORS << "DefaultExecutionEngine: Not initialized.";
    mCurContext = nullptr;
    return false;
  }

  LOG_INFOS << "DefaultExecutionEngine: Starting execution.";

  mEngineState = EngineState::RUNNING;
  mStopFlag = false;
  mActiveTasks = 0;

  { // Scope for mFinalResultsMutex lock
    std::lock_guard<std::mutex> final_results_lock(mFinalResultsMutex);
    mAccumulatedFinalResults.clear();
  }

  for (const auto &node : mGraph->getNodes()) {
    mNodeStates[node]->store(NodeExecutionState::WAITING,
                             std::memory_order_relaxed);
    for (const auto &pair : mNodeInputQueues[node]) {
      pair.second->clear();
    }
  }
  // release engine lock before distributing data and scheduling
  lock.unlock();

  if (!distributeInitialInputs(initialInputs)) {
    std::lock_guard<std::mutex> endLock(mEngineMutex);
    mEngineState = EngineState::ERROR;
    LOG_ERRORS
        << "DefaultExecutionEngine: Failed to distribute initial inputs.";
    mCurContext = nullptr;
    return false;
  }

  if (waitForCompletion) {
    std::unique_lock<std::mutex> completionLock(mEngineMutex);
    mCompletionCondition.wait(completionLock, [this] {
      return mActiveTasks == 0 || mStopFlag.load(std::memory_order_acquire);
    });
    completionLock.unlock();

    std::lock_guard<std::mutex> finalStateLock(mEngineMutex);
    if (mStopFlag.load(std::memory_order_acquire) &&
        mEngineState != EngineState::STOPPED) {
      mEngineState = EngineState::STOPPED;
      LOG_ERRORS << "DefaultExecutionEngine: Execution was stopped.";
    } else if (mActiveTasks == 0 && mEngineState == EngineState::RUNNING) {
      mEngineState = EngineState::IDLE; // Successful completion
      LOG_INFOS << "DefaultExecutionEngine: Execution completed successfully.";
    } else if (mEngineState != EngineState::ERROR &&
               mEngineState != EngineState::STOPPED) {
      if (mEngineState != EngineState::ERROR)
        mEngineState = EngineState::ERROR; // Default to error if stuck
      LOG_ERRORS
          << "DefaultExecutionEngine: Execution finished with activeTasks="
          << mActiveTasks
          << " and state=" << static_cast<int>(mEngineState.load());
    }
    bool result = mEngineState == EngineState::IDLE;
    mCurContext = nullptr;
    return result;
  }
  mCurContext = nullptr;
  return true;
}

void DefaultExecutionEngine::stopExecutionAsync() {
  LOG_INFOS << "DefaultExecutionEngine: stopExecutionAsync called.";
  bool expected = false;
  if (mStopFlag.compare_exchange_strong(expected, true,
                                        std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(mEngineMutex);
    if (mEngineState == EngineState::RUNNING) {
      mEngineState = EngineState::STOPPED;
    }
    mCompletionCondition.notify_all();
  }
}

void DefaultExecutionEngine::stopExecutionSync() {
  LOG_INFOS << "DefaultExecutionEngine: stopExecutionSync called.";
  stopExecutionAsync();
  std::unique_lock<std::mutex> lock(mEngineMutex);
  if (mEngineState == EngineState::RUNNING) {
    mCompletionCondition.wait(lock, [this] {
      return mActiveTasks == 0 || mEngineState == EngineState::STOPPED ||
             mEngineState == EngineState::ERROR;
    });
  }
  // ensure final state is STOPPED if it was stopping.
  if (mEngineState == EngineState::RUNNING) {
    mEngineState = EngineState::STOPPED;
  }
  LOG_INFOS << "DefaultExecutionEngine: Execution fully stopped. Active tasks: "
            << mActiveTasks;
}

void DefaultExecutionEngine::reset() {
  LOG_INFOS << "DefaultExecutionEngine: Resetting.";

  // ensure any ongoing execution is stopped
  stopExecutionSync();

  std::lock_guard<std::mutex> lock(mEngineMutex);
  for (const auto &node : mGraph->getNodes()) {
    if (mNodeStates.count(node)) {
      mNodeStates[node]->store(NodeExecutionState::WAITING,
                               std::memory_order_relaxed);
    }
    if (mNodeInputQueues.count(node)) {
      for (const auto &pair : mNodeInputQueues.at(node)) {
        pair.second->clear();
      }
    }
  }
  mActiveTasks = 0;
  mStopFlag = false;
  mEngineState = EngineState::IDLE;
  LOG_INFOS << "DefaultExecutionEngine: Reset complete.";
}

EngineState DefaultExecutionEngine::getState() const {
  return mEngineState.load(std::memory_order_acquire);
}

std::unordered_map<std::string, NodeExecutionState>
DefaultExecutionEngine::getNodeStates() const {
  std::unordered_map<std::string, NodeExecutionState> result;
  // std::lock_guard<std::mutex> lock(mEngineMutex);
  for (const auto &[nodePtr, stateAtomicPtr] : mNodeStates) {
    if (nodePtr && stateAtomicPtr) {
      result[nodePtr->getName()] =
          stateAtomicPtr->load(std::memory_order_acquire);
    }
  }
  return result;
};

bool DefaultExecutionEngine::distributeInitialInputs(
    const PortDataMap &initialInputs) {
  bool hasScheduledSomething = false;
  for (const auto &node : mGraph->getNodes()) {
    if (mGraph->getInDegree(node) == 0) {
      // Check if this source node needs one of the initial inputs
      if (initialInputs.count(node->getName())) {
        const auto dataPacket = initialInputs.at(node->getName());
        auto expectedPorts = node->getExpectedInputPorts();
        if (!expectedPorts.empty()) {
          // FIXME: Feed to first port
          const std::string &targetPortName = expectedPorts[0];
          if (mNodeInputQueues[node].count(targetPortName)) {
            mNodeInputQueues[node][targetPortName]->push(dataPacket);
            LOG_INFOS << "DefaultExecutionEngine: Distributed initial input to "
                      << node->getName() << ":" << targetPortName;
            hasScheduledSomething = true;
            tryScheduleNode(node);
          } else {
            LOG_ERRORS << "DefaultExecutionEngine: Initial input for "
                       << node->getName() << " - port " << targetPortName
                       << " queue not found.";
          }
        } else {
          // Node takes no named inputs but is a source, maybe it just starts
          LOG_INFOS << "DefaultExecutionEngine: Source node " << node->getName()
                    << " has no input ports, attempting to schedule.";
          hasScheduledSomething = true;
          tryScheduleNode(node); // It might be ready if it expects no inputs
        }
      } else if (node->getExpectedInputPorts().empty()) {
        // Source node that doesn't take external data, e.g., a generator
        LOG_INFOS << "DefaultExecutionEngine: Auto-scheduling source node "
                  << node->getName() << " (no inputs expected).";
        hasScheduledSomething = true;
        tryScheduleNode(node);
      }
    }
  }
  if (!hasScheduledSomething && !initialInputs.empty()) {
    LOG_ERRORS
        << "DefaultExecutionEngine: Initial inputs provided, but no source "
           "nodes consumed them or were scheduled.";
    throw std::runtime_error(
        "DefaultExecutionEngine: Initial inputs provided, but no "
        "source nodes consumed them or were scheduled. This might be an "
        "error "
        "depending on graph structure.");
  }
  // No inputs, no auto-start source nodes
  if (initialInputs.empty() && !hasScheduledSomething) {
    bool foundAnySourceNode = false;
    for (const auto &node : mGraph->getNodes()) {
      if (mGraph->getInDegree(node) == 0) {
        foundAnySourceNode = true;
        break;
      }
    }
    if (foundAnySourceNode) {
      LOG_ERRORS
          << "DefaultExecutionEngine: No initial inputs and no auto-starting "
             "source nodes were scheduled.";
      throw std::runtime_error("There are source nodes but none started.");
    }
  }
  return true; // distribution itself didn't fail, even if nothing was
               // scheduled
}

void DefaultExecutionEngine::tryScheduleNode(
    const std::shared_ptr<NodeBase> &node) {
  if (mStopFlag.load(std::memory_order_acquire))
    return;

  NodeExecutionState currentState =
      mNodeStates[node]->load(std::memory_order_acquire);
  if (currentState != NodeExecutionState::WAITING) {
    return;
  }

  // Lock this specific node's mutex for the check-and-schedule logic
  std::lock_guard<std::mutex> nodeLock(*(mNodeMutexes[node]));

  // Re-check state after acquiring lock, in case it changed
  currentState = mNodeStates[node]->load(std::memory_order_relaxed);
  if (currentState != NodeExecutionState::WAITING) {
    return;
  }

  // Check if all input port queues have data
  bool allInputsReady = true;
  auto expectedPorts = node->getExpectedInputPorts();
  if (expectedPorts.empty() && mGraph->getInDegree(node) > 0) {
    // This node expects no named inputs, but has graph predecessors.
    // This specific scenario needs careful handling of how data flows from
    // unnamed ports. For now, assume if getExpectedInputPorts is empty, it's
    // ready if it's a source, or if data flow is handled differently (e.g.
    // control dependency). If it has in-degree > 0 and no named input ports,
    // it's ambiguous how it gets data. Let's assume for now it's only ready
    // if in-degree is 0.
    if (mGraph->getInDegree(node) > 0) {
      // It has parents but no way to receive data via named ports
      LOG_INFOS << "Debug: Node " << node->getName()
                << " has in-degree but no expected input ports. Cannot "
                   "determine readiness.";
      allInputsReady = false;
    }
  } else {
    for (const auto &portName : expectedPorts) {
      if (!mNodeInputQueues[node].count(portName) ||
          mNodeInputQueues[node][portName]->empty()) {
        allInputsReady = false;
        break;
      }
    }
  }

  if (allInputsReady) {
    if (mNodeStates[node]->compare_exchange_strong(
            currentState, // currentState is WAITING here
            NodeExecutionState::READY, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      mActiveTasks++;
      LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
                << " is READY. Active tasks: " << mActiveTasks;
      mThreadPool->submit(&DefaultExecutionEngine::executeNodeTask, this, node,
                          mCurContext);
    }
  }
}

void DefaultExecutionEngine::executeNodeTask(
    std::shared_ptr<NodeBase> node, std::shared_ptr<PipelineContext> context) {
  if (mStopFlag.load(std::memory_order_acquire)) {
    mNodeStates[node]->store(NodeExecutionState::WAITING,
                             std::memory_order_release); // Or a CANCELLED state
    mActiveTasks--;
    checkCompletionAndNotify();
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " execution cancelled due to stop flag. Active tasks: "
              << mActiveTasks;
    return;
  }

  NodeExecutionState expectedReady = NodeExecutionState::READY;
  if (!mNodeStates[node]->compare_exchange_strong(
          expectedReady, NodeExecutionState::EXECUTING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    // Another thread might have tried to cancel it, or it wasn't READY
    // This case should be rare if scheduling logic is correct
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " was not READY for execution. State: "
              << static_cast<int>(expectedReady) << ". Aborting task.";
    // mActiveTasks was incremented when set to READY. If it's not executed,
    // it should be decremented. However, if it's already EXECUTING by another
    // thread (shouldn't happen with nodeMutex), or COMPLETED/FAILED, then
    // mActiveTasks would be handled by that path. This situation implies a
    // logic flaw or a race not fully covered. For safety, if it wasn't set to
    // EXECUTING by this call, we might not own the mActiveTasks decrement
    // here. The original submitter that set it to READY is responsible. But
    // since we are here, it means this task was submitted. The CAS failed,
    // meaning the state changed from READY. If it changed to EXECUTING by
    // this very thread, fine. If it changed by something else (e.g. reset,
    // stop), that's different. The current logic: READY -> submit -> (here)
    // READY to EXECUTING. If CAS fails, it means it's no longer READY. It
    // could be STOPPED, WAITING (if reset). We only decrement mActiveTasks if
    // we are sure this task won't proceed.
    mActiveTasks--; // It was marked READY, mActiveTasks incremented, but won't
                    // execute now.
    checkCompletionAndNotify();
    return;
  }
  LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
            << " is EXECUTING.";

  PortDataMap inputs;
  PortDataMap outputs;
  bool success = true;

  try {
    // Prepare inputs (pop from queues) - this part needs the node's mutex
    {
      std::lock_guard<std::mutex> node_lock(*(mNodeMutexes[node]));
      for (const auto &port_name : node->getExpectedInputPorts()) {
        // Assume queue is not empty because tryScheduleNode checked
        // However, a pop could fail if queue becomes empty due to external
        // clear (e.g. reset) For simplicity, we assume try_pop succeeds. A
        // robust version would handle failure.
        auto dataItem = mNodeInputQueues[node][port_name]->try_pop();
        if (dataItem.has_value()) {
          inputs[port_name] = dataItem.value();
        } else {
          // This should not happen if readiness check was correct and no
          // external clear
          LOG_ERRORS << "DefaultExecutionEngine: CRITICAL - Input queue for "
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
    LOG_ERRORS << "DefaultExecutionEngine: Node " << node->getName()
               << " execution failed with exception: " << e.what();
    if (mOnErrorCallback) {
      mOnErrorCallback(e.what(), node->getName());
    }
    success = false;
  } catch (...) {
    LOG_ERRORS << "DefaultExecutionEngine: Node " << node->getName()
               << " execution failed with unknown exception.";
    if (mOnErrorCallback) {
      mOnErrorCallback("Unknown exception during node processing",
                       node->getName());
    }
    success = false;
  }

  if (mStopFlag.load(std::memory_order_acquire)) {
    mNodeStates[node]->store(NodeExecutionState::WAITING,
                             std::memory_order_release); // Or CANCELLED
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " processing interrupted by stop flag.";
  } else if (success) {
    mNodeStates[node]->store(NodeExecutionState::COMPLETED,
                             std::memory_order_release);
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " COMPLETED.";
    // Check if this node is a sink node
    bool isSinkNode = false;
    for (const auto &sinkNodePtr : mSinkNodes) {
      if (sinkNodePtr == node) {
        isSinkNode = true;
        break;
      }
    }
    if (isSinkNode) {
      LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
                << " is a sink node. Collecting results.";
      std::lock_guard<std::mutex> finalResultsLock(mFinalResultsMutex);
      for (const auto &pair : outputs) {
        std::string resultKey = node->getName() + ":" + pair.first;
        mAccumulatedFinalResults[resultKey] = pair.second;
        LOG_INFOS << "DefaultExecutionEngine: Added final result with key: "
                  << resultKey;
      }
    }
    propagateOutputAndScheduleDownstream(node, outputs);
  } else {
    mNodeStates[node]->store(NodeExecutionState::FAILED,
                             std::memory_order_release);
    LOG_ERRORS << "DefaultExecutionEngine: Node " << node->getName()
               << " FAILED.";
    // set global mStopFlag on first failure:
    stopExecutionAsync();
  }

  mActiveTasks--;
  LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
            << " task finished. Active tasks: " << mActiveTasks;
  checkCompletionAndNotify();
}

void DefaultExecutionEngine::propagateOutputAndScheduleDownstream(
    const std::shared_ptr<NodeBase> &sourceNode, const PortDataMap &outputs) {
  if (mStopFlag.load(std::memory_order_acquire))
    return;

  auto outgoingEdges = mGraph->getOutgoingEdges(sourceNode);
  for (const auto &edge : outgoingEdges) {
    if (outputs.count(edge.sourcePort)) {
      // This is a shared_ptr
      const auto dataToPropagate = outputs.at(edge.sourcePort);
      auto destNode = edge.destNode;
      auto destPort = edge.destPort;

      if (mNodeInputQueues.count(destNode) &&
          mNodeInputQueues[destNode].count(destPort)) {
        mNodeInputQueues[destNode][destPort]->push(dataToPropagate);
        LOG_INFOS << "DefaultExecutionEngine: Propagated output from "
                  << sourceNode->getName() << ":" << edge.sourcePort << " to "
                  << destNode->getName() << ":" << destPort;
        tryScheduleNode(destNode);
      } else {
        LOG_ERRORS
            << "DefaultExecutionEngine: ERROR - Downstream queue not found for "
            << destNode->getName() << ":" << destPort;
      }
    }
  }
}

void DefaultExecutionEngine::checkCompletionAndNotify() {
  if (mActiveTasks == 0) { // Could also check mStopFlag here
    LOG_INFOS
        << "DefaultExecutionEngine: All active tasks seem to be completed or "
           "pipeline is stopping.";
    // If mEngineState is RUNNING and mActiveTasks becomes 0, it means
    // successful completion of the current workload. If mEngineState is
    // STOPPING, this signals that all tasks have indeed finished.
    EngineState currentPipelineState =
        mEngineState.load(std::memory_order_acquire);

    // Check if it's a successful completion and the callback is set
    if (currentPipelineState == EngineState::RUNNING &&
        !mStopFlag.load(std::memory_order_acquire) && mOnResultCallback) {

      PortDataMap resultsToSend;
      { // Scope for mFinalResultsMutex lock
        std::lock_guard<std::mutex> final_results_lock(mFinalResultsMutex);
        resultsToSend = mAccumulatedFinalResults;
      }
      LOG_INFOS << "DefaultExecutionEngine: Invoking mOnResultCallback with "
                << resultsToSend.size() << " final results.";
      mOnResultCallback(resultsToSend);
    }

    // Notify any waiting threads (e.g., in execute or stopExecutionSync)
    if (currentPipelineState == EngineState::RUNNING) {
      // Note: We only notify. The `execute` or `stopExecutionSync` methods
      // waiting on mCompletionCondition will re-check conditions and update
      // mEngineState properly under mEngineMutex.
      mCompletionCondition.notify_all();
    }
  }
}

void DefaultExecutionEngine::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  mOnResultCallback = std::move(callback);
}

void DefaultExecutionEngine::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {
  mOnErrorCallback = std::move(callback);
}
} // namespace ai_pipe
