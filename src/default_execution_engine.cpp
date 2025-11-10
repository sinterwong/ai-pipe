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
#include "execution_engine_factory.hpp"
#include <logger.hpp>
#include <memory>
#include <thread_pool.hpp>
#include <thread_safe_queue.hpp>

namespace ai_pipe {

DefaultExecutionEngine::DefaultExecutionEngine()
    : m_graph(nullptr), m_threadPool(nullptr),
      m_engineState(EngineState::IDLE), m_activeTasks(0), m_stopFlag(false) {}

DefaultExecutionEngine::~DefaultExecutionEngine() {
  // Ensure graceful shutdown if not already stopped
  if (m_engineState == EngineState::RUNNING) {
    stopExecutionSync();
  }
}

DefaultExecutionEngine::DefaultExecutionEngine(DefaultExecutionEngine &&other) {
  if (m_engineState == EngineState::RUNNING) {
    stopExecutionSync();
  }

  std::lock_guard<std::mutex> lock(other.m_engineMutex);
  std::lock_guard<std::mutex> self_lock(m_engineMutex, std::adopt_lock);
  std::lock_guard<std::mutex> other_lock(other.m_engineMutex, std::adopt_lock);

  m_graph = other.m_graph;
  m_threadPool = std::move(other.m_threadPool);
  m_engineState.store(other.m_engineState.load(), std::memory_order_relaxed);
  m_nodeStates = std::move(other.m_nodeStates);
  m_nodeInputQueues = std::move(other.m_nodeInputQueues);
  m_nodeMutexes = std::move(other.m_nodeMutexes);
  m_activeTasks.store(other.m_activeTasks.load(), std::memory_order_relaxed);
  m_stopFlag.store(other.m_stopFlag.load(), std::memory_order_relaxed);

  other.m_graph = nullptr;
  other.m_threadPool.reset();
  other.m_engineState = EngineState::STOPPED;
  other.m_nodeStates.clear();
  other.m_nodeInputQueues.clear();
  other.m_nodeMutexes.clear();
  other.m_activeTasks = 0;
  other.m_stopFlag = true;
}

DefaultExecutionEngine &
DefaultExecutionEngine::operator=(DefaultExecutionEngine &&other) {
  if (this != &other) {
    if (m_engineState == EngineState::RUNNING) {
      stopExecutionSync();
    }
    std::lock(m_engineMutex, other.m_engineMutex);
    std::lock_guard<std::mutex> self_lock(m_engineMutex, std::adopt_lock);
    std::lock_guard<std::mutex> other_lock(other.m_engineMutex,
                                           std::adopt_lock);

    m_graph = other.m_graph;
    m_threadPool = std::move(other.m_threadPool);
    m_engineState.store(other.m_engineState.load(), std::memory_order_relaxed);
    m_nodeStates = std::move(other.m_nodeStates);
    m_nodeInputQueues = std::move(other.m_nodeInputQueues);
    m_nodeMutexes = std::move(other.m_nodeMutexes);
    m_activeTasks.store(other.m_activeTasks.load(), std::memory_order_relaxed);
    m_stopFlag.store(other.m_stopFlag.load(), std::memory_order_relaxed);

    other.m_graph = nullptr;
    other.m_threadPool.reset();
    other.m_engineState = EngineState::STOPPED;
    other.m_nodeStates.clear();
    other.m_nodeInputQueues.clear();
    other.m_nodeMutexes.clear();
    other.m_activeTasks = 0;
    other.m_stopFlag = true;

    return *this;
  }
  return *this;
}

bool DefaultExecutionEngine::initialize(Graph *graph, uint8_t numWorkers) {
  if (!graph) {
    LOG_ERRORS << "DefaultExecutionEngine: Invalid graph pointer.";
    return false;
  }

  std::lock_guard<std::mutex> lock(m_engineMutex);
  m_graph = graph;
  m_threadPool = std::make_unique<ThreadPool>(numWorkers);

  m_sinkNodes.clear();
  m_nodeStates.clear();
  m_nodeInputQueues.clear();
  m_nodeMutexes.clear();

  for (const auto &node : m_graph->getNodes()) {
    m_nodeStates[node] = std::make_unique<std::atomic<NodeExecutionState>>(
        NodeExecutionState::WAITING);
    m_nodeMutexes[node] = std::make_unique<std::mutex>();

    PortInputQueues portQueues;
    for (const auto &portName : node->getExpectedInputPorts()) {
      portQueues[portName] = std::make_shared<ThreadSafeQueue<PortDataPtr>>();
    }
    m_nodeInputQueues[node] = std::move(portQueues);
  }

  m_activeTasks = 0;
  m_stopFlag = false;
  m_engineState = EngineState::IDLE;

  // Identify sink nodes
  for (const auto &node : m_graph->getNodes()) {
    if (m_graph->getOutDegree(node) == 0) {
      m_sinkNodes.push_back(node);
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
  m_curContext = context;
  std::unique_lock<std::mutex> lock(m_engineMutex);
  if (m_engineState == EngineState::RUNNING) {
    LOG_ERRORS << "DefaultExecutionEngine: Already running. Cannot start new "
                  "execution.";
    m_curContext = nullptr;
    return false;
  }

  if (!m_graph || !m_threadPool) {
    LOG_ERRORS << "DefaultExecutionEngine: Not initialized.";
    m_curContext = nullptr;
    return false;
  }

  LOG_INFOS << "DefaultExecutionEngine: Starting execution.";

  m_engineState = EngineState::RUNNING;
  m_stopFlag = false;
  m_activeTasks = 0;

  { // Scope for m_finalResultsMutex lock
    std::lock_guard<std::mutex> final_results_lock(m_finalResultsMutex);
    m_accumulatedFinalResults.clear();
  }

  for (const auto &node : m_graph->getNodes()) {
    m_nodeStates[node]->store(NodeExecutionState::WAITING,
                              std::memory_order_relaxed);
    for (const auto &pair : m_nodeInputQueues[node]) {
      pair.second->clear();
    }
  }
  // release engine lock before distributing data and scheduling
  lock.unlock();

  if (!distributeInitialInputs(initialInputs)) {
    std::lock_guard<std::mutex> endLock(m_engineMutex);
    m_engineState = EngineState::ERROR;
    LOG_ERRORS
        << "DefaultExecutionEngine: Failed to distribute initial inputs.";
    m_curContext = nullptr;
    return false;
  }

  if (waitForCompletion) {
    std::unique_lock<std::mutex> completionLock(m_engineMutex);
    m_completionCondition.wait(completionLock, [this] {
      return m_activeTasks == 0 || m_stopFlag.load(std::memory_order_acquire);
    });
    completionLock.unlock();

    std::lock_guard<std::mutex> finalStateLock(m_engineMutex);
    if (m_stopFlag.load(std::memory_order_acquire) &&
        m_engineState != EngineState::STOPPED) {
      m_engineState = EngineState::STOPPED;
      LOG_ERRORS << "DefaultExecutionEngine: Execution was stopped.";
    } else if (m_activeTasks == 0 && m_engineState == EngineState::RUNNING) {
      m_engineState = EngineState::IDLE; // Successful completion
      LOG_INFOS << "DefaultExecutionEngine: Execution completed successfully.";
    } else if (m_engineState != EngineState::ERROR &&
               m_engineState != EngineState::STOPPED) {
      if (m_engineState != EngineState::ERROR)
        m_engineState = EngineState::ERROR; // Default to error if stuck
      LOG_ERRORS
          << "DefaultExecutionEngine: Execution finished with activeTasks="
          << m_activeTasks
          << " and state=" << static_cast<int>(m_engineState.load());
    }
    bool result = m_engineState == EngineState::IDLE;
    m_curContext = nullptr;
    return result;
  }
  m_curContext = nullptr;
  return true;
}

void DefaultExecutionEngine::stopExecutionAsync() {
  LOG_INFOS << "DefaultExecutionEngine: stopExecutionAsync called.";
  bool expected = false;
  if (m_stopFlag.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    if (m_engineState == EngineState::RUNNING) {
      m_engineState = EngineState::STOPPED;
    }
    m_completionCondition.notify_all();
  }
}

void DefaultExecutionEngine::stopExecutionSync() {
  LOG_INFOS << "DefaultExecutionEngine: stopExecutionSync called.";
  stopExecutionAsync();
  std::unique_lock<std::mutex> lock(m_engineMutex);
  if (m_engineState == EngineState::RUNNING) {
    m_completionCondition.wait(lock, [this] {
      return m_activeTasks == 0 || m_engineState == EngineState::STOPPED ||
             m_engineState == EngineState::ERROR;
    });
  }
  // ensure final state is STOPPED if it was stopping.
  if (m_engineState == EngineState::RUNNING) {
    m_engineState = EngineState::STOPPED;
  }
  LOG_INFOS << "DefaultExecutionEngine: Execution fully stopped. Active tasks: "
            << m_activeTasks;
}

void DefaultExecutionEngine::reset() {
  LOG_INFOS << "DefaultExecutionEngine: Resetting.";

  // ensure any ongoing execution is stopped
  stopExecutionSync();

  std::lock_guard<std::mutex> lock(m_engineMutex);
  for (const auto &node : m_graph->getNodes()) {
    if (m_nodeStates.count(node)) {
      m_nodeStates[node]->store(NodeExecutionState::WAITING,
                                std::memory_order_relaxed);
    }
    if (m_nodeInputQueues.count(node)) {
      for (const auto &pair : m_nodeInputQueues.at(node)) {
        pair.second->clear();
      }
    }
  }
  m_activeTasks = 0;
  m_stopFlag = false;
  m_engineState = EngineState::IDLE;
  LOG_INFOS << "DefaultExecutionEngine: Reset complete.";
}

EngineState DefaultExecutionEngine::getState() const {
  return m_engineState.load(std::memory_order_acquire);
}

std::unordered_map<std::string, NodeExecutionState>
DefaultExecutionEngine::getNodeStates() const {
  std::unordered_map<std::string, NodeExecutionState> result;
  // std::lock_guard<std::mutex> lock(m_engineMutex);
  for (const auto &[nodePtr, stateAtomicPtr] : m_nodeStates) {
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
  for (const auto &node : m_graph->getNodes()) {
    if (m_graph->getInDegree(node) == 0) {
      // Check if this source node needs one of the initial inputs
      if (initialInputs.count(node->getName())) {
        const auto dataPacket = initialInputs.at(node->getName());
        auto expectedPorts = node->getExpectedInputPorts();
        if (!expectedPorts.empty()) {
          // FIXME: Feed to first port
          const std::string &targetPortName = expectedPorts[0];
          if (m_nodeInputQueues[node].count(targetPortName)) {
            m_nodeInputQueues[node][targetPortName]->push(dataPacket);
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
    for (const auto &node : m_graph->getNodes()) {
      if (m_graph->getInDegree(node) == 0) {
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
    const std::shared_ptr<ILogicNode> &node) {
  if (m_stopFlag.load(std::memory_order_acquire))
    return;

  NodeExecutionState currentState =
      m_nodeStates[node]->load(std::memory_order_acquire);
  if (currentState != NodeExecutionState::WAITING) {
    return;
  }

  // Lock this specific node's mutex for the check-and-schedule logic
  std::lock_guard<std::mutex> nodeLock(*(m_nodeMutexes[node]));

  // Re-check state after acquiring lock, in case it changed
  currentState = m_nodeStates[node]->load(std::memory_order_relaxed);
  if (currentState != NodeExecutionState::WAITING) {
    return;
  }

  // Check if all input port queues have data
  bool allInputsReady = true;
  auto expectedPorts = node->getExpectedInputPorts();
  if (expectedPorts.empty() && m_graph->getInDegree(node) > 0) {
    // This node expects no named inputs, but has graph predecessors.
    // This specific scenario needs careful handling of how data flows from
    // unnamed ports. For now, assume if getExpectedInputPorts is empty, it's
    // ready if it's a source, or if data flow is handled differently (e.g.
    // control dependency). If it has in-degree > 0 and no named input ports,
    // it's ambiguous how it gets data. Let's assume for now it's only ready
    // if in-degree is 0.
    if (m_graph->getInDegree(node) > 0) {
      // It has parents but no way to receive data via named ports
      LOG_INFOS << "Debug: Node " << node->getName()
                << " has in-degree but no expected input ports. Cannot "
                   "determine readiness.";
      allInputsReady = false;
    }
  } else {
    for (const auto &portName : expectedPorts) {
      if (!m_nodeInputQueues[node].count(portName) ||
          m_nodeInputQueues[node][portName]->empty()) {
        allInputsReady = false;
        break;
      }
    }
  }

  if (allInputsReady) {
    if (m_nodeStates[node]->compare_exchange_strong(
            currentState, // currentState is WAITING here
            NodeExecutionState::READY, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      m_activeTasks++;
      LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
                << " is READY. Active tasks: " << m_activeTasks;
      m_threadPool->submit(&DefaultExecutionEngine::executeNodeTask, this,
                           node, m_curContext);
    }
  }
}

void DefaultExecutionEngine::executeNodeTask(
    std::shared_ptr<ILogicNode> node,
    std::shared_ptr<PipelineContext> context) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    m_nodeStates[node]->store(
        NodeExecutionState::WAITING,
        std::memory_order_release); // Or a CANCELLED state
    m_activeTasks--;
    checkCompletionAndNotify();
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " execution cancelled due to stop flag. Active tasks: "
              << m_activeTasks;
    return;
  }

  NodeExecutionState expectedReady = NodeExecutionState::READY;
  if (!m_nodeStates[node]->compare_exchange_strong(
          expectedReady, NodeExecutionState::EXECUTING,
          std::memory_order_acq_rel, std::memory_order_acquire)) {
    // Another thread might have tried to cancel it, or it wasn't READY
    // This case should be rare if scheduling logic is correct
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " was not READY for execution. State: "
              << static_cast<int>(expectedReady) << ". Aborting task.";
    // m_activeTasks was incremented when set to READY. If it's not executed,
    // it should be decremented. However, if it's already EXECUTING by another
    // thread (shouldn't happen with nodeMutex), or COMPLETED/FAILED, then
    // m_activeTasks would be handled by that path. This situation implies a
    // logic flaw or a race not fully covered. For safety, if it wasn't set to
    // EXECUTING by this call, we might not own the m_activeTasks decrement
    // here. The original submitter that set it to READY is responsible. But
    // since we are here, it means this task was submitted. The CAS failed,
    // meaning the state changed from READY. If it changed to EXECUTING by
    // this very thread, fine. If it changed by something else (e.g. reset,
    // stop), that's different. The current logic: READY -> submit -> (here)
    // READY to EXECUTING. If CAS fails, it means it's no longer READY. It
    // could be STOPPED, WAITING (if reset). We only decrement m_activeTasks if
    // we are sure this task won't proceed.
    m_activeTasks--; // It was marked READY, m_activeTasks incremented, but
                     // won't execute now.
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
      std::lock_guard<std::mutex> node_lock(*(m_nodeMutexes[node]));
      for (const auto &port_name : node->getExpectedInputPorts()) {
        // Assume queue is not empty because tryScheduleNode checked
        // However, a pop could fail if queue becomes empty due to external
        // clear (e.g. reset) For simplicity, we assume try_pop succeeds. A
        // robust version would handle failure.
        auto dataItem = m_nodeInputQueues[node][port_name]->try_pop();
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
    if (m_onErrorCallback) {
      m_onErrorCallback(e.what(), node->getName());
    }
    success = false;
  } catch (...) {
    LOG_ERRORS << "DefaultExecutionEngine: Node " << node->getName()
               << " execution failed with unknown exception.";
    if (m_onErrorCallback) {
      m_onErrorCallback("Unknown exception during node processing",
                        node->getName());
    }
    success = false;
  }

  if (m_stopFlag.load(std::memory_order_acquire)) {
    m_nodeStates[node]->store(
        NodeExecutionState::WAITING,
        std::memory_order_release); // Or CANCELLED
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " processing interrupted by stop flag.";
  } else if (success) {
    m_nodeStates[node]->store(NodeExecutionState::COMPLETED,
                              std::memory_order_release);
    LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
              << " COMPLETED.";
    // Check if this node is a sink node
    bool isSinkNode = false;
    for (const auto &sinkNodePtr : m_sinkNodes) {
      if (sinkNodePtr == node) {
        isSinkNode = true;
        break;
      }
    }
    if (isSinkNode) {
      LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
                << " is a sink node. Collecting results.";
      std::lock_guard<std::mutex> finalResultsLock(m_finalResultsMutex);
      for (const auto &pair : outputs) {
        std::string resultKey = node->getName() + ":" + pair.first;
        m_accumulatedFinalResults[resultKey] = pair.second;
        LOG_INFOS << "DefaultExecutionEngine: Added final result with key: "
                  << resultKey;
      }
    }
    propagateOutputAndScheduleDownstream(node, outputs);
  } else {
    m_nodeStates[node]->store(NodeExecutionState::FAILED,
                              std::memory_order_release);
    LOG_ERRORS << "DefaultExecutionEngine: Node " << node->getName()
               << " FAILED.";
    // set global m_stopFlag on first failure:
    stopExecutionAsync();
  }

  m_activeTasks--;
  LOG_INFOS << "DefaultExecutionEngine: Node " << node->getName()
            << " task finished. Active tasks: " << m_activeTasks;
  checkCompletionAndNotify();
}

void DefaultExecutionEngine::propagateOutputAndScheduleDownstream(
    const std::shared_ptr<ILogicNode> &sourceNode, const PortDataMap &outputs) {
  if (m_stopFlag.load(std::memory_order_acquire))
    return;

  auto outgoingEdges = m_graph->getOutgoingEdges(sourceNode);
  for (const auto &edge : outgoingEdges) {
    if (outputs.count(edge.sourcePort)) {
      // This is a shared_ptr
      const auto dataToPropagate = outputs.at(edge.sourcePort);
      auto destNode = edge.destNode;
      auto destPort = edge.destPort;

      if (m_nodeInputQueues.count(destNode) &&
          m_nodeInputQueues[destNode].count(destPort)) {
        m_nodeInputQueues[destNode][destPort]->push(dataToPropagate);
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
  if (m_activeTasks == 0) { // Could also check m_stopFlag here
    LOG_INFOS
        << "DefaultExecutionEngine: All active tasks seem to be completed or "
           "pipeline is stopping.";
    // If m_engineState is RUNNING and m_activeTasks becomes 0, it means
    // successful completion of the current workload. If m_engineState is
    // STOPPING, this signals that all tasks have indeed finished.
    EngineState currentPipelineState =
        m_engineState.load(std::memory_order_acquire);

    // Check if it's a successful completion and the callback is set
    if (currentPipelineState == EngineState::RUNNING &&
        !m_stopFlag.load(std::memory_order_acquire) && m_onResultCallback) {

      PortDataMap resultsToSend;
      { // Scope for m_finalResultsMutex lock
        std::lock_guard<std::mutex> final_results_lock(m_finalResultsMutex);
        resultsToSend = m_accumulatedFinalResults;
      }
      LOG_INFOS << "DefaultExecutionEngine: Invoking m_onResultCallback with "
                << resultsToSend.size() << " final results.";
      m_onResultCallback(resultsToSend);
    }

    // Notify any waiting threads (e.g., in execute or stopExecutionSync)
    if (currentPipelineState == EngineState::RUNNING) {
      // Note: We only notify. The `execute` or `stopExecutionSync` methods
      // waiting on m_completionCondition will re-check conditions and update
      // m_engineState properly under m_engineMutex.
      m_completionCondition.notify_all();
    }
  }
}

void DefaultExecutionEngine::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  m_onResultCallback = std::move(callback);
}

void DefaultExecutionEngine::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {
  m_onErrorCallback = std::move(callback);
}
} // namespace ai_pipe

// Register DefaultExecutionEngine to the factory
AI_PIPE_REGISTER_ENGINE(DefaultExecutionEngine, ai_pipe::DefaultExecutionEngine)
