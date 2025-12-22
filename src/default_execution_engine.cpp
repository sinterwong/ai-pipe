/**
 * @file default_execution_engine.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.2
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#include "default_execution_engine.hpp"
#include "ai_pipe/logger.hpp"
#include "execution_engine_factory.hpp"
#include <stdexcept>

namespace ai_pipe {

void NodeStateManager::initialize(const std::vector<NodePtr> &nodes) {
  m_states.clear();
  for (const auto &node : nodes) {
    m_states[node] = std::make_unique<std::atomic<NodeExecutionState>>(
        NodeExecutionState::WAITING);
  }
}

void NodeStateManager::clear() { m_states.clear(); }

NodeExecutionState NodeStateManager::getState(const NodePtr &node) const {
  auto it = m_states.find(node);
  if (it != m_states.end() && it->second) {
    return it->second->load(std::memory_order_acquire);
  }
  return NodeExecutionState::WAITING;
}

void NodeStateManager::setState(const NodePtr &node, NodeExecutionState state) {
  auto it = m_states.find(node);
  if (it != m_states.end() && it->second) {
    it->second->store(state, std::memory_order_release);
  }
}

bool NodeStateManager::compareAndSetState(const NodePtr &node,
                                          NodeExecutionState expected,
                                          NodeExecutionState desired) {
  auto it = m_states.find(node);
  if (it != m_states.end() && it->second) {
    return it->second->compare_exchange_strong(expected, desired,
                                               std::memory_order_acq_rel,
                                               std::memory_order_acquire);
  }
  return false;
}

std::unordered_map<std::string, NodeExecutionState>
NodeStateManager::getAllStates() const {
  std::unordered_map<std::string, NodeExecutionState> result;
  for (const auto &[node_ptr, state_atomic] : m_states) {
    if (node_ptr && state_atomic) {
      result[node_ptr->getName()] =
          state_atomic->load(std::memory_order_acquire);
    }
  }
  return result;
}

void NodeStateManager::resetAllToWaiting() {
  for (auto &[node_ptr, state_atomic] : m_states) {
    if (state_atomic) {
      state_atomic->store(NodeExecutionState::WAITING,
                          std::memory_order_relaxed);
    }
  }
}

// =============================================================================
// InputQueueManager Implementation
// =============================================================================

void InputQueueManager::initialize(const std::vector<NodePtr> &nodes) {
  m_queues.clear();
  for (const auto &node : nodes) {
    PortQueues port_queues;
    for (const auto &port_name : node->getExpectedInputPorts()) {
      port_queues[port_name] = std::make_shared<ThreadSafeQueue<PortDataPtr>>();
    }
    m_queues[node] = std::move(port_queues);
  }
}

void InputQueueManager::clear() { m_queues.clear(); }

void InputQueueManager::pushToQueue(const NodePtr &node,
                                    const std::string &port_name,
                                    PortDataPtr data) {
  auto node_it = m_queues.find(node);
  if (node_it == m_queues.end()) {
    return;
  }

  auto port_it = node_it->second.find(port_name);
  if (port_it != node_it->second.end() && port_it->second) {
    port_it->second->push(std::move(data));
  }
}

std::optional<PortDataPtr>
InputQueueManager::tryPopFromQueue(const NodePtr &node,
                                   const std::string &port_name) {
  auto node_it = m_queues.find(node);
  if (node_it == m_queues.end()) {
    return std::nullopt;
  }

  auto port_it = node_it->second.find(port_name);
  if (port_it != node_it->second.end() && port_it->second) {
    return port_it->second->tryPop();
  }
  return std::nullopt;
}

bool InputQueueManager::areAllInputsReady(
    const NodePtr &node, const std::vector<std::string> &expected_ports) const {
  auto node_it = m_queues.find(node);
  if (node_it == m_queues.end()) {
    return expected_ports.empty();
  }

  for (const auto &port_name : expected_ports) {
    auto port_it = node_it->second.find(port_name);
    if (port_it == node_it->second.end() || !port_it->second ||
        port_it->second->empty()) {
      return false;
    }
  }
  return true;
}

void InputQueueManager::clearAllQueues() {
  for (auto &[node, port_queues] : m_queues) {
    for (auto &[port_name, queue] : port_queues) {
      if (queue) {
        queue->clear();
      }
    }
  }
}

void InputQueueManager::clearNodeQueues(const NodePtr &node) {
  auto node_it = m_queues.find(node);
  if (node_it != m_queues.end()) {
    for (auto &[port_name, queue] : node_it->second) {
      if (queue) {
        queue->clear();
      }
    }
  }
}

bool InputQueueManager::hasQueue(const NodePtr &node,
                                 const std::string &port_name) const {
  auto node_it = m_queues.find(node);
  if (node_it == m_queues.end()) {
    return false;
  }
  return node_it->second.find(port_name) != node_it->second.end();
}

// =============================================================================
// NodeMutexManager Implementation
// =============================================================================

void NodeMutexManager::initialize(const std::vector<NodePtr> &nodes) {
  m_mutexes.clear();
  for (const auto &node : nodes) {
    m_mutexes[node] = std::make_unique<std::mutex>();
  }
}

void NodeMutexManager::clear() { m_mutexes.clear(); }

std::mutex &NodeMutexManager::getMutex(const NodePtr &node) {
  auto it = m_mutexes.find(node);
  if (it == m_mutexes.end() || !it->second) {
    throw std::runtime_error("NodeMutexManager: Mutex not found for node");
  }
  return *(it->second);
}

// =============================================================================
// DefaultExecutionEngine Implementation
// =============================================================================

DefaultExecutionEngine::DefaultExecutionEngine() = default;

DefaultExecutionEngine::~DefaultExecutionEngine() {
  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }
}

DefaultExecutionEngine::DefaultExecutionEngine(
    DefaultExecutionEngine &&other) noexcept {
  // Stop current execution if running
  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }

  // Lock both objects safely
  std::scoped_lock lock(m_engineMutex, other.m_engineMutex);

  // Transfer ownership
  m_graph = std::exchange(other.m_graph, nullptr);
  m_threadPool = std::move(other.m_threadPool);
  m_currentContext = std::move(other.m_currentContext);

  m_nodeStateManager = std::move(other.m_nodeStateManager);
  m_inputQueueManager = std::move(other.m_inputQueueManager);
  m_nodeMutexManager = std::move(other.m_nodeMutexManager);

  m_engineState.store(other.m_engineState.exchange(EngineState::STOPPED),
                      std::memory_order_relaxed);
  m_activeTasks.store(other.m_activeTasks.exchange(0),
                      std::memory_order_relaxed);
  m_stopFlag.store(other.m_stopFlag.exchange(true), std::memory_order_relaxed);

  m_onResultCallback = std::move(other.m_onResultCallback);
  m_onErrorCallback = std::move(other.m_onErrorCallback);

  m_sinkNodes = std::move(other.m_sinkNodes);
  m_accumulatedFinalResults = std::move(other.m_accumulatedFinalResults);
}

DefaultExecutionEngine &
DefaultExecutionEngine::operator=(DefaultExecutionEngine &&other) noexcept {
  if (this == &other) {
    return *this;
  }

  // Stop current execution if running
  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }

  // Lock both objects safely
  std::scoped_lock lock(m_engineMutex, other.m_engineMutex);

  // Transfer ownership
  m_graph = std::exchange(other.m_graph, nullptr);
  m_threadPool = std::move(other.m_threadPool);
  m_currentContext = std::move(other.m_currentContext);

  m_nodeStateManager = std::move(other.m_nodeStateManager);
  m_inputQueueManager = std::move(other.m_inputQueueManager);
  m_nodeMutexManager = std::move(other.m_nodeMutexManager);

  m_engineState.store(other.m_engineState.exchange(EngineState::STOPPED),
                      std::memory_order_relaxed);
  m_activeTasks.store(other.m_activeTasks.exchange(0),
                      std::memory_order_relaxed);
  m_stopFlag.store(other.m_stopFlag.exchange(true), std::memory_order_relaxed);

  m_onResultCallback = std::move(other.m_onResultCallback);
  m_onErrorCallback = std::move(other.m_onErrorCallback);

  m_sinkNodes = std::move(other.m_sinkNodes);
  m_accumulatedFinalResults = std::move(other.m_accumulatedFinalResults);

  return *this;
}

bool DefaultExecutionEngine::initialize(Graph *graph, uint8_t num_workers) {
  if (!graph) {
    LOG_ERROR_S << "DefaultExecutionEngine: Invalid graph pointer.";
    return false;
  }

  std::lock_guard<std::mutex> lock(m_engineMutex);

  m_graph = graph;
  m_threadPool = std::make_unique<ThreadPool>(num_workers);

  const auto &nodes = m_graph->getNodes();
  m_nodeStateManager.initialize(nodes);
  m_inputQueueManager.initialize(nodes);
  m_nodeMutexManager.initialize(nodes);

  m_sinkNodes.clear();
  m_activeTasks.store(0, std::memory_order_relaxed);
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_engineState.store(EngineState::IDLE, std::memory_order_relaxed);

  identifySinkNodes();

  LOG_INFO_S << "DefaultExecutionEngine: Initialized with "
             << static_cast<int>(num_workers) << " workers.";
  return true;
}

void DefaultExecutionEngine::identifySinkNodes() {
  m_sinkNodes.clear();
  for (const auto &node : m_graph->getNodes()) {
    if (m_graph->getOutDegree(node) == 0) {
      m_sinkNodes.push_back(node);
      LOG_INFO_S << "DefaultExecutionEngine: Identified sink node: "
                 << node->getName();
    }
  }
}

bool DefaultExecutionEngine::execute(const PortDataMap &initial_inputs,
                                     bool wait_for_completion,
                                     std::shared_ptr<PipelineContext> context) {
  m_currentContext = context;

  {
    std::unique_lock<std::mutex> lock(m_engineMutex);

    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      LOG_ERROR_S << "DefaultExecutionEngine: Already running.";
      m_currentContext = nullptr;
      return false;
    }

    if (!m_graph || !m_threadPool) {
      LOG_ERROR_S << "DefaultExecutionEngine: Not initialized.";
      m_currentContext = nullptr;
      return false;
    }

    LOG_TRACE_S << "DefaultExecutionEngine: Starting execution.";

    resetInternalState();
    m_engineState.store(EngineState::RUNNING, std::memory_order_release);
  }

  if (!distributeInitialInputs(initial_inputs)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
    LOG_ERROR_S
        << "DefaultExecutionEngine: Failed to distribute initial inputs.";
    m_currentContext = nullptr;
    return false;
  }

  if (wait_for_completion) {
    return waitForCompletion();
  }

  return true;
}

bool DefaultExecutionEngine::waitForCompletion() {
  std::unique_lock<std::mutex> lock(m_engineMutex);

  m_completionCondition.wait(lock, [this] {
    return m_activeTasks.load(std::memory_order_acquire) == 0 ||
           m_stopFlag.load(std::memory_order_acquire);
  });

  const bool was_stopped = m_stopFlag.load(std::memory_order_acquire);
  const int active_tasks = m_activeTasks.load(std::memory_order_acquire);
  const auto current_state = m_engineState.load(std::memory_order_acquire);

  if (was_stopped && current_state != EngineState::STOPPED) {
    m_engineState.store(EngineState::STOPPED, std::memory_order_release);
    LOG_ERROR_S << "DefaultExecutionEngine: Execution was stopped.";
  } else if (active_tasks == 0 && current_state == EngineState::RUNNING) {
    m_engineState.store(EngineState::IDLE, std::memory_order_release);
    LOG_INFO_S << "DefaultExecutionEngine: Execution completed successfully.";
  } else if (current_state != EngineState::ERROR &&
             current_state != EngineState::STOPPED) {
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
    LOG_ERROR_S << "DefaultExecutionEngine: Execution finished abnormally. "
                << "Active tasks: " << active_tasks;
  }

  const bool success =
      m_engineState.load(std::memory_order_acquire) == EngineState::IDLE;
  m_currentContext = nullptr;
  return success;
}

void DefaultExecutionEngine::resetInternalState() {
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_activeTasks.store(0, std::memory_order_relaxed);

  {
    std::lock_guard<std::mutex> lock(m_finalResultsMutex);
    m_accumulatedFinalResults.clear();
  }

  m_nodeStateManager.resetAllToWaiting();
  m_inputQueueManager.clearAllQueues();
}

void DefaultExecutionEngine::stopExecutionAsync() {
  LOG_INFO_S << "DefaultExecutionEngine: stopExecutionAsync called.";

  bool expected = false;
  if (m_stopFlag.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      m_engineState.store(EngineState::STOPPED, std::memory_order_release);
    }
    m_completionCondition.notify_all();
  }
}

void DefaultExecutionEngine::stopExecutionSync() {
  LOG_INFO_S << "DefaultExecutionEngine: stopExecutionSync called.";

  stopExecutionAsync();

  std::unique_lock<std::mutex> lock(m_engineMutex);
  const auto current_state = m_engineState.load(std::memory_order_acquire);

  if (current_state == EngineState::RUNNING) {
    m_completionCondition.wait(lock, [this] {
      const auto state = m_engineState.load(std::memory_order_acquire);
      return m_activeTasks.load(std::memory_order_acquire) == 0 ||
             state == EngineState::STOPPED || state == EngineState::ERROR;
    });
  }

  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    m_engineState.store(EngineState::STOPPED, std::memory_order_release);
  }

  LOG_INFO_S
      << "DefaultExecutionEngine: Execution fully stopped. Active tasks: "
      << m_activeTasks.load(std::memory_order_acquire);
}

void DefaultExecutionEngine::reset() {
  LOG_INFO_S << "DefaultExecutionEngine: Resetting.";

  stopExecutionSync();

  std::lock_guard<std::mutex> lock(m_engineMutex);

  m_nodeStateManager.resetAllToWaiting();
  m_inputQueueManager.clearAllQueues();

  m_activeTasks.store(0, std::memory_order_relaxed);
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_engineState.store(EngineState::IDLE, std::memory_order_relaxed);

  LOG_INFO_S << "DefaultExecutionEngine: Reset complete.";
}

EngineState DefaultExecutionEngine::getState() const {
  return m_engineState.load(std::memory_order_acquire);
}

std::unordered_map<std::string, NodeExecutionState>
DefaultExecutionEngine::getNodeStates() const {
  return m_nodeStateManager.getAllStates();
}

bool DefaultExecutionEngine::distributeInitialInputs(
    const PortDataMap &initial_inputs) {
  bool has_scheduled = false;

  for (const auto &node : m_graph->getNodes()) {
    const bool is_source = m_graph->getInDegree(node) == 0;
    if (!is_source) {
      continue;
    }

    const auto &expected_ports = node->getExpectedInputPorts();
    const bool has_input = initial_inputs.count(node->getName()) > 0;

    if (has_input) {
      const auto &data_packet = initial_inputs.at(node->getName());
      if (!expected_ports.empty()) {
        const std::string &target_port = expected_ports[0];
        if (m_inputQueueManager.hasQueue(node, target_port)) {
          m_inputQueueManager.pushToQueue(node, target_port, data_packet);
          LOG_TRACE_S << "DefaultExecutionEngine: Distributed input to "
                      << node->getName() << ":" << target_port;
          has_scheduled = true;
          tryScheduleNode(node);
        } else {
          LOG_ERROR_S << "DefaultExecutionEngine: Queue not found for "
                      << node->getName() << ":" << target_port;
        }
      } else {
        LOG_TRACE_S << "DefaultExecutionEngine: Source node " << node->getName()
                    << " has no input ports, scheduling.";
        has_scheduled = true;
        tryScheduleNode(node);
      }
    } else if (expected_ports.empty()) {
      LOG_TRACE_S << "DefaultExecutionEngine: Auto-scheduling source node "
                  << node->getName();
      has_scheduled = true;
      tryScheduleNode(node);
    }
  }

  validateSchedulingResult(initial_inputs, has_scheduled);
  return true;
}

void DefaultExecutionEngine::validateSchedulingResult(
    const PortDataMap &initial_inputs, bool has_scheduled) {
  if (!has_scheduled && !initial_inputs.empty()) {
    LOG_ERROR_S << "DefaultExecutionEngine: Initial inputs provided but no "
                   "source nodes consumed them.";
    throw std::runtime_error(
        "Initial inputs provided but no source nodes consumed them.");
  }

  if (initial_inputs.empty() && !has_scheduled) {
    for (const auto &node : m_graph->getNodes()) {
      if (m_graph->getInDegree(node) == 0) {
        LOG_ERROR_S
            << "DefaultExecutionEngine: Source nodes exist but none started.";
        throw std::runtime_error("Source nodes exist but none started.");
      }
    }
  }
}

void DefaultExecutionEngine::tryScheduleNode(
    const std::shared_ptr<ILogicNode> &node) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  auto current_state = m_nodeStateManager.getState(node);
  if (current_state != NodeExecutionState::WAITING) {
    return;
  }

  std::lock_guard<std::mutex> node_lock(m_nodeMutexManager.getMutex(node));

  // Re-check after acquiring lock
  current_state = m_nodeStateManager.getState(node);
  if (current_state != NodeExecutionState::WAITING) {
    return;
  }

  if (!isNodeReady(node)) {
    return;
  }

  if (m_nodeStateManager.compareAndSetState(node, NodeExecutionState::WAITING,
                                            NodeExecutionState::READY)) {
    m_activeTasks.fetch_add(1, std::memory_order_acq_rel);
    LOG_TRACE_S << "DefaultExecutionEngine: Node " << node->getName()
                << " is READY. Active tasks: "
                << m_activeTasks.load(std::memory_order_acquire);

    auto temp_future = m_threadPool->submit(
        &DefaultExecutionEngine::executeNodeTask, this, node, m_currentContext);
  }
}

bool DefaultExecutionEngine::isNodeReady(
    const std::shared_ptr<ILogicNode> &node) const {
  const auto &expected_ports = node->getExpectedInputPorts();

  if (expected_ports.empty()) {
    // Node with no expected input ports
    if (m_graph->getInDegree(node) > 0) {
      LOG_DEBUG_S << "Node " << node->getName()
                  << " has in-degree but no expected ports.";
      return false;
    }
    return true;
  }

  return m_inputQueueManager.areAllInputsReady(node, expected_ports);
}

void DefaultExecutionEngine::executeNodeTask(
    std::shared_ptr<ILogicNode> node,
    std::shared_ptr<PipelineContext> context) {
  // Check stop flag early
  if (m_stopFlag.load(std::memory_order_acquire)) {
    m_nodeStateManager.setState(node, NodeExecutionState::WAITING);
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    LOG_INFO_S << "DefaultExecutionEngine: Node " << node->getName()
               << " cancelled due to stop flag.";
    return;
  }

  // Transition to EXECUTING state
  if (!m_nodeStateManager.compareAndSetState(node, NodeExecutionState::READY,
                                             NodeExecutionState::EXECUTING)) {
    LOG_INFO_S << "DefaultExecutionEngine: Node " << node->getName()
               << " was not READY. Aborting.";
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  LOG_TRACE_S << "DefaultExecutionEngine: Node " << node->getName()
              << " is EXECUTING.";

  PortDataMap inputs;
  PortDataMap outputs;

  // Gather inputs
  bool success = gatherNodeInputs(node, inputs);

  // Process node
  if (success) {
    success = processNode(node, inputs, outputs, context);
  }

  // Handle completion based on stop flag and success
  if (m_stopFlag.load(std::memory_order_acquire)) {
    m_nodeStateManager.setState(node, NodeExecutionState::WAITING);
    LOG_INFO_S << "DefaultExecutionEngine: Node " << node->getName()
               << " interrupted by stop flag.";
  } else if (success) {
    handleNodeSuccess(node, outputs);
  } else {
    handleNodeFailure(node, "Execution failed");
  }

  m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
  LOG_TRACE_S << "DefaultExecutionEngine: Node " << node->getName()
              << " task finished. Active tasks: "
              << m_activeTasks.load(std::memory_order_acquire);

  checkCompletionAndNotify();
}

bool DefaultExecutionEngine::gatherNodeInputs(
    const std::shared_ptr<ILogicNode> &node, PortDataMap &inputs) {
  std::lock_guard<std::mutex> node_lock(m_nodeMutexManager.getMutex(node));

  for (const auto &port_name : node->getExpectedInputPorts()) {
    auto data_item = m_inputQueueManager.tryPopFromQueue(node, port_name);
    if (data_item.has_value()) {
      inputs[port_name] = data_item.value();
    } else {
      LOG_ERROR_S << "DefaultExecutionEngine: Input queue empty for "
                  << node->getName() << ":" << port_name;
      return false;
    }
  }
  return true;
}

bool DefaultExecutionEngine::processNode(
    const std::shared_ptr<ILogicNode> &node, const PortDataMap &inputs,
    PortDataMap &outputs, const std::shared_ptr<PipelineContext> &context) {
  try {
    node->process(inputs, outputs, context);
    return true;
  } catch (const std::exception &e) {
    LOG_ERROR_S << "DefaultExecutionEngine: Node " << node->getName()
                << " failed: " << e.what();
    if (m_onErrorCallback) {
      m_onErrorCallback(e.what(), node->getName());
    }
    return false;
  } catch (...) {
    LOG_ERROR_S << "DefaultExecutionEngine: Node " << node->getName()
                << " failed with unknown exception.";
    if (m_onErrorCallback) {
      m_onErrorCallback("Unknown exception", node->getName());
    }
    return false;
  }
}

void DefaultExecutionEngine::handleNodeSuccess(
    const std::shared_ptr<ILogicNode> &node, const PortDataMap &outputs) {
  m_nodeStateManager.setState(node, NodeExecutionState::COMPLETED);
  LOG_TRACE_S << "DefaultExecutionEngine: Node " << node->getName()
              << " COMPLETED.";

  if (isSinkNode(node)) {
    collectFinalResults(node, outputs);
  }

  propagateOutputAndScheduleDownstream(node, outputs);
}

void DefaultExecutionEngine::handleNodeFailure(
    const std::shared_ptr<ILogicNode> &node, const std::string &error_msg) {
  m_nodeStateManager.setState(node, NodeExecutionState::FAILED);
  LOG_ERROR_S << "DefaultExecutionEngine: Node " << node->getName()
              << " FAILED: " << error_msg;
  stopExecutionAsync();
}

bool DefaultExecutionEngine::isSinkNode(
    const std::shared_ptr<ILogicNode> &node) const {
  return std::find(m_sinkNodes.begin(), m_sinkNodes.end(), node) !=
         m_sinkNodes.end();
}

void DefaultExecutionEngine::collectFinalResults(
    const std::shared_ptr<ILogicNode> &node, const PortDataMap &outputs) {
  LOG_TRACE_S << "DefaultExecutionEngine: Collecting results from sink node: "
              << node->getName();

  std::lock_guard<std::mutex> lock(m_finalResultsMutex);
  for (const auto &[port_name, data] : outputs) {
    std::string result_key = node->getName() + ":" + port_name;
    m_accumulatedFinalResults[result_key] = data;
    LOG_TRACE_S << "DefaultExecutionEngine: Added final result: " << result_key;
  }
}

void DefaultExecutionEngine::propagateOutputAndScheduleDownstream(
    const std::shared_ptr<ILogicNode> &source_node,
    const PortDataMap &outputs) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  const auto outgoing_edges = m_graph->getOutgoingEdges(source_node);

  for (const auto &edge : outgoing_edges) {
    auto output_it = outputs.find(edge.sourcePort);
    if (output_it == outputs.end()) {
      continue;
    }

    const auto &data = output_it->second;
    const auto &dest_node = edge.destNode;
    const auto &dest_port = edge.destPort;

    if (m_inputQueueManager.hasQueue(dest_node, dest_port)) {
      m_inputQueueManager.pushToQueue(dest_node, dest_port, data);
      LOG_TRACE_S << "DefaultExecutionEngine: Propagated "
                  << source_node->getName() << ":" << edge.sourcePort << " -> "
                  << dest_node->getName() << ":" << dest_port;
      tryScheduleNode(dest_node);
    } else {
      LOG_ERROR_S << "DefaultExecutionEngine: Queue not found for "
                  << dest_node->getName() << ":" << dest_port;
    }
  }
}

void DefaultExecutionEngine::checkCompletionAndNotify() {
  const int active_tasks = m_activeTasks.load(std::memory_order_acquire);
  if (active_tasks != 0) {
    return;
  }

  LOG_TRACE_S << "DefaultExecutionEngine: All tasks completed.";

  const auto current_state = m_engineState.load(std::memory_order_acquire);
  const bool was_stopped = m_stopFlag.load(std::memory_order_acquire);

  // Invoke result callback if successful completion
  if (current_state == EngineState::RUNNING && !was_stopped &&
      m_onResultCallback) {
    PortDataMap results;
    {
      std::lock_guard<std::mutex> lock(m_finalResultsMutex);
      results = m_accumulatedFinalResults;
    }
    LOG_TRACE_S << "DefaultExecutionEngine: Invoking result callback with "
                << results.size() << " results.";
    m_onResultCallback(results);
  }

  if (current_state == EngineState::RUNNING) {
    m_completionCondition.notify_all();
  }
}

void DefaultExecutionEngine::setPipelineResultCallback(
    std::function<void(const PortDataMap &)> callback) {
  m_onResultCallback = std::move(callback);
}

void DefaultExecutionEngine::setPipelineErrorCallback(
    std::function<void(const std::string &, const std::string &)> callback) {
  m_onErrorCallback = std::move(callback);
}

} // namespace ai_pipe

// Register DefaultExecutionEngine to the factory
AI_PIPE_REGISTER_ENGINE(DefaultExecutionEngine, ai_pipe::DefaultExecutionEngine)