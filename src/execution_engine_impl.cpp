/**
 * @file execution_engine_impl.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Implementation of the Unified Execution Engine (PIMPL Wrapper & Impl)
 * @version 1.1
 * @date 2025-12-24
 *
 * @copyright Copyright (c) 2025
 */

#include "execution_engine_impl.hpp"
#include "ai_pipe/logger.hpp"
#include "join_aware_sync_strategy.hpp"
#include "scheduler_strategies.hpp"
#include "sync_strategies.hpp"
#include <algorithm>
#include <cassert>
#include <sstream>

namespace ai_pipe {

// =============================================================================
// ExecutionEngine (PIMPL Wrapper) Implementation
// =============================================================================

std::unique_ptr<ExecutionEngine>
ExecutionEngine::create(const EngineConfig &config) {
  // 创建 Wrapper，Wrapper 内部会创建 Impl
  auto engine = std::make_unique<ExecutionEngine>(config);
  // 配置模式 (会转发给 Impl)
  engine->configureForMode(config.mode);
  return engine;
}

ExecutionEngine::ExecutionEngine() : m_impl(std::make_unique<Impl>()) {}

ExecutionEngine::ExecutionEngine(const EngineConfig &config)
    : m_impl(std::make_unique<Impl>(config)) {}

ExecutionEngine::~ExecutionEngine() = default;

// 移动构造和赋值由 unique_ptr 自动处理
ExecutionEngine::ExecutionEngine(ExecutionEngine &&) noexcept = default;
ExecutionEngine &
ExecutionEngine::operator=(ExecutionEngine &&) noexcept = default;

// -------------------------------------------------------------------------
// Strategy Forwarding
// -------------------------------------------------------------------------

void ExecutionEngine::setSchedulerStrategy(SchedulerStrategyPtr strategy) {
  m_impl->setSchedulerStrategy(std::move(strategy));
}

void ExecutionEngine::setSyncStrategy(SyncStrategyPtr strategy) {
  m_impl->setSyncStrategy(std::move(strategy));
}

void ExecutionEngine::configureForMode(ExecutionMode mode) {
  m_impl->configureForMode(mode);
}

// -------------------------------------------------------------------------
// Core Interface Forwarding
// -------------------------------------------------------------------------

bool ExecutionEngine::initialize(Graph *graph, std::uint8_t num_workers) {
  return m_impl->initialize(graph, num_workers);
}

bool ExecutionEngine::execute(const PortDataMap &initial_inputs,
                              bool wait_for_completion,
                              std::shared_ptr<PipelineContext> context) {
  return m_impl->execute(initial_inputs, wait_for_completion, context);
}

void ExecutionEngine::stopExecutionAsync() { m_impl->stopExecutionAsync(); }

void ExecutionEngine::stopExecutionSync() { m_impl->stopExecutionSync(); }

void ExecutionEngine::reset() { m_impl->reset(); }

EngineState ExecutionEngine::getState() const { return m_impl->getState(); }

std::unordered_map<std::string, NodeExecutionState>
ExecutionEngine::getNodeStates() const {
  return m_impl->getNodeStates();
}

// -------------------------------------------------------------------------
// Callback Registration Forwarding
// -------------------------------------------------------------------------

void ExecutionEngine::setPipelineResultCallback(
    std::function<void(const PortDataMap &)> callback) {
  m_impl->setPipelineResultCallback(std::move(callback));
}

void ExecutionEngine::setPipelineErrorCallback(
    std::function<void(const std::string &, const std::string &)> callback) {
  m_impl->setPipelineErrorCallback(std::move(callback));
}

void ExecutionEngine::setDropCallback(
    std::function<void(const std::string &, std::uint64_t, const std::string &)>
        callback) {
  m_impl->setDropCallback(std::move(callback));
}

// -------------------------------------------------------------------------
// Streaming Interface Forwarding
// -------------------------------------------------------------------------

bool ExecutionEngine::startStreaming(std::shared_ptr<PipelineContext> context) {
  return m_impl->startStreaming(context);
}

void ExecutionEngine::stopStreaming(bool wait_for_drain) {
  m_impl->stopStreaming(wait_for_drain);
}

bool ExecutionEngine::isStreaming() const { return m_impl->isStreaming(); }

QueuePushResult ExecutionEngine::pushInput(const std::string &source_node,
                                           const std::string &port_name,
                                           PortDataPtr data) {
  return m_impl->pushInput(source_node, port_name, std::move(data));
}

QueuePushResult ExecutionEngine::pushInput(const std::string &source_node,
                                           PortDataPtr data) {
  return m_impl->pushInput(source_node, std::move(data));
}

// -------------------------------------------------------------------------
// State & Monitoring Forwarding
// -------------------------------------------------------------------------

EngineStatisticsSnapshot ExecutionEngine::statistics() const {
  return m_impl->statistics();
}

std::size_t ExecutionEngine::queueDepth(const std::string &node_name,
                                        const std::string &port_name) const {
  return m_impl->queueDepth(node_name, port_name);
}

bool ExecutionEngine::hasQueueCapacity(const std::string &node_name,
                                       const std::string &port_name) const {
  return m_impl->hasQueueCapacity(node_name, port_name);
}

bool ExecutionEngine::waitForDrain(std::size_t max_depth,
                                   std::chrono::milliseconds timeout) {
  return m_impl->waitForDrain(max_depth, timeout);
}

// -------------------------------------------------------------------------
// Configuration & Info Forwarding
// -------------------------------------------------------------------------

void ExecutionEngine::setNodeQueueConfig(const std::string &node_name,
                                         const QueueConfig &config) {
  m_impl->setNodeQueueConfig(node_name, config);
}

const EngineConfig &ExecutionEngine::config() const { return m_impl->config(); }

std::string ExecutionEngine::info() const { return m_impl->info(); }

std::string ExecutionEngine::strategyInfo() const {
  return m_impl->strategyInfo();
}

// =============================================================================
// ExecutionEngine::Impl Implementation (The Core Logic)
// =============================================================================

ExecutionEngine::Impl::Impl() : Impl(EngineConfig{}) {}

ExecutionEngine::Impl::Impl(const EngineConfig &config) : m_config(config) {
  m_statistics.reset();
  configureForMode(config.mode);
}

ExecutionEngine::Impl::~Impl() {
  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }
}

ExecutionEngine::Impl::Impl(Impl &&other) noexcept {
  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }

  std::scoped_lock lock(m_engineMutex, other.m_engineMutex);

  m_config = std::move(other.m_config);
  m_nodeQueueConfigs = std::move(other.m_nodeQueueConfigs);
  m_schedulerStrategy = std::move(other.m_schedulerStrategy);
  m_syncStrategy = std::move(other.m_syncStrategy);
  m_graph = std::exchange(other.m_graph, nullptr);
  m_threadPool = std::move(other.m_threadPool);
  m_currentContext = std::move(other.m_currentContext);
  m_nodeStates = std::move(other.m_nodeStates);
  m_nodeNameMap = std::move(other.m_nodeNameMap);
  m_sinkNodes = std::move(other.m_sinkNodes);

  m_engineState.store(other.m_engineState.exchange(EngineState::STOPPED),
                      std::memory_order_relaxed);
  m_activeTasks.store(other.m_activeTasks.exchange(0),
                      std::memory_order_relaxed);
  m_stopFlag.store(other.m_stopFlag.exchange(true), std::memory_order_relaxed);
  m_streamingMode.store(other.m_streamingMode.exchange(false),
                        std::memory_order_relaxed);

  m_accumulatedResults = std::move(other.m_accumulatedResults);
  m_resultCallback = std::move(other.m_resultCallback);
  m_errorCallback = std::move(other.m_errorCallback);
  m_dropCallback = std::move(other.m_dropCallback);
}

ExecutionEngine::Impl &ExecutionEngine::Impl::operator=(Impl &&other) noexcept {
  if (this == &other) {
    return *this;
  }

  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }

  std::scoped_lock lock(m_engineMutex, other.m_engineMutex);

  m_config = std::move(other.m_config);
  m_nodeQueueConfigs = std::move(other.m_nodeQueueConfigs);
  m_schedulerStrategy = std::move(other.m_schedulerStrategy);
  m_syncStrategy = std::move(other.m_syncStrategy);
  m_graph = std::exchange(other.m_graph, nullptr);
  m_threadPool = std::move(other.m_threadPool);
  m_currentContext = std::move(other.m_currentContext);
  m_nodeStates = std::move(other.m_nodeStates);
  m_nodeNameMap = std::move(other.m_nodeNameMap);
  m_sinkNodes = std::move(other.m_sinkNodes);

  m_engineState.store(other.m_engineState.exchange(EngineState::STOPPED),
                      std::memory_order_relaxed);
  m_activeTasks.store(other.m_activeTasks.exchange(0),
                      std::memory_order_relaxed);
  m_stopFlag.store(other.m_stopFlag.exchange(true), std::memory_order_relaxed);
  m_streamingMode.store(other.m_streamingMode.exchange(false),
                        std::memory_order_relaxed);

  m_accumulatedResults = std::move(other.m_accumulatedResults);
  m_resultCallback = std::move(other.m_resultCallback);
  m_errorCallback = std::move(other.m_errorCallback);
  m_dropCallback = std::move(other.m_dropCallback);

  return *this;
}

// -------------------------------------------------------------------------
// Impl: Strategy Configuration
// -------------------------------------------------------------------------

void ExecutionEngine::Impl::setSchedulerStrategy(
    std::unique_ptr<ISchedulerStrategy> strategy) {
  if (m_engineState.load(std::memory_order_acquire) != EngineState::IDLE) {
    LOG_WARNING_S << "ExecutionEngine: Cannot change strategies while running";
    return;
  }
  m_schedulerStrategy = std::move(strategy);
}

void ExecutionEngine::Impl::setSyncStrategy(
    std::unique_ptr<ISyncStrategy> strategy) {
  if (m_engineState.load(std::memory_order_acquire) != EngineState::IDLE) {
    LOG_WARNING_S << "ExecutionEngine: Cannot change strategies while running";
    return;
  }
  m_syncStrategy = std::move(strategy);
}

void ExecutionEngine::Impl::configureForMode(ExecutionMode mode) {
  switch (mode) {
  case ExecutionMode::BATCH:
    m_schedulerStrategy = std::make_unique<BatchSchedulerStrategy>();
    m_syncStrategy = std::make_unique<NoSyncStrategy>();
    break;

  case ExecutionMode::STREAM: {
    StreamSchedulerConfig sched_config;
    sched_config.auto_reschedule = true;
    sched_config.allow_partial_inputs = m_config.allow_partial_inputs;
    sched_config.min_interval = m_config.min_execution_interval;
    m_schedulerStrategy =
        std::make_unique<StreamSchedulerStrategy>(sched_config);
    m_syncStrategy = std::make_unique<JoinAwareSyncStrategy>();
    break;
  }

  case ExecutionMode::HYBRID:
    m_schedulerStrategy = std::make_unique<HybridSchedulerStrategy>();
    m_syncStrategy = std::make_unique<JoinAwareSyncStrategy>();
    break;
  }
}

// -------------------------------------------------------------------------
// Impl: Core Logic
// -------------------------------------------------------------------------

bool ExecutionEngine::Impl::initialize(Graph *graph, std::uint8_t num_workers) {
  if (!graph) {
    LOG_ERROR_S << "ExecutionEngine: Invalid graph pointer.";
    return false;
  }

  std::lock_guard<std::mutex> lock(m_engineMutex);

  m_graph = graph;
  if (num_workers > 0) {
    m_config.num_workers = num_workers;
  }

  m_threadPool = std::make_unique<ThreadPool>(m_config.num_workers);

  // Initialize strategies
  if (m_schedulerStrategy) {
    m_schedulerStrategy->initialize(graph);
  }
  if (m_syncStrategy) {
    m_syncStrategy->initialize(graph);
  }

  // Initialize node states and queues
  initializeNodeStates();
  initializeQueues();

  // Identify sink nodes
  identifySinkNodes();

  // Setup callbacks
  setupDropCallbacks();

  // Reset counters
  m_activeTasks.store(0, std::memory_order_relaxed);
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_streamingMode.store(false, std::memory_order_relaxed);
  m_engineState.store(EngineState::IDLE, std::memory_order_relaxed);
  m_statistics.reset();

  LOG_INFO_S << "ExecutionEngine: Initialized with "
             << static_cast<int>(m_config.num_workers) << " workers, "
             << m_nodeStates.size()
             << " nodes. Mode: " << executionModeToString(m_config.mode);

  return true;
}

bool ExecutionEngine::Impl::execute(const PortDataMap &initial_inputs,
                                    bool wait_for_completion,
                                    std::shared_ptr<PipelineContext> context) {
  m_currentContext = context;

  // Handle streaming mode
  if (m_streamingMode.load(std::memory_order_acquire)) {
    for (const auto &[node_name, data] : initial_inputs) {
      auto result = pushInput(node_name, data);
      if (!result) {
        LOG_ERROR_S << "ExecutionEngine: Failed to push input to " << node_name
                    << ": " << result.message;
        return false;
      }
    }

    if (wait_for_completion) {
      std::unique_lock<std::mutex> lock(m_completionMutex);
      m_completionCV.wait(lock, [this] {
        return m_activeTasks.load(std::memory_order_acquire) == 0 ||
               m_stopFlag.load(std::memory_order_acquire);
      });
    }
    return true;
  }

  // Batch execution mode
  {
    std::unique_lock<std::mutex> lock(m_engineMutex);

    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      LOG_ERROR_S << "ExecutionEngine: Already running.";
      m_currentContext = nullptr;
      return false;
    }

    if (!m_graph || !m_threadPool) {
      LOG_ERROR_S << "ExecutionEngine: Not initialized.";
      m_currentContext = nullptr;
      return false;
    }

    LOG_TRACE_S << "ExecutionEngine: Starting execution.";

    resetInternalState();
    m_engineState.store(EngineState::RUNNING, std::memory_order_release);
  }

  m_statistics.total_executions.fetch_add(1, std::memory_order_relaxed);

  if (!distributeInitialInputs(initial_inputs)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
    LOG_ERROR_S << "ExecutionEngine: Failed to distribute inputs.";
    m_currentContext = nullptr;
    return false;
  }

  if (wait_for_completion) {
    return waitForCompletion();
  }

  return true;
}

void ExecutionEngine::Impl::stopExecutionAsync() {
  LOG_INFO_S << "ExecutionEngine: stopExecutionAsync called.";

  bool expected = false;
  if (m_stopFlag.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      m_engineState.store(EngineState::STOPPED, std::memory_order_release);
    }
    m_completionCV.notify_all();
  }
}

void ExecutionEngine::Impl::stopExecutionSync() {
  LOG_INFO_S << "ExecutionEngine: stopExecutionSync called.";

  stopExecutionAsync();

  {
    std::unique_lock<std::mutex> lock(m_completionMutex);
    // Wait indefinitely until all active tasks complete
    while (m_activeTasks.load(std::memory_order_acquire) > 0) {
      m_completionCV.wait_for(lock, std::chrono::milliseconds{10});
    }
  }

  // State update using engineMutex
  {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      m_engineState.store(EngineState::STOPPED, std::memory_order_release);
    }
  }

  LOG_INFO_S << "ExecutionEngine: Execution fully stopped.";
}

void ExecutionEngine::Impl::reset() {
  LOG_INFO_S << "ExecutionEngine: Resetting.";

  stopExecutionSync();

  std::lock_guard<std::mutex> lock(m_engineMutex);

  // Reset node states and queues
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    if (state->exec_state) {
      state->exec_state->store(NodeExecutionState::WAITING,
                               std::memory_order_relaxed);
    }
    state->execution_count = 0;

    for (auto &[port_name, queue] : state->bounded_queues) {
      if (queue) {
        queue->clear();
        queue->resetStatistics();
      }
    }
    for (auto &[port_name, queue] : state->unbounded_queues) {
      if (queue) {
        queue->clear();
      }
    }
  }

  // Reset strategies
  if (m_schedulerStrategy) {
    m_schedulerStrategy->reset();
  }
  if (m_syncStrategy) {
    m_syncStrategy->reset();
  }

  m_accumulatedResults.clear();
  m_activeTasks.store(0, std::memory_order_relaxed);
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_streamingMode.store(false, std::memory_order_relaxed);
  m_engineState.store(EngineState::IDLE, std::memory_order_relaxed);
  m_statistics.reset();

  LOG_INFO_S << "ExecutionEngine: Reset complete.";
}

EngineState ExecutionEngine::Impl::getState() const {
  return m_engineState.load(std::memory_order_acquire);
}

std::unordered_map<std::string, NodeExecutionState>
ExecutionEngine::Impl::getNodeStates() const {
  std::unordered_map<std::string, NodeExecutionState> result;

  for (const auto &[node_ptr, state] : m_nodeStates) {
    if (state && state->exec_state) {
      result[state->name] = state->exec_state->load(std::memory_order_acquire);
    }
  }

  return result;
}

// -------------------------------------------------------------------------
// Impl: Callback Registration
// -------------------------------------------------------------------------

void ExecutionEngine::Impl::setPipelineResultCallback(
    std::function<void(const PortDataMap &)> callback) {
  m_resultCallback = std::move(callback);
}

void ExecutionEngine::Impl::setPipelineErrorCallback(
    std::function<void(const std::string &, const std::string &)> callback) {
  m_errorCallback = std::move(callback);
}

void ExecutionEngine::Impl::setDropCallback(
    std::function<void(const std::string &, std::uint64_t, const std::string &)>
        callback) {
  m_dropCallback = std::move(callback);
}

// -------------------------------------------------------------------------
// Impl: Streaming Interface
// -------------------------------------------------------------------------

bool ExecutionEngine::Impl::startStreaming(
    std::shared_ptr<PipelineContext> context) {
  std::lock_guard<std::mutex> lock(m_engineMutex);

  if (m_engineState.load(std::memory_order_acquire) != EngineState::IDLE) {
    LOG_ERROR_S << "ExecutionEngine: Cannot start streaming - not idle.";
    return false;
  }

  if (!m_schedulerStrategy->supportsStreaming()) {
    LOG_ERROR_S << "ExecutionEngine: Current scheduler doesn't support "
                   "streaming.";
    return false;
  }

  m_currentContext = context;
  m_streamingMode.store(true, std::memory_order_release);
  m_stopFlag.store(false, std::memory_order_release);
  m_engineState.store(EngineState::RUNNING, std::memory_order_release);
  m_statistics.reset();

  LOG_INFO_S << "ExecutionEngine: Started streaming mode.";
  return true;
}

void ExecutionEngine::Impl::stopStreaming(bool wait_for_drain) {
  // Set stop flag
  {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    if (!m_streamingMode.load(std::memory_order_acquire)) {
      return;
    }
    m_stopFlag.store(true, std::memory_order_release);
  }

  // Notify all waiting threads
  m_completionCV.notify_all();

  // Wait for queues to drain
  if (wait_for_drain) {
    waitForDrain(0, std::chrono::milliseconds{5000});
  }

  // Cannot proceed with state cleanup while tasks are running.
  {
    std::unique_lock<std::mutex> lock(m_completionMutex);
    while (m_activeTasks.load(std::memory_order_acquire) > 0) {
      m_completionCV.wait_for(lock, std::chrono::milliseconds{10});
    }
  }

  // Update state - safe now that all tasks have completed
  {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    m_streamingMode.store(false, std::memory_order_release);
    m_engineState.store(EngineState::IDLE, std::memory_order_release);
  }

  LOG_INFO_S << "ExecutionEngine: Stopped streaming mode.";
}

bool ExecutionEngine::Impl::isStreaming() const {
  return m_streamingMode.load(std::memory_order_acquire);
}

QueuePushResult ExecutionEngine::Impl::pushInput(const std::string &source_node,
                                                 const std::string &port_name,
                                                 PortDataPtr data) {
  if (!isStreaming() &&
      m_engineState.load(std::memory_order_acquire) != EngineState::RUNNING) {
    return QueuePushResult::rejected("Not in streaming/running mode", 0);
  }

  auto node_it = m_nodeNameMap.find(source_node);
  if (node_it == m_nodeNameMap.end()) {
    return QueuePushResult::rejected("Unknown node: " + source_node, 0);
  }

  const auto &node = node_it->second;

  // 确定实际端口名
  std::string actual_port = port_name;
  if (actual_port.empty()) {
    // 优先选择输入端口，如果没有则选择输出端口（支持源节点）
    actual_port = getFirstInputPort(node);
    if (actual_port.empty()) {
      actual_port = getFirstOutputPort(node);
    }
  }

  if (actual_port.empty()) {
    return QueuePushResult::rejected("No ports for node: " + source_node, 0);
  }

  // 判断端口类型并路由数据
  if (isInputPort(node, actual_port)) {
    // 输入端口：直接推送到该节点的输入队列
    pushToQueue(node, actual_port, std::move(data));
    m_statistics.total_queue_pushes.fetch_add(1, std::memory_order_relaxed);
    tryScheduleNode(node);
    auto size = getQueueSize(node, actual_port);
    return QueuePushResult::success(size);
  } else if (isOutputPort(node, actual_port)) {
    // 输出端口：路由到所有下游节点的输入端口
    return routeToDownstream(node, actual_port, std::move(data));
  } else {
    return QueuePushResult::rejected(
        "Port '" + actual_port + "' not found on node: " + source_node, 0);
  }
}

QueuePushResult ExecutionEngine::Impl::pushInput(const std::string &source_node,
                                                 PortDataPtr data) {
  return pushInput(source_node, "", std::move(data));
}

// -------------------------------------------------------------------------
// Impl: State & Monitoring
// -------------------------------------------------------------------------

EngineStatisticsSnapshot ExecutionEngine::Impl::statistics() const {
  return EngineStatisticsSnapshot(m_statistics);
}

std::size_t
ExecutionEngine::Impl::queueDepth(const std::string &node_name,
                                  const std::string &port_name) const {
  auto node_it = m_nodeNameMap.find(node_name);
  if (node_it == m_nodeNameMap.end()) {
    return 0;
  }

  std::string actual_port =
      port_name.empty() ? getFirstInputPort(node_it->second) : port_name;

  return getQueueSize(node_it->second, actual_port);
}

bool ExecutionEngine::Impl::hasQueueCapacity(
    const std::string &node_name, const std::string &port_name) const {
  auto node_it = m_nodeNameMap.find(node_name);
  if (node_it == m_nodeNameMap.end()) {
    return false;
  }

  auto state_it = m_nodeStates.find(node_it->second);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return false;
  }

  std::string actual_port =
      port_name.empty() ? getFirstInputPort(node_it->second) : port_name;

  auto &state = *state_it->second;

  auto bounded_it = state.bounded_queues.find(actual_port);
  if (bounded_it != state.bounded_queues.end() && bounded_it->second) {
    return !bounded_it->second->isFull();
  }

  // Unbounded queues always have capacity
  return true;
}

bool ExecutionEngine::Impl::waitForDrain(std::size_t max_depth,
                                         std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    if (m_stopFlag.load(std::memory_order_acquire)) {
      LOG_TRACE_S << "ExecutionEngine: waitForDrain interrupted by stop flag";
      return true;
    }

    bool all_drained = true;

    for (const auto &[node, state] : m_nodeStates) {
      if (!state)
        continue;

      for (const auto &[port, queue] : state->bounded_queues) {
        if (queue && queue->size() > max_depth) {
          all_drained = false;
          break;
        }
      }
      for (const auto &[port, queue] : state->unbounded_queues) {
        if (queue && queue->size() > max_depth) {
          all_drained = false;
          break;
        }
      }
      if (!all_drained)
        break;
    }

    if (all_drained && m_activeTasks.load(std::memory_order_acquire) == 0) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::microseconds{5});
  }

  LOG_WARNING_S << "ExecutionEngine: waitForDrain timed out";
  return false;
}

// -------------------------------------------------------------------------
// Impl: Configuration
// -------------------------------------------------------------------------

void ExecutionEngine::Impl::setNodeQueueConfig(const std::string &node_name,
                                               const QueueConfig &config) {
  m_nodeQueueConfigs[node_name] = config;
}

// -------------------------------------------------------------------------
// Impl: Information
// -------------------------------------------------------------------------

std::string ExecutionEngine::Impl::info() const {
  std::ostringstream oss;
  oss << "ExecutionEngine{\n"
      << "  mode: " << executionModeToString(m_config.mode) << "\n"
      << "  workers: " << static_cast<int>(m_config.num_workers) << "\n"
      << "  state: " << stateToString(m_engineState.load()) << "\n"
      << "  streaming: " << (isStreaming() ? "yes" : "no") << "\n"
      << "  nodes: " << m_nodeStates.size() << "\n"
      << "  strategies: " << strategyInfo() << "\n"
      << "  statistics: {\n"
      << "    executions: " << m_statistics.total_executions.load() << "\n"
      << "    success_rate: " << m_statistics.successRate() << "%\n"
      << "    frames_processed: " << m_statistics.total_frames_processed.load()
      << "\n"
      << "    frames_dropped: " << m_statistics.total_frames_dropped.load()
      << "\n"
      << "    drop_rate: " << m_statistics.dropRate() << "%\n"
      << "    throughput: " << m_statistics.throughput() << " fps\n"
      << "  }\n"
      << "}";
  return oss.str();
}

std::string ExecutionEngine::Impl::strategyInfo() const {
  std::ostringstream oss;
  oss << "{ scheduler: "
      << (m_schedulerStrategy ? m_schedulerStrategy->name() : "none")
      << ", sync: " << (m_syncStrategy ? m_syncStrategy->name() : "none")
      << " }";
  return oss.str();
}

// -------------------------------------------------------------------------
// Impl: Initialization Helpers
// -------------------------------------------------------------------------

void ExecutionEngine::Impl::initializeNodeStates() {
  m_nodeStates.clear();
  m_nodeNameMap.clear();

  for (const auto &node : m_graph->getNodes()) {
    auto state = std::make_unique<NodeState>(node, node->getName());
    state->queue_config = getNodeQueueConfig(node->getName());

    m_nodeNameMap[node->getName()] = node;
    m_nodeStates[node] = std::move(state);
  }
}

void ExecutionEngine::Impl::initializeQueues() {
  bool use_bounded = m_config.default_queue_capacity > 0;

  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    state->bounded_queues.clear();
    state->unbounded_queues.clear();

    for (const auto &port_name : node_ptr->getExpectedInputPorts()) {
      auto config = state->queue_config;

      if (use_bounded || config.capacity > 0) {
        // Use bounded queue
        std::size_t cap = config.capacity > 0 ? config.capacity
                                              : m_config.default_queue_capacity;

        BoundedDropQueueConfig queue_config{
            .capacity = cap,
            .track_statistics = config.track_statistics,
            .node_name = state->name,
            .port_name = port_name,
        };

        auto queue = std::make_shared<BoundedQueueType>(queue_config);

        // Set drop strategy
        if (config.drop_strategy == "KeepLatest" ||
            config.drop_strategy == "keep_latest") {
          queue->setStrategy(std::make_unique<KeepLatestNStrategy<PortDataPtr>>(
              config.keep_latest_n));
        } else if (config.drop_strategy == "DropTail" ||
                   config.drop_strategy == "drop_tail") {
          queue->setStrategy(std::make_unique<DropTailStrategy<PortDataPtr>>());
        } else {
          queue->setStrategy(std::make_unique<DropHeadStrategy<PortDataPtr>>());
        }

        state->bounded_queues[port_name] = queue;
      } else {
        // Use unbounded queue
        state->unbounded_queues[port_name] =
            std::make_shared<UnboundedQueueType>();
      }
    }
  }
}

void ExecutionEngine::Impl::identifySinkNodes() {
  m_sinkNodes.clear();
  for (const auto &node : m_graph->getNodes()) {
    if (m_graph->getOutDegree(node) == 0) {
      m_sinkNodes.push_back(node);
      LOG_DEBUG_S << "ExecutionEngine: Identified sink node: "
                  << node->getName();
    }
  }
}

void ExecutionEngine::Impl::setupDropCallbacks() {
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    for (auto &[port_name, queue] : state->bounded_queues) {
      if (queue) {
        queue->setDropCallback(
            [this, name = state->name](const DropEvent &event) {
              m_statistics.total_frames_dropped.fetch_add(
                  1, std::memory_order_relaxed);

              if (m_config.enable_drop_logging) {
                LOG_WARNING_S << "ExecutionEngine: Frame dropped at " << name
                              << " - " << event.reason;
              }

              if (m_dropCallback) {
                m_dropCallback(event.node_name, event.frame_id, event.reason);
              }

              // Report to sync strategy
              if (m_syncStrategy && m_syncStrategy->isEnabled() &&
                  event.frame_id != frame_constants::k_invalid_frame_id) {
                (void)m_syncStrategy->reportDrop(name, event.frame_id,
                                                 event.reason);
              }
            });
      }
    }
  }
}

// -------------------------------------------------------------------------
// Impl: Execution Helpers
// -------------------------------------------------------------------------

bool ExecutionEngine::Impl::distributeInitialInputs(
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
        pushToQueue(node, target_port, data_packet);
        LOG_TRACE_S << "ExecutionEngine: Distributed input to "
                    << node->getName() << ":" << target_port;
      }
      has_scheduled = true;
      tryScheduleNode(node);
    } else if (expected_ports.empty()) {
      LOG_TRACE_S << "ExecutionEngine: Auto-scheduling source node "
                  << node->getName();
      has_scheduled = true;
      tryScheduleNode(node);
    }
  }

  if (!has_scheduled && !initial_inputs.empty()) {
    LOG_ERROR_S << "ExecutionEngine: Initial inputs provided but "
                   "no source nodes consumed them.";
    return false;
  }

  return true;
}

void ExecutionEngine::Impl::scheduleReadyNodes() {
  for (auto &[node, state] : m_nodeStates) {
    tryScheduleNode(node);
  }
}

void ExecutionEngine::Impl::tryScheduleNode(const NodePtr &node) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return;
  }

  auto &state = *state_it->second;
  auto current_state = state.exec_state->load(std::memory_order_acquire);

  if (current_state != NodeExecutionState::WAITING) {
    return;
  }

  std::lock_guard<std::mutex> node_lock(*state.mutex);

  // Re-check after acquiring lock
  if (!state.exec_state) {
    return;
  }
  current_state = state.exec_state->load(std::memory_order_acquire);
  if (current_state != NodeExecutionState::WAITING) {
    return;
  }

  // Build scheduling context
  SchedulingContext context;
  context.node = node;
  context.expected_input_ports = node->getExpectedInputPorts();
  context.ready_input_ports = getReadyPorts(node);
  context.execution_count = state.execution_count;
  context.last_execution_time = state.last_execution;
  context.is_source_node = isSourceNode(node);
  context.is_sink_node = isSinkNode(node);
  context.has_initial_input = !context.ready_input_ports.empty();

  auto result = m_schedulerStrategy->shouldSchedule(context);

  if (result.decision == ScheduleDecision::ScheduleNow) {
    scheduleNodeExecution(node);
  }
}

void ExecutionEngine::Impl::scheduleNodeExecution(const NodePtr &node) {
  auto &state = m_nodeStates[node];

  NodeExecutionState expected = NodeExecutionState::WAITING;
  if (!state->exec_state->compare_exchange_strong(
          expected, NodeExecutionState::READY, std::memory_order_acq_rel)) {
    return; // Already scheduled or running
  }

  m_activeTasks.fetch_add(1, std::memory_order_acq_rel);

  // Count executions in streaming mode (batch mode counts in execute())
  if (m_config.mode == ExecutionMode::STREAM) {
    m_statistics.total_executions.fetch_add(1, std::memory_order_relaxed);
  }

  LOG_TRACE_S << "ExecutionEngine: Node " << state->name << " is READY.";

  // Changed: 'this' pointer now refers to the Impl instance
  auto future = m_threadPool->submit(&ExecutionEngine::Impl::executeNodeTask,
                                     this, node, m_currentContext);
}

void ExecutionEngine::Impl::executeNodeTask(
    NodePtr node, std::shared_ptr<PipelineContext> context) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    auto state_it = m_nodeStates.find(node);
    if (state_it != m_nodeStates.end() && state_it->second) {
      state_it->second->exec_state->store(NodeExecutionState::WAITING,
                                          std::memory_order_release);
    }
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  auto &state = *state_it->second;

  // Transition to EXECUTING
  NodeExecutionState expected = NodeExecutionState::READY;
  if (!state.exec_state->compare_exchange_strong(
          expected, NodeExecutionState::EXECUTING, std::memory_order_acq_rel)) {
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  LOG_TRACE_S << "ExecutionEngine: Node " << state.name << " is EXECUTING.";

  PortDataMap inputs;
  PortDataMap outputs;

  auto start_time = std::chrono::steady_clock::now();

  // Gather inputs
  bool success = gatherNodeInputs(node, inputs);

  // Process node
  if (success) {
    success = processNode(node, inputs, outputs, context);
  }

  auto end_time = std::chrono::steady_clock::now();
  auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                         end_time - start_time)
                         .count();
  m_statistics.total_processing_time_us.fetch_add(duration_us,
                                                  std::memory_order_relaxed);

  // Handle completion
  if (m_stopFlag.load(std::memory_order_acquire)) {
    if (state.exec_state) {
      state.exec_state->store(NodeExecutionState::WAITING,
                              std::memory_order_release);
    }
  } else if (success) {
    handleNodeSuccess(node, outputs);
  } else {
    handleNodeFailure(node, "Execution failed");
  }

  m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
  LOG_TRACE_S << "ExecutionEngine: Node " << state.name
              << " task finished. Active tasks: "
              << m_activeTasks.load(std::memory_order_acquire);

  checkCompletionAndNotify();
}

bool ExecutionEngine::Impl::gatherNodeInputs(const NodePtr &node,
                                             PortDataMap &inputs) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return false;
  }

  auto &state = *state_it->second;

  for (const auto &port_name : node->getExpectedInputPorts()) {
    auto data = popFromQueue(node, port_name);
    if (data.has_value()) {
      inputs[port_name] = data.value();
    } else {
      LOG_ERROR_S << "ExecutionEngine: Input queue empty for " << state.name
                  << ":" << port_name;
      return false;
    }
  }

  return true;
}

bool ExecutionEngine::Impl::processNode(
    const NodePtr &node, const PortDataMap &inputs, PortDataMap &outputs,
    const std::shared_ptr<PipelineContext> &context) {
  try {
    node->process(inputs, outputs, context);
    return true;
  } catch (const std::exception &e) {
    LOG_ERROR_S << "ExecutionEngine: Node " << node->getName()
                << " failed: " << e.what();
    if (m_errorCallback) {
      m_errorCallback(e.what(), node->getName());
    }
    return false;
  } catch (...) {
    LOG_ERROR_S << "ExecutionEngine: Node " << node->getName()
                << " failed with unknown exception.";
    if (m_errorCallback) {
      m_errorCallback("Unknown exception", node->getName());
    }
    return false;
  }
}

void ExecutionEngine::Impl::propagateOutputs(const NodePtr &source,
                                             const PortDataMap &outputs) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  const auto outgoing_edges = m_graph->getOutgoingEdges(source);

  for (const auto &edge : outgoing_edges) {
    auto output_it = outputs.find(edge.source_port);
    if (output_it == outputs.end()) {
      continue;
    }

    const auto &data = output_it->second;
    const auto &dest_node = edge.dest_node;
    const auto &dest_port = edge.dest_port;

    pushToQueue(dest_node, dest_port, data);

    LOG_TRACE_S << "ExecutionEngine: Propagated " << source->getName() << ":"
                << edge.source_port << " -> " << dest_node->getName() << ":"
                << dest_port;

    tryScheduleNode(dest_node);
  }
}

void ExecutionEngine::Impl::handleNodeSuccess(const NodePtr &node,
                                              const PortDataMap &outputs) {
  auto &state = m_nodeStates[node];

  state->execution_count++;
  state->last_execution = std::chrono::steady_clock::now();

  // successful_executions 统计所有节点的执行次数
  m_statistics.successful_executions.fetch_add(1, std::memory_order_relaxed);

  // Collect results and count frames ONLY for sink nodes
  if (isSinkNode(node)) {
    // total_frames_processed 只统计完成处理的帧数（sink 节点完成数）
    m_statistics.total_frames_processed.fetch_add(1, std::memory_order_relaxed);
    collectResults(node, outputs);
  }

  // Propagate outputs
  propagateOutputs(node, outputs);

  // Check if node should be rescheduled
  if (m_schedulerStrategy->onNodeComplete(node, true, outputs)) {
    if (state->exec_state) {
      state->exec_state->store(NodeExecutionState::WAITING,
                               std::memory_order_release);
    }
    tryScheduleNode(node);
  } else {
    if (state->exec_state) {
      state->exec_state->store(NodeExecutionState::COMPLETED,
                               std::memory_order_release);
    }
  }

  LOG_TRACE_S << "ExecutionEngine: Node " << state->name << " COMPLETED.";
}

void ExecutionEngine::Impl::handleNodeFailure(const NodePtr &node,
                                              const std::string &error) {
  auto &state = m_nodeStates[node];

  if (state->exec_state) {
    state->exec_state->store(NodeExecutionState::FAILED,
                             std::memory_order_release);
  }

  m_statistics.failed_executions.fetch_add(1, std::memory_order_relaxed);

  LOG_ERROR_S << "ExecutionEngine: Node " << state->name
              << " FAILED: " << error;

  if (!isStreaming()) {
    stopExecutionAsync();
  }
}

void ExecutionEngine::Impl::checkCompletionAndNotify() {
  if (m_streamingMode.load(std::memory_order_acquire)) {
    // In streaming mode, just notify if no active tasks
    if (m_activeTasks.load(std::memory_order_acquire) == 0) {
      m_completionCV.notify_all();
    }
    return;
  }

  const int active_tasks = m_activeTasks.load(std::memory_order_acquire);
  if (active_tasks != 0) {
    return;
  }

  // Build sink execution counts
  std::unordered_map<std::string, std::uint64_t> sink_counts;
  for (const auto &sink : m_sinkNodes) {
    auto state_it = m_nodeStates.find(sink);
    if (state_it != m_nodeStates.end() && state_it->second) {
      sink_counts[sink->getName()] = state_it->second->execution_count;
    }
  }

  auto status =
      m_schedulerStrategy->checkCompletion(active_tasks, 0, sink_counts);

  if (status.is_complete) {
    LOG_TRACE_S << "ExecutionEngine: Execution complete - " << status.reason;

    // Invoke result callback
    if (m_resultCallback) {
      PortDataMap results;
      {
        std::lock_guard<std::mutex> lock(m_resultsMutex);
        results = m_accumulatedResults;
      }
      m_resultCallback(results);
    }
  }

  m_completionCV.notify_all();
}

bool ExecutionEngine::Impl::waitForCompletion() {
  std::unique_lock<std::mutex> lock(m_completionMutex);

  m_completionCV.wait(lock, [this] {
    return m_activeTasks.load(std::memory_order_acquire) == 0 ||
           m_stopFlag.load(std::memory_order_acquire);
  });

  const bool was_stopped = m_stopFlag.load(std::memory_order_acquire);
  const int active_tasks = m_activeTasks.load(std::memory_order_acquire);
  const auto current_state = m_engineState.load(std::memory_order_acquire);

  std::lock_guard<std::mutex> engine_lock(m_engineMutex);

  if (was_stopped && current_state != EngineState::STOPPED) {
    m_engineState.store(EngineState::STOPPED, std::memory_order_release);
    LOG_ERROR_S << "ExecutionEngine: Execution was stopped.";
  } else if (active_tasks == 0 && current_state == EngineState::RUNNING) {
    m_engineState.store(EngineState::IDLE, std::memory_order_release);
    LOG_INFO_S << "ExecutionEngine: Execution completed successfully.";
  } else if (current_state != EngineState::ERROR &&
             current_state != EngineState::STOPPED) {
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
    LOG_ERROR_S << "ExecutionEngine: Execution finished abnormally.";
  }

  const bool success =
      m_engineState.load(std::memory_order_acquire) == EngineState::IDLE;
  m_currentContext = nullptr;
  return success;
}

void ExecutionEngine::Impl::resetInternalState() {
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_activeTasks.store(0, std::memory_order_relaxed);

  {
    std::lock_guard<std::mutex> lock(m_resultsMutex);
    m_accumulatedResults.clear();
  }

  for (auto &[node, state] : m_nodeStates) {
    if (state) {
      state->exec_state->store(NodeExecutionState::WAITING,
                               std::memory_order_relaxed);
      state->execution_count = 0;

      for (auto &[port, queue] : state->bounded_queues) {
        if (queue)
          queue->clear();
      }
      for (auto &[port, queue] : state->unbounded_queues) {
        if (queue)
          queue->clear();
      }
    }
  }
}

// -------------------------------------------------------------------------
// Impl: Queue Operations
// -------------------------------------------------------------------------

void ExecutionEngine::Impl::pushToQueue(const NodePtr &node,
                                        const std::string &port_name,
                                        PortDataPtr data) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return;
  }

  auto &state = *state_it->second;

  auto bounded_it = state.bounded_queues.find(port_name);
  if (bounded_it != state.bounded_queues.end() && bounded_it->second) {
    bounded_it->second->push(std::move(data));
    return;
  }

  auto unbounded_it = state.unbounded_queues.find(port_name);
  if (unbounded_it != state.unbounded_queues.end() && unbounded_it->second) {
    unbounded_it->second->push(std::move(data));
    return;
  }

  LOG_WARNING_S << "ExecutionEngine: No queue found for " << state.name << ":"
                << port_name;
}

std::optional<PortDataPtr>
ExecutionEngine::Impl::popFromQueue(const NodePtr &node,
                                    const std::string &port_name) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return std::nullopt;
  }

  auto &state = *state_it->second;

  auto bounded_it = state.bounded_queues.find(port_name);
  if (bounded_it != state.bounded_queues.end() && bounded_it->second) {
    return bounded_it->second->tryPop();
  }

  auto unbounded_it = state.unbounded_queues.find(port_name);
  if (unbounded_it != state.unbounded_queues.end() && unbounded_it->second) {
    return unbounded_it->second->tryPop();
  }

  return std::nullopt;
}

bool ExecutionEngine::Impl::hasDataInQueue(const NodePtr &node,
                                           const std::string &port_name) const {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return false;
  }

  auto &state = *state_it->second;

  auto bounded_it = state.bounded_queues.find(port_name);
  if (bounded_it != state.bounded_queues.end() && bounded_it->second) {
    return !bounded_it->second->empty();
  }

  auto unbounded_it = state.unbounded_queues.find(port_name);
  if (unbounded_it != state.unbounded_queues.end() && unbounded_it->second) {
    return !unbounded_it->second->empty();
  }

  return false;
}

std::size_t
ExecutionEngine::Impl::getQueueSize(const NodePtr &node,
                                    const std::string &port_name) const {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return 0;
  }

  auto &state = *state_it->second;

  auto bounded_it = state.bounded_queues.find(port_name);
  if (bounded_it != state.bounded_queues.end() && bounded_it->second) {
    return bounded_it->second->size();
  }

  auto unbounded_it = state.unbounded_queues.find(port_name);
  if (unbounded_it != state.unbounded_queues.end() && unbounded_it->second) {
    return unbounded_it->second->size();
  }

  return 0;
}

// -------------------------------------------------------------------------
// Impl: Utility
// -------------------------------------------------------------------------

bool ExecutionEngine::Impl::isSourceNode(const NodePtr &node) const {
  return m_graph->getInDegree(node) == 0;
}

bool ExecutionEngine::Impl::isSinkNode(const NodePtr &node) const {
  return std::find(m_sinkNodes.begin(), m_sinkNodes.end(), node) !=
         m_sinkNodes.end();
}

void ExecutionEngine::Impl::collectResults(const NodePtr &node,
                                           const PortDataMap &outputs) {
  std::lock_guard<std::mutex> lock(m_resultsMutex);
  for (const auto &[port_name, data] : outputs) {
    std::string result_key = node->getName() + ":" + port_name;
    m_accumulatedResults[result_key] = data;
    LOG_TRACE_S << "ExecutionEngine: Collected result: " << result_key;
  }
}

std::vector<std::string>
ExecutionEngine::Impl::getReadyPorts(const NodePtr &node) const {
  std::vector<std::string> ready_ports;

  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return ready_ports;
  }

  auto &state = *state_it->second;

  for (const auto &port_name : node->getExpectedInputPorts()) {
    if (hasDataInQueue(node, port_name)) {
      ready_ports.push_back(port_name);
    }
  }

  return ready_ports;
}

QueueConfig
ExecutionEngine::Impl::getNodeQueueConfig(const std::string &node_name) const {
  auto it = m_nodeQueueConfigs.find(node_name);
  if (it != m_nodeQueueConfigs.end()) {
    return it->second;
  }

  // Return default config
  QueueConfig config;
  config.capacity = m_config.default_queue_capacity;
  config.drop_strategy = m_config.default_drop_strategy;
  config.track_statistics = m_config.enable_statistics;
  return config;
}

std::string
ExecutionEngine::Impl::getFirstInputPort(const NodePtr &node) const {
  auto ports = node->getExpectedInputPorts();
  return ports.empty() ? "" : ports[0];
}

QueuePushResult
ExecutionEngine::Impl::routeToDownstream(const NodePtr &source_node,
                                         const std::string &output_port,
                                         PortDataPtr data) {

  // 获取从该输出端口出发的所有边
  const auto &outgoing_edges = m_graph->getOutgoingEdges(source_node);

  std::size_t routed_count = 0;
  std::size_t total_queue_size = 0;

  for (const auto &edge : outgoing_edges) {
    // 只处理匹配的输出端口
    if (edge.source_port != output_port) {
      continue;
    }

    const auto &dest_node = edge.dest_node;
    const auto &dest_port = edge.dest_port;

    // 对于多个下游节点，需要复制数据（共享指针，低开销）
    PortDataPtr data_copy = data;

    // 推送到下游节点的输入队列
    pushToQueue(dest_node, dest_port, std::move(data_copy));

    m_statistics.total_queue_pushes.fetch_add(1, std::memory_order_relaxed);

    // 尝试调度下游节点
    tryScheduleNode(dest_node);

    // 累计队列大小
    total_queue_size += getQueueSize(dest_node, dest_port);
    routed_count++;

    LOG_TRACE_S << "ExecutionEngine: Routed data from "
                << source_node->getName() << ":" << output_port << " -> "
                << dest_node->getName() << ":" << dest_port;
  }

  if (routed_count == 0) {
    // 没有下游连接，可能是 sink 节点或未连接的端口
    LOG_WARNING_S << "ExecutionEngine: No downstream connections for "
                  << source_node->getName() << ":" << output_port;
    return QueuePushResult::rejected(
        "No downstream connections for port: " + output_port, 0);
  }

  return QueuePushResult::success(total_queue_size);
}

bool ExecutionEngine::Impl::isInputPort(const NodePtr &node,
                                        const std::string &port_name) const {
  const auto &input_ports = node->getExpectedInputPorts();
  return std::find(input_ports.begin(), input_ports.end(), port_name) !=
         input_ports.end();
}

bool ExecutionEngine::Impl::isOutputPort(const NodePtr &node,
                                         const std::string &port_name) const {
  const auto &output_ports = node->getExpectedOutputPorts();
  return std::find(output_ports.begin(), output_ports.end(), port_name) !=
         output_ports.end();
}

std::string
ExecutionEngine::Impl::getFirstOutputPort(const NodePtr &node) const {
  const auto &ports = node->getExpectedOutputPorts();
  return ports.empty() ? "" : ports[0];
}

std::string ExecutionEngine::Impl::stateToString(EngineState state) {
  switch (state) {
  case EngineState::IDLE:
    return "IDLE";
  case EngineState::RUNNING:
    return "RUNNING";
  case EngineState::STOPPED:
    return "STOPPED";
  case EngineState::ERROR:
    return "ERROR";
  }
  return "UNKNOWN";
}

} // namespace ai_pipe