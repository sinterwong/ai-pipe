/**
 * @file execution_engine_impl.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Implementation of the Unified Execution Engine (PIMPL Wrapper & Impl)
 * @version 2.0
 * @date 2025-12-24
 *
 * v2.0: Unified error handling with Result<T>.
 *
 * @copyright Copyright (c) 2025
 */

#include "execution_engine_impl.hpp"
#include "join_aware_sync_strategy.hpp"
#include "logger.hpp"
#include "scheduler_strategies.hpp"
#include "sync_strategies.hpp"
#include <algorithm>
#include <cassert>
#include <sstream>
#include <utility>

namespace ai_pipe {

// =============================================================================
// ExecutionEngine (PIMPL Wrapper) Implementation
// =============================================================================

std::unique_ptr<ExecutionEngine>
ExecutionEngine::create(const EngineConfig &config) {
  auto engine = std::make_unique<ExecutionEngine>(config);
  engine->configureForMode(config.mode);
  return engine;
}

ExecutionEngine::ExecutionEngine() : m_impl(std::make_unique<Impl>()) {}

ExecutionEngine::ExecutionEngine(const EngineConfig &config)
    : m_impl(std::make_unique<Impl>(config)) {}

ExecutionEngine::~ExecutionEngine() = default;

ExecutionEngine::ExecutionEngine(ExecutionEngine &&) noexcept = default;
ExecutionEngine &
ExecutionEngine::operator=(ExecutionEngine &&) noexcept = default;

// -------------------------------------------------------------------------
// Strategy Forwarding
// -------------------------------------------------------------------------

Result<void>
ExecutionEngine::setSchedulerStrategy(SchedulerStrategyPtr strategy) {
  return m_impl->setSchedulerStrategy(std::move(strategy));
}

Result<void> ExecutionEngine::setSyncStrategy(SyncStrategyPtr strategy) {
  return m_impl->setSyncStrategy(std::move(strategy));
}

Result<void> ExecutionEngine::setTraceSink(std::shared_ptr<ITraceSink> sink) {
  return m_impl->setTraceSink(std::move(sink));
}

void ExecutionEngine::configureForMode(ExecutionMode mode) {
  m_impl->configureForMode(mode);
}

// -------------------------------------------------------------------------
// Core Interface Forwarding
// -------------------------------------------------------------------------

Result<void> ExecutionEngine::initialize(Graph *graph,
                                         std::uint8_t num_workers) {
  return m_impl->initialize(graph, num_workers);
}

Result<void>
ExecutionEngine::execute(const PortDataMap &initial_inputs,
                         bool wait_for_completion,
                         std::shared_ptr<PipelineContext> context) {
  return m_impl->execute(initial_inputs, wait_for_completion,
                         std::move(context));
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

Result<void>
ExecutionEngine::startStreaming(std::shared_ptr<PipelineContext> context) {
  return m_impl->startStreaming(std::move(context));
}

void ExecutionEngine::stopStreaming(bool wait_for_drain) {
  m_impl->stopStreaming(wait_for_drain);
}

bool ExecutionEngine::isStreaming() const { return m_impl->isStreaming(); }

Result<PushStatus> ExecutionEngine::pushInput(const std::string &source_node,
                                              const std::string &port_name,
                                              PortDataPtr data) {
  return m_impl->pushInput(source_node, port_name, std::move(data));
}

Result<PushStatus> ExecutionEngine::pushInput(const std::string &source_node,
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

Result<void> ExecutionEngine::waitForDrain(std::size_t max_depth,
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
  // The watchdog posts tasks through the pool and reads node states, so
  // it must stop before either is torn down.
  stopJoinTimeoutWatchdog();

  // Always shut down the thread pool first, regardless of engine state.
  // Worker threads may still be executing tasks that access members
  // (m_nodeStates, m_activeTasks, m_stopFlag, callbacks, etc.) via the
  // captured `this` pointer. C++ destroys members in reverse declaration
  // order, so m_threadPool must be stopped and joined before any member
  // it depends on is destroyed.
  if (m_threadPool && m_threadPool->isRunning()) {
    m_threadPool->shutdown(false);
  }

  // Safe now: no worker can touch nodes anymore.
  teardownNodes();
}

// -------------------------------------------------------------------------
// Impl: Strategy Configuration
// -------------------------------------------------------------------------

Result<void> ExecutionEngine::Impl::setSchedulerStrategy(
    std::unique_ptr<ISchedulerStrategy> strategy) {
  if (m_engineState.load(std::memory_order_acquire) != EngineState::IDLE) {
    return Result<void>::err(
        ErrorCode::InvalidState,
        "Cannot change strategies while engine is running");
  }
  m_schedulerStrategy = std::move(strategy);
  return Result<void>::ok();
}

Result<void> ExecutionEngine::Impl::setSyncStrategy(
    std::unique_ptr<ISyncStrategy> strategy) {
  if (m_engineState.load(std::memory_order_acquire) != EngineState::IDLE) {
    return Result<void>::err(
        ErrorCode::InvalidState,
        "Cannot change strategies while engine is running");
  }
  m_syncStrategy = std::move(strategy);
  return Result<void>::ok();
}

Result<void>
ExecutionEngine::Impl::setTraceSink(std::shared_ptr<ITraceSink> sink) {
  if (m_engineState.load(std::memory_order_acquire) != EngineState::IDLE) {
    return Result<void>::err(ErrorCode::InvalidState,
                             "Cannot change trace sink while engine is "
                             "running");
  }
  m_traceSink = std::move(sink);
  return Result<void>::ok();
}

void ExecutionEngine::Impl::emitTrace(TracePhase phase, const std::string &node,
                                      std::string_view detail,
                                      const PortData *packet, Timestamp start,
                                      std::chrono::microseconds duration) {
  ITraceSink *sink = m_traceSink.get();
  if (!sink) {
    return;
  }
  TraceEvent event;
  event.phase = phase;
  event.node = node;
  event.detail = detail;
  if (packet) {
    event.frame_id = packet->id;
    event.stream_id = packet->stream_id;
  }
  event.start = start;
  event.duration = duration;
  event.thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
  sink->onEvent(event);
}

const PortData *
ExecutionEngine::Impl::primaryFrame(const PortDataMap &packets) {
  for (const auto &[port, packet] : packets) {
    if (packet && packet->hasFrameId()) {
      return packet.get();
    }
  }
  return nullptr;
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

Result<void> ExecutionEngine::Impl::initialize(Graph *graph,
                                               std::uint8_t num_workers) {
  if (!graph) {
    LOG_ERROR_S << "ExecutionEngine: Invalid graph pointer.";
    return Result<void>::err(ErrorCode::InvalidArgument,
                             "Graph pointer is null");
  }

  std::lock_guard<std::mutex> lock(m_engineMutex);

  // Re-initialization with a new graph: release the previous nodes.
  stopJoinTimeoutWatchdog();
  teardownNodes();

  // Compile the graph up front: rejects empty graphs and cycles, and
  // precomputes the routing/topology data the execution paths rely on.
  auto compiled = CompiledGraph::compile(*graph);
  if (!compiled) {
    LOG_ERROR_S << "ExecutionEngine: Graph compilation failed: "
                << compiled.error().toString();
    return Result<void>::err(compiled.error());
  }

  m_graph = graph;
  m_compiledGraph.emplace(std::move(compiled.value()));

  if (num_workers > 0) {
    m_config.num_workers = num_workers;
  }

  m_threadPool = std::make_unique<WorkStealingThreadPool>(m_config.num_workers);

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

  // Cache sync-tracking membership so hot paths skip the strategy lock
  // for nodes outside any sync group.
  const bool sync_active = m_syncStrategy && m_syncStrategy->isEnabled();
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (state) {
      state->sync_tracked =
          sync_active && m_syncStrategy->tracksNode(state->name);
    }
  }

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

  return Result<void>::ok();
}

Result<void>
ExecutionEngine::Impl::execute(const PortDataMap &initial_inputs,
                               bool wait_for_completion,
                               std::shared_ptr<PipelineContext> context) {
  m_currentContext.store(std::move(context));

  // Handle streaming mode
  if (m_streamingMode.load(std::memory_order_acquire)) {
    for (const auto &[node_name, data] : initial_inputs) {
      auto result = pushInput(node_name, data);
      if (!result) {
        LOG_ERROR_S << "ExecutionEngine: Failed to push input to " << node_name
                    << ": " << result.error().message();
        return Result<void>::err(result.error());
      }
    }

    if (wait_for_completion) {
      std::unique_lock<std::mutex> lock(m_completionMutex);
      m_completionCV.wait(lock, [this] {
        return m_activeTasks.load(std::memory_order_acquire) == 0 ||
               m_stopFlag.load(std::memory_order_acquire);
      });
    }
    return Result<void>::ok();
  }

  // Batch execution mode
  {
    std::unique_lock<std::mutex> lock(m_engineMutex);

    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      LOG_ERROR_S << "ExecutionEngine: Already running.";
      m_currentContext.store(nullptr);
      return Result<void>::err(ErrorCode::AlreadyRunning,
                               "Engine is already running");
    }

    if (!m_graph || !m_threadPool) {
      LOG_ERROR_S << "ExecutionEngine: Not initialized.";
      m_currentContext.store(nullptr);
      return Result<void>::err(ErrorCode::NotInitialized,
                               "Engine not initialized (missing graph or "
                               "thread pool)");
    }

    LOG_TRACE_S << "ExecutionEngine: Starting execution.";

    auto setup_result = setupNodes(m_currentContext.load());
    if (!setup_result) {
      m_engineState.store(EngineState::ERROR, std::memory_order_release);
      m_currentContext.store(nullptr);
      return setup_result;
    }

    resetInternalState();
    m_engineState.store(EngineState::RUNNING, std::memory_order_release);
  }

  if (!distributeInitialInputs(initial_inputs)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
    LOG_ERROR_S << "ExecutionEngine: Failed to distribute inputs.";
    m_currentContext.store(nullptr);
    return Result<void>::err(ErrorCode::ExecutionFailed,
                             "Failed to distribute initial inputs to source "
                             "nodes");
  }

  if (wait_for_completion) {
    return waitForCompletion();
  }

  return Result<void>::ok();
}

void ExecutionEngine::Impl::stopExecutionAsync() {
  LOG_INFO_S << "ExecutionEngine: stopExecutionAsync called.";

  bool expected = false;
  if (m_stopFlag.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
    {
      std::lock_guard<std::mutex> lock(m_engineMutex);
      if (m_engineState.load(std::memory_order_acquire) ==
          EngineState::RUNNING) {
        m_engineState.store(EngineState::STOPPED, std::memory_order_release);
      }
    }
    notifyCompletionWaiters();
  }
}

void ExecutionEngine::Impl::stopExecutionSync() {
  LOG_INFO_S << "ExecutionEngine: stopExecutionSync called.";

  stopExecutionAsync();

  {
    std::unique_lock<std::mutex> lock(m_completionMutex);
    m_completionCV.wait(lock, [this] {
      return m_activeTasks.load(std::memory_order_acquire) == 0;
    });
  }

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

  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    state->exec_state.store(NodeExecutionState::WAITING,
                            std::memory_order_relaxed);
    state->execution_count.store(0, std::memory_order_relaxed);

    for (auto &[port_name, queue] : state->lock_free_queues) {
      if (queue) {
        queue->clear();
        queue->resetStatistics();
      }
    }
  }

  if (m_schedulerStrategy) {
    m_schedulerStrategy->reset();
  }
  if (m_syncStrategy) {
    m_syncStrategy->reset();
  }

  teardownNodes();

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
    if (state) {
      result[state->name] = state->exec_state.load(std::memory_order_acquire);
    }
  }

  return result;
}

// -------------------------------------------------------------------------
// Impl: Callback Registration
// -------------------------------------------------------------------------

void ExecutionEngine::Impl::setPipelineResultCallback(
    std::function<void(const PortDataMap &)> callback) {
  std::lock_guard<std::mutex> lock(m_callbackMutex);
  m_resultCallback = std::move(callback);
}

void ExecutionEngine::Impl::setPipelineErrorCallback(
    std::function<void(const std::string &, const std::string &)> callback) {
  std::lock_guard<std::mutex> lock(m_callbackMutex);
  m_errorCallback = std::move(callback);
}

void ExecutionEngine::Impl::setDropCallback(
    std::function<void(const std::string &, std::uint64_t, const std::string &)>
        callback) {
  std::lock_guard<std::mutex> lock(m_callbackMutex);
  m_dropCallback = std::move(callback);
}

void ExecutionEngine::Impl::invokeResultCallback(const PortDataMap &results) {
  std::function<void(const PortDataMap &)> callback;
  {
    std::lock_guard<std::mutex> lock(m_callbackMutex);
    callback = m_resultCallback;
  }
  if (!callback) {
    return;
  }
  try {
    callback(results);
  } catch (const std::exception &e) {
    LOG_ERROR_S << "ExecutionEngine: Result callback threw: " << e.what();
  } catch (...) {
    LOG_ERROR_S << "ExecutionEngine: Result callback threw unknown exception";
  }
}

void ExecutionEngine::Impl::invokeErrorCallback(const std::string &message,
                                                const std::string &node_name) {
  std::function<void(const std::string &, const std::string &)> callback;
  {
    std::lock_guard<std::mutex> lock(m_callbackMutex);
    callback = m_errorCallback;
  }
  if (!callback) {
    return;
  }
  try {
    callback(message, node_name);
  } catch (const std::exception &e) {
    LOG_ERROR_S << "ExecutionEngine: Error callback threw: " << e.what();
  } catch (...) {
    LOG_ERROR_S << "ExecutionEngine: Error callback threw unknown exception";
  }
}

void ExecutionEngine::Impl::invokeDropCallback(const std::string &node_name,
                                               std::uint64_t frame_id,
                                               const std::string &reason) {
  std::function<void(const std::string &, std::uint64_t, const std::string &)>
      callback;
  {
    std::lock_guard<std::mutex> lock(m_callbackMutex);
    callback = m_dropCallback;
  }
  if (!callback) {
    return;
  }
  try {
    callback(node_name, frame_id, reason);
  } catch (const std::exception &e) {
    LOG_ERROR_S << "ExecutionEngine: Drop callback threw: " << e.what();
  } catch (...) {
    LOG_ERROR_S << "ExecutionEngine: Drop callback threw unknown exception";
  }
}

// -------------------------------------------------------------------------
// Impl: Streaming Interface
// -------------------------------------------------------------------------

Result<void> ExecutionEngine::Impl::startStreaming(
    std::shared_ptr<PipelineContext> context) {
  std::lock_guard<std::mutex> lock(m_engineMutex);

  if (m_engineState.load(std::memory_order_acquire) != EngineState::IDLE) {
    LOG_ERROR_S << "ExecutionEngine: Cannot start streaming - not idle.";
    return Result<void>::err(ErrorCode::InvalidState,
                             "Cannot start streaming - engine is not idle");
  }

  if (!m_schedulerStrategy->supportsStreaming()) {
    LOG_ERROR_S << "ExecutionEngine: Current scheduler doesn't support "
                   "streaming.";
    return Result<void>::err(
        ErrorCode::StreamingNotSupported,
        "Current scheduler strategy does not support streaming mode");
  }

  auto setup_result = setupNodes(context);
  if (!setup_result) {
    return setup_result;
  }

  m_currentContext.store(std::move(context));
  m_streamingMode.store(true, std::memory_order_release);
  m_stopFlag.store(false, std::memory_order_release);
  m_engineState.store(EngineState::RUNNING, std::memory_order_release);
  m_statistics.reset();

  if (m_config.join_wait_timeout.count() > 0 && !m_multiInputIndices.empty()) {
    startJoinTimeoutWatchdog();
  }

  LOG_INFO_S << "ExecutionEngine: Started streaming mode.";
  return Result<void>::ok();
}

void ExecutionEngine::Impl::stopStreaming(bool wait_for_drain) {
  {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    if (!m_streamingMode.load(std::memory_order_acquire)) {
      return;
    }
    m_stopFlag.store(true, std::memory_order_release);
  }

  stopJoinTimeoutWatchdog();

  notifyCompletionWaiters();

  if (wait_for_drain) {
    (void)waitForDrain(0, std::chrono::milliseconds{5000});
  }

  {
    std::unique_lock<std::mutex> lock(m_completionMutex);
    m_completionCV.wait(lock, [this] {
      return m_activeTasks.load(std::memory_order_acquire) == 0;
    });
  }

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

Result<PushStatus>
ExecutionEngine::Impl::pushInput(const std::string &source_node,
                                 const std::string &port_name,
                                 PortDataPtr data) {
  if (!isStreaming() &&
      m_engineState.load(std::memory_order_acquire) != EngineState::RUNNING) {
    return Result<PushStatus>::err(ErrorCode::NotStreaming,
                                   "Not in streaming/running mode");
  }

  auto node_it = m_nodeNameMap.find(source_node);
  if (node_it == m_nodeNameMap.end()) {
    return Result<PushStatus>::err(Error::nodeNotFound(source_node));
  }

  const auto &node = node_it->second;

  std::string actual_port = port_name;
  if (actual_port.empty()) {
    actual_port = getFirstInputPort(node);
    if (actual_port.empty()) {
      actual_port = getFirstOutputPort(node);
    }
  }

  if (actual_port.empty()) {
    return Result<PushStatus>::err(ErrorCode::PortNotFound,
                                   "No ports for node: " + source_node,
                                   source_node);
  }

  if (isInputPort(node, actual_port)) {
    stampIncomingFrame(data);
    if (!pushToQueue(node, actual_port, std::move(data))) {
      return Result<PushStatus>::err(
          ErrorCode::QueueRejected,
          "Queue full (DropTail) on port: " + actual_port, source_node);
    }
    m_statistics.total_queue_pushes.fetch_add(1, std::memory_order_relaxed);
    m_statistics.total_input_frames.fetch_add(1, std::memory_order_relaxed);
    tryScheduleNode(node);
    auto size = getQueueSize(node, actual_port);
    return PushStatus::enqueued(size);
  } else if (isOutputPort(node, actual_port)) {
    return routeToDownstream(node, actual_port, data);
  } else {
    return Result<PushStatus>::err(
        Error::portNotFound(actual_port, source_node));
  }
}

Result<PushStatus>
ExecutionEngine::Impl::pushInput(const std::string &source_node,
                                 PortDataPtr data) {
  return pushInput(source_node, "", std::move(data));
}

// -------------------------------------------------------------------------
// Impl: State & Monitoring
// -------------------------------------------------------------------------

EngineStatisticsSnapshot ExecutionEngine::Impl::statistics() const {
  EngineStatisticsSnapshot snapshot(m_statistics);

  snapshot.node_stats.reserve(m_nodeStates.size());
  for (const auto &[node_ptr, state] : m_nodeStates) {
    if (!state) {
      continue;
    }
    auto node_stats = state->stats.snapshot(state->name);
    std::size_t queue_depth = 0;
    for (const auto *queue : state->input_queues) {
      if (queue) {
        queue_depth += queue->size();
      }
    }
    node_stats.current_queue_depth = queue_depth;
    snapshot.node_stats.push_back(std::move(node_stats));
  }

  return snapshot;
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

  auto queue_it = state.lock_free_queues.find(actual_port);
  if (queue_it != state.lock_free_queues.end() && queue_it->second) {
    return !queue_it->second->isFull();
  }

  return true;
}

Result<void>
ExecutionEngine::Impl::waitForDrain(std::size_t max_depth,
                                    std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;

  // Every task completion fires notifyCompletionWaiters(), so queue
  // drain progress always re-triggers the predicate; no sleep polling.
  std::unique_lock<std::mutex> lock(m_completionMutex);
  const bool drained =
      m_completionCV.wait_until(lock, deadline, [this, max_depth] {
        if (m_stopFlag.load(std::memory_order_acquire)) {
          return true;
        }
        return m_activeTasks.load(std::memory_order_acquire) == 0 &&
               allQueuesDrained(max_depth);
      });

  if (drained) {
    if (m_stopFlag.load(std::memory_order_acquire)) {
      LOG_TRACE_S << "ExecutionEngine: waitForDrain interrupted by stop flag";
    }
    return Result<void>::ok();
  }

  LOG_WARNING_S << "ExecutionEngine: waitForDrain timed out";
  return Result<void>::err(ErrorCode::ExecutionTimeout,
                           "waitForDrain timed out after " +
                               std::to_string(timeout.count()) + "ms");
}

bool ExecutionEngine::Impl::allQueuesDrained(std::size_t max_depth) const {
  for (const auto &[node, state] : m_nodeStates) {
    if (!state) {
      continue;
    }
    for (const auto &[port, queue] : state->lock_free_queues) {
      if (queue && queue->size() > max_depth) {
        return false;
      }
    }
  }
  return true;
}

void ExecutionEngine::Impl::notifyCompletionWaiters() {
  {
    std::lock_guard<std::mutex> lock(m_completionMutex);
  }
  m_completionCV.notify_all();
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
      << "    frames_processed: " << m_statistics.total_output_frames.load()
      << "\n"
      << "    frames_dropped: " << m_statistics.total_dropped_frames.load()
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

Result<void> ExecutionEngine::Impl::setupNodes(
    const std::shared_ptr<PipelineContext> &context) {
  if (!m_setUpNodes.empty()) {
    return Result<void>::ok(); // Already set up
  }

  for (const auto index : m_compiledGraph->topologicalOrder()) {
    const auto &node = m_compiledGraph->node(index);
    Result<void> result = Result<void>::ok();
    try {
      result = node->setup(context);
    } catch (const std::exception &e) {
      result = Result<void>::err(Error::nodeException(
          std::string("setup threw: ") + e.what(), node->getName()));
    } catch (...) {
      result = Result<void>::err(Error(ErrorCode::NodeUnknownException,
                                       "setup threw unknown exception",
                                       node->getName()));
    }

    if (!result) {
      LOG_ERROR_S << "ExecutionEngine: setup failed for " << node->getName()
                  << ": " << result.error().toString();
      teardownNodes();
      return result;
    }
    m_setUpNodes.push_back(node);
  }

  LOG_DEBUG_S << "ExecutionEngine: " << m_setUpNodes.size() << " nodes set up.";
  return Result<void>::ok();
}

void ExecutionEngine::Impl::teardownNodes() noexcept {
  for (auto it = m_setUpNodes.rbegin(); it != m_setUpNodes.rend(); ++it) {
    try {
      (*it)->teardown();
    } catch (...) {
      // teardown() is contractually noexcept; guard against rogue nodes.
      LOG_ERROR_S << "ExecutionEngine: teardown threw for " << (*it)->getName();
    }
  }
  m_setUpNodes.clear();
}

void ExecutionEngine::Impl::initializeNodeStates() {
  m_nodeStates.clear();
  m_nodeNameMap.clear();
  m_statesByIndex.assign(m_compiledGraph->nodeCount(), nullptr);
  m_multiInputIndices.clear();

  for (const auto &node : m_graph->getNodes()) {
    auto state = std::make_unique<NodeState>(node, node->getName());
    state->queue_config = getNodeQueueConfig(node->getName());
    state->index = m_compiledGraph->indexOfPtr(node.get());
    if (state->index != CompiledGraph::k_invalid_index) {
      m_statesByIndex[state->index] = state.get();
    }

    m_nodeNameMap[node->getName()] = node;
    m_nodeStates[node] = std::move(state);
  }
}

void ExecutionEngine::Impl::initializeQueues() {
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    state->lock_free_queues.clear();
    state->input_ports = node_ptr->getExpectedInputPorts();
    state->input_queues.clear();

    for (const auto &port_name : state->input_ports) {
      auto config = state->queue_config;

      std::size_t cap = config.capacity > 0 ? config.capacity
                                            : m_config.default_queue_capacity;
      if (cap == 0) {
        cap = 1024;
      }

      LockFreeDropPolicy drop_policy = LockFreeDropPolicy::DropHead;
      if (config.drop_strategy == "KeepLatest" ||
          config.drop_strategy == "keep_latest") {
        drop_policy = LockFreeDropPolicy::KeepLatest;
      } else if (config.drop_strategy == "DropTail" ||
                 config.drop_strategy == "drop_tail") {
        drop_policy = LockFreeDropPolicy::DropTail;
      }

      LockFreeNodeQueue<PortDataPtr>::Config queue_config{
          .capacity = cap,
          .drop_policy = drop_policy,
          .keep_latest_n = config.keep_latest_n,
          .track_statistics = config.track_statistics,
          .node_name = state->name,
          .port_name = port_name,
      };

      auto queue = std::make_shared<LockFreeQueueType>(queue_config);
      state->input_queues.push_back(queue.get());
      state->lock_free_queues[port_name] = std::move(queue);
    }

    if (state->input_ports.size() > 1 &&
        state->index != CompiledGraph::k_invalid_index) {
      m_multiInputIndices.push_back(state->index);
    }
  }
}

void ExecutionEngine::Impl::setupDropCallbacks() {
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    for (auto &[port_name, queue] : state->lock_free_queues) {
      if (queue) {
        queue->setFrameIdAccessor(
            [](const PortDataPtr &item) -> std::optional<FrameId> {
              if (item && item->hasFrameId()) {
                return item->frameId();
              }
              return std::nullopt;
            });
        queue->setDropCallback(
            [this, name = state->name](const DropEvent &event) {
              m_statistics.total_dropped_frames.fetch_add(
                  1, std::memory_order_relaxed);

              if (m_config.enable_drop_logging) {
                LOG_WARNING_S << "ExecutionEngine: Frame dropped at " << name
                              << " - " << event.reason;
              }

              invokeDropCallback(event.node_name, event.frame_id, event.reason);

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

  for (const auto source_index : m_compiledGraph->sourceNodes()) {
    const auto &node = m_compiledGraph->node(source_index);

    const auto &expected_ports = node->getExpectedInputPorts();
    const bool has_input = initial_inputs.count(node->getName()) > 0;

    if (has_input) {
      const auto &data_packet = initial_inputs.at(node->getName());
      stampIncomingFrame(data_packet);
      if (!expected_ports.empty()) {
        const std::string &target_port = expected_ports[0];
        if (!pushToQueue(node, target_port, data_packet)) {
          LOG_ERROR_S << "ExecutionEngine: Failed to distribute input to "
                      << node->getName() << ":" << target_port;
          return false;
        }
        m_statistics.total_input_frames.fetch_add(1, std::memory_order_relaxed);
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

void ExecutionEngine::Impl::tryScheduleNode(const NodePtr &node) {
  if (!m_compiledGraph) {
    return;
  }
  const auto index = m_compiledGraph->indexOfPtr(node.get());
  if (index != CompiledGraph::k_invalid_index) {
    tryScheduleNode(index);
  }
}

void ExecutionEngine::Impl::tryScheduleNode(CompiledGraph::NodeIndex index) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  NodeState *state_ptr = m_statesByIndex[index];
  if (!state_ptr) {
    return;
  }
  auto &state = *state_ptr;

  if (state.exec_state.load(std::memory_order_acquire) !=
      NodeExecutionState::WAITING) {
    return;
  }

  // No per-node lock: concurrent attempts may both evaluate the strategy,
  // but only one will win the WAITING->READY CAS in scheduleNodeExecution.
  SchedulingContext context;
  context.node = state.node;
  context.expected_input_count =
      static_cast<std::uint32_t>(state.input_ports.size());
  for (std::size_t i = 0; i < state.input_queues.size(); ++i) {
    if (state.input_queues[i] && !state.input_queues[i]->empty()) {
      context.ready_input_count++;
      if (i < 64) {
        context.ready_port_mask |= (std::uint64_t{1} << i);
      }
    }
  }
  context.execution_count =
      state.execution_count.load(std::memory_order_relaxed);
  context.last_execution_time = state.lastExecution();
  context.is_source_node = m_compiledGraph->isSource(index);
  context.is_sink_node = m_compiledGraph->isSink(index);
  context.has_initial_input = context.ready_input_count > 0;

  auto result = m_schedulerStrategy->shouldSchedule(context);

  if (result.decision == ScheduleDecision::ScheduleNow) {
    scheduleNodeExecution(state);
  }
}

void ExecutionEngine::Impl::scheduleNodeExecution(NodeState &state) {
  NodeExecutionState expected = NodeExecutionState::WAITING;
  if (!state.exec_state.compare_exchange_strong(
          expected, NodeExecutionState::READY, std::memory_order_acq_rel)) {
    return;
  }

  m_activeTasks.fetch_add(1, std::memory_order_acq_rel);

  LOG_TRACE_S << "ExecutionEngine: Node " << state.name << " is READY.";

  const bool posted = m_threadPool->post(
      [this, index = state.index, context = m_currentContext.load(),
       scheduled_at = std::chrono::steady_clock::now()]() mutable {
        const auto delay =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - scheduled_at);
        if (m_config.enable_statistics && delay.count() > 0) {
          m_statistics.total_schedule_time_us.fetch_add(
              static_cast<std::uint64_t>(delay.count()),
              std::memory_order_relaxed);
        }
        if (NodeState *scheduled = m_statesByIndex[index]) {
          emitTrace(TracePhase::Schedule, scheduled->name, {}, nullptr,
                    scheduled_at, delay);
        }
        executeNodeTask(index, context);
      });

  if (!posted) {
    // Pool is shutting down (or submission timed out): roll back so the
    // active-task count and node state stay consistent.
    LOG_WARNING_S << "ExecutionEngine: Failed to post task for node "
                  << state.name;
    state.exec_state.store(NodeExecutionState::WAITING,
                           std::memory_order_release);
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
  }
}

void ExecutionEngine::Impl::executeNodeTask(
    CompiledGraph::NodeIndex index,
    const std::shared_ptr<PipelineContext> &context) {
  NodeState *state_ptr = m_statesByIndex[index];
  if (!state_ptr) {
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }
  auto &state = *state_ptr;

  if (m_stopFlag.load(std::memory_order_acquire)) {
    state.exec_state.store(NodeExecutionState::WAITING,
                           std::memory_order_release);
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  // Transition to EXECUTING
  NodeExecutionState expected = NodeExecutionState::READY;
  if (!state.exec_state.compare_exchange_strong(
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
  bool inputs_ready = gatherNodeInputs(state, inputs);

  if (!inputs_ready) {
    if (m_streamingMode.load(std::memory_order_acquire) &&
        !m_stopFlag.load(std::memory_order_acquire)) {
      LOG_TRACE_S << "ExecutionEngine: Node " << state.name
                  << " inputs transiently unavailable, rescheduling.";
      state.exec_state.store(NodeExecutionState::WAITING,
                             std::memory_order_release);
      // Reschedule BEFORE decrementing: scheduleNodeExecution() increments
      // m_activeTasks, so this ordering prevents the counter from
      // transiently dipping to zero while data is still queued, which
      // would wake execute(wait_for_completion=true) waiters too early.
      tryScheduleNode(index);
      m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
      checkCompletionAndNotify();
      return;
    }
    handleNodeFailure(state, Error(ErrorCode::InputUnavailable,
                                   "Input queue empty", state.name));
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  // Process node - now returns Result<void> with rich error context
  m_statistics.total_executions.fetch_add(1, std::memory_order_relaxed);
  const auto process_start = std::chrono::steady_clock::now();
  auto process_result = processNode(state.node, inputs, outputs, context);

  auto end_time = std::chrono::steady_clock::now();
  emitTrace(TracePhase::Execute, state.name,
            process_result ? std::string_view{} : std::string_view{"failed"},
            primaryFrame(inputs), process_start,
            std::chrono::duration_cast<std::chrono::microseconds>(
                end_time - process_start));
  auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(
                         end_time - start_time)
                         .count();
  m_statistics.total_processing_time_us.fetch_add(duration_us,
                                                  std::memory_order_relaxed);

  if (m_config.enable_statistics) {
    state.stats.recordExecution(process_result.isOk(),
                                static_cast<std::uint64_t>(duration_us));
    if (process_result.isOk()) {
      state.stats.recordIo(inputs.size(), outputs.size());
    }
  }

  // Handle completion
  if (m_stopFlag.load(std::memory_order_acquire)) {
    state.exec_state.store(NodeExecutionState::WAITING,
                           std::memory_order_release);
  } else if (process_result) {
    inheritFrameIdentity(inputs, outputs);

    // End-to-end frame latency: measured when a frame reaches a sink,
    // from its pipeline-entry timestamp to completion.
    if (m_config.enable_statistics && m_compiledGraph->isSink(index)) {
      for (const auto &[port, packet] : inputs) {
        if (packet && packet->timestamp != Timestamp{}) {
          const auto latency =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  end_time - packet->timestamp)
                  .count();
          if (latency > 0) {
            m_statistics.recordLatency(static_cast<std::uint64_t>(latency));
          }
          break;
        }
      }
    }

    // Advance the sync watermark for tracked nodes so the coordinator
    // can prune drop records the whole group has moved past.
    if (state.sync_tracked && m_syncStrategy) {
      for (const auto &[port, packet] : inputs) {
        if (packet && packet->hasFrameId()) {
          m_syncStrategy->markProcessed(state.name, packet->frameId());
          break;
        }
      }
    }

    handleNodeSuccess(state, outputs);
  } else {
    handleNodeFailure(state, process_result.error());
  }

  m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
  LOG_TRACE_S << "ExecutionEngine: Node " << state.name
              << " task finished. Active tasks: "
              << m_activeTasks.load(std::memory_order_acquire);

  checkCompletionAndNotify();
}

bool ExecutionEngine::Impl::gatherNodeInputs(NodeState &state,
                                             PortDataMap &inputs) {
  // Multi-input nodes get frame-aligned gathering whenever a sync
  // strategy is active; alignment is driven purely by frame ids, so it
  // does not require the node to be mapped into a sync group.
  // Non-default alignment policies are an explicit opt-in and align
  // regardless of the sync strategy (multi-stream pipelines need
  // pairing even without cross-branch drop coordination).
  if (state.input_ports.size() > 1) {
    // Consume the watchdog's timeout flag up front: if the gather below
    // succeeds anyway (the missing input arrived in the meantime), the
    // flag must not linger and degrade a later, unrelated wait.
    const bool degrade_requested =
        m_config.join_wait_timeout.count() > 0 &&
        state.degrade_pending.exchange(false, std::memory_order_acq_rel);

    bool gathered = false;
    bool aligned_path = true;
    switch (m_config.alignment_policy) {
    case AlignmentPolicy::StreamFrameId:
      gathered = gatherStreamAlignedInputs(state, inputs);
      break;
    case AlignmentPolicy::Timestamp:
      gathered = gatherTimestampAlignedInputs(state, inputs);
      break;
    case AlignmentPolicy::FrameId:
      if (m_syncStrategy && m_syncStrategy->isEnabled()) {
        gathered = gatherAlignedInputs(state, inputs);
      } else {
        aligned_path = false;
      }
      break;
    }

    if (aligned_path) {
      if (!gathered && degrade_requested) {
        gathered = degradeJoinGather(state, inputs);
      }
      return gathered;
    }
    // No aligned path (FrameId without a sync strategy): the plain
    // gather below pops destructively, so a degraded partial read must
    // happen before it touches the queues.
    if (degrade_requested) {
      bool any_port_dry = false;
      for (const auto *queue : state.input_queues) {
        if (!queue || queue->empty()) {
          any_port_dry = true;
          break;
        }
      }
      if (any_port_dry) {
        return degradeJoinGather(state, inputs);
      }
    }
  }

  for (std::size_t i = 0; i < state.input_ports.size(); ++i) {
    auto *queue = state.input_queues[i];
    for (;;) {
      auto data = queue ? queue->tryPop() : std::nullopt;
      if (!data.has_value()) {
        LOG_TRACE_S << "ExecutionEngine: Input queue empty for " << state.name
                    << ":" << state.input_ports[i];
        return false;
      }
      // Coordinated drop: a sibling branch already dropped this frame,
      // so processing it here would only feed a doomed pairing.
      if (state.sync_tracked && data.value() && data.value()->hasFrameId() &&
          m_syncStrategy->shouldDrop(state.name, data.value()->frameId())) {
        recordSyncDrop(state, state.input_ports[i], data.value()->frameId(),
                       "coordinated sync drop");
        continue;
      }
      recordDequeue(data.value());
      inputs[state.input_ports[i]] = std::move(data.value());
      break;
    }
  }

  return true;
}

bool ExecutionEngine::Impl::gatherAlignedInputs(NodeState &state,
                                                PortDataMap &inputs) {
  const std::size_t port_count = state.input_ports.size();
  std::vector<FrameId> head_ids(port_count, 0);

  for (;;) {
    // Phase A: peek every head (discarding pending coordinated drops)
    // and find the newest frame id among them.
    FrameId target = 0;
    for (std::size_t i = 0; i < port_count; ++i) {
      auto *queue = state.input_queues[i];
      for (;;) {
        auto head = queue ? queue->tryPeek() : std::nullopt;
        if (!head.has_value()) {
          return false; // Port not ready; caller reschedules
        }
        const FrameId frame =
            head.value() ? head.value()->frameId() : FrameId{0};
        if (state.sync_tracked &&
            frame != frame_constants::k_invalid_frame_id &&
            m_syncStrategy->shouldDrop(state.name, frame)) {
          (void)queue->tryPop();
          recordSyncDrop(state, state.input_ports[i], frame,
                         "coordinated sync drop");
          continue;
        }
        head_ids[i] = frame;
        if (frame > target) {
          target = frame;
        }
        break;
      }
    }

    // Phase B: all heads aligned (or wildcard 0)? Pop and deliver.
    bool aligned = true;
    for (std::size_t i = 0; i < port_count; ++i) {
      if (head_ids[i] != frame_constants::k_invalid_frame_id &&
          head_ids[i] != target) {
        aligned = false;
        break;
      }
    }

    if (aligned) {
      for (std::size_t i = 0; i < port_count; ++i) {
        auto data = state.input_queues[i]->tryPop();
        if (!data.has_value()) {
          return false; // Should not happen under single-consumer contract
        }
        recordDequeue(data.value());
        inputs[state.input_ports[i]] = std::move(data.value());
      }
      return true;
    }

    // Phase C: discard lagging heads - their partner frames were lost
    // on a sibling branch, so they can never be paired.
    for (std::size_t i = 0; i < port_count; ++i) {
      if (head_ids[i] != frame_constants::k_invalid_frame_id &&
          head_ids[i] < target) {
        (void)state.input_queues[i]->tryPop();
        recordSyncDrop(state, state.input_ports[i], head_ids[i],
                       "frame alignment drop");
        if (state.sync_tracked) {
          (void)m_syncStrategy->reportDrop(state.name, head_ids[i],
                                           "frame alignment");
        }
      }
    }
    // Loop: re-peek with fresh heads (each pass discards >= 1 frame, so
    // this terminates once queues stabilize or a port runs dry).
  }
}

std::optional<PortDataPtr>
ExecutionEngine::Impl::peekAlignmentHead(NodeState &state,
                                         std::size_t port_index) {
  auto *queue = state.input_queues[port_index];
  for (;;) {
    auto head = queue ? queue->tryPeek() : std::nullopt;
    if (!head.has_value()) {
      return std::nullopt;
    }
    const FrameId frame = head.value() ? head.value()->frameId() : FrameId{0};
    if (state.sync_tracked && frame != frame_constants::k_invalid_frame_id &&
        m_syncStrategy->shouldDrop(state.name, frame)) {
      (void)queue->tryPop();
      recordSyncDrop(state, state.input_ports[port_index], frame,
                     "coordinated sync drop");
      continue;
    }
    return head;
  }
}

void ExecutionEngine::Impl::dropStaleHead(NodeState &state,
                                          std::size_t port_index,
                                          const PortData &head,
                                          const char *reason) {
  (void)state.input_queues[port_index]->tryPop();
  recordSyncDrop(state, state.input_ports[port_index], head.frameId(), reason);
  if (state.sync_tracked && head.hasFrameId()) {
    (void)m_syncStrategy->reportDrop(state.name, head.frameId(), reason);
  }
}

bool ExecutionEngine::Impl::degradeJoinGather(NodeState &state,
                                              PortDataMap &inputs) {
  const std::size_t port_count = state.input_ports.size();
  std::vector<std::optional<PortDataPtr>> heads(port_count);

  std::size_t available = 0;
  for (std::size_t i = 0; i < port_count; ++i) {
    heads[i] = peekAlignmentHead(state, i);
    if (heads[i].has_value()) {
      ++available;
    }
  }
  if (available == 0 || available == port_count) {
    // Nothing is stuck (all dry, or the missing input arrived between
    // the failed gather and now): let the normal path handle it.
    return false;
  }

  // Reference = the oldest available head; that is the frame that has
  // been waiting past the timeout. Null packets and unassigned ids are
  // wildcards, pairing with anything.
  const PortData *reference = nullptr;
  for (std::size_t i = 0; i < port_count; ++i) {
    if (!heads[i].has_value() || !heads[i].value()) {
      continue;
    }
    const PortData &head = *heads[i].value();
    if (m_config.alignment_policy != AlignmentPolicy::Timestamp &&
        !head.hasFrameId()) {
      continue;
    }
    if (!reference || head.timestamp < reference->timestamp) {
      reference = &head;
    }
  }

  auto pairs_with_reference = [&](const PortData &head) {
    if (!reference) {
      return true; // All wildcards
    }
    switch (m_config.alignment_policy) {
    case AlignmentPolicy::StreamFrameId:
      return !head.hasFrameId() || (head.stream_id == reference->stream_id &&
                                    head.id == reference->id);
    case AlignmentPolicy::Timestamp: {
      const auto diff = head.timestamp >= reference->timestamp
                            ? head.timestamp - reference->timestamp
                            : reference->timestamp - head.timestamp;
      return diff <= m_config.alignment_tolerance;
    }
    case AlignmentPolicy::FrameId:
      break;
    }
    return !head.hasFrameId() || head.id == reference->id;
  };

  m_statistics.total_join_timeouts.fetch_add(1, std::memory_order_relaxed);

  const bool deliver =
      m_config.join_timeout_policy == JoinTimeoutPolicy::PartialInputs;
  bool delivered = false;
  for (std::size_t i = 0; i < port_count; ++i) {
    if (!heads[i].has_value()) {
      continue;
    }
    const PortDataPtr &head = heads[i].value();
    if (head && !pairs_with_reference(*head)) {
      continue; // Not part of the stuck frame; leave for normal pairing
    }
    if (deliver) {
      auto data = state.input_queues[i]->tryPop();
      if (!data.has_value()) {
        continue;
      }
      recordDequeue(data.value());
      inputs[state.input_ports[i]] = std::move(data.value());
      delivered = true;
    } else {
      dropStaleHead(state, i, head ? *head : PortData{},
                    "join wait timeout - frame skipped");
    }
  }

  if (m_config.enable_drop_logging) {
    LOG_DEBUG_S << "ExecutionEngine: Join timeout at " << state.name << " - "
                << (deliver ? "executing with partial inputs"
                            : "skipping stuck frame");
  }

  return deliver && delivered;
}

void ExecutionEngine::Impl::startJoinTimeoutWatchdog() {
  stopJoinTimeoutWatchdog();
  {
    std::lock_guard<std::mutex> lock(m_watchdogMutex);
    m_watchdogStop = false;
  }
  m_joinWatchdog = std::thread([this] { joinTimeoutWatchdogLoop(); });
}

void ExecutionEngine::Impl::stopJoinTimeoutWatchdog() {
  {
    std::lock_guard<std::mutex> lock(m_watchdogMutex);
    m_watchdogStop = true;
  }
  m_watchdogCV.notify_all();
  if (m_joinWatchdog.joinable()) {
    m_joinWatchdog.join();
  }
}

void ExecutionEngine::Impl::joinTimeoutWatchdogLoop() {
  const auto timeout = m_config.join_wait_timeout;
  const auto tick =
      std::chrono::milliseconds(std::max<std::int64_t>(1, timeout.count() / 4));

  std::unique_lock<std::mutex> lock(m_watchdogMutex);
  for (;;) {
    if (m_watchdogCV.wait_for(lock, tick, [this] { return m_watchdogStop; })) {
      return;
    }
    if (m_stopFlag.load(std::memory_order_acquire)) {
      continue; // Draining: leave remaining frames alone
    }

    const auto now = std::chrono::steady_clock::now();
    for (const auto index : m_multiInputIndices) {
      NodeState *state = m_statesByIndex[index];
      if (!state) {
        continue;
      }

      std::size_t ready = 0;
      for (const auto *queue : state->input_queues) {
        if (queue && !queue->empty()) {
          ++ready;
        }
      }
      if (ready == 0 || ready == state->input_queues.size()) {
        state->join_wait_since_ticks.store(0, std::memory_order_relaxed);
        continue;
      }

      const auto since_ticks =
          state->join_wait_since_ticks.load(std::memory_order_relaxed);
      if (since_ticks == 0) {
        state->join_wait_since_ticks.store(now.time_since_epoch().count(),
                                           std::memory_order_relaxed);
        continue;
      }
      const auto since = std::chrono::steady_clock::time_point{
          std::chrono::steady_clock::duration{since_ticks}};
      if (now - since < timeout) {
        continue;
      }

      // Timed out: restart the window and request a degraded execution.
      // If the node is currently EXECUTING the schedule attempt loses
      // the CAS and the pending flag is consumed (possibly as a no-op)
      // by that execution's own gather - never lost, never duplicated.
      state->join_wait_since_ticks.store(now.time_since_epoch().count(),
                                         std::memory_order_relaxed);
      state->degrade_pending.store(true, std::memory_order_release);
      scheduleNodeExecution(*state);
    }
  }
}

bool ExecutionEngine::Impl::gatherStreamAlignedInputs(NodeState &state,
                                                      PortDataMap &inputs) {
  const std::size_t port_count = state.input_ports.size();
  std::vector<PortDataPtr> heads(port_count);

  for (;;) {
    // Phase A: peek every head (discarding pending coordinated drops).
    for (std::size_t i = 0; i < port_count; ++i) {
      auto head = peekAlignmentHead(state, i);
      if (!head.has_value()) {
        return false; // Port not ready; caller reschedules
      }
      heads[i] = std::move(head.value());
    }

    // Phase B: aligned when every non-wildcard head carries the same
    // (stream, frame). Wildcard = unassigned id, matches anything.
    bool have_key = false;
    StreamId key_stream = frame_constants::k_default_stream_id;
    FrameId key_frame = frame_constants::k_invalid_frame_id;
    bool aligned = true;
    for (std::size_t i = 0; i < port_count; ++i) {
      const auto &head = heads[i];
      if (!head || !head->hasFrameId()) {
        continue;
      }
      if (!have_key) {
        have_key = true;
        key_stream = head->stream_id;
        key_frame = head->id;
      } else if (head->stream_id != key_stream || head->id != key_frame) {
        aligned = false;
        break;
      }
    }

    if (aligned) {
      for (std::size_t i = 0; i < port_count; ++i) {
        auto data = state.input_queues[i]->tryPop();
        if (!data.has_value()) {
          return false; // Should not happen under single-consumer contract
        }
        recordDequeue(data.value());
        inputs[state.input_ports[i]] = std::move(data.value());
      }
      return true;
    }

    // Phase C: discard heads that can never be paired. A FIFO port's
    // entry timestamps never decrease, so a head strictly older than
    // the newest head has lost its partner on the newest port. When no
    // head is strictly older (identical timestamps), fall back to
    // dropping the smallest (stream, frame) heads - deterministic and
    // guarantees progress, since misaligned heads cannot all be equal.
    Timestamp newest_ts{};
    for (std::size_t i = 0; i < port_count; ++i) {
      if (heads[i] && heads[i]->hasFrameId() &&
          heads[i]->timestamp > newest_ts) {
        newest_ts = heads[i]->timestamp;
      }
    }

    bool dropped_by_time = false;
    for (std::size_t i = 0; i < port_count; ++i) {
      if (heads[i] && heads[i]->hasFrameId() &&
          heads[i]->timestamp < newest_ts) {
        dropStaleHead(state, i, *heads[i], "stream alignment drop");
        dropped_by_time = true;
      }
    }

    if (!dropped_by_time) {
      std::size_t min_index = port_count;
      for (std::size_t i = 0; i < port_count; ++i) {
        if (!heads[i] || !heads[i]->hasFrameId()) {
          continue;
        }
        if (min_index == port_count ||
            std::make_pair(heads[i]->stream_id, heads[i]->id) <
                std::make_pair(heads[min_index]->stream_id,
                               heads[min_index]->id)) {
          min_index = i;
        }
      }
      for (std::size_t i = 0; i < port_count; ++i) {
        if (heads[i] && heads[i]->hasFrameId() &&
            heads[i]->stream_id == heads[min_index]->stream_id &&
            heads[i]->id == heads[min_index]->id) {
          dropStaleHead(state, i, *heads[i], "stream alignment drop");
        }
      }
    }
    // Loop: each pass discards >= 1 frame, so this terminates once
    // queues stabilize or a port runs dry.
  }
}

bool ExecutionEngine::Impl::gatherTimestampAlignedInputs(NodeState &state,
                                                         PortDataMap &inputs) {
  const std::size_t port_count = state.input_ports.size();
  std::vector<PortDataPtr> heads(port_count);

  for (;;) {
    for (std::size_t i = 0; i < port_count; ++i) {
      auto head = peekAlignmentHead(state, i);
      if (!head.has_value()) {
        return false; // Port not ready; caller reschedules
      }
      heads[i] = std::move(head.value());
    }

    // Null packets cannot be timestamp-matched; treat them as wildcards
    // the same way unassigned frame ids behave under id alignment.
    Timestamp min_ts = Timestamp::max();
    Timestamp max_ts = Timestamp::min();
    for (const auto &head : heads) {
      if (!head) {
        continue;
      }
      min_ts = std::min(min_ts, head->timestamp);
      max_ts = std::max(max_ts, head->timestamp);
    }

    if (min_ts > max_ts || max_ts - min_ts <= m_config.alignment_tolerance) {
      for (std::size_t i = 0; i < port_count; ++i) {
        auto data = state.input_queues[i]->tryPop();
        if (!data.has_value()) {
          return false; // Should not happen under single-consumer contract
        }
        recordDequeue(data.value());
        inputs[state.input_ports[i]] = std::move(data.value());
      }
      return true;
    }

    // Per-port timestamps are non-decreasing (stamped in arrival
    // order), so a head lagging the newest head by more than the
    // tolerance can never pair with anything the newest port will
    // produce. At least one head satisfies this (min_ts), so every
    // pass makes progress.
    for (std::size_t i = 0; i < port_count; ++i) {
      if (heads[i] &&
          max_ts - heads[i]->timestamp > m_config.alignment_tolerance) {
        dropStaleHead(state, i, *heads[i], "timestamp alignment drop");
      }
    }
  }
}

void ExecutionEngine::Impl::recordDequeue(const PortDataPtr &data) {
  if (!m_config.enable_statistics) {
    return;
  }
  m_statistics.total_queue_pops.fetch_add(1, std::memory_order_relaxed);
  if (data && data->timestamp != Timestamp{}) {
    const auto age = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - data->timestamp)
                         .count();
    if (age > 0) {
      m_statistics.total_wait_time_us.fetch_add(static_cast<std::uint64_t>(age),
                                                std::memory_order_relaxed);
    }
  }
}

void ExecutionEngine::Impl::recordSyncDrop(const NodeState &state,
                                           const std::string &port_name,
                                           FrameId frame_id,
                                           const char *reason) {
  m_statistics.total_dropped_frames.fetch_add(1, std::memory_order_relaxed);

  if (m_config.enable_drop_logging) {
    LOG_DEBUG_S << "ExecutionEngine: Dropped frame " << frame_id << " at "
                << state.name << ":" << port_name << " - " << reason;
  }

  invokeDropCallback(state.name, frame_id, reason);
}

Result<void> ExecutionEngine::Impl::processNode(
    const NodePtr &node, const PortDataMap &inputs, PortDataMap &outputs,
    const std::shared_ptr<PipelineContext> &context) {
  try {
    node->process(inputs, outputs, context);
    return Result<void>::ok();
  } catch (const std::exception &e) {
    LOG_ERROR_S << "ExecutionEngine: Node " << node->getName()
                << " failed: " << e.what();

    auto error = Error::nodeException(e.what(), node->getName());

    // Notify via callback (for backward compatibility with pipeline layer)
    invokeErrorCallback(e.what(), node->getName());

    return Result<void>(std::move(error));
  } catch (...) {
    LOG_ERROR_S << "ExecutionEngine: Node " << node->getName()
                << " failed with unknown exception.";

    auto error = Error(ErrorCode::NodeUnknownException, "Unknown exception",
                       node->getName());

    invokeErrorCallback("Unknown exception", node->getName());

    return Result<void>(std::move(error));
  }
}

void ExecutionEngine::Impl::propagateOutputs(NodeState &source_state,
                                             const PortDataMap &outputs) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  for (const auto &edge : m_compiledGraph->outEdges(source_state.index)) {
    auto output_it = outputs.find(edge.source_port);
    if (output_it == outputs.end()) {
      continue;
    }

    NodeState *dest = m_statesByIndex[edge.dest_node];
    if (!dest) {
      continue;
    }

    // Direct indexed queue access: no node hashing, no port-name lookup.
    LockFreeQueueType *queue = edge.dest_port_index < dest->input_queues.size()
                                   ? dest->input_queues[edge.dest_port_index]
                                   : nullptr;
    if (!queue) {
      LOG_WARNING_S << "ExecutionEngine: No queue found for " << dest->name
                    << ":" << edge.dest_port;
      continue;
    }

    if (!queue->push(output_it->second)) {
      // DropTail rejection: account it; the downstream queue is full,
      // so the node already has data to consume.
      recordQueueRejection(dest->name, edge.dest_port);
      continue;
    }

    LOG_TRACE_S << "ExecutionEngine: Propagated " << source_state.name << ":"
                << edge.source_port << " -> " << dest->name << ":"
                << edge.dest_port;

    tryScheduleNode(edge.dest_node);
  }
}

void ExecutionEngine::Impl::handleNodeSuccess(NodeState &state,
                                              const PortDataMap &outputs) {
  state.execution_count.fetch_add(1, std::memory_order_relaxed);
  state.last_execution_ticks.store(
      std::chrono::steady_clock::now().time_since_epoch().count(),
      std::memory_order_relaxed);

  m_statistics.successful_executions.fetch_add(1, std::memory_order_relaxed);

  if (m_compiledGraph->isSink(state.index)) {
    m_statistics.total_output_frames.fetch_add(1, std::memory_order_relaxed);
    collectResults(state.node, outputs);
  }

  // Propagate outputs
  const auto propagate_start = std::chrono::steady_clock::now();
  propagateOutputs(state, outputs);
  if (m_traceSink && !outputs.empty()) {
    emitTrace(TracePhase::Propagate, state.name, {}, primaryFrame(outputs),
              propagate_start,
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - propagate_start));
  }

  // Check if node should be rescheduled
  if (m_schedulerStrategy->onNodeComplete(state.node, true, outputs)) {
    state.exec_state.store(NodeExecutionState::WAITING,
                           std::memory_order_release);
    tryScheduleNode(state.index);
  } else {
    state.exec_state.store(NodeExecutionState::COMPLETED,
                           std::memory_order_release);
  }

  LOG_TRACE_S << "ExecutionEngine: Node " << state.name << " COMPLETED.";
}

void ExecutionEngine::Impl::handleNodeFailure(NodeState &state,
                                              const Error &error) {
  m_statistics.failed_executions.fetch_add(1, std::memory_order_relaxed);

  LOG_ERROR_S << "ExecutionEngine: Node " << state.name
              << " FAILED: " << error.toString();

  if (isStreaming()) {
    // Streaming semantics: a node exception is a per-frame event, not a
    // pipeline-fatal one. The failing frame was already consumed from
    // the queues, so return the node to service for subsequent frames -
    // previously it stayed FAILED forever and stranded its queued data.
    // (Error visibility is preserved: statistics, logs, and the error
    // callback fired from processNode.)
    state.exec_state.store(NodeExecutionState::WAITING,
                           std::memory_order_release);
    tryScheduleNode(state.index);
    return;
  }

  state.exec_state.store(NodeExecutionState::FAILED, std::memory_order_release);
  stopExecutionAsync();
}

void ExecutionEngine::Impl::checkCompletionAndNotify() {
  if (m_streamingMode.load(std::memory_order_acquire)) {
    // Notify on every completion (not only at zero active tasks):
    // waitForDrain waiters track queue depths, which change with each
    // task, and notifying an empty waiter list is nearly free.
    notifyCompletionWaiters();
    return;
  }

  const int active_tasks = m_activeTasks.load(std::memory_order_acquire);
  if (active_tasks != 0) {
    return;
  }

  std::unordered_map<std::string, std::uint64_t> sink_counts;
  if (m_compiledGraph) {
    for (const auto sink_index : m_compiledGraph->sinkNodes()) {
      const NodeState *state = m_statesByIndex[sink_index];
      if (state) {
        sink_counts[state->name] =
            state->execution_count.load(std::memory_order_relaxed);
      }
    }
  }

  auto status =
      m_schedulerStrategy->checkCompletion(active_tasks, 0, sink_counts);

  if (status.is_complete) {
    LOG_TRACE_S << "ExecutionEngine: Execution complete - " << status.reason;

    // Fire-and-forget executions have no synchronous waiter to restore the
    // engine state, so a completed batch returns to IDLE here; the CAS
    // leaves ERROR/STOPPED outcomes untouched
    EngineState expected = EngineState::RUNNING;
    m_engineState.compare_exchange_strong(expected, EngineState::IDLE,
                                          std::memory_order_acq_rel);

    PortDataMap results;
    {
      std::lock_guard<std::mutex> lock(m_resultsMutex);
      results = m_accumulatedResults;
    }
    // Guarded invocation: a throwing user callback must not skip the
    // notifyCompletionWaiters() below, or waitForCompletion() hangs.
    invokeResultCallback(results);
  }

  notifyCompletionWaiters();
}

Result<void> ExecutionEngine::Impl::waitForCompletion() {
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
    m_currentContext.store(nullptr);
    return Result<void>::err(ErrorCode::ExecutionStopped,
                             "Execution was stopped externally");
  } else if (active_tasks == 0 && (current_state == EngineState::RUNNING ||
                                   current_state == EngineState::IDLE)) {
    // IDLE means checkCompletionAndNotify already restored the state
    m_engineState.store(EngineState::IDLE, std::memory_order_release);
    LOG_INFO_S << "ExecutionEngine: Execution completed successfully.";
    m_currentContext.store(nullptr);
    return Result<void>::ok();
  } else if (current_state != EngineState::ERROR &&
             current_state != EngineState::STOPPED) {
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
    LOG_ERROR_S << "ExecutionEngine: Execution finished abnormally.";
    m_currentContext.store(nullptr);
    return Result<void>::err(ErrorCode::ExecutionFailed,
                             "Execution finished abnormally");
  }

  m_currentContext.store(nullptr);
  // Already in ERROR or STOPPED state
  return Result<void>::err(ErrorCode::ExecutionFailed,
                           "Execution ended in " +
                               stateToString(current_state) + " state");
}

void ExecutionEngine::Impl::resetInternalState() {
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_activeTasks.store(0, std::memory_order_relaxed);
  m_nextFrameId.store(1, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lock(m_streamStampMutex);
    m_nextStreamFrameId.clear();
  }

  {
    std::lock_guard<std::mutex> lock(m_resultsMutex);
    m_accumulatedResults.clear();
  }

  for (auto &[node, state] : m_nodeStates) {
    if (state) {
      state->exec_state.store(NodeExecutionState::WAITING,
                              std::memory_order_relaxed);
      state->execution_count.store(0, std::memory_order_relaxed);
      state->join_wait_since_ticks.store(0, std::memory_order_relaxed);
      state->degrade_pending.store(false, std::memory_order_relaxed);
      state->stats.reset();

      for (auto &[port, queue] : state->lock_free_queues) {
        if (queue)
          queue->clear();
      }
    }
  }
}

// -------------------------------------------------------------------------
// Impl: Queue Operations
// -------------------------------------------------------------------------

bool ExecutionEngine::Impl::pushToQueue(const NodePtr &node,
                                        const std::string &port_name,
                                        PortDataPtr data) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return false;
  }

  auto &state = *state_it->second;

  auto queue_it = state.lock_free_queues.find(port_name);
  if (queue_it != state.lock_free_queues.end() && queue_it->second) {
    // Copy the identity header before the queue takes ownership: once
    // pushed, a consumer may pop and release the packet concurrently.
    PortData trace_header;
    const bool tracing = m_traceSink != nullptr;
    if (tracing && data) {
      trace_header.id = data->id;
      trace_header.stream_id = data->stream_id;
    }
    if (queue_it->second->push(std::move(data))) {
      if (tracing) {
        emitTrace(TracePhase::Enqueue, state.name, port_name, &trace_header,
                  std::chrono::steady_clock::now(),
                  std::chrono::microseconds{0});
      }
      return true;
    }
    recordQueueRejection(state.name, port_name);
    return false;
  }

  LOG_WARNING_S << "ExecutionEngine: No queue found for " << state.name << ":"
                << port_name;
  return false;
}

void ExecutionEngine::Impl::stampIncomingFrame(const PortDataPtr &data) {
  if (!data || data->hasFrameId()) {
    return;
  }
  // Controlled mutable window: a packet at the injection boundary was
  // just created by the producer and is not yet visible to any consumer,
  // so stamping identity here cannot race with readers.
  auto &packet = const_cast<PortData &>(*data);
  if (m_config.alignment_policy == AlignmentPolicy::StreamFrameId) {
    // Ids must be monotonic within a stream: a shared global counter
    // would leave gaps in every stream and (stream, frame) pairs would
    // never repeat across ports.
    std::lock_guard<std::mutex> lock(m_streamStampMutex);
    packet.id = ++m_nextStreamFrameId[packet.stream_id];
  } else {
    packet.id = m_nextFrameId.fetch_add(1, std::memory_order_relaxed);
  }
  if (packet.timestamp == Timestamp{}) {
    packet.timestamp = std::chrono::steady_clock::now();
  }
}

void ExecutionEngine::Impl::inheritFrameIdentity(const PortDataMap &inputs,
                                                 PortDataMap &outputs) {
  if (inputs.empty()) {
    return;
  }

  // Use the first input's identity as the frame of record. Multi-input
  // joins with diverging frame ids are the sync subsystem's concern
  // (Phase 4); inheriting a deterministic representative keeps identity
  // flowing through linear segments.
  const PortDataPtr *primary = nullptr;
  for (const auto &[port, packet] : inputs) {
    if (packet && packet->hasFrameId()) {
      primary = &packet;
      break;
    }
  }
  if (!primary) {
    return;
  }

  for (auto &[port, packet] : outputs) {
    if (packet && !packet->hasFrameId()) {
      // A packet without an id is by construction fresh from this node
      // (forwarded packets already carry identity), so it has no other
      // readers yet and the mutable window is safe.
      auto &fresh = const_cast<PortData &>(*packet);
      fresh.id = (*primary)->id;
      fresh.stream_id = (*primary)->stream_id;
      if (fresh.timestamp == Timestamp{}) {
        fresh.timestamp = (*primary)->timestamp;
      }
    }
  }
}

void ExecutionEngine::Impl::recordQueueRejection(const std::string &node_name,
                                                 const std::string &port_name) {
  m_statistics.queue_full_events.fetch_add(1, std::memory_order_relaxed);
  m_statistics.total_dropped_frames.fetch_add(1, std::memory_order_relaxed);

  if (m_config.enable_drop_logging) {
    LOG_WARNING_S << "ExecutionEngine: Frame rejected at " << node_name << ":"
                  << port_name << " - DropTail queue full";
  }

  invokeDropCallback(node_name, frame_constants::k_invalid_frame_id,
                     "DropTail queue full");
}

std::size_t
ExecutionEngine::Impl::getQueueSize(const NodePtr &node,
                                    const std::string &port_name) const {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return 0;
  }

  auto &state = *state_it->second;

  auto queue_it = state.lock_free_queues.find(port_name);
  if (queue_it != state.lock_free_queues.end() && queue_it->second) {
    return queue_it->second->size();
  }

  return 0;
}

// -------------------------------------------------------------------------
// Impl: Utility
// -------------------------------------------------------------------------

void ExecutionEngine::Impl::collectResults(const NodePtr &node,
                                           const PortDataMap &outputs) {
  std::lock_guard<std::mutex> lock(m_resultsMutex);
  for (const auto &[port_name, data] : outputs) {
    std::string result_key = node->getName() + ":" + port_name;
    m_accumulatedResults[result_key] = data;
    LOG_TRACE_S << "ExecutionEngine: Collected result: " << result_key;
  }
}

QueueConfig
ExecutionEngine::Impl::getNodeQueueConfig(const std::string &node_name) const {
  auto it = m_nodeQueueConfigs.find(node_name);
  if (it != m_nodeQueueConfigs.end()) {
    return it->second;
  }

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

Result<PushStatus>
ExecutionEngine::Impl::routeToDownstream(const NodePtr &source_node,
                                         const std::string &output_port,
                                         const PortDataPtr &data) {

  const auto src_index = m_compiledGraph->indexOfPtr(source_node.get());
  if (src_index == CompiledGraph::k_invalid_index) {
    return Result<PushStatus>::err(Error::nodeNotFound(source_node->getName()));
  }

  std::size_t routed_count = 0;
  std::size_t rejected_count = 0;
  std::size_t total_queue_size = 0;

  for (const auto &edge : m_compiledGraph->outEdges(src_index)) {
    if (edge.source_port != output_port) {
      continue;
    }

    const auto &dest_node = m_compiledGraph->node(edge.dest_node);
    const auto &dest_port = edge.dest_port;

    PortDataPtr data_copy = data;

    if (!pushToQueue(dest_node, dest_port, std::move(data_copy))) {
      rejected_count++;
      continue;
    }

    m_statistics.total_queue_pushes.fetch_add(1, std::memory_order_relaxed);
    m_statistics.total_input_frames.fetch_add(1, std::memory_order_relaxed);

    tryScheduleNode(dest_node);

    total_queue_size += getQueueSize(dest_node, dest_port);
    routed_count++;

    LOG_TRACE_S << "ExecutionEngine: Routed data from "
                << source_node->getName() << ":" << output_port << " -> "
                << dest_node->getName() << ":" << dest_port;
  }

  if (routed_count == 0) {
    if (rejected_count > 0) {
      return Result<PushStatus>::err(
          ErrorCode::QueueRejected,
          "All downstream queues rejected data for port: " + output_port,
          source_node->getName());
    }
    LOG_WARNING_S << "ExecutionEngine: No downstream connections for "
                  << source_node->getName() << ":" << output_port;
    return Result<PushStatus>::err(ErrorCode::NoDownstreamConnection,
                                   "No downstream connections for port: " +
                                       output_port,
                                   source_node->getName());
  }

  if (rejected_count > 0) {
    return PushStatus::dropped(
        "rejected on " + std::to_string(rejected_count) + " of " +
            std::to_string(routed_count + rejected_count) +
            " downstream branches",
        total_queue_size);
  }

  return PushStatus::enqueued(total_queue_size);
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
