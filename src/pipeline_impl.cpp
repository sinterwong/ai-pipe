/**
 * @file pipeline_impl.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Pipeline implementation
 * @version 1.0
 * @date 2025-12-24
 *
 * @copyright Copyright (c) 2025
 */

#include "pipeline_impl.hpp"
#include "ai_pipe/logger.hpp"
#include <stdexcept>

namespace ai_pipe {

// =============================================================================
// Pipeline::Impl Implementation
// =============================================================================

Pipeline::Impl::Impl() { LOG_INFO_S << "Pipeline::Impl: Constructed"; }

Pipeline::Impl::~Impl() {
  if (isRunning() || isStreaming()) {
    try {
      if (isStreaming()) {
        stop(false);
      } else {
        cancel();
        wait();
      }
    } catch (const std::exception &e) {
      LOG_ERROR_S << "Pipeline::Impl: Exception during shutdown: " << e.what();
    }
  }
  LOG_INFO_S << "Pipeline::Impl: Destructed";
}

Pipeline::Impl::Impl(Impl &&other) noexcept {
  std::scoped_lock lock(m_stateMutex, other.m_stateMutex, m_observersMutex,
                        other.m_observersMutex);

  m_graph = std::move(other.m_graph);
  m_engine = std::move(other.m_engine);
  m_context = std::move(other.m_context);
  m_options = std::move(other.m_options);
  m_customScheduler = std::move(other.m_customScheduler);
  m_customSync = std::move(other.m_customSync);
  m_state.store(other.m_state.exchange(PipelineState::UNINITIALIZED));
  m_observers = std::move(other.m_observers);

  LOG_INFO_S << "Pipeline::Impl: Move constructed";
}

Pipeline::Impl &Pipeline::Impl::operator=(Impl &&other) noexcept {
  if (this == &other) {
    return *this;
  }

  if (isRunning() || isStreaming()) {
    if (isStreaming()) {
      stop(false);
    } else {
      cancel();
      wait();
    }
  }

  std::scoped_lock lock(m_stateMutex, other.m_stateMutex, m_observersMutex,
                        other.m_observersMutex);

  m_graph = std::move(other.m_graph);
  m_engine = std::move(other.m_engine);
  m_context = std::move(other.m_context);
  m_options = std::move(other.m_options);
  m_customScheduler = std::move(other.m_customScheduler);
  m_customSync = std::move(other.m_customSync);
  m_state.store(other.m_state.exchange(PipelineState::UNINITIALIZED));
  m_observers = std::move(other.m_observers);

  LOG_INFO_S << "Pipeline::Impl: Move assigned";
  return *this;
}

bool Pipeline::Impl::initialize(Graph &&graph,
                                std::shared_ptr<PipelineContext> context,
                                const PipelineOptions &options,
                                std::unique_ptr<ISchedulerStrategy> scheduler,
                                std::unique_ptr<ISyncStrategy> sync) {
  LOG_INFO_S << "Pipeline::Impl: Initializing with mode="
             << executionModeToString(options.mode)
             << ", workers=" << static_cast<int>(options.num_workers);

  std::lock_guard<std::mutex> lock(m_stateMutex);

  // Store options
  m_options = options;

  // Create graph
  m_graph = std::make_unique<Graph>(std::move(graph));

  // Validate graph
  if (m_graph->hasCycle()) {
    LOG_ERROR_S << "Pipeline::Impl: Graph contains cycle";
    m_state.store(PipelineState::ERROR, std::memory_order_release);
    return false;
  }

  // Store custom strategies if provided
  m_customScheduler = std::move(scheduler);
  m_customSync = std::move(sync);

  // Create execution engine with configuration
  auto engine_config = buildEngineConfig();
  m_engine = ExecutionEngine::create(engine_config);

  if (!m_engine) {
    LOG_ERROR_S << "Pipeline::Impl: Failed to create execution engine";
    m_state.store(PipelineState::ERROR, std::memory_order_release);
    return false;
  }

  // Set custom strategies if provided
  if (m_customScheduler) {
    m_engine->setSchedulerStrategy(m_customScheduler->clone());
  }
  if (m_customSync) {
    m_engine->setSyncStrategy(m_customSync->clone());
  }

  // Initialize engine
  if (!m_engine->initialize(m_graph.get(), options.num_workers)) {
    LOG_ERROR_S << "Pipeline::Impl: Engine initialization failed";
    m_state.store(PipelineState::ERROR, std::memory_order_release);
    return false;
  }

  // Store context
  m_context =
      context ? std::move(context) : std::make_shared<PipelineContext>();

  // Setup callbacks
  setupEngineCallbacks();

  m_state.store(PipelineState::IDLE, std::memory_order_release);
  LOG_INFO_S << "Pipeline::Impl: Initialized successfully";
  return true;
}

EngineConfig Pipeline::Impl::buildEngineConfig() const {
  EngineConfig config;
  config.mode = m_options.mode;
  config.num_workers = m_options.num_workers;
  config.default_queue_capacity = m_options.queue_capacity;
  config.default_drop_strategy = m_options.drop_strategy;
  config.enable_sync_coordination = m_options.enable_sync_coordination;
  config.enable_statistics = m_options.enable_statistics;
  return config;
}

void Pipeline::Impl::setupEngineCallbacks() {
  m_engine->setPipelineResultCallback([this](const PortDataMap &results) {
    {
      std::lock_guard<std::mutex> lock(m_resultsMutex);
      m_lastResults = results;
      m_lastError.clear();
      m_lastErrorNode.clear();
    }
    notifyExecutionCompleted(results);
  });

  m_engine->setPipelineErrorCallback(
      [this](const std::string &error, const std::string &node_name) {
        {
          std::lock_guard<std::mutex> lock(m_resultsMutex);
          m_lastError = error;
          m_lastErrorNode = node_name;
        }
        m_state.store(PipelineState::ERROR, std::memory_order_release);
        notifyExecutionFailed(error, node_name);
      });

  m_engine->setDropCallback([this](const std::string &node_name,
                                   std::uint64_t frame_id,
                                   const std::string &reason) {
    notifyFrameDropped(node_name, frame_id, reason);
  });
}

// -------------------------------------------------------------------------
// Batch Execution
// -------------------------------------------------------------------------

ExecutionResult
Pipeline::Impl::run(const PortDataMap &inputs,
                    std::optional<std::chrono::milliseconds> timeout) {
  if (!validateState("run")) {
    return createErrorResult("Pipeline not ready for execution");
  }

  transitionTo(PipelineState::RUNNING);
  m_executionStart = std::chrono::steady_clock::now();
  notifyExecutionStarted();

  // Execute synchronously
  bool success = m_engine->execute(inputs, true, m_context);

  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - m_executionStart);

  // Handle timeout (if specified and exceeded)
  if (timeout.has_value() && elapsed > timeout.value()) {
    cancel();
    transitionTo(PipelineState::ERROR);
    return createErrorResult("Execution timed out");
  }

  if (success) {
    transitionTo(PipelineState::IDLE);
    std::lock_guard<std::mutex> lock(m_resultsMutex);
    return createSuccessResult(m_lastResults, elapsed);
  } else {
    std::lock_guard<std::mutex> lock(m_resultsMutex);
    return createErrorResult(m_lastError.empty() ? "Execution failed"
                                                 : m_lastError);
  }
}

std::future<ExecutionResult>
Pipeline::Impl::runAsync(const PortDataMap &inputs) {
  auto promise = std::make_shared<std::promise<ExecutionResult>>();
  std::future<ExecutionResult> future = promise->get_future();

  if (!validateState("runAsync")) {
    promise->set_value(createErrorResult("Pipeline not ready for execution"));
    return future;
  }

  transitionTo(PipelineState::RUNNING);
  auto start_time = std::chrono::steady_clock::now();
  m_executionStart = start_time;
  notifyExecutionStarted();

  // Setup completion handlers that fulfill the promise
  auto promise_ptr = promise;

  m_engine->setPipelineResultCallback(
      [this, promise_ptr, start_time](const PortDataMap &results) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);

        transitionTo(PipelineState::IDLE);

        {
          std::lock_guard<std::mutex> lock(m_resultsMutex);
          m_lastResults = results;
        }

        promise_ptr->set_value(createSuccessResult(results, elapsed));
        notifyExecutionCompleted(results);
      });

  m_engine->setPipelineErrorCallback(
      [this, promise_ptr, start_time](const std::string &error,
                                      const std::string &node_name) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);

        transitionTo(PipelineState::ERROR);

        ExecutionResult result{
            .success = false,
            .outputs = {},
            .error_message = error + " (node: " + node_name + ")",
            .elapsed = elapsed,
        };
        promise_ptr->set_value(result);
        notifyExecutionFailed(error, node_name);
      });

  // Start async execution
  bool started = m_engine->execute(inputs, false, m_context);

  if (!started) {
    transitionTo(PipelineState::ERROR);
    promise->set_value(createErrorResult("Failed to start execution"));
  }

  return future;
}

bool Pipeline::Impl::submit(const PortDataMap &inputs) {
  if (!validateState("submit")) {
    return false;
  }

  transitionTo(PipelineState::RUNNING);
  m_executionStart = std::chrono::steady_clock::now();
  notifyExecutionStarted();

  bool success = m_engine->execute(inputs, false, m_context);

  if (!success) {
    transitionTo(PipelineState::ERROR);
    LOG_ERROR_S << "Pipeline::Impl: Submit failed to start execution";
  }

  return success;
}

// -------------------------------------------------------------------------
// Streaming Interface
// -------------------------------------------------------------------------

bool Pipeline::Impl::start(std::shared_ptr<PipelineContext> context) {
  if (!validateState("start streaming")) {
    return false;
  }

  if (m_options.mode != ExecutionMode::STREAM &&
      m_options.mode != ExecutionMode::HYBRID) {
    LOG_ERROR_S << "Pipeline::Impl: Cannot start streaming in batch mode";
    return false;
  }

  if (context) {
    m_context = context;
  }

  transitionTo(PipelineState::RUNNING);
  m_executionStart = std::chrono::steady_clock::now();

  if (!m_engine->startStreaming(m_context)) {
    transitionTo(PipelineState::ERROR);
    LOG_ERROR_S << "Pipeline::Impl: Failed to start streaming";
    return false;
  }

  LOG_INFO_S << "Pipeline::Impl: Streaming started";
  return true;
}

void Pipeline::Impl::stop(bool wait_for_drain) {
  if (!m_engine || !m_engine->isStreaming()) {
    return;
  }

  m_engine->stopStreaming(wait_for_drain);
  transitionTo(PipelineState::IDLE);
  LOG_INFO_S << "Pipeline::Impl: Streaming stopped";
}

QueuePushResult Pipeline::Impl::pushInput(const std::string &source_node,
                                          const std::string &port_name,
                                          PortDataPtr data) {
  if (!m_engine) {
    return QueuePushResult::rejected("Engine not initialized", 0);
  }

  return m_engine->pushInput(source_node, port_name, std::move(data));
}

bool Pipeline::Impl::isStreaming() const {
  return m_engine && m_engine->isStreaming();
}

std::size_t Pipeline::Impl::queueDepth(const std::string &node_name) const {
  if (!m_engine)
    return 0;
  return m_engine->queueDepth(node_name);
}

bool Pipeline::Impl::hasQueueCapacity(const std::string &node_name) const {
  if (!m_engine)
    return false;
  return m_engine->hasQueueCapacity(node_name);
}

bool Pipeline::Impl::waitForDrain(std::size_t max_depth,
                                  std::chrono::milliseconds timeout) {
  if (!m_engine)
    return true;
  return m_engine->waitForDrain(max_depth, timeout);
}

// -------------------------------------------------------------------------
// Control
// -------------------------------------------------------------------------

void Pipeline::Impl::cancel() {
  if (!isRunning() && !isStreaming()) {
    return;
  }

  LOG_INFO_S << "Pipeline::Impl: Cancelling execution";

  if (m_engine) {
    if (isStreaming()) {
      m_engine->stopStreaming(false);
    } else {
      m_engine->stopExecutionAsync();
    }
  }

  transitionTo(PipelineState::STOPPING);
}

void Pipeline::Impl::wait() {
  if (!m_engine) {
    return;
  }

  auto current_state = m_state.load(std::memory_order_acquire);
  if (current_state == PipelineState::RUNNING ||
      current_state == PipelineState::STOPPING) {
    // Wait for engine to complete
    while (m_engine->getState() == EngineState::RUNNING) {
      std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
  }
}

void Pipeline::Impl::reset() {
  LOG_INFO_S << "Pipeline::Impl: Resetting";

  if (isStreaming()) {
    stop(false);
  } else if (isRunning()) {
    cancel();
    wait();
  }

  if (m_engine) {
    m_engine->reset();
  }

  {
    std::lock_guard<std::mutex> lock(m_resultsMutex);
    m_lastResults.clear();
    m_lastError.clear();
    m_lastErrorNode.clear();
  }

  transitionTo(PipelineState::IDLE);
  LOG_INFO_S << "Pipeline::Impl: Reset complete";
}

// -------------------------------------------------------------------------
// Status
// -------------------------------------------------------------------------

bool Pipeline::Impl::isReady() const {
  auto current_state = m_state.load(std::memory_order_acquire);
  return current_state == PipelineState::IDLE && m_engine && m_graph;
}

bool Pipeline::Impl::isRunning() const {
  auto current_state = m_state.load(std::memory_order_acquire);
  return current_state == PipelineState::RUNNING ||
         current_state == PipelineState::STOPPING;
}

bool Pipeline::Impl::hasError() const {
  return m_state.load(std::memory_order_acquire) == PipelineState::ERROR;
}

PipelineState Pipeline::Impl::state() const {
  return m_state.load(std::memory_order_acquire);
}

EngineState Pipeline::Impl::engineState() const {
  if (!m_engine) {
    return EngineState::ERROR;
  }
  return m_engine->getState();
}

std::unordered_map<std::string, NodeExecutionState>
Pipeline::Impl::nodeStates() const {
  if (!m_engine) {
    return {};
  }
  return m_engine->getNodeStates();
}

ExecutionMode Pipeline::Impl::mode() const { return m_options.mode; }

EngineStatisticsSnapshot Pipeline::Impl::statistics() const {
  if (!m_engine) {
    return {};
  }
  return m_engine->statistics();
}

// -------------------------------------------------------------------------
// Accessors
// -------------------------------------------------------------------------

const Graph &Pipeline::Impl::graph() const {
  if (!m_graph) {
    throw std::runtime_error("Pipeline: Graph not initialized");
  }
  return *m_graph;
}

PipelineContext &Pipeline::Impl::context() {
  if (!m_context) {
    m_context = std::make_shared<PipelineContext>();
  }
  return *m_context;
}

const PipelineContext &Pipeline::Impl::context() const {
  if (!m_context) {
    throw std::runtime_error("Pipeline: Context not initialized");
  }
  return *m_context;
}

std::string Pipeline::Impl::info() const {
  if (!m_engine) {
    return "Pipeline: Not initialized";
  }
  return m_engine->info();
}

// -------------------------------------------------------------------------
// Observer Management
// -------------------------------------------------------------------------

void Pipeline::Impl::addObserver(std::shared_ptr<IPipelineObserver> observer) {
  std::lock_guard<std::mutex> lock(m_observersMutex);
  m_observers.push_back(std::move(observer));
}

void Pipeline::Impl::removeObserver(
    const std::shared_ptr<IPipelineObserver> &observer) {
  std::lock_guard<std::mutex> lock(m_observersMutex);
  m_observers.erase(
      std::remove(m_observers.begin(), m_observers.end(), observer),
      m_observers.end());
}

// -------------------------------------------------------------------------
// Internal Helpers
// -------------------------------------------------------------------------

bool Pipeline::Impl::validateState(const char *operation) const {
  auto current_state = m_state.load(std::memory_order_acquire);

  if (current_state == PipelineState::UNINITIALIZED) {
    LOG_ERROR_S << "Pipeline: Cannot " << operation << " - not initialized";
    return false;
  }

  if (current_state == PipelineState::ERROR) {
    LOG_ERROR_S << "Pipeline: Cannot " << operation
                << " - in error state. Call reset() first.";
    return false;
  }

  if (current_state == PipelineState::RUNNING ||
      current_state == PipelineState::STOPPING) {
    LOG_ERROR_S << "Pipeline: Cannot " << operation << " - already running";
    return false;
  }

  if (!m_engine || !m_graph) {
    LOG_ERROR_S << "Pipeline: Cannot " << operation
                << " - missing engine or graph";
    return false;
  }

  return true;
}

void Pipeline::Impl::transitionTo(PipelineState new_state) {
  auto old_state = m_state.exchange(new_state, std::memory_order_acq_rel);
  if (old_state != new_state) {
    LOG_TRACE_S << "Pipeline: State " << static_cast<int>(old_state) << " -> "
                << static_cast<int>(new_state);
  }
}

void Pipeline::Impl::notifyExecutionStarted() {
  std::lock_guard<std::mutex> lock(m_observersMutex);
  for (auto &observer : m_observers) {
    if (observer) {
      try {
        observer->onExecutionStarted();
      } catch (const std::exception &e) {
        LOG_ERROR_S << "Pipeline: Observer exception: " << e.what();
      }
    }
  }
}

void Pipeline::Impl::notifyExecutionCompleted(const PortDataMap &results) {
  std::lock_guard<std::mutex> lock(m_observersMutex);
  for (auto &observer : m_observers) {
    if (observer) {
      try {
        observer->onExecutionCompleted(results);
      } catch (const std::exception &e) {
        LOG_ERROR_S << "Pipeline: Observer exception: " << e.what();
      }
    }
  }
}

void Pipeline::Impl::notifyExecutionFailed(const std::string &error,
                                           const std::string &node_name) {
  std::lock_guard<std::mutex> lock(m_observersMutex);
  for (auto &observer : m_observers) {
    if (observer) {
      try {
        observer->onExecutionFailed(error, node_name);
      } catch (const std::exception &e) {
        LOG_ERROR_S << "Pipeline: Observer exception: " << e.what();
      }
    }
  }
}

void Pipeline::Impl::notifyFrameDropped(const std::string &node_name,
                                        std::uint64_t frame_id,
                                        const std::string &reason) {
  std::lock_guard<std::mutex> lock(m_observersMutex);
  for (auto &observer : m_observers) {
    if (observer) {
      try {
        observer->onFrameDropped(node_name, frame_id, reason);
      } catch (const std::exception &e) {
        LOG_ERROR_S << "Pipeline: Observer exception: " << e.what();
      }
    }
  }
}

ExecutionResult
Pipeline::Impl::createSuccessResult(const PortDataMap &outputs,
                                    std::chrono::milliseconds elapsed) const {
  return ExecutionResult{
      .success = true,
      .outputs = outputs,
      .error_message = {},
      .elapsed = elapsed,
  };
}

ExecutionResult
Pipeline::Impl::createErrorResult(const std::string &message) const {
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - m_executionStart);

  return ExecutionResult{
      .success = false,
      .outputs = {},
      .error_message = message,
      .elapsed = elapsed,
  };
}

// =============================================================================
// Pipeline Public Interface Implementation
// =============================================================================

Pipeline::Pipeline() : m_impl(std::make_unique<Impl>()) {}

Pipeline::~Pipeline() = default;

Pipeline::Pipeline(Pipeline &&other) noexcept = default;

Pipeline &Pipeline::operator=(Pipeline &&other) noexcept = default;

PipelineBuilder Pipeline::create() { return PipelineBuilder{}; }

// Batch execution
ExecutionResult Pipeline::run(const PortDataMap &inputs) {
  return m_impl->run(inputs, std::nullopt);
}

ExecutionResult Pipeline::run(const PortDataMap &inputs,
                              std::chrono::milliseconds timeout) {
  return m_impl->run(inputs, timeout);
}

std::future<ExecutionResult> Pipeline::runAsync(const PortDataMap &inputs) {
  return m_impl->runAsync(inputs);
}

bool Pipeline::submit(const PortDataMap &inputs) {
  return m_impl->submit(inputs);
}

// Streaming interface
bool Pipeline::start() { return m_impl->start(nullptr); }

bool Pipeline::start(std::shared_ptr<PipelineContext> context) {
  return m_impl->start(std::move(context));
}

void Pipeline::stop(bool wait_for_drain) { m_impl->stop(wait_for_drain); }

QueuePushResult Pipeline::pushInput(const std::string &source_node,
                                    PortDataPtr data) {
  return m_impl->pushInput(source_node, "", std::move(data));
}

QueuePushResult Pipeline::pushInput(const std::string &source_node,
                                    const std::string &port_name,
                                    PortDataPtr data) {
  return m_impl->pushInput(source_node, port_name, std::move(data));
}

bool Pipeline::isStreaming() const { return m_impl->isStreaming(); }

std::size_t Pipeline::queueDepth(const std::string &node_name) const {
  return m_impl->queueDepth(node_name);
}

bool Pipeline::hasQueueCapacity(const std::string &node_name) const {
  return m_impl->hasQueueCapacity(node_name);
}

bool Pipeline::waitForDrain(std::size_t max_depth,
                            std::chrono::milliseconds timeout) {
  return m_impl->waitForDrain(max_depth, timeout);
}

// Control
void Pipeline::cancel() { m_impl->cancel(); }

void Pipeline::wait() { m_impl->wait(); }

void Pipeline::reset() { m_impl->reset(); }

// Status
bool Pipeline::isReady() const { return m_impl->isReady(); }

bool Pipeline::isRunning() const { return m_impl->isRunning(); }

bool Pipeline::hasError() const { return m_impl->hasError(); }

PipelineState Pipeline::state() const { return m_impl->state(); }

EngineState Pipeline::engineState() const { return m_impl->engineState(); }

std::unordered_map<std::string, NodeExecutionState>
Pipeline::nodeStates() const {
  return m_impl->nodeStates();
}

ExecutionMode Pipeline::mode() const { return m_impl->mode(); }

EngineStatisticsSnapshot Pipeline::statistics() const {
  return m_impl->statistics();
}

// Accessors
const Graph &Pipeline::graph() const { return m_impl->graph(); }

PipelineContext &Pipeline::context() { return m_impl->context(); }

const PipelineContext &Pipeline::context() const { return m_impl->context(); }

std::string Pipeline::info() const { return m_impl->info(); }

// Observer management
void Pipeline::addObserver(std::shared_ptr<IPipelineObserver> observer) {
  m_impl->addObserver(std::move(observer));
}

void Pipeline::removeObserver(
    const std::shared_ptr<IPipelineObserver> &observer) {
  m_impl->removeObserver(observer);
}

bool Pipeline::initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          const PipelineOptions &options,
                          std::unique_ptr<ISchedulerStrategy> scheduler,
                          std::unique_ptr<ISyncStrategy> sync) {
  return m_impl->initialize(std::move(graph), std::move(context), options,
                            std::move(scheduler), std::move(sync));
}

// =============================================================================
// PipelineBuilder Implementation
// =============================================================================

struct PipelineBuilder::BuilderState {
  std::optional<Graph> graph;
  std::shared_ptr<PipelineContext> context;
  PipelineOptions options;
  std::vector<std::shared_ptr<IPipelineObserver>> observers;
  std::shared_ptr<CallbackObserver> callback_observer;
  std::unique_ptr<ISchedulerStrategy> scheduler_strategy;
  std::unique_ptr<ISyncStrategy> sync_strategy;
};

PipelineBuilder::PipelineBuilder()
    : m_state(std::make_unique<BuilderState>()) {}

PipelineBuilder::~PipelineBuilder() = default;

PipelineBuilder::PipelineBuilder(PipelineBuilder &&) noexcept = default;

PipelineBuilder &
PipelineBuilder::operator=(PipelineBuilder &&) noexcept = default;

// Core configuration
PipelineBuilder &PipelineBuilder::withGraph(Graph graph) {
  m_state->graph = std::move(graph);
  return *this;
}

PipelineBuilder &
PipelineBuilder::withContext(std::shared_ptr<PipelineContext> context) {
  m_state->context = std::move(context);
  return *this;
}

PipelineBuilder &PipelineBuilder::withMode(ExecutionMode mode) {
  m_state->options.mode = mode;
  // Auto-enable sync coordination for stream mode
  if (mode == ExecutionMode::STREAM || mode == ExecutionMode::HYBRID) {
    m_state->options.enable_sync_coordination = true;
  }
  return *this;
}

PipelineBuilder &PipelineBuilder::withWorkers(std::uint8_t count) {
  m_state->options.num_workers = count;
  return *this;
}

PipelineBuilder &
PipelineBuilder::withTimeout(std::chrono::milliseconds timeout) {
  m_state->options.execution_timeout = timeout;
  return *this;
}

PipelineBuilder &PipelineBuilder::withOptions(PipelineOptions options) {
  m_state->options = std::move(options);
  return *this;
}

// Streaming configuration
PipelineBuilder &PipelineBuilder::withQueueCapacity(std::size_t capacity) {
  m_state->options.queue_capacity = capacity;
  return *this;
}

PipelineBuilder &PipelineBuilder::withDropStrategy(std::string strategy) {
  m_state->options.drop_strategy = std::move(strategy);
  return *this;
}

PipelineBuilder &PipelineBuilder::withSyncCoordination(bool enable) {
  m_state->options.enable_sync_coordination = enable;
  return *this;
}

// Strategy configuration
PipelineBuilder &PipelineBuilder::withSchedulerStrategy(
    std::unique_ptr<ISchedulerStrategy> strategy) {
  m_state->scheduler_strategy = std::move(strategy);
  return *this;
}

PipelineBuilder &
PipelineBuilder::withSyncStrategy(std::unique_ptr<ISyncStrategy> strategy) {
  m_state->sync_strategy = std::move(strategy);
  return *this;
}

// Callbacks
PipelineBuilder &
PipelineBuilder::onResult(std::function<void(const PortDataMap &)> callback) {
  if (!m_state->callback_observer) {
    m_state->callback_observer = std::make_shared<CallbackObserver>();
  }
  m_state->callback_observer->onResult(std::move(callback));
  return *this;
}

PipelineBuilder &PipelineBuilder::onError(
    std::function<void(const std::string &, const std::string &)> callback) {
  if (!m_state->callback_observer) {
    m_state->callback_observer = std::make_shared<CallbackObserver>();
  }
  m_state->callback_observer->onError(std::move(callback));
  return *this;
}

PipelineBuilder &PipelineBuilder::onDrop(
    std::function<void(const std::string &, std::uint64_t, const std::string &)>
        callback) {
  if (!m_state->callback_observer) {
    m_state->callback_observer = std::make_shared<CallbackObserver>();
  }
  m_state->callback_observer->onDrop(std::move(callback));
  return *this;
}

PipelineBuilder &
PipelineBuilder::withObserver(std::shared_ptr<IPipelineObserver> observer) {
  m_state->observers.push_back(std::move(observer));
  return *this;
}

// Build
Pipeline PipelineBuilder::build() {
  auto result = tryBuild();
  if (!result.has_value()) {
    throw std::runtime_error(
        "Pipeline: Failed to build - invalid configuration");
  }
  return std::move(result.value());
}

std::optional<Pipeline> PipelineBuilder::tryBuild() {
  if (!m_state->graph.has_value()) {
    LOG_ERROR_S << "PipelineBuilder: No graph provided";
    return std::nullopt;
  }

  Pipeline pipeline;

  if (!pipeline.initialize(std::move(m_state->graph.value()), m_state->context,
                           m_state->options,
                           std::move(m_state->scheduler_strategy),
                           std::move(m_state->sync_strategy))) {
    return std::nullopt;
  }

  // Add observers
  if (m_state->callback_observer) {
    pipeline.addObserver(m_state->callback_observer);
  }

  for (auto &observer : m_state->observers) {
    pipeline.addObserver(std::move(observer));
  }

  return pipeline;
}

} // namespace ai_pipe
