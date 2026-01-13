/**
 * @file pipeline_impl.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Pipeline implementation
 * @version 0.3
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#include "pipeline_impl.hpp"
#include "ai_pipe/execution_engine_factory.hpp"
#include "ai_pipe/logger.hpp"
#include <stdexcept>

namespace ai_pipe {

// =============================================================================
// Pipeline::Impl Implementation
// =============================================================================

Pipeline::Impl::Impl() { LOG_INFO_S << "Pipeline::Impl: Constructed"; }

Pipeline::Impl::~Impl() {
  if (isRunning()) {
    try {
      cancel();
      wait();
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
  m_state.store(other.m_state.exchange(PipelineState::UNINITIALIZED));
  m_observers = std::move(other.m_observers);

  LOG_INFO_S << "Pipeline::Impl: Move constructed";
}

Pipeline::Impl &Pipeline::Impl::operator=(Impl &&other) noexcept {
  if (this == &other) {
    return *this;
  }

  if (isRunning()) {
    cancel();
    wait();
  }

  std::scoped_lock lock(m_stateMutex, other.m_stateMutex, m_observersMutex,
                        other.m_observersMutex);

  m_graph = std::move(other.m_graph);
  m_engine = std::move(other.m_engine);
  m_context = std::move(other.m_context);
  m_options = std::move(other.m_options);
  m_state.store(other.m_state.exchange(PipelineState::UNINITIALIZED));
  m_observers = std::move(other.m_observers);

  LOG_INFO_S << "Pipeline::Impl: Move assigned";
  return *this;
}

bool Pipeline::Impl::initialize(Graph &&graph,
                                std::shared_ptr<PipelineContext> context,
                                const PipelineOptions &options) {
  LOG_INFO_S << "Pipeline::Impl: Initializing with engine="
             << options.engine_type
             << ", workers=" << static_cast<int>(options.num_workers);

  std::lock_guard<std::mutex> lock(m_stateMutex);

  // Create graph
  m_graph = std::make_unique<Graph>(std::move(graph));

  // Validate graph
  if (m_graph->hasCycle()) {
    LOG_ERROR_S << "Pipeline::Impl: Graph contains cycle";
    m_state.store(PipelineState::ERROR, std::memory_order_release);
    return false;
  }

  // Create execution engine
  m_engine = createExecutionEngine(options.engine_type);
  if (!m_engine) {
    LOG_ERROR_S << "Pipeline::Impl: Failed to create engine: "
                << options.engine_type;
    m_state.store(PipelineState::ERROR, std::memory_order_release);
    return false;
  }

  // Initialize engine
  if (!m_engine->initialize(m_graph.get(), options.num_workers)) {
    LOG_ERROR_S << "Pipeline::Impl: Engine initialization failed";
    m_state.store(PipelineState::ERROR, std::memory_order_release);
    return false;
  }

  // Store context and options
  m_context =
      context ? std::move(context) : std::make_shared<PipelineContext>();
  m_options = options;

  // Setup callbacks
  setupEngineCallbacks();

  m_state.store(PipelineState::IDLE, std::memory_order_release);
  LOG_INFO_S << "Pipeline::Impl: Initialized successfully";
  return true;
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
}

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
  // Note: These callbacks may be called from worker threads
  auto promise_ptr = promise; // Capture shared_ptr

  m_engine->setPipelineResultCallback(
      [this, promise_ptr, start_time](const PortDataMap &results) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start_time);

        transitionTo(PipelineState::IDLE);

        // Store results for observers
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

void Pipeline::Impl::cancel() {
  if (!isRunning()) {
    return;
  }

  LOG_INFO_S << "Pipeline::Impl: Cancelling execution";
  m_engine->stopExecutionAsync();
  transitionTo(PipelineState::STOPPING);
}

void Pipeline::Impl::wait() {
  if (m_state.load(std::memory_order_acquire) == PipelineState::UNINITIALIZED) {
    return;
  }

  if (m_engine) {
    m_engine->stopExecutionSync();
  }

  auto current = m_state.load(std::memory_order_acquire);
  if (current == PipelineState::RUNNING || current == PipelineState::STOPPING) {
    transitionTo(PipelineState::IDLE);
  }
}

void Pipeline::Impl::reset() {
  LOG_INFO_S << "Pipeline::Impl: Resetting";

  if (isRunning()) {
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

  if (m_state.load(std::memory_order_acquire) != PipelineState::UNINITIALIZED) {
    transitionTo(PipelineState::IDLE);
  }

  LOG_INFO_S << "Pipeline::Impl: Reset complete";
}

bool Pipeline::Impl::isReady() const {
  auto current = m_state.load(std::memory_order_acquire);
  return current == PipelineState::IDLE;
}

bool Pipeline::Impl::isRunning() const {
  auto current = m_state.load(std::memory_order_acquire);
  return current == PipelineState::RUNNING ||
         current == PipelineState::STOPPING;
}

bool Pipeline::Impl::hasError() const {
  return m_state.load(std::memory_order_acquire) == PipelineState::ERROR;
}

PipelineState Pipeline::Impl::state() const {
  return m_state.load(std::memory_order_acquire);
}

EngineState Pipeline::Impl::engineState() const {
  return m_engine ? m_engine->getState() : EngineState::IDLE;
}

std::unordered_map<std::string, NodeExecutionState>
Pipeline::Impl::nodeStates() const {
  return m_engine ? m_engine->getNodeStates()
                  : std::unordered_map<std::string, NodeExecutionState>{};
}

const Graph &Pipeline::Impl::graph() const {
  if (!m_graph) {
    throw std::runtime_error("Pipeline: Graph not initialized");
  }
  return *m_graph;
}

PipelineContext &Pipeline::Impl::context() {
  if (!m_context) {
    throw std::runtime_error("Pipeline: Context not initialized");
  }
  return *m_context;
}

const PipelineContext &Pipeline::Impl::context() const {
  if (!m_context) {
    throw std::runtime_error("Pipeline: Context not initialized");
  }
  return *m_context;
}

void Pipeline::Impl::addObserver(std::shared_ptr<IPipelineObserver> observer) {
  if (!observer)
    return;

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

bool Pipeline::Impl::validateState(const char *operation) const {
  auto current = m_state.load(std::memory_order_acquire);

  if (current == PipelineState::UNINITIALIZED) {
    LOG_ERROR_S << "Pipeline: Cannot " << operation << " - not initialized";
    return false;
  }

  if (current == PipelineState::RUNNING) {
    LOG_ERROR_S << "Pipeline: Cannot " << operation << " - already running";
    return false;
  }

  if (current == PipelineState::ERROR) {
    LOG_WARNING_S << "Pipeline: " << operation
                  << " called in ERROR state, attempting reset";
    // Allow retry after error - caller should reset if needed
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

void Pipeline::cancel() { m_impl->cancel(); }

void Pipeline::wait() { m_impl->wait(); }

void Pipeline::reset() { m_impl->reset(); }

bool Pipeline::isReady() const { return m_impl->isReady(); }

bool Pipeline::isRunning() const { return m_impl->isRunning(); }

bool Pipeline::hasError() const { return m_impl->hasError(); }

PipelineState Pipeline::state() const { return m_impl->state(); }

EngineState Pipeline::engineState() const { return m_impl->engineState(); }

std::unordered_map<std::string, NodeExecutionState>
Pipeline::nodeStates() const {
  return m_impl->nodeStates();
}

const Graph &Pipeline::graph() const { return m_impl->graph(); }

PipelineContext &Pipeline::context() { return m_impl->context(); }

const PipelineContext &Pipeline::context() const { return m_impl->context(); }

void Pipeline::addObserver(std::shared_ptr<IPipelineObserver> observer) {
  m_impl->addObserver(std::move(observer));
}

void Pipeline::removeObserver(
    const std::shared_ptr<IPipelineObserver> &observer) {
  m_impl->removeObserver(observer);
}

bool Pipeline::initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          const PipelineOptions &options) {
  return m_impl->initialize(std::move(graph), std::move(context), options);
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
};

PipelineBuilder::PipelineBuilder()
    : m_state(std::make_unique<BuilderState>()) {}

PipelineBuilder::~PipelineBuilder() = default;

PipelineBuilder::PipelineBuilder(PipelineBuilder &&) noexcept = default;

PipelineBuilder &
PipelineBuilder::operator=(PipelineBuilder &&) noexcept = default;

PipelineBuilder &PipelineBuilder::withGraph(Graph graph) {
  m_state->graph = std::move(graph);
  return *this;
}

PipelineBuilder &
PipelineBuilder::withContext(std::shared_ptr<PipelineContext> context) {
  m_state->context = std::move(context);
  return *this;
}

PipelineBuilder &PipelineBuilder::withWorkers(std::uint8_t count) {
  m_state->options.num_workers = count;
  return *this;
}

PipelineBuilder &PipelineBuilder::withEngine(std::string engine_type) {
  m_state->options.engine_type = std::move(engine_type);
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

PipelineBuilder &
PipelineBuilder::withObserver(std::shared_ptr<IPipelineObserver> observer) {
  m_state->observers.push_back(std::move(observer));
  return *this;
}

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
                           m_state->options)) {
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