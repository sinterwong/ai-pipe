/**
 * @file pipeline_impl.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-04
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "pipeline_impl.hpp"
#include "ai_pipe/types.hpp"
#include "execution_engine_factory.hpp"
#include <logger.hpp>
#include <memory>

namespace ai_pipe {
Pipeline::Impl::Impl()
    : m_graph(nullptr), m_executionEngine(nullptr), m_context(nullptr),
      m_state(PipelineState::UNINITIALIZED) {
  LOG_INFOS << "Pipeline::Impl default constructed.";
}

Pipeline::Impl::~Impl() {
  // Ensure graceful shutdown
  if (m_state == PipelineState::RUNNING || m_state == PipelineState::STOPPING) {
    try {
      stop();
    } catch (const std::exception &e) {
      LOG_ERRORS << "Pipeline::Impl destructor: Exception during stop: "
                 << e.what();
    } catch (...) {
      LOG_ERRORS << "Pipeline::Impl destructor: Unknown exception during stop.";
    }
  }
  LOG_INFOS << "Pipeline::Impl destructed.";
}

Pipeline::Impl::Impl(Impl &&other) noexcept
    : m_graph(std::move(other.m_graph)),
      m_executionEngine(std::move(other.m_executionEngine)),
      m_context(std::move(other.m_context)), m_state(other.m_state.load()),
      m_onPipelineError(std::move(other.m_onPipelineError)),
      m_onPipelineResult(std::move(other.m_onPipelineResult)) {
  LOG_INFOS << "Pipeline::Impl move constructed.";
}

Pipeline::Impl &Pipeline::Impl::operator=(Impl &&other) noexcept {
  if (this != &other) {
    // Properly handle self-assignment if necessary, though for move it's less
    // common Ensure current pipeline is stopped before overwriting
    if (m_state == PipelineState::RUNNING ||
        m_state == PipelineState::STOPPING) {
      stop();
    }
    m_graph = std::move(other.m_graph);
    m_executionEngine = std::move(other.m_executionEngine);
    m_context = std::move(other.m_context);
    m_state.store(other.m_state.load());
    m_onPipelineError = std::move(other.m_onPipelineError);
    m_onPipelineResult = std::move(other.m_onPipelineResult);
  }
  LOG_INFOS << "Pipeline::Impl move assigned.";
  return *this;
}

bool Pipeline::Impl::initialize(Graph &&graph,
                                std::shared_ptr<PipelineContext> ctx,
                                const PipelineConfig &config) {
  LOG_INFOS << "Pipeline::Impl initializing with provided graph, engineType: "
            << config.engineType
            << ", numWorkers: " << (int)config.numWorkers;
  m_graph = std::make_unique<Graph>(std::move(graph));

  // Use the factory of ExecutionEngine with specified type
  m_executionEngine = createExecutionEngine(config.engineType);
  m_context = ctx ? std::move(ctx) : std::make_shared<PipelineContext>();

  if (m_graph->hasCycle()) {
    LOG_ERRORS
        << "Pipeline::Impl: Graph contains a cycle. Initialization failed.";
    m_state = PipelineState::ERROR;
    return false;
  }

  if (!m_executionEngine->initialize(m_graph.get(), config.numWorkers)) {
    LOG_ERRORS << "Pipeline::Impl: Failed to initialize execution engine.";
    m_state = PipelineState::ERROR;
    return false;
  }

  m_state = PipelineState::IDLE;
  LOG_INFOS << "Pipeline::Impl: Initialized successfully with provided graph.";
  return true;
}

bool Pipeline::Impl::start() {
  if (m_state != PipelineState::IDLE) {
    LOG_WARNINGS
        << "Pipeline::Impl: Cannot start, not in IDLE state. Current state: "
        << static_cast<int>(m_state.load());
    return false;
  }
  if (!m_executionEngine || !m_graph) {
    LOG_ERRORS << "Pipeline::Impl: Not initialized properly (engine or graph "
                  "missing).";
    return false;
  }

  m_state = PipelineState::RUNNING;
  return true;
}

bool Pipeline::Impl::stop() {
  PipelineState current_state = m_state.load();
  if (current_state != PipelineState::RUNNING &&
      current_state != PipelineState::STOPPING) {
    LOG_WARNINGS
        << "Pipeline::Impl: Cannot stop, not in RUNNING or STOPPING state. "
        << "Current state: " << static_cast<int>(current_state);
    return false;
  }

  if (!m_executionEngine) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine missing, cannot stop.";
    // to stop.
    m_state = PipelineState::ERROR;
    return false;
  }

  LOG_INFOS << "Pipeline::Impl: Stopping...";
  m_state = PipelineState::STOPPING;

  m_executionEngine->stopExecutionSync();

  m_state = PipelineState::IDLE;
  LOG_INFOS << "Pipeline::Impl: Stopped successfully.";
  return true;
}

PipelineState Pipeline::Impl::getState() const { return m_state.load(); }

EngineState Pipeline::Impl::getEngineState() const {
  if (!m_executionEngine)
    return EngineState::IDLE;
  return m_executionEngine->getState();
}

std::unordered_map<std::string, NodeExecutionState>
Pipeline::Impl::getNodeStates() const {
  if (!m_executionEngine)
    return {};
  return m_executionEngine->getNodeStates();
}

void Pipeline::Impl::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  m_onPipelineResult = std::move(callback);
  if (m_executionEngine) {
    m_executionEngine->setPipelineResultCallback(
        [this](const PortDataMap &results) {
          if (this->m_onPipelineResult) {
            this->m_onPipelineResult(results);
          }
        });
  }
}

void Pipeline::Impl::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {

  m_onPipelineError = std::move(callback);
  if (m_executionEngine) {
    m_executionEngine->setPipelineErrorCallback(
        [this](const std::string &errorMsg, const std::string &nodeName) {
          m_state = PipelineState::ERROR;
          if (this->m_onPipelineError) {
            this->m_onPipelineError(errorMsg, nodeName);
          }
        });
  }
}

void Pipeline::Impl::reset() {
  LOG_INFOS << "Pipeline::Impl: Resetting...";
  if (m_state == PipelineState::RUNNING || m_state == PipelineState::STOPPING) {
    stop();
  }
  m_graph = std::make_unique<Graph>();
  m_context = std::make_shared<PipelineContext>();
  // Use the factory of ExecutionEngine
  m_executionEngine = createExecutionEngine();

  m_onPipelineResult = nullptr;
  m_onPipelineError = nullptr;
  m_state = PipelineState::UNINITIALIZED;
  LOG_INFOS << "Pipeline::Impl: Reset complete. Ready for re-initialization.";
}

bool Pipeline::Impl::feedDataAsync(const PortDataMap &initialInputs) {
  if (m_state != PipelineState::RUNNING) {
    LOG_ERRORS << "Pipeline::Impl: Cannot feed data, not in RUNNING state. "
                  "Current state: "
               << static_cast<int>(m_state.load());
    return false;
  }
  if (!m_executionEngine) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine is not available.";
    return false;
  }
  if (!m_context) {
    LOG_ERRORS << "Pipeline::Impl: Pipeline context is not initialized.";
    return false;
  }
  LOG_INFOS
      << "Pipeline::Impl: Asynchronously feeding data to execution engine.";
  return m_executionEngine->execute(std::move(initialInputs), false,
                                    m_context);
}

std::future<bool>
Pipeline::Impl::feedDataAndGetResultFuture(const PortDataMap &initialInputs) {
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();

  if (m_state != PipelineState::RUNNING) {
    LOG_ERRORS << "Pipeline::Impl: Cannot feed data, not in RUNNING state. "
                  "Current state: "
               << static_cast<int>(m_state.load());
    promise.set_value(false);
    return future;
  }
  if (!m_executionEngine) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine is not available.";
    promise.set_value(false);
    return future;
  }

  if (!m_context) {
    LOG_ERRORS << "Pipeline::Impl: Pipeline context is not initialized.";
    promise.set_value(false);
    return future;
  }

  LOG_INFOS << "Pipeline::Impl: Submitting data for future-based notification "
               "(simplified).";
  bool submitted =
      m_executionEngine->execute(std::move(initialInputs), true, m_context);
  promise.set_value(submitted);

  return future;
}
} // namespace ai_pipe
