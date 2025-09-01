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
#include "default_execution_engine.hpp"
#include <logger.hpp>
#include <memory>
#include <nlohmann/json.hpp>

namespace ai_pipe {
Pipeline::Impl::Impl()
    : mGraph(nullptr), mExecutionEngine(nullptr), mContext(nullptr),
      mState(PipelineState::UNINITIALIZED) {
  LOG_INFOS << "Pipeline::Impl default constructed.";
}

Pipeline::Impl::~Impl() {
  // Ensure graceful shutdown
  if (mState == PipelineState::RUNNING || mState == PipelineState::STOPPING) {
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
    : mGraph(std::move(other.mGraph)),
      mExecutionEngine(std::move(other.mExecutionEngine)),
      mContext(std::move(other.mContext)), mState(other.mState.load()),
      mOnPipelineError(std::move(other.mOnPipelineError)),
      mOnPipelineResult(std::move(other.mOnPipelineResult)) {
  LOG_INFOS << "Pipeline::Impl move constructed.";
}

Pipeline::Impl &Pipeline::Impl::operator=(Impl &&other) noexcept {
  if (this != &other) {
    // Properly handle self-assignment if necessary, though for move it's less
    // common Ensure current pipeline is stopped before overwriting
    if (mState == PipelineState::RUNNING || mState == PipelineState::STOPPING) {
      stop();
    }
    mGraph = std::move(other.mGraph);
    mExecutionEngine = std::move(other.mExecutionEngine);
    mContext = std::move(other.mContext);
    mState.store(other.mState.load());
    mOnPipelineError = std::move(other.mOnPipelineError);
    mOnPipelineResult = std::move(other.mOnPipelineResult);
  }
  LOG_INFOS << "Pipeline::Impl move assigned.";
  return *this;
}

bool Pipeline::Impl::initialize(Graph &&graph,
                                std::shared_ptr<PipelineContext> ctx,
                                uint8_t numWorkers) {
  LOG_INFOS << "Pipeline::Impl initializing with provided graph, numWorkers: "
            << (int)numWorkers;
  mGraph = std::make_unique<Graph>(std::move(graph));

  // TODO: Use the factory of ExecutionEngine
  mExecutionEngine = std::make_unique<DefaultExecutionEngine>();
  mContext = ctx ? std::move(ctx) : std::make_shared<PipelineContext>();

  if (mGraph->hasCycle()) {
    LOG_ERRORS
        << "Pipeline::Impl: Graph contains a cycle. Initialization failed.";
    mState = PipelineState::ERROR;
    return false;
  }

  if (!mExecutionEngine->initialize(mGraph.get(), numWorkers)) {
    LOG_ERRORS << "Pipeline::Impl: Failed to initialize execution engine.";
    mState = PipelineState::ERROR;
    return false;
  }

  mState = PipelineState::IDLE;
  LOG_INFOS << "Pipeline::Impl: Initialized successfully with provided graph.";
  return true;
}

bool Pipeline::Impl::start() {
  if (mState != PipelineState::IDLE) {
    LOG_WARNINGS
        << "Pipeline::Impl: Cannot start, not in IDLE state. Current state: "
        << static_cast<int>(mState.load());
    return false;
  }
  if (!mExecutionEngine || !mGraph) {
    LOG_ERRORS << "Pipeline::Impl: Not initialized properly (engine or graph "
                  "missing).";
    return false;
  }

  mState = PipelineState::RUNNING;
  return true;
}

bool Pipeline::Impl::stop() {
  PipelineState current_state = mState.load();
  if (current_state != PipelineState::RUNNING &&
      current_state != PipelineState::STOPPING) {
    LOG_WARNINGS
        << "Pipeline::Impl: Cannot stop, not in RUNNING or STOPPING state. "
        << "Current state: " << static_cast<int>(current_state);
    return false;
  }

  if (!mExecutionEngine) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine missing, cannot stop.";
    // to stop.
    mState = PipelineState::ERROR;
    return false;
  }

  LOG_INFOS << "Pipeline::Impl: Stopping...";
  mState = PipelineState::STOPPING;

  mExecutionEngine->stopExecutionSync();

  mState = PipelineState::IDLE;
  LOG_INFOS << "Pipeline::Impl: Stopped successfully.";
  return true;
}

PipelineState Pipeline::Impl::getState() const { return mState.load(); }

EngineState Pipeline::Impl::getEngineState() const {
  if (!mExecutionEngine)
    return EngineState::IDLE;
  return mExecutionEngine->getState();
}

std::unordered_map<std::string, NodeExecutionState>
Pipeline::Impl::getNodeStates() const {
  if (!mExecutionEngine)
    return {};
  return mExecutionEngine->getNodeStates();
}

void Pipeline::Impl::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  mOnPipelineResult = std::move(callback);
  if (mExecutionEngine) {
    mExecutionEngine->setPipelineResultCallback(
        [this](const PortDataMap &results) {
          if (this->mOnPipelineResult) {
            this->mOnPipelineResult(results);
          }
        });
  }
}

void Pipeline::Impl::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {

  mOnPipelineError = std::move(callback);
  if (mExecutionEngine) {
    mExecutionEngine->setPipelineErrorCallback(
        [this](const std::string &errorMsg, const std::string &nodeName) {
          mState = PipelineState::ERROR;
          if (this->mOnPipelineError) {
            this->mOnPipelineError(errorMsg, nodeName);
          }
        });
  }
}

void Pipeline::Impl::reset() {
  LOG_INFOS << "Pipeline::Impl: Resetting...";
  if (mState == PipelineState::RUNNING || mState == PipelineState::STOPPING) {
    stop();
  }
  mGraph = std::make_unique<Graph>();
  mContext = std::make_shared<PipelineContext>();
  // TODO: Use the factory of ExecutionEngine
  mExecutionEngine = std::make_unique<DefaultExecutionEngine>();

  mOnPipelineResult = nullptr;
  mOnPipelineError = nullptr;
  mState = PipelineState::UNINITIALIZED;
  LOG_INFOS << "Pipeline::Impl: Reset complete. Ready for re-initialization.";
}

bool Pipeline::Impl::feedDataAsync(const PortDataMap &initialInputs) {
  if (mState != PipelineState::RUNNING) {
    LOG_ERRORS << "Pipeline::Impl: Cannot feed data, not in RUNNING state. "
                  "Current state: "
               << static_cast<int>(mState.load());
    return false;
  }
  if (!mExecutionEngine) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine is not available.";
    return false;
  }
  if (!mContext) {
    LOG_ERRORS << "Pipeline::Impl: Pipeline context is not initialized.";
    return false;
  }
  LOG_INFOS
      << "Pipeline::Impl: Asynchronously feeding data to execution engine.";
  return mExecutionEngine->execute(std::move(initialInputs), false, mContext);
}

std::future<bool>
Pipeline::Impl::feedDataAndGetResultFuture(const PortDataMap &initialInputs) {
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();

  if (mState != PipelineState::RUNNING) {
    LOG_ERRORS << "Pipeline::Impl: Cannot feed data, not in RUNNING state. "
                  "Current state: "
               << static_cast<int>(mState.load());
    promise.set_value(false);
    return future;
  }
  if (!mExecutionEngine) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine is not available.";
    promise.set_value(false);
    return future;
  }

  if (!mContext) {
    LOG_ERRORS << "Pipeline::Impl: Pipeline context is not initialized.";
    promise.set_value(false);
    return future;
  }

  LOG_INFOS << "Pipeline::Impl: Submitting data for future-based notification "
               "(simplified).";
  bool submitted =
      mExecutionEngine->execute(std::move(initialInputs), true, mContext);
  promise.set_value(submitted);

  return future;
}
} // namespace ai_pipe
