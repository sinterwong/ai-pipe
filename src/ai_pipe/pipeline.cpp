/**
 * @file pipeline.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#include <fstream>
#include <memory>

#include "node_param_types.hpp"
#include "node_registrar.hpp"
#include "pipe_types.hpp"
#include "pipeline.hpp"

#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe {
[[maybe_unused]] static NodeRegistrar &registrar = NodeRegistrar::getInstance();

Pipeline::Pipeline()
    : graph_(nullptr), executionEngine_(nullptr), context_(nullptr),
      state_(PipelineState::IDLE) {
  LOG_INFOS << "Pipeline default constructed.";
}

Pipeline::~Pipeline() {
  // Ensure graceful shutdown
  if (state_ == PipelineState::RUNNING || state_ == PipelineState::STOPPING) {
    try {
      stop();
    } catch (const std::exception &e) {
      LOG_ERRORS << "Pipeline destructor: Exception during stop: " << e.what();
    } catch (...) {
      LOG_ERRORS << "Pipeline destructor: Unknown exception during stop.";
    }
  }
  LOG_INFOS << "Pipeline destructed.";
}

Pipeline::Pipeline(Pipeline &&other) noexcept
    : graph_(std::move(other.graph_)),
      executionEngine_(std::move(other.executionEngine_)),
      context_(std::move(other.context_)), state_(other.state_.load()),
      onPipelineError_(std::move(other.onPipelineError_)),
      onPipelineResult_(std::move(other.onPipelineResult_)) {
  other.state_ = PipelineState::STOPPED; // Or IDLE, depending on semantics
  LOG_INFOS << "Pipeline move constructed.";
}

Pipeline &Pipeline::operator=(Pipeline &&other) noexcept {
  if (this != &other) {
    // Properly handle self-assignment if necessary, though for move it's less
    // common Ensure current pipeline is stopped before overwriting
    if (state_ == PipelineState::RUNNING || state_ == PipelineState::STOPPING) {
      stop();
    }
    graph_ = std::move(other.graph_);
    executionEngine_ = std::move(other.executionEngine_);
    context_ = std::move(other.context_);
    state_ = other.state_.load();
    onPipelineError_ = std::move(other.onPipelineError_);
    onPipelineResult_ = std::move(other.onPipelineResult_);

    other.state_ = PipelineState::STOPPED; // Or IDLE
  }
  LOG_INFOS << "Pipeline move assigned.";
  return *this;
}

bool Pipeline::initialize(const PipelineConfig &config,
                          std::shared_ptr<PipelineContext> ctx) {
  LOG_INFOS << "Pipeline initializing with config: " << config.graphConfigPath
            << ", numWorkers: " << (int)config.numWorkers;
  try {
    context_ = ctx ? std::move(ctx) : std::make_shared<PipelineContext>();

    // Build graph from the configuration file
    graph_ =
        std::make_unique<Graph>(buildGraphFromConfig(config.graphConfigPath));

    executionEngine_ = std::make_unique<ExecutionEngine>();

    if (graph_->hasCycle()) {
      LOG_ERRORS << "Pipeline: Graph contains a cycle. Initialization failed.";
      state_ = PipelineState::ERROR;
      return false;
    }
    if (graph_->getNodes().empty()) {
      LOG_WARNINGS << "Pipeline: Graph is empty after loading config.";
      // Depending on requirements, this might be an error or just a warning.
    }

    // Initialize the execution engine with the graph and number of workers
    if (!executionEngine_->initialize(graph_.get(), config.numWorkers)) {
      LOG_ERRORS << "Pipeline: Failed to initialize execution engine.";
      state_ = PipelineState::ERROR;
      return false;
    }

    // Set callbacks on the execution engine
    executionEngine_->setPipelineResultCallback(
        [this](const PortDataMap &results) {
          if (this->onPipelineResult_) {
            this->onPipelineResult_(results);
          }
        });
    executionEngine_->setPipelineErrorCallback(
        [this](const std::string &errorMsg, const std::string &nodeName) {
          if (this->onPipelineError_) {
            this->onPipelineError_(errorMsg, nodeName);
          }
        });

    state_ = PipelineState::IDLE;
    LOG_INFOS << "Pipeline: Initialized successfully.";
    return true;
  } catch (const std::exception &e) {
    LOG_ERRORS << "Pipeline initialization failed: " << e.what();
    state_ = PipelineState::ERROR;
    return false;
  }
}

// This version is useful for programmatic graph construction (e.g. testing or
// dynamic pipelines)
bool Pipeline::initializeWithGraph(Graph &&graph,
                                   std::shared_ptr<PipelineContext> ctx,
                                   uint8_t numWorkers) {
  LOG_INFOS << "Pipeline initializing with provided graph, numWorkers: "
            << (int)numWorkers;
  graph_ = std::make_unique<Graph>(
      std::move(graph)); // Take ownership of the provided graph
  context_ = ctx ? std::move(ctx) : std::make_shared<PipelineContext>();

  if (graph_->hasCycle()) {
    LOG_ERRORS << "Pipeline: Graph contains a cycle. Initialization failed.";
    state_ = PipelineState::ERROR;
    return false;
  }

  if (!executionEngine_->initialize(graph_.get(), numWorkers)) {
    LOG_ERRORS << "Pipeline: Failed to initialize execution engine.";
    state_ = PipelineState::ERROR;
    return false;
  }

  state_ = PipelineState::IDLE;
  LOG_INFOS << "Pipeline: Initialized successfully with provided graph.";
  return true;
}

bool Pipeline::start() {
  if (state_ != PipelineState::IDLE) {
    LOG_WARNINGS << "Pipeline: Cannot start, not in IDLE state. Current state: "
                 << static_cast<int>(state_.load());
    return false;
  }
  if (!executionEngine_ || !graph_) {
    LOG_ERRORS
        << "Pipeline: Not initialized properly (engine or graph missing).";
    return false;
  }

  state_ = PipelineState::RUNNING;
  return true;
}

bool Pipeline::stop() {
  PipelineState current_state = state_.load(); // Read atomic state once
  if (current_state != PipelineState::RUNNING &&
      current_state != PipelineState::STOPPING) {
    LOG_WARNINGS << "Pipeline: Cannot stop, not in RUNNING or STOPPING state. "
                 << "Current state: " << static_cast<int>(current_state);
    return false;
  }

  if (!executionEngine_) {
    LOG_ERRORS << "Pipeline: Execution engine missing, cannot stop.";
    // Consider what state to set here. If engine is missing, pipeline is
    // unusable. ERROR state might be appropriate if it was previously RUNNING.
    // If it was already STOPPING, maybe move to STOPPED or ERROR.
    // For now, matching existing behavior of setting to ERROR if it was trying
    // to stop.
    state_ = PipelineState::ERROR;
    return false;
  }

  LOG_INFOS << "Pipeline: Stopping...";
  state_ = PipelineState::STOPPING;

  executionEngine_->stopExecutionSync(); // Delegate to ExecutionEngine

  state_ = PipelineState::STOPPED;
  LOG_INFOS << "Pipeline: Stopped successfully.";
  return true;
}

PipelineState Pipeline::getState() const { return state_.load(); }

std::unordered_map<std::string, NodeExecutionState>
Pipeline::getNodeStates() const {
  if (!executionEngine_)
    return {};
  return executionEngine_->getNodeStates();
}

void Pipeline::setPipelineResultCallback(
    std::function<void(const PortDataMap &finalResults)> callback) {
  onPipelineResult_ = std::move(callback);
  if (executionEngine_) {
    executionEngine_->setPipelineResultCallback(
        [this](const PortDataMap &results) {
          if (this->onPipelineResult_) {
            this->onPipelineResult_(results);
          }
        });
  }
}

void Pipeline::setPipelineErrorCallback(
    std::function<void(const std::string &errorMsg,
                       const std::string &nodeName)>
        callback) {

  onPipelineError_ = std::move(callback);
  if (executionEngine_) {
    executionEngine_->setPipelineErrorCallback(
        [this](const std::string &errorMsg, const std::string &nodeName) {
          if (this->onPipelineError_) {
            this->onPipelineError_(errorMsg, nodeName);
          }
        });
  }
}

void Pipeline::reset() {
  LOG_INFOS << "Pipeline: Resetting...";
  if (state_ == PipelineState::RUNNING || state_ == PipelineState::STOPPING) {
    stop(); // Ensure pipeline is stopped first
  }
  graph_ = std::make_unique<Graph>();
  context_ = std::make_shared<PipelineContext>();
  executionEngine_ = std::make_unique<ExecutionEngine>();

  onPipelineResult_ = nullptr;
  onPipelineError_ = nullptr;
  state_ = PipelineState::IDLE;
  LOG_INFOS << "Pipeline: Reset complete. Ready for re-initialization.";
}

bool Pipeline::feedDataAsync(const PortDataMap &initialInputs) {
  if (state_ != PipelineState::RUNNING) {
    LOG_ERRORS
        << "Pipeline: Cannot feed data, not in RUNNING state. Current state: "
        << static_cast<int>(state_.load());
    return false;
  }
  if (!executionEngine_) {
    LOG_ERRORS << "Pipeline: Execution engine is not available.";
    return false;
  }
  if (!context_) {
    LOG_ERRORS << "Pipeline: Pipeline context is not initialized.";
    return false;
  }
  LOG_INFOS << "Pipeline: Asynchronously feeding data to execution engine.";
  return executionEngine_->execute(std::move(initialInputs), false, context_);
}

std::future<bool>
Pipeline::feedDataAndGetResultFuture(const PortDataMap &initialInputs) {
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();

  if (state_ != PipelineState::RUNNING) {
    LOG_ERRORS
        << "Pipeline: Cannot feed data, not in RUNNING state. Current state: "
        << static_cast<int>(state_.load());
    promise.set_value(false);
    return future;
  }
  if (!executionEngine_) {
    LOG_ERRORS << "Pipeline: Execution engine is not available.";
    promise.set_value(false);
    return future;
  }

  if (!context_) {
    LOG_ERRORS << "Pipeline: Pipeline context is not initialized.";
    promise.set_value(false);
    return future;
  }

  LOG_INFOS << "Pipeline: Submitting data for future-based notification "
               "(simplified).";
  bool submitted =
      executionEngine_->execute(std::move(initialInputs), true, context_);
  promise.set_value(submitted);

  return future;
}

Graph Pipeline::buildGraphFromConfig(const std::string &configPath) {
  LOG_INFOS << "Building graph from config: " << configPath;
  Graph newGraph;

  std::ifstream file(configPath);
  if (!file.is_open()) {
    LOG_ERRORS << "Failed to open graph config file: " << configPath;
    throw std::runtime_error("Failed to open graph config file: " + configPath);
  }

  nlohmann::json j;
  try {
    file >> j;
  } catch (const nlohmann::json::parse_error &e) {
    LOG_ERRORS << "Failed to parse graph config JSON: " << e.what();
    throw std::runtime_error("Failed to parse graph config JSON: " +
                             std::string(e.what()));
  }

  if (!j.contains("nodes") || !j["nodes"].is_array()) {
    LOG_ERRORS << "Config missing 'nodes' array or it's not an array.";
    throw std::runtime_error("Config missing 'nodes' array or not an array.");
  }

  for (const auto &nodeConfig : j["nodes"]) {
    std::string name = nodeConfig.at("name").get<std::string>();
    std::string type = nodeConfig.at("type").get<std::string>();
    LOG_INFOS << "Creating node: " << name << " of type: " << type;

    NodeConstructParams creationParams;
    creationParams.setParam("name", name);

    auto it = s_paramHandlers.find(type);
    if (it != s_paramHandlers.end()) {
      it->second(nodeConfig, creationParams, name, type);
    } else {
      LOG_ERRORS << "Unknown node type in config: " << type;
      throw std::runtime_error("Unknown node type: " + type);
    }

    auto node = NodeFactory::instance().create(type, creationParams);
    if (!node) {
      LOG_ERRORS << "Failed to create node: " << name << " of type: " << type;
      throw std::runtime_error("Failed to create node: " + name);
    }
    newGraph.addNode(node);
  }

  // add edges
  if (j.contains("edges") && j["edges"].is_array()) {
    for (const auto &edgeConfig : j["edges"]) {
      std::string fromNodeName = edgeConfig.at("from_node").get<std::string>();
      std::string fromPortName = edgeConfig.at("from_port").get<std::string>();
      std::string toNodeName = edgeConfig.at("to_node").get<std::string>();
      std::string toPortName = edgeConfig.at("to_port").get<std::string>();

      LOG_INFOS << "Attempting to add edge from " << fromNodeName << ":"
                << fromPortName << " to " << toNodeName << ":" << toPortName;

      if (!newGraph.addEdge(fromNodeName, fromPortName, toNodeName,
                            toPortName)) {
        LOG_ERRORS << "Failed to add edge from " << fromNodeName << ":"
                   << fromPortName << " to " << toNodeName << ":" << toPortName
                   << " (check if nodes exist and ports are correctly named).";
        throw std::runtime_error("Failed to add edge: " + fromNodeName + ":" +
                                 fromPortName + " -> " + toNodeName + ":" +
                                 toPortName);
      }
    }
  } else {
    LOG_WARNINGS << "Config does not contain 'edges' array. Graph may be "
                    "disconnected.";
  }

  LOG_INFOS << "Graph built successfully from config. Nodes: "
            << newGraph.getNodes().size()
            << ", Edges: " << newGraph.getEdges().size();
  return newGraph;
}

} // namespace ai_pipe