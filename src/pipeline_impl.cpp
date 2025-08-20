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
#include "ai_pipe/node_registrar.hpp"
#include "ai_pipe/types.hpp"
#include "type_safe_factory.hpp"
#include <fstream>
#include <logger.hpp>
#include <memory>
#include <nlohmann/json.hpp>

namespace ai_pipe {
Pipeline::Impl::Impl()
    : graph_(nullptr), executionEngine_(nullptr), context_(nullptr),
      state_(PipelineState::IDLE) {
  LOG_INFOS << "Pipeline::Impl default constructed.";
}

Pipeline::Impl::~Impl() {
  // Ensure graceful shutdown
  if (state_ == PipelineState::RUNNING || state_ == PipelineState::STOPPING) {
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
    : graph_(std::move(other.graph_)),
      executionEngine_(std::move(other.executionEngine_)),
      context_(std::move(other.context_)), state_(other.state_.load()),
      onPipelineError_(std::move(other.onPipelineError_)),
      onPipelineResult_(std::move(other.onPipelineResult_)) {
  other.state_ = PipelineState::STOPPED; // Or IDLE, depending on semantics
  LOG_INFOS << "Pipeline::Impl move constructed.";
}

Pipeline::Impl &Pipeline::Impl::operator=(Impl &&other) noexcept {
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
  LOG_INFOS << "Pipeline::Impl move assigned.";
  return *this;
}

bool Pipeline::Impl::initialize(const PipelineConfig &config,
                                std::shared_ptr<PipelineContext> ctx) {
  LOG_INFOS << "Pipeline::Impl initializing with config: "
            << config.graphConfigPath
            << ", numWorkers: " << (int)config.numWorkers;
  try {
    context_ = ctx ? std::move(ctx) : std::make_shared<PipelineContext>();

    // Build graph from the configuration file
    graph_ =
        std::make_unique<Graph>(buildGraphFromConfig(config.graphConfigPath));

    executionEngine_ = std::make_unique<ExecutionEngine>();

    if (graph_->hasCycle()) {
      LOG_ERRORS
          << "Pipeline::Impl: Graph contains a cycle. Initialization failed.";
      state_ = PipelineState::ERROR;
      return false;
    }
    if (graph_->getNodes().empty()) {
      LOG_WARNINGS << "Pipeline::Impl: Graph is empty after loading config.";
    }

    // Initialize the execution engine with the graph and number of workers
    if (!executionEngine_->initialize(graph_.get(), config.numWorkers)) {
      LOG_ERRORS << "Pipeline::Impl: Failed to initialize execution engine.";
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
    LOG_INFOS << "Pipeline::Impl: Initialized successfully.";
    return true;
  } catch (const std::exception &e) {
    LOG_ERRORS << "Pipeline::Impl initialization failed: " << e.what();
    state_ = PipelineState::ERROR;
    return false;
  }
}

bool Pipeline::Impl::initializeWithGraph(Graph &&graph,
                                         std::shared_ptr<PipelineContext> ctx,
                                         uint8_t numWorkers) {
  LOG_INFOS << "Pipeline::Impl initializing with provided graph, numWorkers: "
            << (int)numWorkers;
  graph_ = std::make_unique<Graph>(std::move(graph));
  context_ = ctx ? std::move(ctx) : std::make_shared<PipelineContext>();

  if (graph_->hasCycle()) {
    LOG_ERRORS
        << "Pipeline::Impl: Graph contains a cycle. Initialization failed.";
    state_ = PipelineState::ERROR;
    return false;
  }

  if (!executionEngine_->initialize(graph_.get(), numWorkers)) {
    LOG_ERRORS << "Pipeline::Impl: Failed to initialize execution engine.";
    state_ = PipelineState::ERROR;
    return false;
  }

  state_ = PipelineState::IDLE;
  LOG_INFOS << "Pipeline::Impl: Initialized successfully with provided graph.";
  return true;
}

bool Pipeline::Impl::start() {
  if (state_ != PipelineState::IDLE) {
    LOG_WARNINGS
        << "Pipeline::Impl: Cannot start, not in IDLE state. Current state: "
        << static_cast<int>(state_.load());
    return false;
  }
  if (!executionEngine_ || !graph_) {
    LOG_ERRORS << "Pipeline::Impl: Not initialized properly (engine or graph "
                  "missing).";
    return false;
  }

  state_ = PipelineState::RUNNING;
  return true;
}

bool Pipeline::Impl::stop() {
  PipelineState current_state = state_.load();
  if (current_state != PipelineState::RUNNING &&
      current_state != PipelineState::STOPPING) {
    LOG_WARNINGS
        << "Pipeline::Impl: Cannot stop, not in RUNNING or STOPPING state. "
        << "Current state: " << static_cast<int>(current_state);
    return false;
  }

  if (!executionEngine_) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine missing, cannot stop.";
    // to stop.
    state_ = PipelineState::ERROR;
    return false;
  }

  LOG_INFOS << "Pipeline::Impl: Stopping...";
  state_ = PipelineState::STOPPING;

  executionEngine_->stopExecutionSync();

  state_ = PipelineState::STOPPED;
  LOG_INFOS << "Pipeline::Impl: Stopped successfully.";
  return true;
}

PipelineState Pipeline::Impl::getState() const { return state_.load(); }

std::unordered_map<std::string, NodeExecutionState>
Pipeline::Impl::getNodeStates() const {
  if (!executionEngine_)
    return {};
  return executionEngine_->getNodeStates();
}

void Pipeline::Impl::setPipelineResultCallback(
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

void Pipeline::Impl::setPipelineErrorCallback(
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

void Pipeline::Impl::reset() {
  LOG_INFOS << "Pipeline::Impl: Resetting...";
  if (state_ == PipelineState::RUNNING || state_ == PipelineState::STOPPING) {
    stop();
  }
  graph_ = std::make_unique<Graph>();
  context_ = std::make_shared<PipelineContext>();
  executionEngine_ = std::make_unique<ExecutionEngine>();

  onPipelineResult_ = nullptr;
  onPipelineError_ = nullptr;
  state_ = PipelineState::IDLE;
  LOG_INFOS << "Pipeline::Impl: Reset complete. Ready for re-initialization.";
}

bool Pipeline::Impl::feedDataAsync(const PortDataMap &initialInputs) {
  if (state_ != PipelineState::RUNNING) {
    LOG_ERRORS << "Pipeline::Impl: Cannot feed data, not in RUNNING state. "
                  "Current state: "
               << static_cast<int>(state_.load());
    return false;
  }
  if (!executionEngine_) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine is not available.";
    return false;
  }
  if (!context_) {
    LOG_ERRORS << "Pipeline::Impl: Pipeline context is not initialized.";
    return false;
  }
  LOG_INFOS
      << "Pipeline::Impl: Asynchronously feeding data to execution engine.";
  return executionEngine_->execute(std::move(initialInputs), false, context_);
}

std::future<bool>
Pipeline::Impl::feedDataAndGetResultFuture(const PortDataMap &initialInputs) {
  std::promise<bool> promise;
  std::future<bool> future = promise.get_future();

  if (state_ != PipelineState::RUNNING) {
    LOG_ERRORS << "Pipeline::Impl: Cannot feed data, not in RUNNING state. "
                  "Current state: "
               << static_cast<int>(state_.load());
    promise.set_value(false);
    return future;
  }
  if (!executionEngine_) {
    LOG_ERRORS << "Pipeline::Impl: Execution engine is not available.";
    promise.set_value(false);
    return future;
  }

  if (!context_) {
    LOG_ERRORS << "Pipeline::Impl: Pipeline context is not initialized.";
    promise.set_value(false);
    return future;
  }

  LOG_INFOS << "Pipeline::Impl: Submitting data for future-based notification "
               "(simplified).";
  bool submitted =
      executionEngine_->execute(std::move(initialInputs), true, context_);
  promise.set_value(submitted);

  return future;
}

Graph Pipeline::Impl::buildGraphFromConfig(const std::string &configPath) {
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

    if (!NodeParamParserFactory::instance().isRegistered(type)) {
      LOG_ERRORS << "Node parameter parser for type " << type
                 << " not registered.";
      throw std::runtime_error("Node parameter parser for type " + type +
                               " not registered.");
    }

    auto parser = NodeParamParserFactory::instance().create(type);
    parser->parse(nodeConfig, creationParams, name, type);

    auto node = NodeCreatorFactory::instance().create(type, creationParams);
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
