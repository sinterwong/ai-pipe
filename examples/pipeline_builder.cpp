/**
 * @file pipeline_builder.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "pipeline_builder.hpp"
#include "node_registrar.hpp"
#include <fstream>
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {

Pipeline
PipelineBuilder::buildPipelineFromConfig(const std::string &graphConfigPath,
                                         std::shared_ptr<PipelineContext> ctx,
                                         uint8_t numWorkers) {
  LOG_INFOS << "Pipeline::Impl initializing with config: " << graphConfigPath
            << ", numWorkers: " << (int)numWorkers;
  Pipeline pipeline;
  Graph graph;
  PipelineConfig config;

  // Build graph and extract pipeline config from the same config file
  std::tie(graph, config) = buildGraphAndConfigFromFile(graphConfigPath);

  // Override numWorkers if specified
  if (numWorkers > 0) {
    config.numWorkers = numWorkers;
  }

  if (graph.hasCycle()) {
    LOG_ERRORS << "PipelineBuilder: Graph contains a cycle. Pipeline "
                  "initialization failed.";
    throw std::runtime_error("Graph contains a cycle.");
  }
  if (graph.getNodes().empty()) {
    LOG_WARNINGS << "PipelineBuilder: Graph is empty after loading config.";
  }

  LOG_INFOS << "PipelineBuilder: Using engine type: " << config.engineType
            << ", workers: " << (int)config.numWorkers;
  pipeline.initialize(std::move(graph), ctx, config);
  return pipeline;
}

Graph PipelineBuilder::buildGraphFromConfig(const std::string &configPath) {
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

std::tuple<Graph, PipelineConfig>
PipelineBuilder::buildGraphAndConfigFromFile(const std::string &configPath) {
  LOG_INFOS << "Building graph and config from file: " << configPath;

  std::ifstream file(configPath);
  if (!file.is_open()) {
    LOG_ERRORS << "Failed to open config file: " << configPath;
    throw std::runtime_error("Failed to open config file: " + configPath);
  }

  nlohmann::json j;
  try {
    file >> j;
  } catch (const nlohmann::json::parse_error &e) {
    LOG_ERRORS << "Failed to parse config JSON: " << e.what();
    throw std::runtime_error("Failed to parse config JSON: " +
                             std::string(e.what()));
  }

  // Extract pipeline config
  PipelineConfig config;
  if (j.contains("pipeline")) {
    const auto &pipelineConfig = j["pipeline"];
    if (pipelineConfig.contains("engine_type") &&
        pipelineConfig["engine_type"].is_string()) {
      config.engineType = pipelineConfig["engine_type"].get<std::string>();
      LOG_INFOS << "Config: engine_type = " << config.engineType;
    }
    if (pipelineConfig.contains("num_workers") &&
        pipelineConfig["num_workers"].is_number_integer()) {
      config.numWorkers =
          pipelineConfig["num_workers"].get<uint8_t>();
      LOG_INFOS << "Config: num_workers = " << (int)config.numWorkers;
    }
  } else {
    LOG_INFOS
        << "No 'pipeline' config section found, using default configuration.";
  }

  // Build graph (reuse existing logic)
  Graph newGraph;

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

  // Add edges
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

  LOG_INFOS << "Graph and config built successfully. Nodes: "
            << newGraph.getNodes().size()
            << ", Edges: " << newGraph.getEdges().size()
            << ", Engine: " << config.engineType
            << ", Workers: " << (int)config.numWorkers;

  return std::make_tuple(std::move(newGraph), config);
}

} // namespace ai_pipe::examples