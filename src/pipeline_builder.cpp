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
#include "ai_pipe/builder.hpp"
#include "ai_pipe/node_registrar.hpp"
#include <fstream>
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe {

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

} // namespace ai_pipe