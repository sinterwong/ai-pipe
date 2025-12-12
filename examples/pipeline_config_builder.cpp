/**
 * @file pipeline_config_builder.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Implementation of PipelineConfigBuilder
 * @version 0.2
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#include "pipeline_config_builder.hpp"
#include "node_registrar.hpp"
#include <fstream>
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {

// =============================================================================
// Public Interface
// =============================================================================

Pipeline
PipelineConfigBuilder::buildPipeline(const std::string &config_path,
                                     std::shared_ptr<PipelineContext> context,
                                     std::uint8_t num_workers) {
  LOG_INFOS << "PipelineConfigBuilder: Building pipeline from " << config_path
            << ", workers=" << static_cast<int>(num_workers);

  // Load graph and options from config
  auto [graph, options] = buildGraphAndOptions(config_path);

  // Override num_workers if specified
  if (num_workers > 0) {
    options.num_workers = num_workers;
  }

  // Validate graph
  if (graph.hasCycle()) {
    LOG_ERRORS << "PipelineConfigBuilder: Graph contains a cycle";
    throw std::runtime_error("Graph contains a cycle");
  }

  if (graph.getNodes().empty()) {
    LOG_WARNINGS << "PipelineConfigBuilder: Graph is empty after loading";
  }

  LOG_INFOS << "PipelineConfigBuilder: Using engine=" << options.engine_type
            << ", workers=" << static_cast<int>(options.num_workers);

  // Build pipeline using fluent API
  return Pipeline::create()
      .withGraph(std::move(graph))
      .withContext(context ? std::move(context)
                           : std::make_shared<PipelineContext>())
      .withOptions(options)
      .build();
}

Pipeline
PipelineConfigBuilder::buildPipeline(const std::string &config_path,
                                     std::shared_ptr<PipelineContext> context,
                                     const PipelineOptions &options) {
  LOG_INFOS << "PipelineConfigBuilder: Building pipeline from " << config_path
            << " with explicit options";

  auto graph = buildGraph(config_path);

  if (graph.hasCycle()) {
    LOG_ERRORS << "PipelineConfigBuilder: Graph contains a cycle";
    throw std::runtime_error("Graph contains a cycle");
  }

  return Pipeline::create()
      .withGraph(std::move(graph))
      .withContext(context ? std::move(context)
                           : std::make_shared<PipelineContext>())
      .withOptions(options)
      .build();
}

std::optional<Pipeline> PipelineConfigBuilder::tryBuildPipeline(
    const std::string &config_path, std::shared_ptr<PipelineContext> context,
    std::uint8_t num_workers) {
  try {
    auto [graph, options] = buildGraphAndOptions(config_path);

    if (num_workers > 0) {
      options.num_workers = num_workers;
    }

    if (graph.hasCycle()) {
      LOG_ERRORS << "PipelineConfigBuilder: Graph contains a cycle";
      return std::nullopt;
    }

    return Pipeline::create()
        .withGraph(std::move(graph))
        .withContext(context ? std::move(context)
                             : std::make_shared<PipelineContext>())
        .withOptions(options)
        .tryBuild();

  } catch (const std::exception &e) {
    LOG_ERRORS << "PipelineConfigBuilder: Failed to build pipeline: "
               << e.what();
    return std::nullopt;
  }
}

Graph PipelineConfigBuilder::buildGraph(const std::string &config_path) {
  LOG_INFOS << "PipelineConfigBuilder: Building graph from " << config_path;

  auto json = loadJsonFile(config_path);
  return parseGraphFromJson(json);
}

std::tuple<Graph, PipelineOptions>
PipelineConfigBuilder::buildGraphAndOptions(const std::string &config_path) {
  LOG_INFOS << "PipelineConfigBuilder: Building graph and options from "
            << config_path;

  auto json = loadJsonFile(config_path);

  Graph graph = parseGraphFromJson(json);
  PipelineOptions options = parseOptionsFromJson(json);

  LOG_INFOS << "PipelineConfigBuilder: Built successfully. Nodes: "
            << graph.getNodes().size() << ", Edges: " << graph.getEdges().size()
            << ", Engine: " << options.engine_type
            << ", Workers: " << static_cast<int>(options.num_workers);

  return {std::move(graph), options};
}

// =============================================================================
// Internal Helpers
// =============================================================================

nlohmann::json PipelineConfigBuilder::loadJsonFile(const std::string &path) {
  std::ifstream file(path);
  if (!file.is_open()) {
    LOG_ERRORS << "PipelineConfigBuilder: Failed to open file: " << path;
    throw std::runtime_error("Failed to open config file: " + path);
  }

  nlohmann::json json;
  try {
    file >> json;
  } catch (const nlohmann::json::parse_error &e) {
    LOG_ERRORS << "PipelineConfigBuilder: JSON parse error: " << e.what();
    throw std::runtime_error("Failed to parse JSON: " + std::string(e.what()));
  }

  return json;
}

Graph PipelineConfigBuilder::parseGraphFromJson(const nlohmann::json &json) {
  Graph graph;

  // Parse nodes
  if (!json.contains("nodes") || !json["nodes"].is_array()) {
    LOG_ERRORS << "PipelineConfigBuilder: Missing 'nodes' array";
    throw std::runtime_error("Config missing 'nodes' array");
  }

  for (const auto &node_config : json["nodes"]) {
    std::string name = node_config.at("name").get<std::string>();
    std::string type = node_config.at("type").get<std::string>();

    LOG_INFOS << "PipelineConfigBuilder: Creating node '" << name
              << "' of type '" << type << "'";

    // Check if parser is registered
    if (!NodeParamParserFactory::instance().isRegistered(type)) {
      LOG_ERRORS << "PipelineConfigBuilder: Unknown node type: " << type;
      throw std::runtime_error("Node type not registered: " + type);
    }

    // Parse node parameters
    NodeConstructParams params;
    params.setParam("name", name);

    auto parser = NodeParamParserFactory::instance().create(type);
    parser->parse(node_config, params, name, type);

    // Create node instance
    auto node = NodeCreatorFactory::instance().create(type, params);
    if (!node) {
      LOG_ERRORS << "PipelineConfigBuilder: Failed to create node: " << name;
      throw std::runtime_error("Failed to create node: " + name);
    }

    graph.addNode(node);
  }

  // Parse edges
  if (json.contains("edges") && json["edges"].is_array()) {
    for (const auto &edge_config : json["edges"]) {
      std::string from_node = edge_config.at("from_node").get<std::string>();
      std::string from_port = edge_config.at("from_port").get<std::string>();
      std::string to_node = edge_config.at("to_node").get<std::string>();
      std::string to_port = edge_config.at("to_port").get<std::string>();

      LOG_INFOS << "PipelineConfigBuilder: Adding edge " << from_node << ":"
                << from_port << " -> " << to_node << ":" << to_port;

      if (!graph.addEdge(from_node, from_port, to_node, to_port)) {
        LOG_ERRORS << "PipelineConfigBuilder: Failed to add edge";
        throw std::runtime_error("Failed to add edge: " + from_node + ":" +
                                 from_port + " -> " + to_node + ":" + to_port);
      }
    }
  } else {
    LOG_WARNINGS << "PipelineConfigBuilder: No 'edges' array found";
  }

  LOG_INFOS << "PipelineConfigBuilder: Graph parsed. Nodes: "
            << graph.getNodes().size()
            << ", Edges: " << graph.getEdges().size();

  return graph;
}

PipelineOptions
PipelineConfigBuilder::parseOptionsFromJson(const nlohmann::json &json) {
  PipelineOptions options;

  if (!json.contains("pipeline")) {
    LOG_INFOS << "PipelineConfigBuilder: No 'pipeline' section, using defaults";
    return options;
  }

  const auto &pipeline_config = json["pipeline"];

  // Parse engine_type
  if (pipeline_config.contains("engine_type") &&
      pipeline_config["engine_type"].is_string()) {
    options.engine_type = pipeline_config["engine_type"].get<std::string>();
    LOG_INFOS << "PipelineConfigBuilder: engine_type = " << options.engine_type;
  }

  // Parse num_workers
  if (pipeline_config.contains("num_workers") &&
      pipeline_config["num_workers"].is_number_integer()) {
    options.num_workers = pipeline_config["num_workers"].get<std::uint8_t>();
    LOG_INFOS << "PipelineConfigBuilder: num_workers = "
              << static_cast<int>(options.num_workers);
  }

  // Parse execution_timeout (optional, in milliseconds)
  if (pipeline_config.contains("execution_timeout_ms") &&
      pipeline_config["execution_timeout_ms"].is_number_integer()) {
    auto timeout_ms = pipeline_config["execution_timeout_ms"].get<int64_t>();
    options.execution_timeout = std::chrono::milliseconds{timeout_ms};
    LOG_INFOS << "PipelineConfigBuilder: execution_timeout = " << timeout_ms
              << "ms";
  }

  // Parse auto_reset_on_error (optional)
  if (pipeline_config.contains("auto_reset_on_error") &&
      pipeline_config["auto_reset_on_error"].is_boolean()) {
    options.auto_reset_on_error =
        pipeline_config["auto_reset_on_error"].get<bool>();
    LOG_INFOS << "PipelineConfigBuilder: auto_reset_on_error = "
              << std::boolalpha << options.auto_reset_on_error;
  }

  return options;
}

} // namespace ai_pipe::examples