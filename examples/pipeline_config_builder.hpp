/**
 * @file pipeline_config_builder.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Utility for building Pipeline from JSON configuration files
 * @version 0.2
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_PIPELINE_CONFIG_BUILDER_HPP
#define AI_PIPE_PIPELINE_CONFIG_BUILDER_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/pipeline.hpp"
#include "nlohmann/json.hpp"
#include <optional>
#include <string>
#include <tuple>

namespace ai_pipe::examples {

/**
 * @brief Utility class for building Pipeline from JSON configuration files
 *
 * This class provides static methods to:
 * - Build a Graph from JSON configuration
 * - Build a Pipeline with options from JSON configuration
 * - Extract both Graph and PipelineOptions from a single config file
 *
 * Note: This is distinct from ai_pipe::PipelineBuilder which is the fluent
 * builder pattern for constructing Pipeline instances programmatically.
 *
 * Usage:
 * @code
 *   // Build pipeline from config file
 *   auto pipeline = PipelineConfigBuilder::buildPipeline(
 *       "config.json", context, 4);
 *
 *   // Or with custom options
 *   auto pipeline = PipelineConfigBuilder::buildPipeline(
 *       "config.json", context, options);
 *
 *   // Or just build the graph
 *   auto graph = PipelineConfigBuilder::buildGraph("config.json");
 * @endcode
 */
class PipelineConfigBuilder {
public:
  /**
   * @brief Build a complete Pipeline from JSON configuration
   * @param config_path Path to JSON configuration file
   * @param context Shared pipeline context (optional, created if null)
   * @param num_workers Number of worker threads (overrides config if > 0)
   * @return Configured Pipeline ready for execution
   * @throws std::runtime_error on configuration or initialization errors
   */
  static Pipeline
  buildPipeline(const std::string &config_path,
                std::shared_ptr<PipelineContext> context = nullptr,
                std::uint8_t num_workers = 0);

  /**
   * @brief Build a Pipeline with explicit options
   * @param config_path Path to JSON configuration file
   * @param context Shared pipeline context
   * @param options Pipeline options (overrides config file settings)
   * @return Configured Pipeline ready for execution
   */
  static Pipeline buildPipeline(const std::string &config_path,
                                std::shared_ptr<PipelineContext> context,
                                const PipelineOptions &options);

  /**
   * @brief Try to build a Pipeline, returning nullopt on failure
   * @param config_path Path to JSON configuration file
   * @param context Shared pipeline context
   * @param num_workers Number of worker threads
   * @return Pipeline if successful, nullopt otherwise
   */
  static std::optional<Pipeline>
  tryBuildPipeline(const std::string &config_path,
                   std::shared_ptr<PipelineContext> context = nullptr,
                   std::uint8_t num_workers = 0);

  /**
   * @brief Build only a Graph from JSON configuration
   * @param config_path Path to JSON configuration file
   * @return Constructed Graph
   * @throws std::runtime_error on parsing or validation errors
   */
  static Graph buildGraph(const std::string &config_path);

  /**
   * @brief Build both Graph and PipelineOptions from configuration
   * @param config_path Path to JSON configuration file
   * @return Tuple of (Graph, PipelineOptions)
   * @throws std::runtime_error on parsing errors
   */
  static std::tuple<Graph, PipelineOptions>
  buildGraphAndOptions(const std::string &config_path);

  // -------------------------------------------------------------------------
  // Legacy API Compatibility (deprecated)
  // -------------------------------------------------------------------------

  /**
   * @brief Build pipeline from config (legacy name)
   * @deprecated Use buildPipeline() instead
   */
  [[deprecated("Use buildPipeline() instead")]]
  static Pipeline
  buildPipelineFromConfig(const std::string &config_path,
                          std::shared_ptr<PipelineContext> context = nullptr,
                          std::uint8_t num_workers = 1) {
    return buildPipeline(config_path, std::move(context), num_workers);
  }

  /**
   * @brief Build graph from config (legacy name)
   * @deprecated Use buildGraph() instead
   */
  [[deprecated("Use buildGraph() instead")]]
  static Graph buildGraphFromConfig(const std::string &config_path) {
    return buildGraph(config_path);
  }

private:
  // Internal helpers
  static nlohmann::json loadJsonFile(const std::string &path);
  static Graph parseGraphFromJson(const nlohmann::json &json);
  static PipelineOptions parseOptionsFromJson(const nlohmann::json &json);
};
} // namespace ai_pipe::examples

#endif // AI_PIPE_PIPELINE_CONFIG_BUILDER_HPP