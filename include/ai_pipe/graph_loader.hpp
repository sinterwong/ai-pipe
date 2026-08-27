#ifndef AI_PIPE_GRAPH_LOADER_HPP
#define AI_PIPE_GRAPH_LOADER_HPP

#include "ai_pipe/error.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/pipeline.hpp"
#include <string>

namespace ai_pipe {

/**
 * @brief Parsed pipeline description: assembled graph plus engine options
 *
 * The graph is ready to hand to PipelineBuilder::withGraph; options
 * reflect the document's "options" object applied on top of the mode's
 * factory defaults (PipelineOptions::batch / PipelineOptions::stream).
 */
struct PipelineDescription {
  Graph graph;
  PipelineOptions options;
};

/**
 * @brief Assemble a Graph from a JSON document
 *
 * Parses "nodes" and "edges" (a top-level "options" object is allowed
 * and ignored, so a full pipeline document is also a valid graph
 * document). Every node is instantiated through @p registry; unknown
 * type names, duplicate node names, invalid edges, cycles, and schema
 * violations (unknown keys, wrong value types) are reported as errors.
 *
 * @param json_text UTF-8 JSON document text
 * @param registry Registry resolving node type names (defaults to the
 *                 process-wide singleton)
 * @return The assembled graph, or InvalidConfiguration /
 *         GraphCycleDetected with a message locating the offending
 *         entity
 */
[[nodiscard]] Result<Graph>
loadGraphFromJson(const std::string &json_text,
                  const NodeRegistry &registry = NodeRegistry::instance());

/** @brief File variant of loadGraphFromJson */
[[nodiscard]] Result<Graph>
loadGraphFromJsonFile(const std::string &path,
                      const NodeRegistry &registry = NodeRegistry::instance());

/**
 * @brief Load a full pipeline description (graph + engine options)
 *
 * In addition to loadGraphFromJson, parses the optional "options"
 * object into PipelineOptions. Defaults come from the selected mode's
 * factory ("batch" -> PipelineOptions::batch(), "stream" ->
 * PipelineOptions::stream()); explicit keys override them.
 */
[[nodiscard]] Result<PipelineDescription>
loadPipelineFromJson(const std::string &json_text,
                     const NodeRegistry &registry = NodeRegistry::instance());

/** @brief File variant of loadPipelineFromJson */
[[nodiscard]] Result<PipelineDescription> loadPipelineFromJsonFile(
    const std::string &path,
    const NodeRegistry &registry = NodeRegistry::instance());

} // namespace ai_pipe

#endif // AI_PIPE_GRAPH_LOADER_HPP
