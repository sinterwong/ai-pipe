/**
 * @file graph_loader.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Declarative JSON graph loading built on NodeRegistry
 * @version 0.1
 * @date 2026-07-10
 *
 * Loads a pipeline description - nodes (type/name/config), edges, and
 * engine options - from a JSON document and assembles a Graph by
 * instantiating each node through the NodeRegistry. This is the
 * config-file counterpart to programmatic graph construction: node
 * implementations register themselves once (AI_PIPE_REGISTER_NODE) and
 * applications describe pipelines as data.
 *
 * Document schema (see docs/JSON_Graph_Loader.md for the full
 * reference):
 *
 * @code{.json}
 * {
 *   "nodes": [
 *     {"type": "MyDetectorNode", "name": "detector",
 *      "config": {"threshold": 0.5}}
 *   ],
 *   "edges": [
 *     {"from": "decoder.output", "to": "detector.input"}
 *   ],
 *   "options": {"mode": "stream", "num_workers": 4}
 * }
 * @endcode
 *
 * Availability: the loader is an optional component gated by the CMake
 * option AI_PIPE_WITH_JSON (vendored, header-only nlohmann/json; the
 * core library stays dependency-free when OFF). The functions below are
 * always declared and linkable; when built without JSON support they
 * return ErrorCode::InvalidConfiguration. Query
 * jsonGraphLoaderAvailable() to detect support at runtime.
 *
 * @copyright Copyright (c) 2026
 */

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

/** @brief True when the library was built with AI_PIPE_WITH_JSON */
[[nodiscard]] bool jsonGraphLoaderAvailable();

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
