#include "ai_pipe/graph_loader.hpp"

#ifdef AI_PIPE_WITH_JSON

#include <cstdint>
#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>
#include <sstream>
#include <vector>

namespace ai_pipe {
namespace {

using nlohmann::json;

Result<void> err(std::string message) {
  return Result<void>::err(ErrorCode::InvalidConfiguration, std::move(message));
}

// Reject document keys the schema does not define (typo guard)
Result<void> checkKnownKeys(const json &obj, const char *what,
                            std::initializer_list<const char *> allowed) {
  for (const auto &item : obj.items()) {
    bool known = false;
    for (const char *key : allowed) {
      if (item.key() == key) {
        known = true;
        break;
      }
    }
    if (!known) {
      return err(std::string("Unknown key '") + item.key() + "' in " + what);
    }
  }
  return Result<void>::ok();
}

// Convert a JSON config value into a typed PortData param
//
// Scalar mapping: bool -> bool, integer -> int64_t, float -> double,
// string -> std::string. Homogeneous arrays map to std::vector of the
// element type (a numeric array containing any float becomes
// std::vector<double>). Nested objects/arrays and nulls are rejected -
// node configs are flat parameter bags by design.
Result<void> setConfigParam(PortData &config, const std::string &node_name,
                            const std::string &key, const json &value) {
  auto fail = [&](const char *reason) {
    return err("Node '" + node_name + "' config key '" + key + "': " + reason);
  };

  switch (value.type()) {
  case json::value_t::boolean:
    config.setParam(key, value.get<bool>());
    return Result<void>::ok();
  case json::value_t::number_integer:
    config.setParam(key, value.get<std::int64_t>());
    return Result<void>::ok();
  case json::value_t::number_unsigned:
    if (value.get<std::uint64_t>() >
        static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
      return fail("integer value exceeds int64 range");
    }
    config.setParam(key, value.get<std::int64_t>());
    return Result<void>::ok();
  case json::value_t::number_float:
    config.setParam(key, value.get<double>());
    return Result<void>::ok();
  case json::value_t::string:
    config.setParam(key, value.get<std::string>());
    return Result<void>::ok();
  case json::value_t::array: {
    bool all_int = true;
    bool all_number = true;
    bool all_string = true;
    for (const auto &element : value) {
      all_string = all_string && element.is_string();
      all_number = all_number && element.is_number();
      all_int = all_int && element.is_number_integer();
    }
    if (value.empty()) {
      return fail("empty arrays are ambiguous; omit the key or use a "
                  "non-empty typed array");
    }
    if (all_string) {
      config.setParam(key, value.get<std::vector<std::string>>());
      return Result<void>::ok();
    }
    if (all_int) {
      config.setParam(key, value.get<std::vector<std::int64_t>>());
      return Result<void>::ok();
    }
    if (all_number) {
      config.setParam(key, value.get<std::vector<double>>());
      return Result<void>::ok();
    }
    return fail("arrays must be homogeneous numbers or strings");
  }
  default:
    return fail("unsupported value type (allowed: bool, integer, float, "
                "string, flat array of numbers or strings)");
  }
}

// Parse one edge endpoint: "node.port" string or {node, port}
//
// The compact string form splits on a single mandatory '.'; names that
// themselves contain dots must use the object form.
Result<void> parseEndpoint(const json &endpoint, const char *side,
                           std::size_t edge_index, std::string &node,
                           std::string &port) {
  const std::string where =
      "edge #" + std::to_string(edge_index) + " '" + side + "'";
  if (endpoint.is_string()) {
    const auto text = endpoint.get<std::string>();
    const auto dot = text.find('.');
    if (dot == std::string::npos || dot != text.rfind('.') || dot == 0 ||
        dot + 1 == text.size()) {
      return err(where +
                 ": expected \"node.port\" with exactly one '.' "
                 "(got '" +
                 text + "'); use the object form for dotted names");
    }
    node = text.substr(0, dot);
    port = text.substr(dot + 1);
    return Result<void>::ok();
  }
  if (endpoint.is_object()) {
    if (auto known = checkKnownKeys(endpoint, where.c_str(), {"node", "port"});
        !known) {
      return known;
    }
    if (!endpoint.contains("node") || !endpoint["node"].is_string()) {
      return err(where + ": object form requires a string 'node'");
    }
    node = endpoint["node"].get<std::string>();
    port = endpoint.value("port", std::string{});
    if (endpoint.contains("port") && !endpoint["port"].is_string()) {
      return err(where + ": 'port' must be a string");
    }
    return Result<void>::ok();
  }
  return err(where + ": expected a \"node.port\" string or "
                     "{\"node\": ..., \"port\": ...} object");
}

// Read a non-negative integer option, bounded to [min, max]
Result<std::uint64_t> parseUintOption(const json &value, const char *key,
                                      std::uint64_t min, std::uint64_t max) {
  if (!value.is_number_unsigned() && !value.is_number_integer()) {
    return Result<std::uint64_t>::err(ErrorCode::InvalidConfiguration,
                                      std::string("Option '") + key +
                                          "' must be an integer");
  }
  if (value.is_number_integer() && value.get<std::int64_t>() < 0) {
    return Result<std::uint64_t>::err(ErrorCode::InvalidConfiguration,
                                      std::string("Option '") + key +
                                          "' must be non-negative");
  }
  const auto parsed = value.get<std::uint64_t>();
  if (parsed < min || parsed > max) {
    return Result<std::uint64_t>::err(
        ErrorCode::InvalidConfiguration,
        std::string("Option '") + key + "' out of range [" +
            std::to_string(min) + ", " + std::to_string(max) + "]");
  }
  return parsed;
}

Result<PipelineOptions> parseOptions(const json &doc) {
  if (!doc.contains("options")) {
    return PipelineOptions{};
  }
  const json &opts = doc["options"];
  if (!opts.is_object()) {
    return Result<PipelineOptions>::err(ErrorCode::InvalidConfiguration,
                                        "'options' must be an object");
  }
  if (auto known = checkKnownKeys(
          opts, "'options'",
          {"mode", "num_workers", "execution_timeout_ms", "queue_capacity",
           "drop_strategy", "enable_sync_coordination", "enable_statistics",
           "alignment_policy", "alignment_tolerance_us", "join_wait_timeout_ms",
           "join_timeout_policy"});
      !known) {
    return Result<PipelineOptions>::err(known.error());
  }

  // Mode selects the factory defaults; explicit keys then override, so
  // e.g. {"mode": "stream"} alone yields the same sane defaults as
  // PipelineOptions::stream().
  PipelineOptions options;
  if (opts.contains("mode")) {
    if (!opts["mode"].is_string()) {
      return Result<PipelineOptions>::err(ErrorCode::InvalidConfiguration,
                                          "Option 'mode' must be a string");
    }
    const auto mode = opts["mode"].get<std::string>();
    if (mode == "batch") {
      options = PipelineOptions::batch();
    } else if (mode == "stream") {
      options = PipelineOptions::stream();
    } else {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'mode' must be \"batch\" or \"stream\" (got '" + mode + "')");
    }
  }

  if (opts.contains("num_workers")) {
    auto workers = parseUintOption(opts["num_workers"], "num_workers", 1, 255);
    if (!workers) {
      return Result<PipelineOptions>::err(workers.error());
    }
    options.num_workers = static_cast<std::uint8_t>(workers.value());
  }
  if (opts.contains("execution_timeout_ms")) {
    auto timeout =
        parseUintOption(opts["execution_timeout_ms"], "execution_timeout_ms", 0,
                        std::numeric_limits<std::int64_t>::max());
    if (!timeout) {
      return Result<PipelineOptions>::err(timeout.error());
    }
    options.execution_timeout =
        std::chrono::milliseconds(static_cast<std::int64_t>(timeout.value()));
  }
  if (opts.contains("queue_capacity")) {
    auto capacity = parseUintOption(opts["queue_capacity"], "queue_capacity", 0,
                                    std::numeric_limits<std::size_t>::max());
    if (!capacity) {
      return Result<PipelineOptions>::err(capacity.error());
    }
    options.queue_capacity = static_cast<std::size_t>(capacity.value());
  }
  if (opts.contains("drop_strategy")) {
    if (!opts["drop_strategy"].is_string()) {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'drop_strategy' must be a string");
    }
    options.drop_strategy = opts["drop_strategy"].get<std::string>();
  }
  if (opts.contains("enable_sync_coordination")) {
    if (!opts["enable_sync_coordination"].is_boolean()) {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'enable_sync_coordination' must be a boolean");
    }
    options.enable_sync_coordination =
        opts["enable_sync_coordination"].get<bool>();
  }
  if (opts.contains("enable_statistics")) {
    if (!opts["enable_statistics"].is_boolean()) {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'enable_statistics' must be a boolean");
    }
    options.enable_statistics = opts["enable_statistics"].get<bool>();
  }
  if (opts.contains("alignment_policy")) {
    if (!opts["alignment_policy"].is_string()) {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'alignment_policy' must be a string");
    }
    const auto policy = opts["alignment_policy"].get<std::string>();
    if (policy == "frame_id") {
      options.alignment_policy = AlignmentPolicy::FrameId;
    } else if (policy == "stream_frame_id") {
      options.alignment_policy = AlignmentPolicy::StreamFrameId;
    } else if (policy == "timestamp") {
      options.alignment_policy = AlignmentPolicy::Timestamp;
    } else {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'alignment_policy' must be \"frame_id\", "
          "\"stream_frame_id\" or \"timestamp\" (got '" +
              policy + "')");
    }
  }
  if (opts.contains("alignment_tolerance_us")) {
    auto tolerance = parseUintOption(opts["alignment_tolerance_us"],
                                     "alignment_tolerance_us", 0,
                                     std::numeric_limits<std::int64_t>::max());
    if (!tolerance) {
      return Result<PipelineOptions>::err(tolerance.error());
    }
    options.alignment_tolerance =
        std::chrono::microseconds(static_cast<std::int64_t>(tolerance.value()));
  }
  if (opts.contains("join_wait_timeout_ms")) {
    auto timeout =
        parseUintOption(opts["join_wait_timeout_ms"], "join_wait_timeout_ms", 0,
                        std::numeric_limits<std::int64_t>::max());
    if (!timeout) {
      return Result<PipelineOptions>::err(timeout.error());
    }
    options.join_wait_timeout =
        std::chrono::milliseconds(static_cast<std::int64_t>(timeout.value()));
  }
  if (opts.contains("join_timeout_policy")) {
    if (!opts["join_timeout_policy"].is_string()) {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'join_timeout_policy' must be a string");
    }
    const auto policy = opts["join_timeout_policy"].get<std::string>();
    if (policy == "partial_inputs") {
      options.join_timeout_policy = JoinTimeoutPolicy::PartialInputs;
    } else if (policy == "skip_frame") {
      options.join_timeout_policy = JoinTimeoutPolicy::SkipFrame;
    } else {
      return Result<PipelineOptions>::err(
          ErrorCode::InvalidConfiguration,
          "Option 'join_timeout_policy' must be \"partial_inputs\" or "
          "\"skip_frame\" (got '" +
              policy + "')");
    }
  }
  return options;
}

Result<Graph> assembleGraph(const json &doc, const NodeRegistry &registry) {
  if (!doc.is_object()) {
    return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                              "Top-level JSON value must be an object");
  }
  if (auto known = checkKnownKeys(doc, "top-level document",
                                  {"nodes", "edges", "options"});
      !known) {
    return Result<Graph>::err(known.error());
  }
  if (!doc.contains("nodes") || !doc["nodes"].is_array() ||
      doc["nodes"].empty()) {
    return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                              "'nodes' must be a non-empty array");
  }

  Graph graph;
  for (const json &entry : doc["nodes"]) {
    if (!entry.is_object()) {
      return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                "Each 'nodes' entry must be an object");
    }
    if (auto known = checkKnownKeys(entry, "a 'nodes' entry",
                                    {"type", "name", "config"});
        !known) {
      return Result<Graph>::err(known.error());
    }
    if (!entry.contains("type") || !entry["type"].is_string() ||
        !entry.contains("name") || !entry["name"].is_string()) {
      return Result<Graph>::err(
          ErrorCode::InvalidConfiguration,
          "Each 'nodes' entry requires string 'type' and 'name'");
    }
    const auto type = entry["type"].get<std::string>();
    const auto name = entry["name"].get<std::string>();
    if (name.empty()) {
      return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                "Node names must be non-empty");
    }

    PortData config;
    if (entry.contains("config")) {
      if (!entry["config"].is_object()) {
        return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                  "Node '" + name +
                                      "': 'config' must be an object");
      }
      for (const auto &item : entry["config"].items()) {
        if (auto set = setConfigParam(config, name, item.key(), item.value());
            !set) {
          return Result<Graph>::err(set.error());
        }
      }
    }

    auto node = registry.create(type, name, config);
    if (!node) {
      std::string message = "Node '";
      message += name;
      message += "' (type '";
      message += type;
      message += "') could not be created: ";
      message += node.error().toString();
      return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                std::move(message));
    }
    if (!graph.addNode(node.value())) {
      return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                "Duplicate node name: '" + name + "'");
    }
  }

  if (doc.contains("edges")) {
    if (!doc["edges"].is_array()) {
      return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                "'edges' must be an array");
    }
    std::size_t index = 0;
    for (const json &entry : doc["edges"]) {
      if (!entry.is_object()) {
        return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                  "edge #" + std::to_string(index) +
                                      ": each entry must be an object");
      }
      const std::string where = "edge #" + std::to_string(index);
      if (auto known = checkKnownKeys(entry, where.c_str(), {"from", "to"});
          !known) {
        return Result<Graph>::err(known.error());
      }
      if (!entry.contains("from") || !entry.contains("to")) {
        return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                  where + ": requires 'from' and 'to'");
      }
      std::string from_node;
      std::string from_port;
      std::string to_node;
      std::string to_port;
      if (auto parsed =
              parseEndpoint(entry["from"], "from", index, from_node, from_port);
          !parsed) {
        return Result<Graph>::err(parsed.error());
      }
      if (auto parsed =
              parseEndpoint(entry["to"], "to", index, to_node, to_port);
          !parsed) {
        return Result<Graph>::err(parsed.error());
      }
      // addEdge validates existence of both nodes, declared ports,
      // payload-type agreement, and duplicate edges; details are logged
      // by the graph, so the error here locates the offending entry.
      if (!graph.addEdge(from_node, from_port, to_node, to_port)) {
        std::string message = where;
        message += " (";
        message += from_node;
        message += '.';
        message += from_port;
        message += " -> ";
        message += to_node;
        message += '.';
        message += to_port;
        message += ") was rejected: check node names, declared ports, "
                   "payload types, and duplicates (see log for details)";
        return Result<Graph>::err(ErrorCode::InvalidConfiguration,
                                  std::move(message));
      }
      ++index;
    }
  }

  if (graph.hasCycle()) {
    return Result<Graph>::err(ErrorCode::GraphCycleDetected,
                              "The described graph contains a cycle");
  }
  return Result<Graph>::ok(std::move(graph));
}

Result<json> parseDocument(const std::string &json_text) {
  try {
    return Result<json>::ok(json::parse(json_text));
  } catch (const json::parse_error &e) {
    return Result<json>::err(ErrorCode::InvalidConfiguration,
                             std::string("JSON parse error: ") + e.what());
  }
}

Result<std::string> readFile(const std::string &path) {
  std::ifstream stream(path, std::ios::binary);
  if (!stream) {
    return Result<std::string>::err(ErrorCode::InvalidArgument,
                                    "Cannot open JSON graph file: " + path);
  }
  std::ostringstream buffer;
  buffer << stream.rdbuf();
  return buffer.str();
}

} // namespace

bool jsonGraphLoaderAvailable() { return true; }

Result<Graph> loadGraphFromJson(const std::string &json_text,
                                const NodeRegistry &registry) {
  auto doc = parseDocument(json_text);
  if (!doc) {
    return Result<Graph>::err(doc.error());
  }
  return assembleGraph(doc.value(), registry);
}

Result<Graph> loadGraphFromJsonFile(const std::string &path,
                                    const NodeRegistry &registry) {
  auto text = readFile(path);
  if (!text) {
    return Result<Graph>::err(text.error());
  }
  return loadGraphFromJson(text.value(), registry);
}

Result<PipelineDescription> loadPipelineFromJson(const std::string &json_text,
                                                 const NodeRegistry &registry) {
  auto doc = parseDocument(json_text);
  if (!doc) {
    return Result<PipelineDescription>::err(doc.error());
  }
  auto graph = assembleGraph(doc.value(), registry);
  if (!graph) {
    return Result<PipelineDescription>::err(graph.error());
  }
  auto options = parseOptions(doc.value());
  if (!options) {
    return Result<PipelineDescription>::err(options.error());
  }
  return PipelineDescription{std::move(graph).value(), options.value()};
}

Result<PipelineDescription>
loadPipelineFromJsonFile(const std::string &path,
                         const NodeRegistry &registry) {
  auto text = readFile(path);
  if (!text) {
    return Result<PipelineDescription>::err(text.error());
  }
  return loadPipelineFromJson(text.value(), registry);
}

} // namespace ai_pipe

#else // !AI_PIPE_WITH_JSON

namespace ai_pipe {
namespace {

template <typename T> Result<T> unavailable() {
  return Result<T>::err(
      ErrorCode::InvalidConfiguration,
      "AI Pipe was built without JSON support; reconfigure with "
      "-DAI_PIPE_WITH_JSON=ON to enable the graph loader");
}

} // namespace

bool jsonGraphLoaderAvailable() { return false; }

Result<Graph> loadGraphFromJson(const std::string &, const NodeRegistry &) {
  return unavailable<Graph>();
}

Result<Graph> loadGraphFromJsonFile(const std::string &, const NodeRegistry &) {
  return unavailable<Graph>();
}

Result<PipelineDescription> loadPipelineFromJson(const std::string &,
                                                 const NodeRegistry &) {
  return unavailable<PipelineDescription>();
}

Result<PipelineDescription> loadPipelineFromJsonFile(const std::string &,
                                                     const NodeRegistry &) {
  return unavailable<PipelineDescription>();
}

} // namespace ai_pipe

#endif // AI_PIPE_WITH_JSON
