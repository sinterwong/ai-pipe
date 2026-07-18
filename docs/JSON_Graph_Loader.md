# JSON Graph Loader

Declarative pipeline construction from JSON documents (F1, post-v0.5.0
roadmap). Node implementations register themselves once with the
`NodeRegistry` (`AI_PIPE_REGISTER_NODE` /
`AI_PIPE_REGISTER_NODE_WITH_CONFIG`); applications then describe the
graph — nodes, edges, and engine options — as data instead of C++.

## Enabling

The loader is an optional component gated by the CMake option
`AI_PIPE_WITH_JSON` (default `OFF`), backed by the vendored,
header-only `nlohmann/json` consumed privately — enabling it adds no
link-time or install-time dependency, and the core library stays
dependency-free when it is off.

```sh
cmake -B build -DAI_PIPE_WITH_JSON=ON ...
```

The API (`ai_pipe/graph_loader.hpp`) is always declared and linkable.
When the library was built without JSON support, every loader function
returns `ErrorCode::InvalidConfiguration`; call
`jsonGraphLoaderAvailable()` to detect support at runtime.

## API

```cpp
#include "ai_pipe/graph_loader.hpp"

// Graph only ("options" allowed but ignored):
Result<Graph> loadGraphFromJson(const std::string &json_text,
                                const NodeRegistry &registry = ...);
Result<Graph> loadGraphFromJsonFile(const std::string &path, ...);

// Graph + engine options:
Result<PipelineDescription> loadPipelineFromJson(...);
Result<PipelineDescription> loadPipelineFromJsonFile(...);

// struct PipelineDescription { Graph graph; PipelineOptions options; };
```

Typical use:

```cpp
auto description = ai_pipe::loadPipelineFromJsonFile("pipeline.json");
if (!description) { /* description.error().toString() */ }

auto pipeline = ai_pipe::Pipeline::create()
                    .withGraph(std::move(description.value().graph))
                    .withOptions(description.value().options)
                    .build();
```

## Document schema

```json
{
  "nodes": [
    {"type": "DecoderNode", "name": "decoder"},
    {"type": "DetectorNode", "name": "detector",
     "config": {
       "threshold": 0.5,
       "max_batch": 8,
       "model": "yolo.onnx",
       "enabled": true,
       "scales": [0.5, 1.0, 2.0],
       "classes": ["car", "person"]
     }}
  ],
  "edges": [
    {"from": "decoder.output", "to": "detector.input"},
    {"from": {"node": "decoder", "port": "output"},
     "to":   {"node": "detector", "port": "input"}}
  ],
  "options": {
    "mode": "stream",
    "num_workers": 4,
    "execution_timeout_ms": 0,
    "queue_capacity": 16,
    "drop_strategy": "DropHead",
    "enable_sync_coordination": true,
    "enable_statistics": true
  }
}
```

The schema is strict: unknown keys anywhere (top level, node entries,
edge entries, options) are rejected with an error naming the key, so
typos fail loudly instead of being silently ignored.

### `nodes` (required, non-empty array)

| Key      | Type   | Notes                                            |
|----------|--------|--------------------------------------------------|
| `type`   | string | Registered type name (`NodeRegistry`)            |
| `name`   | string | Graph-unique instance name, non-empty            |
| `config` | object | Optional; forwarded to the node factory (below)  |

Config values map to typed `PortData` params, matching what
`AI_PIPE_REGISTER_NODE_WITH_CONFIG` factories read:

| JSON value           | C++ param type              |
|----------------------|-----------------------------|
| `true` / `false`     | `bool`                      |
| integer              | `std::int64_t`              |
| float                | `double`                    |
| string               | `std::string`               |
| array of integers    | `std::vector<std::int64_t>` |
| numeric array w/ any float | `std::vector<double>` |
| array of strings     | `std::vector<std::string>`  |

Nulls, nested objects/arrays, mixed-type arrays, and empty arrays are
rejected — node configs are flat parameter bags by design. Note the
integer/double distinction is exact: a node reading
`param<double>("x")` will not find `"x": 1` (an integer); write
`1.0` in the document or read `int64_t` in the node.

### `edges` (optional array)

Each entry is `{"from": <endpoint>, "to": <endpoint>}`. An endpoint is
either the compact string `"node.port"` (exactly one `.`) or the object
`{"node": "...", "port": "..."}` — use the object form when a name
itself contains a dot, or to omit `port` for nodes that declare no
ports. Edges get the full `Graph::addEdge` validation: both nodes must
exist, ports must be declared, declared payload types must agree, and
duplicates are rejected. A cyclic description fails with
`GraphCycleDetected`.

### `options` (optional object)

`mode` (`"batch"` | `"stream"`) selects the factory defaults
(`PipelineOptions::batch()` / `PipelineOptions::stream()`); the
remaining keys override individual fields, so `{"mode": "stream"}`
alone is a fully sane streaming configuration.

| Key                        | Type    | Constraint            |
|----------------------------|---------|-----------------------|
| `mode`                     | string  | `"batch"`/`"stream"`  |
| `num_workers`              | integer | 1–255                 |
| `execution_timeout_ms`     | integer | >= 0 (0 = no timeout) |
| `queue_capacity`           | integer | >= 0                  |
| `drop_strategy`            | string  | e.g. `"DropHead"`     |
| `enable_sync_coordination` | boolean |                       |
| `enable_statistics`        | boolean |                       |
| `alignment_policy`         | string  | `"frame_id"`/`"stream_frame_id"`/`"timestamp"` |
| `alignment_tolerance_us`   | integer | >= 0 (`timestamp` policy pairing tolerance) |
| `join_wait_timeout_ms`     | integer | >= 0 (0 = wait indefinitely)  |
| `join_timeout_policy`      | string  | `"partial_inputs"`/`"skip_frame"` |

## Error reporting

All failures come back as `Result` errors (never exceptions):
`InvalidConfiguration` with a message locating the offending entity
(node name, config key, edge index, or option key); JSON syntax errors
include nlohmann's parse diagnostics; `GraphCycleDetected` for cyclic
graphs; `InvalidArgument` for unreadable files. Port/type-level edge
rejections additionally log details via the framework logger.
