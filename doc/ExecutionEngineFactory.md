# ExecutionEngine Factory Usage Guide

## Overview

The ExecutionEngine Factory is a core extension mechanism of ai-pipe, allowing users to register and use different execution engine implementations. You can easily switch between different execution engines through configuration files or code without modifying the core code.

## Architecture Design

### Factory Pattern
ai-pipe uses a unified factory pattern to manage components:
- **NodeFactory**: Manages the creation of computation nodes
- **ExecutionEngineFactory**: Manages the creation of execution engines

### Core Components

1. **IExecutionEngine**: Execution engine interface
2. **ExecutionEngineFactory**: Engine factory singleton
3. **AI_PIPE_REGISTER_ENGINE**: Registration macro
4. **PipelineConfig**: Configuration structure

## Registering a New ExecutionEngine

### Step 1: Implement the IExecutionEngine Interface

```cpp
// my_custom_engine.hpp
#ifndef MY_CUSTOM_ENGINE_HPP
#define MY_CUSTOM_ENGINE_HPP

#include "execution_engine_base.hpp"

namespace my_project {

class MyCustomEngine : public ai_pipe::IExecutionEngine {
public:
  MyCustomEngine();
  ~MyCustomEngine() override;

  bool initialize(ai_pipe::Graph *graph, uint8_t numWorkers) override;
  bool execute(const ai_pipe::PortDataMap &initialInputs,
               bool waitForCompletion,
               std::shared_ptr<ai_pipe::PipelineContext> context) override;

  void stopExecutionAsync() override;
  void stopExecutionSync() override;
  void reset() override;

  ai_pipe::EngineState getState() const override;

  void setPipelineResultCallback(
      std::function<void(const ai_pipe::PortDataMap &)> callback) override;
  void setPipelineErrorCallback(
      std::function<void(const std::string &, const std::string &)> callback) override;

  std::unordered_map<std::string, ai_pipe::NodeExecutionState>
  getNodeStates() const override;

private:
  // Your custom implementation details
};

} // namespace my_project

#endif
```

### Step 2: Register with the Factory

```cpp
// my_custom_engine.cpp
#include "my_custom_engine.hpp"
#include "execution_engine_factory.hpp"

namespace my_project {

MyCustomEngine::MyCustomEngine() {
  // Implementation
}

// ... implement other interface methods ...

} // namespace my_project

// Register the engine with the factory
AI_PIPE_REGISTER_ENGINE(MyCustomEngine, my_project::MyCustomEngine)
```

## Usage via Configuration File

### JSON Configuration Format

```json
{
  "pipeline": {
    "engine_type": "MyCustomEngine",
    "num_workers": 4
  },
  "nodes": [
    {
      "name": "node1",
      "type": "ThroughPassNode",
      "params": {...}
    }
  ],
  "edges": [
    {
      "from_node": "node1",
      "from_port": "output",
      "to_node": "node2",
      "to_port": "input"
    }
  ]
}
```

### Configuration Description

- **pipeline** (optional): Pipeline configuration section
  - **engine_type** (optional): Execution engine type name, defaults to "DefaultExecutionEngine"
  - **num_workers** (optional): Number of worker threads, defaults to 1

### Using PipelineBuilder

```cpp
#include "pipeline_builder.hpp"

using namespace ai_pipe;
using namespace ai_pipe::examples;

// Build the Pipeline from a configuration file (automatically reads engine_type)
auto pipeline = PipelineBuilder::buildPipelineFromConfig(
    "config/my_pipeline.json",
    std::make_shared<PipelineContext>()
);

// Start and run
pipeline.start();
pipeline.feedDataAsync(initialInputs);
```

## Usage via Code

### Method 1: Using PipelineConfig

```cpp
#include "ai_pipe/pipeline.hpp"
#include "ai_pipe/config.hpp"

using namespace ai_pipe;

// Create a configuration
PipelineConfig config;
config.engineType = "MyCustomEngine";
config.numWorkers = 4;

// Initialize the Pipeline
Pipeline pipeline;
Graph graph = buildYourGraph();
auto context = std::make_shared<PipelineContext>();

pipeline.initialize(std::move(graph), context, config);
```

### Method 2: Creating the Engine Directly

```cpp
#include "execution_engine_factory.hpp"

using namespace ai_pipe;

// Create a specific type of engine
auto engine = createExecutionEngine("MyCustomEngine");

// Or use the default engine
auto defaultEngine = createExecutionEngine();
```

### Method 3: Backward-Compatible Approach

```cpp
// Use the old API (defaults to DefaultExecutionEngine)
pipeline.initialize(std::move(graph), context, 4);  // numWorkers = 4
```

## Built-in Execution Engines

### DefaultExecutionEngine

The default execution engine, providing basic parallel execution capabilities.

**Features:**
- Thread pool for parallel execution
- Dataflow-driven scheduling
- Node state management
- Error handling and callbacks

**Applicable Scenarios:**
- General DAG pipelines
- Relatively uniform node execution times
- Moderate data volumes

**Configuration Example:**
```json
{
  "pipeline": {
    "engine_type": "DefaultExecutionEngine",
    "num_workers": 4
  }
}
```

## Advanced Usage

### Custom Engine Parameters

If your engine requires additional parameters, you can extend `EngineConstructParams`:

```cpp
#include "execution_engine_factory.hpp"

// Create parameters
EngineConstructParams params;
params.setParam("buffer_size", 1024);
params.setParam("timeout_ms", 5000);

// Create the engine with parameters
auto engine = createExecutionEngine("MyCustomEngine", params);
```

Then handle the parameters in the registration macro:

```cpp
#define AI_PIPE_REGISTER_ENGINE_WITH_PARAMS(EngineType, EngineClass)          \
  namespace {                                                                  \
  [[maybe_unused]] const auto ___##EngineType##Registration__ =               \
      ai_pipe::ExecutionEngineFactory::instance().registerCreator(            \
          #EngineType,                                                         \
          [](const ai_pipe::EngineConstructParams &params)                     \
              -> std::shared_ptr<ai_pipe::IExecutionEngine> {                  \
            auto bufferSize = params.getOptionalParam<int>("buffer_size")      \
                                  .value_or(512);                              \
            auto timeout = params.getOptionalParam<int>("timeout_ms")          \
                               .value_or(1000);                                \
            return std::make_shared<EngineClass>(bufferSize, timeout);         \
          });                                                                  \
  }
```

### Querying Available Engines at Runtime

```cpp
// Check if an engine is registered
if (ExecutionEngineFactory::instance().isRegistered("MyCustomEngine")) {
    std::cout << "MyCustomEngine is available" << std::endl;
}
```

## Example: Implementing an Engine with Backpressure

```cpp
// backpressure_engine.hpp
class BackpressureEngine : public IExecutionEngine {
public:
  BackpressureEngine(size_t maxQueueSize = 100);

  bool execute(const PortDataMap &initialInputs,
               bool waitForCompletion,
               std::shared_ptr<PipelineContext> context) override {
    // Check queue size
    if (inputQueue_.size() >= maxQueueSize_) {
      // Apply backpressure: drop or wait
      return false;
    }

    // Normal execution
    return executeInternal(initialInputs, waitForCompletion, context);
  }

private:
  size_t maxQueueSize_;
  std::queue<PortDataMap> inputQueue_;
};

// Registration
AI_PIPE_REGISTER_ENGINE(BackpressureEngine, BackpressureEngine)
```

Configuration for usage:
```json
{
  "pipeline": {
    "engine_type": "BackpressureEngine",
    "num_workers": 4
  }
}
```

## Best Practices

1. **Naming Convention**: Use PascalCase for engine type names, e.g., "DefaultExecutionEngine"
2. **Thread Safety**: Ensure your engine implementation is thread-safe
3. **Error Handling**: Use `setPipelineErrorCallback` appropriately to report errors
4. **Resource Management**: Clean up resources correctly in `stopExecutionSync()`
5. **Logging**: Use `LOG_INFOS`/`LOG_ERRORS` to record key events
6. **Testing**: Write unit and integration tests for new engines

## Troubleshooting

### "Engine Not Registered" Error

```
Factory error: Class 'MyEngine' not registered for base type 'IExecutionEngine'
```

**Solutions:**
1. Ensure the `AI_PIPE_REGISTER_ENGINE` macro is used
2. Ensure the engine implementation file is compiled and linked
3. Check for correct spelling of the engine name

### Configuration File Not Taking Effect

**Checklist:**
1. Is the JSON format correct?
2. Is the "pipeline" section nested correctly?
3. Is "engine_type" spelled correctly?
4. Are you using `buildPipelineFromConfig` instead of manual parsing?

## References

- [IExecutionEngine Interface Documentation](../src/execution_engine_base.hpp)
- [DefaultExecutionEngine Implementation](../src/default_execution_engine.hpp)
- [Factory Pattern Implementation](../src/api/ai_pipe/type_safe_factory.hpp)
- [Example Configuration File](../assets/conf/through_pass_pipe_with_engine.json)
