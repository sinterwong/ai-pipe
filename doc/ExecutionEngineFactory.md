# ExecutionEngine Factory 使用指南

## 概述

ExecutionEngine Factory 是 ai-pipe 的核心扩展机制，允许用户注册和使用不同的执行引擎实现。通过配置文件或代码，您可以轻松切换不同的执行引擎，而无需修改核心代码。

## 架构设计

### 工厂模式
ai-pipe 采用统一的工厂模式管理组件：
- **NodeFactory**: 管理计算节点的创建
- **ExecutionEngineFactory**: 管理执行引擎的创建

### 核心组件

1. **IExecutionEngine**: 执行引擎接口
2. **ExecutionEngineFactory**: 引擎工厂单例
3. **AI_PIPE_REGISTER_ENGINE**: 注册宏
4. **PipelineConfig**: 配置结构

## 注册新的 ExecutionEngine

### 步骤 1: 实现 IExecutionEngine 接口

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

### 步骤 2: 注册到工厂

```cpp
// my_custom_engine.cpp
#include "my_custom_engine.hpp"
#include "execution_engine_factory.hpp"

namespace my_project {

MyCustomEngine::MyCustomEngine() {
  // Implementation
}

// ... 实现其他接口方法 ...

} // namespace my_project

// 注册引擎到工厂
AI_PIPE_REGISTER_ENGINE(MyCustomEngine, my_project::MyCustomEngine)
```

## 通过配置文件使用

### JSON 配置格式

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

### 配置说明

- **pipeline** (可选): Pipeline 配置节
  - **engine_type** (可选): 执行引擎类型名称，默认为 "DefaultExecutionEngine"
  - **num_workers** (可选): 工作线程数，默认为 1

### 使用 PipelineBuilder

```cpp
#include "pipeline_builder.hpp"

using namespace ai_pipe;
using namespace ai_pipe::examples;

// 从配置文件构建 Pipeline (自动读取 engine_type)
auto pipeline = PipelineBuilder::buildPipelineFromConfig(
    "config/my_pipeline.json",
    std::make_shared<PipelineContext>()
);

// 启动并运行
pipeline.start();
pipeline.feedDataAsync(initialInputs);
```

## 通过代码使用

### 方式 1: 使用 PipelineConfig

```cpp
#include "ai_pipe/pipeline.hpp"
#include "ai_pipe/config.hpp"

using namespace ai_pipe;

// 创建配置
PipelineConfig config;
config.engineType = "MyCustomEngine";
config.numWorkers = 4;

// 初始化 Pipeline
Pipeline pipeline;
Graph graph = buildYourGraph();
auto context = std::make_shared<PipelineContext>();

pipeline.initialize(std::move(graph), context, config);
```

### 方式 2: 直接创建引擎

```cpp
#include "execution_engine_factory.hpp"

using namespace ai_pipe;

// 创建特定类型的引擎
auto engine = createExecutionEngine("MyCustomEngine");

// 或者使用默认引擎
auto defaultEngine = createExecutionEngine();
```

### 方式 3: 向后兼容的方式

```cpp
// 使用旧的 API（默认使用 DefaultExecutionEngine）
pipeline.initialize(std::move(graph), context, 4);  // numWorkers = 4
```

## 内置执行引擎

### DefaultExecutionEngine

默认的执行引擎，提供基本的并行执行能力。

**特性:**
- 线程池并行执行
- 数据流驱动调度
- 节点状态管理
- 错误处理和回调

**适用场景:**
- 一般的 DAG 流水线
- 节点执行时间较均匀
- 数据量适中

**配置示例:**
```json
{
  "pipeline": {
    "engine_type": "DefaultExecutionEngine",
    "num_workers": 4
  }
}
```

## 高级用法

### 自定义引擎参数

如果您的引擎需要额外参数，可以扩展 EngineConstructParams:

```cpp
#include "execution_engine_factory.hpp"

// 创建参数
EngineConstructParams params;
params.setParam("buffer_size", 1024);
params.setParam("timeout_ms", 5000);

// 使用参数创建引擎
auto engine = createExecutionEngine("MyCustomEngine", params);
```

然后在注册宏中处理参数：

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

### 运行时查询可用引擎

```cpp
// 检查引擎是否已注册
if (ExecutionEngineFactory::instance().isRegistered("MyCustomEngine")) {
    std::cout << "MyCustomEngine is available" << std::endl;
}
```

## 示例：实现带背压机制的引擎

```cpp
// backpressure_engine.hpp
class BackpressureEngine : public IExecutionEngine {
public:
  BackpressureEngine(size_t maxQueueSize = 100);

  bool execute(const PortDataMap &initialInputs,
               bool waitForCompletion,
               std::shared_ptr<PipelineContext> context) override {
    // 检查队列大小
    if (inputQueue_.size() >= maxQueueSize_) {
      // 应用背压：丢弃或等待
      return false;
    }

    // 正常执行
    return executeInternal(initialInputs, waitForCompletion, context);
  }

private:
  size_t maxQueueSize_;
  std::queue<PortDataMap> inputQueue_;
};

// 注册
AI_PIPE_REGISTER_ENGINE(BackpressureEngine, BackpressureEngine)
```

配置使用：
```json
{
  "pipeline": {
    "engine_type": "BackpressureEngine",
    "num_workers": 4
  }
}
```

## 最佳实践

1. **命名约定**: 引擎类型名称使用 PascalCase，例如 "DefaultExecutionEngine"
2. **线程安全**: 确保您的引擎实现是线程安全的
3. **错误处理**: 适当使用 setPipelineErrorCallback 报告错误
4. **资源管理**: 在 stopExecutionSync() 中正确清理资源
5. **日志记录**: 使用 LOG_INFOS/LOG_ERRORS 记录关键事件
6. **测试**: 为新引擎编写单元测试和集成测试

## 故障排查

### 引擎未注册错误

```
Factory error: Class 'MyEngine' not registered for base type 'IExecutionEngine'
```

**解决方案:**
1. 确保使用了 `AI_PIPE_REGISTER_ENGINE` 宏
2. 确保引擎实现文件被编译并链接
3. 检查引擎名称拼写是否正确

### 配置文件不生效

**检查清单:**
1. JSON 格式是否正确
2. "pipeline" 节是否正确嵌套
3. "engine_type" 拼写是否正确
4. 使用 `buildPipelineFromConfig` 而不是手动解析

## 参考资料

- [IExecutionEngine 接口文档](../src/execution_engine_base.hpp)
- [DefaultExecutionEngine 实现](../src/default_execution_engine.hpp)
- [Factory 模式实现](../src/api/ai_pipe/type_safe_factory.hpp)
- [示例配置文件](../assets/conf/through_pass_pipe_with_engine.json)
