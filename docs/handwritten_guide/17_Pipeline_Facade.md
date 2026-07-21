# 17. Pipeline 门面与 Builder 模式 (Pipeline)

在前 16 篇深度文档中，我们手搓并重构了 AI Pipe 底层所有的并发结构、调度引擎、对齐模块以及可观测机制。

然而，我们不能强求框架的使用者直接去和复杂的 `ExecutionEngine`、`ISchedulerStrategy` 或者是 PIMPL 各种内部状态去进行高并发、高门槛的直接交互。
`Pipeline` 扮演了整个框架的 **最高层门面（Facade）**，并配合 `PipelineBuilder` （构建者模式）以及一套极其严格的 **错误分级与管线状态机契约**，为用户提供极其清爽、流畅、且不可能用错的 Fluent 链式构建 API。

---

## 1. 核心设计原理

### 1.1 顶级管线状态机（Pipeline State Machine）
一个健壮的管线，其生命周期拥有极其严格的状态流转约束。任何越界的状态操作（例如对一个未初始化的管线调用 `start`，或者对正在运行的管线再次调用 `start`）都必须被安全、确定性地拦截：

```
+--------------------+
|   UNINITIALIZED    |
+--------------------+
          |  (Build Successful)
          v
+--------------------+
|        IDLE        |
+--------------------+
     |          ^
  (Start)    (Stop)
     v          |
+--------------------+          (Fatal Error Occurs)
|      RUNNING       |==================================+
+--------------------+                                  |
     |          ^                                       v
  (Pause)    (Resume)                           +--------------------+
     v          |                               |       ERROR        |
+--------------------+                          +--------------------+
|      PAUSED        |                                  |
+--------------------+                               (Reset)
                                                        v
                                                [Back to IDLE]
```

1.  **流式模式错误分级契约（Fatal vs Non-Fatal Error Contract）**：
    这是 AI Pipe 架构设计的精髓。当节点内产生 `process` 异常时，管线根据执行模式，触发完全不同的状态演进：
    *   **BATCH 模式（Fatal）**：
        由于一过性运行要求完美的数据确定性，任一节点抛出异常，都会导致当前 execute 被判定为 **Fatal Error**。管线瞬间中止运行，状态机强制切换到 **`ERROR`** 状态。此时，用户若再次强行调用 `run()` 或者是 `start()`，状态机会无条件抛出“处于错误状态、必须调用 `reset()` 清理”的保护性拦截错误。
    *   **STREAM 模式（Non-Fatal, Per-Frame Fail）**：
        实时视频流可能由于单帧图像损毁发生解码异常。这种单帧错误被引擎断定为 **Non-Fatal Per-Frame Error**。出错帧直接被消费、丢弃、回收，并触发 `onError` 观察者回调。
        **核心表现**：管线绝对不会因单帧异常而崩溃退出，状态机依然维持坚挺的 **`RUNNING`** 状态，继续无缝接受用户的 `pushInput()`，实现了强大的 7×24 工业级鲁棒保活。

---

## 2. 核心巧思与实现细节

### 2.1 观察者解耦模式（Observer Pattern）
如何让用户在主控线程上，无感、无竞争、且低延迟地捕获到管线内部 Worker 线程产生的各种事件（如结果输出、节点报错、协同丢帧等）？

**实现巧思**：
设计 `IPipelineObserver` 接口，并在 `Pipeline` 门面中支持链式注册 `CallbackObserver`：
```cpp
class IPipelineObserver {
public:
    virtual ~IPipelineObserver() = default;
    virtual void onResult(const PortDataMap& outputs) = 0;
    virtual void onError(const std::string& node, const Error& error) = 0;
    virtual void onDrop(const std::string& node, uint64_t frameId, const std::string& reason) = 0;
};
```
*   在引擎执行到 Sink 节点输出或协同丢帧时，Worker 线程直接安全调用注册进来的 `Observer` 方法。
*   由于 `Observer` 的方法被多个 Worker 并发回调，用户在实现具体的 callback 时，其回调函数**必须是线程安全（Thread-safe）且极其廉价的**（如果有重型消费如写盘、显示，应在回调内部推入用户的业务缓冲队列中去处理）。

### 2.2 Fluent Builder 模式完美链式配置
如何让整个框架参数（如 worker 数、队列限制、背压丢弃策略、同步协调、观察者回调）的配置极其优雅、甚至可以在 IDE 中实现无脑自动补全？

**实现巧思**：
实现 `PipelineBuilder` 类。Builder 内部维护一个 `EngineConfig` 结构，所有的设置方法均返回 **Builder 自身引用（`PipelineBuilder&`）**：
```cpp
class PipelineBuilder {
public:
    PipelineBuilder& withWorkers(uint8_t count) {
        m_config.num_workers = count;
        return *this;
    }
    PipelineBuilder& withQueueCapacity(size_t cap) {
        m_config.default_queue_capacity = cap;
        return *this;
    }
    PipelineBuilder& withSyncCoordination(bool enable) {
        m_config.enable_sync_coordination = enable;
        return *this;
    }
    PipelineBuilder& withDropStrategy(std::string strategy) {
        m_config.default_drop_strategy = std::move(strategy);
        return *this;
    }
    // ...
    Result<Pipeline> build(); // 终极组装
};
```
通过该 Fluent API 设计，用户只需：
```cpp
auto pipeline = Pipeline::create()
    .withGraph(std::move(graph))
    .withWorkers(8)
    .withQueueCapacity(16)
    .withSyncCoordination(true)
    .build();
```
配置在一行内呵成一气，极其舒适，且不透明地隐藏了所有底层微内核初始化细节。

---

## 3. 手搓实现参考骨架

你可以根据以下极其惊艳的顶级门面 `Pipeline` 与 `PipelineBuilder` 骨架进行手搓复习：

```cpp
enum class PipelineState {
    Uninitialized,
    Idle,
    Running,
    Error
};

class Pipeline {
public:
    class Impl; // 继续运用 PIMPL

    explicit Pipeline(std::unique_ptr<Impl> impl) : m_impl(std::move(impl)) {}
    ~Pipeline() = default;

    // Fluent API 起点
    static PipelineBuilder create();

    Result<void> start() {
        return m_impl->start();
    }

    void stop() {
        m_impl->stop();
    }

    Result<PushStatus> pushInput(const std::string& node, std::shared_ptr<DataPacket> data) {
        return m_impl->pushInput(node, std::move(data));
    }

    Result<ExecutionOutput> run(const PortDataMap& inputs) {
        return m_impl->run(inputs);
    }

    void reset() {
        m_impl->reset();
    }

    PipelineState state() const {
        return m_impl->state();
    }

private:
    std::unique_ptr<Impl> m_impl;
};

// ==================== 管线具体实现骨架 ====================
class Pipeline::Impl {
public:
    Impl(Graph graph, EngineConfig config)
        : m_graph(std::move(graph)), m_config(config), m_state(PipelineState::Idle) {
        m_engine = ExecutionEngine::create(m_config);
        m_engine->initialize(&m_graph, m_config.num_workers);
    }

    Result<void> start() {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        if (m_state == PipelineState::Running) {
            return Error(ErrorCode::InvalidArgument, "Pipeline already running");
        }
        if (m_state == PipelineState::Error) {
            return Error(ErrorCode::InvalidArgument, "Pipeline in Error state, call reset() first");
        }

        m_engine->startStreaming();
        m_state = PipelineState::Running;
        return {};
    }

    void stop() {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        if (m_state != PipelineState::Running) return;
        m_engine->stopStreaming(true); // wait for drain
        m_state = PipelineState::Idle;
    }

    Result<PushStatus> pushInput(const std::string& node, std::shared_ptr<DataPacket> data) {
        // 热路径不加锁，状态采用 atomic load 校验快速通道
        if (m_state != PipelineState::Running) {
            return Error(ErrorCode::InvalidArgument, "Pipeline not running, cannot push input");
        }
        return m_engine->pushInput(node, "input", std::move(data));
    }

    void reset() {
        std::lock_guard<std::mutex> lock(m_stateMutex);
        m_engine->reset();
        m_state = PipelineState::Idle;
    }

    PipelineState state() const { return m_state; }

private:
    Graph m_graph;
    EngineConfig m_config;
    std::unique_ptr<ExecutionEngine> m_engine;

    mutable std::mutex m_stateMutex;
    PipelineState m_state;
};
```

手搓顶级门面 `Pipeline` 期间，细细体会“将极其丑陋复杂的并发状态机和引擎路由，用最清爽干净的 Facade 包裹起来”的架构智慧。它是框架与开发者之间沟通的桥梁，也是你完满手搓、复习并封神整个 AI Pipe 并发框架的最华丽收尾。
