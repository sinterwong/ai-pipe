# AI Pipe 框架设计说明文档

> **版本**: v0.5.0（与 `ai_pipe_version.hpp` 保持一致）
> **变更概览**: 见 `CHANGELOG.md` 与 `docs/Migration_Guide.md`；节点编写规范见 `docs/Node_Development_Guide.md`
> **作者**: Sinter Wong (sintercver@gmail.com)  
> **日期**: 2026-02  
> **标准**: C++20  
> **依赖**: 零第三方依赖（纯 C++20 标准库）

---

## 1. 概述

AI Pipe 是一个高性能 DAG（有向无环图）数据流处理框架，专为深度学习推理、视频处理等高吞吐场景设计。框架通过策略模式实现组件可插拔，支持批处理、流式处理和混合处理三种执行模式，并提供背压管理、帧同步和性能监控能力。

### 1.1 核心设计目标

- **高吞吐低延迟**：Lock-Free MPMC 队列，Work-Stealing 线程池实现 85% 线性扩展效率
- **零第三方依赖**：完全基于 C++20 标准库，便于嵌入式和交叉编译环境部署
- **策略可插拔**：调度策略、同步策略、丢弃策略均可自定义替换，无需修改引擎代码
- **接口-实现分离**：PIMPL 模式隔离内部实现，用户仅需依赖纯接口头文件

### 1.2 支持的执行模式

| 模式 | 语义 | 队列 | 完成条件 | 典型场景 |
|------|------|------|----------|----------|
| **BATCH** | 单次遍历 | 无界/不启用 | 所有 Sink 执行一次 | 离线推理、图片批处理 |
| **STREAM** | 持续流式 | 有界+丢弃策略 | 手动停止 | 视频流、实时推理 |
| **HYBRID** | 混合 | 有界+丢弃策略 | 手动停止 | 批+流混合负载 |

---

## 2. 系统架构

### 2.1 分层架构

```
┌──────────────────────────────────────────────────────┐
│                  用户应用层                           │
│         Pipeline / PipelineBuilder (pipeline.hpp)    │
├──────────────────────────────────────────────────────┤
│                  执行引擎层                           │
│         ExecutionEngine (execution_engine.hpp)       │
│            ┌───────────┬───────────┐                 │   
│            │ Scheduler │   Sync    │                 │
│            │ Strategy  │ Strategy  │                 │
│            └───────────┴───────────┘                 │
├──────────────────────────────────────────────────────┤
│                  核心基础设施层                       │
│   Graph │ ThreadPool │ LockFreeQueue │ Context       │
├──────────────────────────────────────────────────────┤
│                  数据与类型层                         │
│   DataPacket │ FrameMetadata │ PortDataMap │ Enums   │
└──────────────────────────────────────────────────────┘
```

### 2.2 模块依赖关系

```
ai_pipe.hpp (统一入口)
├── data_types.hpp ← data_packet.hpp
├── enum.hpp
├── edge.hpp ← i_logic_node.hpp ← context.hpp
├── graph.hpp
├── execution_types.hpp (EngineConfig, LatencyHistogram, Statistics)
├── frame_metadata.hpp
├── i_scheduler_strategy.hpp
├── i_sync_strategy.hpp
├── execution_engine.hpp (PIMPL 公共接口)
├── pipeline.hpp (高层 API)
└── frame_metadata.hpp
```

内部实现（用户不直接包含）：

```
execution_engine_impl.hpp / .cpp
├── lock_free_queue.hpp (Lock-Free MPMC)
├── work_stealing_thread_pool.hpp
├── scheduler_strategies.hpp (Batch/Stream/Hybrid)
├── join_aware_sync_strategy.hpp
├── sync_coordinator.hpp
```

---

## 3. 核心接口设计

### 3.1 节点接口 — `ILogicNode`

所有计算节点的基类，定义了 DAG 中的最小处理单元。

```cpp
class ILogicNode {
public:
  ILogicNode(const std::string name);
  virtual ~ILogicNode();

  const std::string &getName() const;

  // 核心处理方法：从 inputs 读取数据，处理后写入 outputs
  virtual void process(const PortDataMap &inputs,
                       PortDataMap &outputs,
                       std::shared_ptr<PipelineContext> context = nullptr) = 0;

  // 声明期望的输入/输出端口名
  virtual std::vector<std::string> getExpectedInputPorts() const;
  virtual std::vector<std::string> getExpectedOutputPorts() const;
};
```

**设计要点**：

- 端口采用命名字符串标识，支持灵活的多输入多输出拓扑
- `PortDataMap` 为 `std::map<std::string, std::shared_ptr<DataPacket>>`，键为端口名
- `PipelineContext` 可选传入，提供资源访问、取消令牌、日志、指标收集等上下文能力
- 节点通过 `getExpectedInputPorts()` 声明自身需要哪些输入端口，调度器据此判断就绪状态

### 3.2 数据包 — `DataPacket`

```cpp
struct DataPacket {
  DataPacketId id;                          // uint64_t 唯一标识
  std::map<std::string, std::any> params;   // 类型擦除的键值参数表

  template <typename T> T getParam(const std::string &key) const;
  template <typename T> std::optional<T> getOptionalParam(const std::string &key) const;
  template <typename T> void setParam(const std::string &key, T value);
  bool has(const std::string &key) const;
};
```

**设计要点**：

- 使用 `std::any` 实现类型擦除，单一容器承载任意类型参数
- 通过 `shared_ptr<DataPacket>` 传递，零拷贝在节点间共享数据
- `getParam<T>()` 在类型不匹配时抛出详细异常，便于调试

### 3.3 图结构 — `Graph`

```cpp
class Graph {
public:
  bool addNode(const std::shared_ptr<ILogicNode> &node);
  bool addEdge(const std::string &source_node_name,
               const std::string &source_port,
               const std::string &dest_node_name,
               const std::string &dest_port);

  // 拓扑查询
  int getInDegree(const std::shared_ptr<ILogicNode> &node) const;
  int getOutDegree(const std::shared_ptr<ILogicNode> &node) const;
  const std::vector<std::shared_ptr<ILogicNode>> &
      getOutgoingNeighbors(const std::shared_ptr<ILogicNode> &node) const;
  const std::vector<std::shared_ptr<ILogicNode>> &
      getIncomingNeighbors(const std::shared_ptr<ILogicNode> &node) const;

  // 环检测
  bool hasCycle() const;
};
```

**设计要点**：

- 支持 Move 语义，禁止拷贝，所有权清晰
- 邻接表（`m_adjListOut` / `m_adjListIn`）和入度表（`m_inDegree`）同步维护，O(1) 拓扑查询
- 基于 DFS 的环检测在初始化阶段校验图合法性
- `Edge` 结构记录完整的源节点/端口 → 目标节点/端口映射，支持命名端口的灵活连接

### 3.4 管线上下文 — `PipelineContext`

线程安全的全局执行上下文，贯穿整个管线生命周期。

```cpp
class PipelineContext : public std::enable_shared_from_this<PipelineContext> {
  // 资源管理（读写锁保护）
  template <typename T> void setResource(const std::string &name, std::shared_ptr<T> resource);
  template <typename T> std::shared_ptr<T> getResource(const std::string &name) const;

  // 服务注册（按类型索引）
  template <typename T> void setService(std::shared_ptr<T> service);
  template <typename T> std::shared_ptr<T> getService() const;

  // 执行跟踪与指标
  void beginExecution();
  ExecutionMetrics endExecution();
  void beginNodeExecution(const std::string &node_name);
  void endNodeExecution(const std::string &node_name, bool success, ...);

  // 协作取消
  CancellationToken &cancellation();

  // 日志适配器
  void setLoggerAdapter(std::shared_ptr<ILoggerAdapter> adapter);
  void logInfo(const std::string &node_name, const std::string &message);

  // 进度报告
  ProgressReporter &progressReporter(const std::string &node_name);
};
```

**设计要点**：

- **资源管理**：`std::shared_mutex` 保护，支持并发读、互斥写
- **服务注册**：`std::type_index` 索引，同类型服务全局单例
- **取消支持**：`CancellationToken` 基于 `std::atomic<bool>`，acquire/release 语义
- **日志桥接**：`ILoggerAdapter` 抽象接口，可适配 glog、spdlog 等任意日志系统
- **RAII 辅助**：`ScopedNodeExecution` 自动管理节点执行的开始/结束和指标收集

---

## 4. 策略接口设计

框架通过三个策略接口实现执行行为的可插拔定制，所有策略接口均为纯抽象类，不包含任何内部实现依赖。

### 4.1 调度策略 — `ISchedulerStrategy`

控制节点何时被调度执行以及执行何时完成。

```cpp
class ISchedulerStrategy {
public:
  // 判断节点是否应被调度
  virtual ScheduleResult shouldSchedule(const SchedulingContext &context) const = 0;

  // 节点完成后是否需要重新调度
  virtual bool onNodeComplete(const std::shared_ptr<ILogicNode> &node,
                              bool success, const PortDataMap &outputs) = 0;

  // 检查整体执行是否完成
  virtual CompletionStatus checkCompletion(
      std::size_t active_node_count,
      std::size_t pending_node_count,
      const std::unordered_map<std::string, std::uint64_t> &sink_execution_counts) const = 0;

  // 策略元信息
  virtual CompletionSemantics completionSemantics() const = 0;
  virtual bool supportsStreaming() const = 0;
  virtual std::string name() const = 0;
  virtual std::unique_ptr<ISchedulerStrategy> clone() const = 0;
};
```

**调度决策枚举** (`ScheduleDecision`)：

| 值 | 含义 |
|---|------|
| `ScheduleNow` | 立即调度执行 |
| `WaitForInputs` | 等待更多输入就绪 |
| `SkipExecution` | 跳过本轮执行 |
| `DeferToNextCycle` | 延迟到下一调度周期（可附带重试延迟） |

**调度上下文** (`SchedulingContext`)：

```cpp
struct SchedulingContext {
  std::shared_ptr<ILogicNode> node;
  std::vector<std::string> expected_input_ports;  // 节点声明的所有输入端口
  std::vector<std::string> ready_input_ports;     // 当前有数据的端口
  std::size_t pending_predecessor_count;          // 待完成的前驱数
  bool is_source_node;                            // 是否为源节点
  bool is_sink_node;                              // 是否为汇节点
  bool has_initial_input;                         // 是否有初始输入数据
  std::uint64_t execution_count;                  // 已执行次数
};
```

**内置策略实现**：

| 策略 | 调度逻辑 | 完成语义 | 重调度 |
|------|---------|---------|--------|
| `BatchSchedulerStrategy` | 所有输入就绪才调度 | `SinglePass`：所有 Sink 执行一次即完成 | 不重调度 |
| `StreamSchedulerStrategy` | 支持部分输入、速率限制 | `Continuous`：永不自动完成 | 成功后自动重调度 |
| `HybridSchedulerStrategy` | 所有输入就绪即调度 | `Continuous` | 成功后重调度 |

### 4.2 同步策略 — `ISyncStrategy`

处理并行分支的帧同步与协调丢弃。

```cpp
class ISyncStrategy {
public:
  virtual void initialize(const Graph *graph) = 0;
  virtual void reset() = 0;

  // 同步组注册
  virtual void registerSyncGroup(const SyncGroupId &group_id,
                                 const std::vector<BranchId> &branch_ids,
                                 const std::string &join_node = "") = 0;
  virtual void mapNodeToGroup(const std::string &node_name,
                              const SyncGroupId &group_id,
                              const BranchId &branch_id) = 0;

  // 丢弃协调
  virtual std::vector<BranchId> reportDrop(const std::string &node_name,
                                           FrameId frame_id,
                                           const std::string &reason) = 0;
  virtual bool shouldDrop(const std::string &node_name, FrameId frame_id) const = 0;

  // 水位线追踪
  virtual void markProcessed(const std::string &node_name, FrameId frame_id) = 0;
  virtual FrameId getWatermark(const SyncGroupId &group_id) const = 0;
};
```

**核心概念**：

- **SyncGroup（同步组）**：共享同一汇合节点的一组并行分支
- **Drop Propagation（丢弃传播）**：当一条分支因背压丢弃帧 N 时，通知其他分支也丢弃帧 N，确保汇合点数据对齐
- **Watermark（水位线）**：同步组内所有分支都已处理完成的最小帧 ID

**内置策略实现**：

| 策略 | 特点 |
|------|------|
| `NoSyncStrategy` | 空操作，适用于 BATCH 模式 |
| `JoinAwareSyncStrategy` | 反向 BFS 检测 Fork-Join 对，解决虚假耦合和深度盲问题（早期的 CoordinatedSyncStrategy 已于 P4.4 移除） |

`JoinAwareSyncStrategy` 解决的两个关键问题：

1. **虚假耦合（False Coupling）**：仅同步确实会汇合的分支，不会让不相干的并行分支互相影响
2. **深度盲（Depth Blindness）**：将 Fork 到 Join 之间整条路径上的所有节点映射到逻辑分支，确保深层节点的丢弃事件也能触发协调

> 引擎完整驱动同步策略：DataPacket 携带 FrameId，多输入节点按帧对齐
> 取数（落后帧丢弃并上报），路径节点经 `shouldDrop` 提前丢弃协调帧，
> `markProcessed` 推进水位线。端到端行为由
> `tests/test_sync_integration.cpp` 验证。

## 5. 执行引擎

### 5.1 公共接口 — `ExecutionEngine`

采用 PIMPL 模式，公共头文件不暴露任何内部实现细节。

```cpp
class ExecutionEngine {
public:
  // 工厂方法
  static std::unique_ptr<ExecutionEngine> create(const EngineConfig &config = {});

  // 策略注入
  void setSchedulerStrategy(std::unique_ptr<ISchedulerStrategy> strategy);
  void setSyncStrategy(std::unique_ptr<ISyncStrategy> strategy);
  void configureForMode(ExecutionMode mode);

  // 核心接口
  bool initialize(Graph *graph, std::uint8_t num_workers = 0);
  bool execute(const PortDataMap &initial_inputs,
               bool wait_for_completion = true,
               std::shared_ptr<PipelineContext> context = nullptr);
  void stopExecutionAsync();
  void stopExecutionSync();

  // 流式接口
  bool startStreaming(std::shared_ptr<PipelineContext> context = nullptr);
  void stopStreaming(bool wait_for_drain = true);
  Result<PushStatus> pushInput(const std::string &source_node,
                            const std::string &port_name,
                            PortDataPtr data);

  // 监控
  EngineStatisticsSnapshot statistics() const;
  std::size_t queueDepth(const std::string &node_name, ...) const;
  bool waitForDrain(std::size_t max_depth, std::chrono::milliseconds timeout);

  // 回调
  void setPipelineResultCallback(std::function<void(const PortDataMap &)> callback);
  void setPipelineErrorCallback(
      std::function<void(const std::string &, const std::string &)> callback);
  void setDropCallback(
      std::function<void(const std::string &, std::uint64_t, const std::string &)> callback);

private:
  class Impl;
  std::unique_ptr<Impl> m_impl;   // PIMPL
};
```

**便捷工厂函数**：

```cpp
auto engine = createBatchEngine(4);    // 4 Worker 批处理引擎
auto engine = createStreamEngine(4, 16); // 4 Worker、队列容量 16 的流式引擎
auto engine = createHybridEngine(4, 16); // 混合模式引擎
```

### 5.2 内部执行流程

```
initialize()
  ├── 构建 NodeState（含 Lock-Free 队列）
  ├── 识别 Source/Sink 节点
  ├── 初始化同步策略（拓扑分析）
  └── 创建 WorkStealingThreadPool

execute() / startStreaming()
  ├── 分发初始输入到 Source 节点队列
  └── scheduleReadyNodes()
        └── 遍历所有节点
              ├── 构建 SchedulingContext
              ├── 调用 m_schedulerStrategy->shouldSchedule()
              └── ScheduleNow → 提交到线程池

executeNodeTask(node)
  ├── gatherNodeInputs()  — 从 Lock-Free 队列 pop
  ├── processNode()       — 调用 node->process()
  ├── propagateOutputs()  — push 到下游节点队列
  ├── handleNodeSuccess() / handleNodeFailure()
  ├── onNodeComplete()    — 策略决定是否重调度
  └── checkCompletionAndNotify()
```

### 5.3 引擎配置 — `EngineConfig`

```cpp
struct EngineConfig {
  ExecutionMode mode = ExecutionMode::BATCH;
  std::uint8_t num_workers = 4;

  std::size_t default_queue_capacity = 0;     // 0 = 无界
  std::string default_drop_strategy = "DropHead";

  bool enable_sync_coordination = false;      // 启用帧同步
  bool allow_partial_inputs = false;          // 允许部分输入即调度
  std::chrono::milliseconds min_execution_interval{0}; // 最小执行间隔

  AlignmentPolicy alignment_policy = AlignmentPolicy::FrameId; // 多输入对齐键
  std::chrono::microseconds alignment_tolerance{33000};       // Timestamp 策略容差

  std::chrono::milliseconds join_wait_timeout{0};             // Join 等待上限（0 = 无限等待）
  JoinTimeoutPolicy join_timeout_policy =                     // 超时降级策略
      JoinTimeoutPolicy::PartialInputs;

  bool enable_statistics = true;
  bool enable_drop_logging = true;
};
```

---

## 6. 高层管线 API

### 6.1 Pipeline 与 PipelineBuilder

`Pipeline` 是面向用户的最高层 API，封装了 Graph + ExecutionEngine + Context 的完整生命周期管理。

```cpp
// Builder 模式构建管线
auto pipeline = Pipeline::create()
    .withGraph(std::move(graph))
    .withMode(ExecutionMode::STREAM)
    .withWorkers(8)
    .withQueueCapacity(32)
    .withSyncCoordination(true)
    .withDropStrategy("DropHead")
    .onResult([](const PortDataMap &outputs) {
        // 处理结果
    })
    .onError([](const std::string &error, const std::string &node) {
        // 处理错误
    })
    .onDrop([](const std::string &node, uint64_t frame_id, const std::string &reason) {
        // 丢帧通知
    })
    .build();
```

**批处理模式使用**：

```cpp
ExecutionResult result = pipeline.run(inputs);
if (result) {
    // result.outputs 包含所有 Sink 节点的输出
}

// 异步执行
auto future = pipeline.runAsync(inputs);
auto result = future.get();
```

**流式模式使用**：

```cpp
pipeline.start();

while (capturing) {
    auto data = std::make_shared<PortData>();
    data->setParam("frame", frame);

    auto r = pipeline.pushInput("source_node", data);
    if (r.isDropped()) {
        // 背压丢弃
    }
}

pipeline.stop(true);  // wait_for_drain = true
```

### 6.2 观察者模式 — `IPipelineObserver`

```cpp
class IPipelineObserver {
public:
  virtual void onExecutionStarted() {}
  virtual void onExecutionCompleted(const PortDataMap &) {}
  virtual void onExecutionFailed(const std::string &, const std::string &) {}
  virtual void onFrameDropped(const std::string &, std::uint64_t, const std::string &) {}
};
```

可通过继承 `IPipelineObserver` 或使用内置的 `CallbackObserver` 链式注册回调。

**错误分级契约（R1.2）**：节点异常按执行模式分级。批模式下节点异常是
pipeline-fatal——执行中止，`PipelineState` 进入 `ERROR`，需 `reset()`
恢复。流式模式下节点异常是 per-frame 事件——出错帧被消费丢弃，节点回到
服务，管线保持 `RUNNING` 并继续接受 `pushInput`；错误通过
`onExecutionFailed` 通知观察者（每个坏帧一次），不改变门面状态。

### 6.3 便捷工厂

```cpp
auto batch_pipe  = makeBatchPipeline(std::move(graph), 4);
auto stream_pipe = makeStreamPipeline(std::move(graph), 4, 16);
```

---

## 7. 高性能基础设施

### 7.1 Lock-Free MPMC 队列 — `LockFreeNodeQueue`

基于 Dmitry Vyukov 有界 MPMC 队列算法实现。

**关键设计**：

- **Power-of-2 容量**：位运算取模（`index & mask_`），消除除法开销
- **Cache-Line 对齐 Slot**：`alignas(k_cache_line_size)` 防止伪共享
- **Sequence Tag**：每个 Slot 带原子序列号，解决 ABA 问题
- **宽松内存序**：非同步路径使用 `relaxed`，仅在同步点使用 `acquire/release`
- **集成丢弃策略**：`DropHead`（CAS 推进 head）、`DropTail`（拒绝新入）、`KeepLatest`

**KeepLatest 并发语义**（F3，完整契约见 `pushKeepLatest` 注释）：push 恒成功，
背压完全表现为驱逐（逐条计数并回调）。"至多保留 N 帧"对单生产者严格成立；
P 个并发生产者下 evict-then-push 非原子，竞争窗口内 size 可短暂达到
N + P − 1（不超过容量），且若推送恰好停在窗口内，超出会持续到下一次操作。
该超出是自愈的：每次 push 先驱逐到 size < N 再入队，后续任意一次无竞争
push 即恢复 ≤ N。需要硬性 "任意时刻 ≤ N" 的调用方必须在外部串行化生产者。

### 7.2 Work-Stealing 线程池

**特性**：

- 每个 Worker 线程持有本地任务队列
- 空闲时从其他 Worker 偷取任务，实现负载均衡
- 有界全局任务队列，可配置超时提交
- 优雅关闭（等待所有任务完成）和立即关闭两种模式
- 线程安全的统计信息

**性能**：8 Worker 场景下相比原始 Mutex 线程池实现 85% 吞吐提升。

---

## 8. 帧元数据与同步

### 8.1 帧元数据接口 — `IFrameMetadata`

```cpp
class IFrameMetadata {
public:
  virtual FrameId frameId() const = 0;      // 帧ID（流内单调递增）
  virtual StreamId streamId() const = 0;     // 流ID（多源场景）
  virtual Timestamp timestamp() const = 0;   // 高精度时间戳

  virtual bool shouldSyncWith(const IFrameMetadata &other) const = 0;
  virtual int compareTo(const IFrameMetadata &other) const = 0;
  virtual std::unique_ptr<IFrameMetadata> clone() const = 0;
};
```

**内置实现**：

| 类 | 同步策略 | 适用场景 |
|---|---------|---------|
| `BasicFrameMetadata` | 帧 ID 精确匹配 | 单源或 ID 对齐的多源 |
| `TimestampFrameMetadata` | 时间戳容差匹配（默认 33ms） | 多摄像头时间同步 |

**工厂**：

```cpp
auto meta = FrameMetadataFactory::createBasic(stream_id);  // 自增 ID
auto eos  = FrameMetadataFactory::createEndOfStream();       // 流结束标记
```

**特殊常量**：

- `k_invalid_frame_id = 0`：无效帧
- `k_end_of_stream_frame_id = UINT64_MAX`：流结束标记
- `k_max_frame_drift = 100`：最大允许帧偏移

### 8.2 多流对齐 — `AlignmentPolicy`（F5）

多输入（Join）节点的对齐取数由 `EngineConfig::alignment_policy` 选择对齐键；
非默认策略是显式选择，即使未安装同步策略也会启用对齐取数：

| 策略 | 配对条件 | 适用场景 |
|------|---------|---------|
| `FrameId`（默认） | 各端口队头 FrameId 精确相等 | 单流，或 ID 全局唯一的多流（如引擎自动编号） |
| `StreamFrameId` | 各端口队头 (stream_id, frame_id) 二元组相等 | 多流各自独立编号（跨流 ID 可能碰撞） |
| `Timestamp` | 队头时间戳极差 ≤ `alignment_tolerance`（默认 33ms） | 多摄像头无共享帧号，仅按采集时间配对 |

**滞留帧丢弃契约**（保证取数循环单调推进，不会死锁在永失配对的队头）：

- `StreamFrameId`：队头不齐时，丢弃入线时间戳**严格早于**最新队头的帧——
  端口队列 FIFO 且入线时间戳单调不减，更早的帧在最新端口上已错过配对窗口。
  各队头时间戳完全相同时（病态情形）按最小 (stream, frame) 确定性丢弃。
  该模式下引擎对未编号包的自动 FrameId 改为**流内单调**（按 stream_id 分桶计数）。
- `Timestamp`：丢弃落后最新队头超过容差的帧（同样依赖端口内时间戳单调不减）。
  该策略完全忽略 FrameId。

**边界**：跨分支丢弃协调（`ISyncStrategy` 的 reportDrop/shouldDrop 及水位线）
仍以 FrameId 为键。多流场景下若跨流 ID 碰撞，协调记录可能跨流误伤——
需要精确协调时应保证 ID 全局唯一（引擎默认编号即满足），或仅依赖对齐层丢弃。
时间戳来源：入线时若包未携带时间戳由引擎盖章（`stampIncomingFrame`）；
用户自带采集时间戳时需自行保证端口内单调不减。

### 8.3 Join 对齐超时降级 — `JoinTimeoutPolicy`（F6）

默认行为（`join_wait_timeout = 0`）：落后分支由重调度机制自然等待，
永失配对的帧由对齐层丢弃——即"宁等待也不要不完整数据"。为相反偏好
（"宁要不完整数据也不要等待"）提供可配置降级（**仅流模式**；批模式对缺失
输入立即失败，不存在等待）：

| 策略 | 超时后的行为 |
|------|------------|
| `PartialInputs`（默认） | 以已就绪端口执行节点；缺失端口在 `PortDataMap` 中不出现，节点须自行容忍 |
| `SkipFrame` | 丢弃卡住的队头帧（drop 回调 reason 为 `join wait timeout`），让后续帧正常配对 |

**机制**：部分就绪的 Join 在无新数据到达时不会被重调度，超时需要主动唤醒——
仅当 `join_wait_timeout > 0` 且图中存在多输入节点时，引擎启动一个轻量看门狗
线程（随 `startStreaming`/`stopStreaming` 起停），以 timeout/4 为节拍扫描
多输入节点：持续部分就绪超过上限者被调度一次降级执行。降级取数选取已就绪
端口中**最老的配对集**（按当前 `AlignmentPolicy` 语义），超时精度为 ±一个
节拍。降级次数计入 `EngineStatistics::total_join_timeouts`。

---

## 9. 统计与监控

### 9.1 延迟直方图 — `LatencyHistogram`

16 个桶位，覆盖 < 10μs 到 ≥ 500ms 的全延迟谱：

```
<10us | <25us | <50us | <100us | <250us | <500us | <1ms | <2.5ms |
<5ms  | <10ms | <25ms | <50ms  | <100ms | <250ms | <500ms | >=500ms
```

所有计数器为 `std::atomic<uint64_t>`，wait-free 无锁更新。

### 9.2 引擎统计 — `EngineStatistics` / `EngineStatisticsSnapshot`

**原子实时统计**（`EngineStatistics`）：

- 执行计数（总/成功/失败；按节点执行次数统一计数）
- 帧计数（输入/输出/丢弃）
- 队列事件（push/pop/满）
- 时间统计（处理时间/出队帧龄/调度延迟）
- 端到端延迟直方图（sink 完成时记录）

**快照**（`EngineStatisticsSnapshot`）：

```cpp
EngineStatisticsSnapshot snap = engine->statistics();

snap.successRate();         // 成功率 %
snap.dropRate();            // 丢弃率 %
snap.throughput();          // 吞吐量 frames/s
snap.avgProcessingTimeUs(); // 平均处理时间 μs
snap.latencyPercentiles();  // p50/p90/p95/p99/p99.9
snap.histogramData();       // 直方图分布
snap.node_stats;            // 每节点统计
```

### 9.3 节点级统计 — `NodeStatistics`

每个节点独立的原子统计（`AtomicNodeStatistics`），随快照填充：

- 执行/成功/失败计数
- 处理时间（总/最小/最大）
- 输入/输出计数
- 当前队列深度

### 9.4 执行追踪 — `ITraceSink` / `ChromeTraceSink`（F7）

把第 9 章的聚合数字升级为 per-frame 时间线：注入 `ITraceSink`
（`ExecutionEngine::setTraceSink` / `Pipeline::setTraceSink`，仅 IDLE 时可换）
后，引擎在四个生命周期点发出 `TraceEvent`：

| Phase | 语义 | 类型 |
|-------|------|------|
| `Enqueue` | 包被节点输入队列接收（含 detail=端口名） | 瞬时 |
| `Schedule` | READY → worker 取走（span = 调度延迟） | 区间 |
| `Execute` | `process()` 调用（携带输入帧的 frame/stream id） | 区间 |
| `Propagate` | 输出路由到下游队列 | 区间 |

**约定**：`onEvent` 在 worker 线程/入线线程上并发调用且处于热路径——sink
必须线程安全且廉价；`TraceEvent` 的 string_view 字段仅在调用期间有效，
留存需拷贝。未安装 sink 时每个埋点只花一次指针判空。

**内置导出**：`ChromeTraceSink` 缓冲全部事件（互斥保护，适合测试/有界
采集，不适合无界 7×24），`toJson()`/`writeFile()` 输出 Chrome Trace Event
格式——chrome://tracing 或 https://ui.perfetto.dev 直接打开；frame_id/
stream_id 挂在 args 上可按帧切片。

---

## 10. 日志系统

### 10.1 日志适配器架构

**日志级别**：`Trace → Debug → Info → Warning → Error`

**内置适配器**：
- `NullLoggerAdapter`：丢弃所有日志
- `ConsoleLoggerAdapter`：stdout 输出（开发调试）
- `MemoryLoggerAdapter`：内存捕获（单元测试）

**自定义适配器示例**：

```cpp
class SpdlogAdapter : public ILoggerAdapter {
public:
  void log(PipeLogLevel level, const std::string &node_name,
           const std::string &message) override {
    std::string formatted = "[" + node_name + "] " + message;
    switch (level) {
      case PipeLogLevel::KInfo:    spdlog::info(formatted); break;
      case PipeLogLevel::KError:   spdlog::error(formatted); break;
      // ...
    }
  }
};

ctx->setLoggerAdapter(std::make_shared<SpdlogAdapter>());
```

---

## 11. 构建与集成

### 11.1 CMake 集成

```cmake
find_package(ai_pipe REQUIRED)
target_link_libraries(my_app PRIVATE ai_pipe::ai_pipe)
```

**安装产物**：

- `lib/libai_pipe.so` — 共享库
- `include/ai_pipe/` — 公共头文件（仅接口，无内部实现头）
- `lib/cmake/ai_pipe/ai_pipe-config.cmake` — CMake 包配置（find_package 默认搜索路径）

### 11.2 公共头文件 vs 内部头文件

| 用户可包含头文件 |
|---|
| `ai_pipe.hpp`（统一入口）|
| `version.hpp` |
| `enum.hpp` |
| `edge.hpp` |
| `graph.hpp` |
| `data_packet.hpp` 
| `data_types.hpp` 
| `i_logic_node.hpp` 
| `i_scheduler_strategy.hpp` 
| `i_sync_strategy.hpp`|
| `execution_engine.hpp`|
| `execution_types.hpp` |
| `frame_metadata.hpp` |
| `context.hpp` |
| `pipeline.hpp`|

用户只需 `#include "ai_pipe/ai_pipe.hpp"` 即可获得完整公共 API。

---

## 12. 扩展指南

### 12.1 自定义节点

```cpp
class MyInferenceNode : public ai_pipe::ILogicNode {
public:
  MyInferenceNode() : ILogicNode("inference") {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> ctx) override {
    // 使用 RAII 辅助管理指标收集
    ScopedNodeExecution scope(ctx, getName());

    // 检查取消
    scope.checkCancellation();

    // 获取共享资源
    auto model = ctx->getResource<MyModel>("model");

    // 读取输入
    auto frame = inputs.at("input")->getParam<cv::Mat>("frame");

    // 推理
    auto result = model->infer(frame);

    // 写入输出
    auto output = std::make_shared<PortData>();
    output->setParam("result", result);
    outputs["output"] = output;
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }
};
```

### 12.2 自定义调度策略

```cpp
class PrioritySchedulerStrategy : public ai_pipe::ISchedulerStrategy {
public:
  ScheduleResult shouldSchedule(const SchedulingContext &ctx) const override {
    // 高优先级节点即使部分输入也可调度
    if (isHighPriority(ctx.node) && !ctx.ready_input_ports.empty()) {
      return ScheduleResult::scheduleNow("high priority");
    }
    if (ctx.allInputsReady()) {
      return ScheduleResult::scheduleNow("all ready");
    }
    return ScheduleResult::waitForInputs("waiting");
  }

  // 实现其余纯虚方法...
};
```

### 12.3 自定义同步策略

```cpp
class TimestampSyncStrategy : public ai_pipe::ISyncStrategy {
  // 基于时间戳容差而非帧ID进行同步
  // 适用于多传感器融合场景
};
```

### 12.4 动态节点插件 — `PluginLoader`（F8）

节点可打包为共享库在运行时加载（`ai_pipe/plugin.hpp`）：

```cpp
// 插件侧（编译为 MODULE 共享库，链接 libai_pipe.so）
AI_PIPE_PLUGIN("my_detector_pack");   // 导出版本握手描述符
AI_PIPE_REGISTER_NODE(MyDetectorNode); // 静态初始化时注册（dlopen 触发）

// 宿主侧
ai_pipe::PluginLoader loader;
auto loaded = loader.load("plugins/libmy_detector_pack.so");
// 或整目录扫描（非递归，按路径排序保证确定性）：
auto all = loader.loadDirectory("plugins/");
// loaded->registered_types 列出该插件贡献的节点类型
```

**ABI 边界与版本握手**：

- 插件必须与宿主链接**同一个** `libai_pipe.so`（NodeRegistry 单例与跨界
  C++ 类型都在其中），并使用 ABI 兼容的工具链/编译选项——握手无法检测
  工具链 ABI 漂移，这是文档化的边界而非可验证项。
- 握手经 C-linkage 符号 `ai_pipe_plugin_descriptor` 进行（跨 dlopen 边界
  只读一个 standard-layout C 结构体）：插件协议修订号
  （`k_plugin_abi_version`）须精确相等；框架版本 pre-1.0 要求
  major.minor 相同。
- 注册发生在 dlopen 静态初始化期间、握手之前（机制使然），故加载器对
  注册表做前后快照：握手失败时回滚该插件注册的全部节点类型再 dlclose，
  错误码为 `PluginSymbolMissing` / `PluginVersionMismatch`。
- 卸载（`unload`/析构）先反注册再 dlclose；调用方须保证该插件创建的节点
  实例已全部销毁。注意 GCC 的 STB_GNU_UNIQUE 符号会使 glibc 将库标记为
  NODELETE（dlclose 不真正卸载、重载不会重跑注册）——插件建议以
  `-fno-gnu-unique` 编译。

---

## 13. 设计模式总结

| 模式 | 应用位置 | 作用 |
|------|---------|------|
| **Strategy** | ISchedulerStrategy / ISyncStrategy | 执行行为可插拔替换 |
| **PIMPL** | ExecutionEngine / Pipeline | 接口-实现二进制隔离 |
| **Builder** | PipelineBuilder | 流畅的管线配置 API |
| **Observer** | IPipelineObserver | 事件通知解耦 |
| **Factory** | createBatchEngine() / FrameMetadataFactory | 简化对象创建 |
| **Adapter** | ILoggerAdapter | 日志系统桥接 |
| **RAII** | ScopedNodeExecution | 自动管理节点执行生命周期 |
| **Type Erasure** | DataPacket (std::any) / PipelineContext | 通用数据传递 |

---

## 附录 A：关键类型速查

```cpp
// 数据流类型
using PortData    = DataPacket;
using PortDataPtr = std::shared_ptr<PortData>;
using PortDataMap = std::map<std::string, PortDataPtr>;

// 帧同步类型
using FrameId     = std::uint64_t;
using StreamId    = std::uint32_t;
using Timestamp   = std::chrono::steady_clock::time_point;
using SyncGroupId = std::string;
using BranchId    = std::string;

// 状态枚举
enum class NodeExecutionState { WAITING, READY, EXECUTING, COMPLETED, FAILED };
enum class EngineState         { IDLE, RUNNING, STOPPED, ERROR };
enum class PipelineState       { UNINITIALIZED, IDLE, RUNNING, STOPPING, ERROR };
enum class ExecutionMode       { BATCH, STREAM, HYBRID };
```

## 附录 B：完整使用示例

```cpp
#include "ai_pipe/ai_pipe.hpp"
using namespace ai_pipe;

// 1. 定义自定义节点
class PreprocessNode : public ILogicNode {
public:
  PreprocessNode() : ILogicNode("preprocess") {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto input = inputs.at("input");
    auto output = std::make_shared<PortData>();
    output->id = input->id;
    output->setParam("preprocessed", true);
    outputs["output"] = output;
  }

  std::vector<std::string> getExpectedInputPorts() const override  { return {"input"}; }
  std::vector<std::string> getExpectedOutputPorts() const override { return {"output"}; }
};

class DetectionNode : public ILogicNode { /* ... */ };
class RenderNode    : public ILogicNode { /* ... */ };

int main() {
  // 2. 构建 DAG 图
  Graph graph;
  auto preprocess = std::make_shared<PreprocessNode>();
  auto detection  = std::make_shared<DetectionNode>();
  auto render     = std::make_shared<RenderNode>();

  graph.addNode(preprocess);
  graph.addNode(detection);
  graph.addNode(render);

  graph.addEdge("preprocess", "output", "detection", "input");
  graph.addEdge("detection",  "output", "render",    "input");

  // 3. 构建并启动流式管线
  auto pipeline = Pipeline::create()
      .withGraph(std::move(graph))
      .withMode(ExecutionMode::STREAM)
      .withWorkers(4)
      .withQueueCapacity(16)
      .onResult([](const PortDataMap &results) {
          std::cout << "Pipeline produced output" << std::endl;
      })
      .build();

  pipeline.start();

  // 4. 持续推送数据
  for (int i = 0; i < 1000; ++i) {
    auto data = std::make_shared<PortData>();
    data->id = i + 1;
    data->setParam("frame_index", i);
    pipeline.pushInput("preprocess", data);
  }

  // 5. 等待排空并停止
  pipeline.waitForDrain();
  pipeline.stop();

  // 6. 查看统计
  auto stats = pipeline.statistics();
  std::cout << "Throughput: " << stats.throughput << " fps\n";
  std::cout << "Drop rate: " << stats.drop_rate << "%\n";

  return 0;
}
```
