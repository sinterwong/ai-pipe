# 02. AI Pipe 框架组件全景概览

在开始全量手搓每个独立组件之前，必须从上帝视角建立整套框架的宏观全局认识。AI Pipe 是一个专为 AI 管道及高吞吐实时数据流场景打造的、**高度组件化与策略化**的执行系统。

本章将为你梳理框架的**全景架构蓝图**、**15 个核心组件职责**、**核心数据包生命周期与所有权模型**，以及 **Batch 与 Stream 两种执行模式的本质区别**。

---

## 1. 框架全景架构蓝图

AI Pipe 采用了典型的**分层微内核架构**，核心引擎完全通过**策略模式（Strategy Pattern）**进行驱动。高层的管线门面（Facade）仅作为调度和状态维护的外壳。

```
+----------------------------------------------------------------------------------+
|                                  1. 高层门面与构建层                              |
|          Pipeline (Facade)  <====[Builder]====  PipelineBuilder                 |
+----------------------------------------------------------------------------------+
                                        | (持有并驱动)
                                        v
+----------------------------------------------------------------------------------+
|                                  2. 策略化微内核引擎                             |
|                           ExecutionEngine (PIMPL)                                |
|        +--------------------------+          +---------------------------+       |
|        |    ISchedulerStrategy    |          |       ISyncStrategy       |       |
|        | (Batch/Stream Scheduler) |          |  (Join-Aware / No Sync)   |       |
|        +--------------------------+          +---------------------------+       |
+----------------------------------------------------------------------------------+
          | (调度任务)                              | (管理节点队列、同步与对齐)
          v                                         v
+----------------------------------------------------------------------------------+
|                                  3. 基础设施层                                   |
|   +--------------------------+  +--------------------------+  +----------------+ |
|   |  WorkStealingThreadPool  |  |   LockFreeNodeQueue      |  |PipelineContext | |
|   | (LIFO/FIFO Work Steal)   |  | (Vyukov MPMC CacheAlign) |  | (Shared Resource)|
|   +--------------------------+  +--------------------------+  +----------------+ |
+----------------------------------------------------------------------------------+
                                        | (运行其上)
                                        v
+----------------------------------------------------------------------------------+
|                                  4. DAG 拓扑定义层                               |
|        Graph  <----(包含)----  ILogicNode (Named Ports) <---- DataPacket          |
+----------------------------------------------------------------------------------+
                                        | (可观测与扩展)
                                        v
+----------------------------------------------------------------------------------+
|                                  5. 插件与可观测辅助                             |
|   +--------------------------+  +--------------------------+  +----------------+ |
|   |       PluginLoader       |  |     ChromeTraceSink      |  |LatencyHistogram| |
|   |  (dlopen / Rollback)     |  | (Chrome Trace / Perfetto)|  | (Lock-Free)    | |
|   +--------------------------+  +--------------------------+  +----------------+ |
+----------------------------------------------------------------------------------+
```

---

## 2. 15 个核心组件职责一览

下表梳理了你在手搓过程中需要完成的 15 个最核心的组件及其职责，以及它们所在的物理文件：

| 序号 | 组件名称 | 物理文件 | 核心职责 |
|---|---|---|---|
| 1 | `Result<T> / Error` | `data_types.hpp` | 统一的 Monadic 错误传导器，彻底消灭标准异常，提供零开销成功路径。 |
| 2 | `LockFreeNodeQueue` | `lock_free_queue.hpp` | Bounded MPMC 环形缓冲，实现 Lock-Free 的 push/pop 路径，集成 Drop 机制。 |
| 3 | `WorkStealingThreadPool`| `work_stealing_thread_pool.hpp` | 工作窃取线程池。每线程独立 LIFO 本地队列，空闲时 FIFO 窃取他人，降低锁竞争。 |
| 4 | `PipelineContext` | `context.hpp` | 全局资源与服务定位器，管理 CancellationToken 和 ScopedNodeExecution 指标收集。 |
| 5 | `Graph` | `graph.hpp` | DAG 拓扑管理器。在端点间通过命名端口（Named Ports）建立 Edge，执行基于 DFS 的环检测。 |
| 6 | `ExecutionEngine` | `execution_engine.hpp` | 内核引擎。采用 PIMPL 模式，驱动调度与同步，是整个系统的“发动机”。 |
| 7 | `ISchedulerStrategy` | `i_scheduler_strategy.hpp` | 调度策略接口。控制节点何时被激活（SchedulingContext 评估）以及何时重调度。 |
| 8 | `ISyncStrategy` | `i_sync_strategy.hpp` | 同步策略接口。管理 SyncGroup，实现跨分支协同丢弃（Drop Propagation）。|
| 9 | `IFrameMetadata` | `frame_metadata.hpp` | 帧元数据，携带单调 FrameId、StreamId 和时间戳，用于多输入对齐的唯一键。 |
| 10 | `JoinAwareSyncStrategy`| `join_aware_sync_strategy.hpp`| 具体的同步策略实现。能自动识别 Fork-Join 对，攻克虚假耦合和深度盲。 |
| 11 | `Multi-Stream Alignment`| `frame_alignment.hpp` | 多输入节点的队头数据对齐引擎，在隊頭不齐时执行单调丢弃逻辑。 |
| 12 | `Join Watchdog` | `execution_engine_impl.cpp`| 引擎内部的轻量看门狗扫描线程，唤醒因部分就绪而卡死的 Join 节点，执行降级。 |
| 13 | `LatencyHistogram` | `execution_types.hpp` | 无锁高精度 16 桶位延迟直方图，通过 atomic CAS 记录微秒级端到端处理耗时。 |
| 14 | `ChromeTraceSink` | `trace.hpp` | 实现 `ITraceSink` 接口。低开销捕获 Enqueue, Schedule, Execute 性能事件，支持导出 Perfetto JSON。 |
| 15 | `PluginLoader` | `plugin_loader.cpp` | 动态加载器。加载 C-Linkage 节点插件。若加载发生 ABI 冲突或符号缺失，能回滚注册表快照。 |

---

## 3. 核心数据包生命周期与所有权模型

在 AI Pipe 中，流转的基本单元是 `DataPacket`（通常化名为 `PortData`）。
为了实现**微秒级超低延迟**并保证**无锁并行分支的并发安全性**，框架设计了一套极其严密且优雅的**包所有权与生命周期模型**：

### 3.1 零拷贝与不可变性契约（Immutable Packet Model）
*   **不可变传递**：一旦 `DataPacket` 被推入管线（进入 Input 端口队列），该 Packet 就被视为**只读的（Immutable）**。
*   **PortDataPtr 的本质**：在 C++ 中，它是 `std::shared_ptr<const DataPacket>`。强制的 `const` 约束保证了当下游存在多个并行分支（Fan-out）同时读取该包时，不会发生数据竞争（Data Race）。
*   **零拷贝 Fan-out**：下游并行节点 A、B、C 共享同一个 `PortDataPtr` 智能指针。它们直接并发访问里面的数据参数，不需要进行任何内存拷贝。

```
                       +-------------------+
                       |    Source Node    |
                       +-------------------+
                                 |  (产生并发布)
                                 v
                     PortDataPtr (shared_ptr<const DataPacket>)
                                 |
           +---------------------+---------------------+
           |                     |                     |
           v                     v                     v
+---------------------+ +---------------------+ +---------------------+
|   Parallel Node A   | |   Parallel Node B   | |   Parallel Node C   |
|   (Read-Only Data)  | |   (Read-Only Data)  | |   (Read-Only Data)  |
+---------------------+ +---------------------+ +---------------------+
```

### 3.2 写入避坑：Copy-on-Write（写时复制）机制
如果某个下游节点（例如 `Parallel Node B`）在接收到该包后，必须要修改其中的参数，应该如何处理？
*   **严禁直接修改**：由于持有的是 `const DataPacket`，编译器会直接阻止你修改。
*   **写时复制（COW）逃生通道**：节点必须显式调用 `ai_pipe::mutableCopy(packet)`。该函数会克隆一份全新的、可写的 `MutablePortDataPtr`（即 `std::shared_ptr<DataPacket>`）。
*   **效果**：修改仅作用于克隆后的新包上，不会影响到并行分支 A 和 C 所共享的只读原始包。

---

## 4. BATCH 与 STREAM 模式的底层对比

这是理解框架调度机制的分水岭。你所实现的微内核执行引擎，必须根据 `ExecutionMode` 切换截然不同的底层控制流：

| 特性维度 | BATCH（批处理模式） | STREAM（实时流式模式） |
|---|---|---|
| **核心定位** | 离线、一过性数据处理。 | 实时、不间断、多路并发、连续数据流处理。 |
| **队列特征** | 节点队列通常不启用（或者视为无界）。数据单向穿梭。 | 节点队列为**有界（Bounded）**。必须配置丢弃策略（Drop Policy）控制背压。 |
| **控制环路** | 执行一次 `run()` 即启动一次完整的 DAG 拓扑遍历。 | 运行 `start()` 后，引擎进入无限常驻循环。数据由 `pushInput()` 异步注入。 |
| **就绪与重调度** | 当且仅当节点的所有前驱节点全部执行完毕，才调度该节点。 | 调度器 `StreamScheduler` 根据节点各队列的数据到达事件，高频反复激活并调度节点。 |
| **完成语义** | **SinglePass**：当 DAG 所有的 Sink（叶子节点）都成功执行且完成过一次，整趟执行宣告结束。 | **Continuous**：永不自动完成。必须由外部显式调用 `stop()` 并触发 Drain 排空。 |
| **错误传导** | **Pipeline-Fatal**：任一节点异常会导致管线瞬间崩溃（进入 ERROR 状态），必须重置。 | **Per-Frame Failure**：单帧运行失败不影响管线状态。当前帧被丢弃，引擎继续运行下一帧。 |
| **帧同步** | 通常不需要。无 FrameId 对齐，无协同丢弃。 | **核心诉求**。必须支持 Fork-Join 组内分支的丢帧协同和水位线对齐。 |

---

## 5. 小结

通过本章，我们建立了全局的宏观视图。AI Pipe 的高吞吐来自 LockFree 队列与 WorkStealing 线程池的无缝协作；而它的低延迟与高扩展性则得益于不可变的只读包设计（Immutable Packet Mode）。

现在，我们准备开启阶段一：核心并发与工具基础设施的手搓之旅。首先迎接你的是最具挑战性的高并发结构 —— **Dmitry Vyukov Lock-Free Bounded MPMC 队列**。
