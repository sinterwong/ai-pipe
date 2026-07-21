# 01. AI Pipe 手搓路线图与阶段规划

欢迎来到 AI Pipe 框架的“全量手搓”复习与重构之旅。本指南旨在通过将框架拆解为 4 个由浅入深、逻辑自洽的重构阶段，引导你重新认识并构建这个高性能、零依赖、基于 C++20 标准的 DAG 管道执行引擎。

通过手搓本项目，你将深度掌握：
1. **现代 C++ 内存序与无锁并发编程**（Vyukov MPMC 队列、KeepLatest 并发自愈等）。
2. **多线程并发模型与负载均衡**（Work-Stealing 本地/全局混合调度、CAS 槽位所有权等）。
3. **基于有向无环图（DAG）的拓扑排序、环检测与多路数据对齐机制**。
4. **复杂异步系统中的可观测性设计**（无锁高精度 Latency 16-bucket 直方图、基于 Chrome Trace 的非侵入式热路径追踪）。
5. **高性能 PIMPL 隔离、动态 C-Linkage 插件加载（加载器状态回滚）等高级系统架构巧思**。

---

## 1. 依赖关系与模块演进视图

在手搓之前，理解各模块之间的依赖关系至关重要。一个模块的编译和测试不应依赖于未实现的下游模块。以下是手搓顺序的严密依赖拓扑：

```
+------------------------------------------------------------+
| 阶段一：核心基础设施 (Utility & Concurrency)                 |
| [Error/Result] -> [LockFreeQueue] -> [ThreadPool] -> [Context] |
+------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------+
| 阶段二：DAG 拓扑与调度引擎 (DAG & Basic Engine)             |
| [Graph] -> [Scheduler Strategy] -> [ExecutionEngine PIMPL] |
+------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------+
| 阶段三：多流对齐与同步协调 (Alignment & Sync)               |
| [FrameMetadata] -> [SyncStrategy] -> [Alignment & Watchdog] |
+------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------+
| 阶段四：观测、插件与门面 (Observability & Interface)         |
| [Metrics/Histogram] -> [TraceSink] -> [PluginLoader] -> [Pipeline] |
+------------------------------------------------------------+
```

---

## 2. 四大重构阶段及目标

### 阶段一：核心并发与工具基础设施 (底座)
*   **重构组件**：`Result<T> / Error`、`LockFreeNodeQueue`、`WorkStealingThreadPool`、`PipelineContext`
*   **核心目标**：
    1.  建立“全链路无异常”的 Monadic 错误处理。
    2.  手搓 Dmitry Vyukov 无锁有界队列算法加 CPU Cache-Line 对齐，支持 KeepLatest 并发自愈驱逐。
    3.  手搓本地 LIFO/全局 FIFO 双端队列的工作窃取线程池。
    4.  编写线程安全的全局上下文，实现共享资源/服务注册。
*   **自测标志**：并发下 1000 万次 Push/Pop 零数据丢失、零伪共享；线程池任务窃取率、CPU 利用率在多核下呈现高线性扩展。

### 阶段二：DAG 拓扑、调度与核心执行引擎
*   **重构组件**：`Graph`（环检测）、`ISchedulerStrategy`（调度策略接口）、`BatchScheduler`、`StreamScheduler`、`ExecutionEngine` (PIMPL)
*   **核心目标**：
    1.  实现 O(1) 的邻接表/入度查询和拓扑检验。
    2.  运用 PIMPL 模式设计 `ExecutionEngine` 骨架，彻底隔离公共 API 与内部并发/调度实现细节。
    3.  完成 `executeNodeTask` 的核心状态机流转：输入 Gather -> 运行 `process` -> 输出下游 Propagate。
*   **自测标志**：能构建任意多路分支、合并分支的 DAG 并实现单次 Batch 运行，在 4 线程下比单线程有显著的并行加速。

### 阶段三：多流对齐、同步与超时降级 (核心难点)
*   **重构组件**：`IFrameMetadata`、`ISyncStrategy`、`JoinAwareSyncStrategy`、多流对齐机制（`StreamFrameId`、`Timestamp` 策略）、Join 看门狗超时唤醒机制
*   **核心目标**：
    1.  解决多分支合流时的“深度盲（Depth Blindness）”和“虚假耦合（False Coupling）”问题。
    2.  实现多输入 Join 节点的数据配对。当队头数据不齐时，安全地进行落后帧/滞留帧的单调丢弃。
    3.  手搓看门狗扫描线程，实现多输入节点在超时未就绪时的 `PartialInputs` 降级或 `SkipFrame` 丢弃。
*   **自测标志**：多摄像头、多视频流不齐推流时，合流节点能实现精准的帧对齐；高频丢包/背压时，协同丢弃机制（Drop Propagation）能避免整个 Graph 死锁。

### 阶段四：可观测性、追踪、插件系统与门面 (完善)
*   **重构组件**：`LatencyHistogram`、`AtomicNodeStatistics`、`ITraceSink / ChromeTraceSink`、`PluginLoader`、`Pipeline / PipelineBuilder`
*   **核心目标**：
    1.  利用无锁 CAS 构建高精度的 16 桶位延迟分布直方图。
    2.  设计高并发热路径极低开销的 `TraceEvent` 埋点系统，可直接生成适配 Chrome UI Perfetto 的性能追踪 JSON。
    3.  实现支持 dlopen/dlclose 的动态节点加载插件。**核心难点在于版本握手失败时反注册并安全回滚注册表快照**。
    4.  封装 `Pipeline` 顶级门面，支持 Builder 流式配置与错误分级契约。
*   **自测标志**：在 streaming 运行期间，无开销地获取实时 FPS、P99 延迟；能够通过 JSON 加载动态库节点并运行，且在卸载时零内存泄漏。

---

## 3. 手搓与复习策略

1.  **逐级开发，物理隔离**：在未完成阶段一之前，不要开始写阶段二的代码。你会发现阶段二的每个关键并发设计（如 `NodeState` 中的队列和重调度）都建立在阶段一的坚实基础上。
2.  **对照编译，消灭异常**：全框架遵循 C++20。严格检查所有指针判空，确保全链路核心计算路径上不含有任何 `throw` 关键字。全部错误通过 `Result<T>` 传导。
3.  **结合测试复习**：手搓每个组件时，应先阅读对应的测试源文件（例如重写 `LockFreeQueue` 时先阅读 `tests/test_lock_free_queue.cpp`）。测试中包含该组件所要保证的最严苛的并发契约与临界条件，这是最直观的设计指南。

现在，让我们从宏观全景概览（02. Framework Overview）开始，开启通往 C++ 系统级架构师的探索。
