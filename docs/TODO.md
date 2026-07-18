# AI Pipe TODO

> v0.4.0 架构审计（35 项）、v0.5.0 增强（F1–F9）、2026-07 的 R1–R4
> 重构（正确性修复 / 命名与 API 边界 / 装饰性 API 清理 / 架构冗余，共
> 20 项）均已完成，记录见 git 历史。以下为剩余工作。

## R5. 验证与冻结

- [ ] **R5.1 真实业务负载验证**：至少一个生产场景 7×24 长期运行。实施中
  撞到的能力墙按需拉动 R6 对应条目（预计首先是 R6.1 EOS 与 R6.3 内存
  策略）。

- [ ] **R5.2 ABI/SemVer 政策文档**：1.0 冻结前明确公共头文件清单、ABI
  兼容承诺与弃用流程。

- [ ] **R5.3 公共 API 终审**：逐头文件 review。关注点：头文件自洽性
  （compile-check 已有 CI）、命名一致性、`Timestamp` 用 steady_clock
  对跨进程/多机时间戳对齐的语义边界在文档中写明；一并处理下面的
  N1/N2。

## R6. 能力扩展

每项动手前先在 docs/design/ 写设计短文（目标、非目标、与现有语义的
冲突分析），实际优先级以 R5.1 真实负载的需求为准。

- [ ] **R6.1 流内 EOS / flush 协议**：源无法宣告流结束、无 flush 传播、
  下游不知道"不会再有帧"；`stopStreaming(wait_for_drain)` 是外部整体
  停机，替代不了流内 EOS，处理有限输入（视频文件）时是硬缺口。设计
  要点：EOS 标记如何过 join（多输入端口的合流语义）、如何与对齐/丢弃
  协调、sink 完成通知。落地后收尾 `k_end_of_stream_frame_id` 的语义
  （目前仅是 sync_coordinator 的 watermark 哨兵，头文件已注明；若最终
  不做流内 EOS，则从 frame_constants 移除对外暴露）。

- [ ] **R6.2 编译期类型安全的节点层**：数据面目前全是 `std::any` +
  字符串键（DataPacket 参数、context 资源），`portPayloadType` 仅是
  运行时可选检查。在 ILogicNode 之上加模板化 TypedNode 糖层（声明式
  输入/输出端口类型），接线错误编译期报错，底层协议不变。验收：
  examples 中至少一个示例全程用 TypedNode 编写。（F9 基准否决的是
  索引化端口的性能收益，与本条的类型安全目标无冲突。）

- [ ] **R6.3 内存策略：packet 池与 allocator 注入点**：每个 packet 都是
  独立堆分配的 shared_ptr，无对象池、无 allocator hook、无设备内存/
  零拷贝概念。30fps × 多路流下分配 churn 是真实成本。最小落地：
  DataPacket 池化分配接口 + 引擎内部容器的复用审计；设备内存抽象仅做
  设计预留。

- [ ] **R6.4 反馈环（delay-edge）**：纯 DAG 无法表达 tracking、时域
  平滑等"输出回喂上游"拓扑。候选设计：带显式延迟语义的反馈边
  （初始帧注入 + z^-1 语义），保持无环调度性质不变。批模式完成语义
  受影响，与 F15（微批处理）统筹设计。

- [ ] **R6.5 子图组合**：pipeline 不能作为节点嵌套复用。候选设计：
  `SubgraphNode`（内部持有 CompiledGraph，端口映射到外层）或图级
  compose API；也是 JSON loader 的自然延伸（子图引用）。

- [ ] **R6.6 示例体系**：examples/ 目前仅一个 dummy.cpp。至少补齐：
  ① 多路摄像头 + 推理 + 对齐 join 的流式示例；② 视频文件批/流处理
  示例（依赖 R6.1）；③ TypedNode + JSON 构图 + 插件的组合示例。
  示例纳入 CI 编译。

## 已知问题

- [ ] **N1. 批模式完成通知可能重复触发**：任务完成路径上，两个 worker
  可能先后把 `m_activeTasks` 读成 0 并都走进
  `checkCompletionAndNotify` 的完成分支——引擎状态 CAS 保证状态只翻转
  一次，但 result 回调/observer 通知可能重复。runAsync 的 promise 已用
  done 标志自防，常驻回调的重复 `onExecutionCompleted` 对 observer
  可见。随 R5.3 定契约（at-least-once vs exactly-once）或在引擎加
  一次性完成闩。

- [ ] **N2. 策略热替换不触发 initialize**：`setSchedulerStrategy`/
  `setSyncStrategy` 在 IDLE 时可调用，但只有
  `ExecutionEngine::initialize()` 会对策略跑
  `initialize(CompiledGraph&)`——引擎已 initialize 后再 set 的策略拿
  不到拓扑（JoinAware 将没有任何 sync group）。门面路径不受影响
  （先 set 后 initialize）。随 R5.3 决定：set 时若已有 CompiledGraph
  即补跑 initialize，或在文档写明调用顺序契约。

## 性能观察项（跟踪记录见 docs/Performance_Report.md §5.4）

- [ ] **F13. 高 worker 数轻量帧场景的调度竞争**：worker 数远超有效并行
  度时流式吞吐下降；候选方向是 StreamScheduler 自适应并行度。

- [ ] **F14. 大队列容量的吞吐衰减**：容量增大时环形缓冲缓存局部性变差；
  候选方向是自适应容量或分段缓冲。

- [ ] **F15. 深线性管线微批处理**：线性链数据依赖限制强扩展性；帧级
  流水线重叠可突破，但会改变批模式完成语义，需设计（与 R6.4 统筹）。

## 明确不做

- **PortDataMap 字符串键的性能化替换**：F9 已用基准数据定为保留（收益
  < 0.15%，证据见 Performance_Report §7）；重开条件是 R5.1 真实负载
  profile 中 PortDataMap 进入热点前列。R6.2 是类型安全糖层、不改运行时
  协议，不受此约束。
- **无锁队列 / 线程池唤醒协议 / KeepLatest 契约 / join 看门狗**：实现与
  注释契约一致，不动。
- **install/ 目录疑似头文件双份**：未被 git 跟踪（构建产物），非问题。
