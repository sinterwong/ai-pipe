# AI Pipe 重构与演进 TODO（post-v0.5.0，2026-07-18 全面俯瞰版）

> 前两版 TODO（v0.4.0 架构审计 35 项、v0.5.0 后增强项 F1–F9）均已完成，
> 记录见 git 历史。本文件基于 2026-07-18 对全框架的再次俯瞰重新规划，
> 并按「顶级框架」标准补充了第二轮发现（装饰性 API 与能力边界）。
>
> **定位**：个人长期打磨的框架，无外部用户、无兼容包袱。唯一目标是达到
> 顶尖个人项目水平——需要起手产品时，它就是现成的顶级框架。
>
> **原则：先清算、后建设。** 改名、语义修复、架构合一放最前面——这类改动
> 越晚做成本越高（代码量增长、习惯固化、文档扩散）。所有已知的正确性缺陷、
> 装饰性 API、架构冗余、命名硬伤在 R1–R4 一次清掉，之后才进入建设期。
>
> 规则不变：每完成一项勾选并注明提交（一项一提交）；条目动手前先确认
> 前提仍成立（行号会漂移，以符号名定位）。

## 状态图例

- [ ] 未开始　[x] 已完成　（进行中的条目在行尾标注 `(WIP)`）

---

## R1. 正确性清算（门面层缺陷，最高优先级）

本次俯瞰的核心结论：引擎内核经两轮审计已扎实，但 Pipeline 门面层的打磨
明显落后，存在真实缺陷（非风格问题）。

- [x] **R1.1 runAsync 回调重绑缺陷**：`Pipeline::Impl::runAsync`
  （pipeline_impl.cpp）把携带 shared promise 的 lambda 设进引擎
  result/error 回调后永不恢复。后续 `run()`/`submit()` 完成时触发陈旧
  回调 → 对已满足的 promise 二次 `set_value()` 抛 `std::future_error`
  → 异常从 `checkCompletionAndNotify()` 穿出，跳过末尾
  `notifyCompletionWaiters()`，`waitForCompletion()` 无超时可能永久挂起
  （异常被线程池吞掉，无征兆）。两个正交修复都要做：
  ① runAsync 完成后恢复常驻回调（或改一次性回调语义）；
  ② 引擎调用用户回调处加异常防护，保证 notify 必达。补回归测试：
  runAsync 后接 run() 不挂起、不重复触发。
  ——已修复：① runAsync 的 promise 回调改一次性（`done` 标志防重复
  set_value），完成/出错/启动失败三条路径都先恢复常驻回调再兑现
  promise；② 引擎三个用户回调注册加 `m_callbackMutex`，调用统一走
  `invoke*Callback`（持锁取副本、锁外调用、catch-all 防护），
  `notifyCompletionWaiters()` 必达且回调内重注册安全。回归测试
  `PipelineAsyncTest.RunAfterRunAsyncCompletesWithoutRefire`（修复前
  该测试挂起）。提交 `fix: runAsync one-shot callbacks + guarded engine
  callback invocation`。

- [x] **R1.2 门面/引擎状态机漂移——错误分级**：流式模式下节点异常是
  per-frame 事件（`handleNodeFailure` 让节点回到服务），但
  `setupEngineCallbacks` 的 error 回调无条件把 `PipelineState` 打成
  ERROR。结果：一个坏帧后引擎照常 RUNNING、pushInput 照常工作，而门面
  `isRunning()==false`、`validateState` 拒绝一切操作直到 reset。
  修复方向：错误分级（per-frame vs pipeline-fatal），流式 per-frame
  错误只走 observer 通知不改门面状态；`PipelineState` 尽量从引擎状态
  派生而非自维护一份。
  ——已修复：常驻 error 回调按 `isStreaming()` 分级——流式 per-frame
  错误只记 m_lastError + observer 通知，不再置 ERROR；批模式维持
  pipeline-fatal。「从引擎派生」评估后否决：门面 RUNNING 是提交时刻的
  同步占位（AlreadyRunning 语义依赖它先于引擎转态），改派生会开出并发
  二次提交窗口，理由记录在 pipeline_impl.hpp m_state 注释。契约写入
  Framework_Design §6.2。回归测试
  `PipelineStreamingTest.NodeExceptionIsPerFrameNotPipelineFatal`。
  提交 `fix: grade node errors per-frame vs pipeline-fatal in the
  facade`。

- [x] **R1.3 run(timeout) 是事后检查**：`Pipeline::Impl::run` 先同步等
  执行完、再看耗时是否超限——节点挂死时 timeout 完全不起作用，与 API
  签名暗示不符。无兼容包袱，直接实现真超时：内部改
  `wait_for` + 超时触发 `cancel()`，超时后引擎状态与队列残留要有明确
  契约（写入 pipeline.hpp 注释）。补测试：慢节点场景 timeout 生效。
  与 R3.1（取消令牌接线）联动设计。
  ——已实现：`ExecutionEngine::execute` 增可选 timeout，
  `waitForCompletion(timeout)` 用 `wait_for` 定界；到期取消 token
  （协作通道，接 R3.1）+ 触发停机协议，立即返回 ExecutionTimeout，
  不等挂死节点。契约（未排干队列残留、需 reset()、不合作节点跑到
  process() 返回为止）写入 pipeline.hpp run(timeout) 注释与
  execution_engine.hpp。测试
  `PipelineTimeoutTest.TimeoutFiresWhileNodeHangs`（挂死节点 100ms 即
  返回，修复前永久阻塞）/ `TimeoutCancelsCooperativeNode`。提交
  `fix: make run(timeout) a real bounded wait (R1.3)`。

- [x] **R1.4 execute() 过早写 m_currentContext**：
  `ExecutionEngine::Impl::execute` 在校验 AlreadyRunning 之前
  `m_currentContext.store()`，错误路径又置 null——并发误调用第二个
  execute 会清掉在跑执行的 context，之后新调度的任务拿到 null context。
  把 store 挪到状态校验通过之后。
  ——已修复：store 移入 engineMutex 临界区、校验全部通过之后；
  AlreadyRunning/NotInitialized 错误路径不再碰 context。顺带：流式路径
  不再覆盖 startStreaming 装入的 context（同族缺陷，execute 的 context
  实参在流式下本就无消费者）。回归测试
  `ExecutionEngineTest.RejectedConcurrentExecuteKeepsRunningContext`。
  提交 `fix: claim m_currentContext only after execute() validation
  (R1.4)`。

- [x] **R1.5 删除 Pipeline::Impl 的移动操作**：引擎回调捕获 Impl 的
  `this`，而 Pipeline 走 `unique_ptr<Impl>` 移动（Impl 地址稳定），
  Impl 自身的移动构造/赋值实际走不到——一旦被用上就是悬垂捕获。直接
  delete，与引擎 Impl 的做法（显式 delete + 注释理由）对齐。
  ——已删除：移动构造/赋值定义整体移除，声明改显式 delete + 地址稳定性
  注释（与 ExecutionEngine::Impl 同款）。Pipeline 门面的移动语义不变
  （unique_ptr 搬移）。提交 `refactor: delete Pipeline::Impl move
  operations (R1.5)`。

- [x] **R1.6 DropEvent 字段语义修复**：`LockFreeNodeQueue::notifyDrop`
  把 `capacity()` 赋给 `queue_size_before`，字段名与含义不符。改为真实
  的 drop 前队列长度（或删掉该字段改为 capacity 字段，取其一，语义与
  命名必须一致）。
  ——已按裁决修复：保留字段名，五个驱逐点在 evictOne 前采样队列长度传入
  notifyDrop；并发下为瞬时快照（DropEvent 字段注释注明）。测试
  `LockFreeDropStrategyTest.DropEvent_QueueSizeBeforeIsRealSize`
  （KeepLatest 窗口 3/容量 8 场景区分新旧语义）。提交 `fix: report the
  real pre-drop queue length in DropEvent (R1.6)`。

## R2. 命名与 API 边界清算（无兼容包袱，现在做而不是拖到 1.0）

- [x] **R2.1 DataPacket 迁出 common_utils 命名空间**：核心公共类型顶着
  `ai_pipe::common_utils` 工具命名空间（data_packet.hpp），迁至
  `ai_pipe` 根命名空间，`common_utils` 别名一并清除（无外部用户，不留
  过渡别名）。连带检查 data_types.hpp 里的相关 using。
  ——已迁移：data_packet.hpp 改 `namespace ai_pipe`，不留别名；
  data_types.hpp 的 `PortData` using、两个测试文件、
  Framework_Design/Node_Development_Guide 引用同步更新。全库
  `common_utils` 归零。提交 `refactor: move DataPacket to the ai_pipe
  root namespace (R2.1)`。

- [x] **R2.2 移除异常双轨，统一 Result**：删除
  `DataPacket::getParam`/`getOptionalParam` 的抛异常路径与
  `CancellationToken::throwIfCancelled`，`param<T>()`/`TypedParam::read`
  成为唯一取参通路（`TypedParam::get/tryGet` 随之改签名或删除）。
  节点侧 `process()` 抛异常仍由引擎捕获转 Error——那是防御边界，不属于
  双轨。全库及测试、docs/Node_Development_Guide.md 同步迁移。
  ——已移除：getParam/getOptionalParam/TypedParam::get/tryGet/
  throwIfCancelled 全部删除；`ScopedNodeExecution::checkCancellation`
  改为查询式 `cancellationRequested()`。测试迁移为 Result 断言
  （valueOr 默认值语义补测试），loader/registry/benchmark 调用点、
  README、Framework_Design §3.2、Node_Development_Guide §2、
  Migration_Guide 同步。process() 抛异常仍是引擎防御边界（文档明确）。
  提交 `refactor!: remove the exception dual-track, Result is the only
  error channel (R2.2)`。

- [x] **R2.3 日志通路统一**：公共 API 有 `ILoggerAdapter`
  （context.hpp），但引擎/门面的 `LOG_*_S` 全走私有 logger 单例；
  logger.hpp 私有化后，链接方无法重定向或静音框架自身日志。冻结前必须
  给出公共控制面：最小方案是公共头暴露全局级别 + sink 注入（桥接到
  内部 Logger::addCallback）；理想方案是框架内部日志也走 adapter 抽象，
  两条通路合一。决策后落地并文档化。
  ——已按最小方案落地：新公共头 `ai_pipe/engine_log.hpp`
  （setEngineLogLevel / engineLogLevel / setEngineConsoleLogging /
  addEngineLogSink / removeEngineLogSink），engine_log.cpp 桥接
  Logger 单例；通路合一作为后续方向记录在头文件注释与
  Framework_Design §10.2。测试 `LoggerTest.PublicSurface*`/
  `PublicSink*`。提交 `feat: public control surface for framework
  logging (R2.3)`。

- [x] **R2.4 "Lock-Free" 宣称精确化**：`tryPeek` 与生产者侧驱逐共用
  `m_headMutex`（lock_free_queue.hpp），多输入 join 的对齐 gather 每次
  peek 都拿锁。实现注释已诚实，README 与 Framework_Design 的措辞对齐
  事实（"核心 push/pop 无锁；peek/驱逐路径互斥保护"）。
  ——已对齐：README 特性条目改为 "Lock-Free Core Queue Paths" 并注明
  peek/驱逐互斥；Framework_Design §7.1 增补精确边界段（含 join gather
  每次 peek 拿锁的代价说明）。纯文档改动。提交 `docs: state the exact
  lock-free boundary of the MPMC queue (R2.4)`。

## R3. 装饰性 API 清算（接线或删除——公共接口不留摆设）

第二轮俯瞰发现的一类硬伤：公共 API 中存在、但引擎从未消费的机制。
对顶级框架而言，装饰性 API 比没有更糟——每一项都必须二选一：接线成
真实语义，或从公共接口删除。

- [x] **R3.1 CancellationToken 无人消费**：全 src 无任何 `isCancelled()`
  读取点（仅 context 自身 reset）；`Pipeline::cancel()` 走引擎
  stopFlag，不碰 token。裁决方向（倾向接线，与 R1.3 真超时共用机制）：
  cancel()/run 超时统一触发 token；引擎在调度点（tryScheduleNode /
  executeNodeTask 入口）检查 token 与 stopFlag 等效对待；节点内长任务
  的协作式检查写入 Node_Development_Guide。若裁决为删除，则 context
  中 token 及相关 API 一并移除。
  ——已按裁决接线：引擎新增 `isStopRequested()`（stopFlag ∪ token，
  token 取消经 stopExecutionAsync 一次性转换进停机协议），
  tryScheduleNode / executeNodeTask 入口统一走它；execute/startStreaming
  开始时复位 token（上次取消不污染本次）；`Pipeline::cancel()` 双通道
  （token + 引擎停机）。协作式检查写入 Node_Development_Guide §6。测试
  `PipelineCancellationTest.CancelReachesNodeThroughToken` /
  `DirectTokenCancelStopsScheduling`。提交 `feat: wire CancellationToken
  into the engine stop protocol (R3.1)`。

- [x] **R3.2 DeferToNextCycle 的 delay 是死数据**：`tryScheduleNode`
  只响应 `ScheduleNow`，没有定时器消费 defer——`min_execution_interval`
  限流触发后节点只能等下一个数据事件重新评估，尾帧可能无限滞留。
  裁决方向：泛化 join 看门狗为引擎级定时唤醒（timer 驱动的延迟
  reschedule），使 Defer(delay) 语义成真；或从
  `ScheduleDecision`/`ScheduleResult` 删除 Defer 与 min_interval 配置。
  补测试：限流下尾帧最终被执行。
  ——已按裁决接线：引擎级 defer 定时器（懒启动线程 + 最小堆 +
  per-node defer_pending 去重），`tryScheduleNode` 消费
  DeferToNextCycle 并按 retry_delay 定时重评估；顺带修复
  StreamSchedulerStrategy 的限流检查位置（原先排在就绪返回之后，输入
  就绪时永远够不到——min_interval 实际从未限过流）。测试
  `PipelineStreamingTest.RateLimitedTailFrameEventuallyExecutes`。
  提交 `feat: engine defer timer makes DeferToNextCycle real (R3.2)`。

- [x] **R3.3 HybridSchedulerStrategy 名不副实**：注释宣称 "per-node
  configuration of scheduling behavior"，实现无一行 per-node 配置，
  实际等于"无 partial-inputs 的 Stream"。要么实现 per-node 调度配置
  （需求存疑），要么删除该策略与 `ExecutionMode::HYBRID`（连带
  configureForMode/JSON loader/文档/测试），把模式收敛为 BATCH/STREAM。
  倾向删除：模式越少，语义越硬。
  ——已按裁决删除：HybridSchedulerStrategy、ExecutionMode::HYBRID、
  EngineConfig::hybrid()、createHybridEngine、configureForMode HYBRID
  分支、门面 start()/withMode 的 HYBRID 分支、全部 Hybrid 测试
  （策略套件/门面端到端套件/工厂/配置）与 benchmark 工厂、README/
  Framework_Design/Migration_Guide/Performance_Report 措辞，模式收敛为
  BATCH/STREAM。核实：JSON loader 本就只接受 batch/stream，无需改动。
  提交 `refactor!: remove HybridSchedulerStrategy and
  ExecutionMode::HYBRID (R3.3)`。

- [x] **R3.4 死配置旋钮清理**：`StreamSchedulerConfig::min_input_ratio`
  默认 1.0 且仅在 allow_partial_inputs 下参与判断，组合语义含混；
  逐一核对 `EngineConfig`/`PipelineOptions`/`QueueConfig` 全部字段，
  每个旋钮要么有测试覆盖的真实语义，要么删除。
  ——审计结论：真死旋钮一个——`EngineConfig::enable_sync_coordination`
  （三处公共面暴露、loader 解析、门面拷贝，但引擎从不读取，STREAM 恒装
  JoinAware）。裁决接线而非删除：STREAM 下按该旗标选
  JoinAware/NoSync，字段注释写明语义与 setSyncStrategy 覆盖关系。
  `min_input_ratio` 保留（0.5/0.8 已有测试），默认 1.0→0.0：单开
  allow_partial_inputs 即为"任一输入就绪可调度"，不再是形同虚设的
  组合陷阱。其余字段（QueueConfig 全部、EngineConfig/PipelineOptions
  其余）逐一核实均有真实消费点；`min_execution_interval` 已由 R3.2
  变为真实语义。测试
  `ExecutionEngineTest.SyncCoordinationKnobSelectsSyncStrategy`、
  `StreamSchedulerConfigTest.PartialInputsAloneSchedulesOnAnyReadyInput`。
  提交 `fix: wire enable_sync_coordination; sane partial-inputs default
  (R3.4)`。

- [x] **R3.5 EOS 常量语义定案（依赖 R6.1 设计）**：
  `k_end_of_stream_frame_id` 目前只在 sync_coordinator 里当哨兵最大值
  用，框架没有 EOS 协议。若 R6.1 落地流内 EOS，则该常量获得真实语义；
  若裁决不做，则从 frame_constants 中移除对外暴露，避免暗示不存在的
  能力。本条目跟踪最终清理动作。
  ——按裁决本轮只做现状标注（EOS 设计未启动，不删常量）：
  frame_metadata.hpp 在常量与 isEndOfStream()/createEndOfStream() 处
  明确写出「无 EOS 协议、目前仅作 watermark 哨兵、真实语义待 R6.1
  或届时移除」。最终清理动作转由 R6.1 落地时收尾。提交 `docs: mark
  the EOS constant as protocol-less pending R6.1 (R3.5)`。

## R4. 架构冗余清算

- [x] **R4.1 引擎节点查找结构收敛**：`m_nodeStates`
  （unordered_map<NodePtr, unique_ptr<NodeState>>）、`m_nodeNameMap`、
  `m_statesByIndex` 三份并存。收敛为按 `CompiledGraph::NodeIndex` 索引
  的 `vector<unique_ptr<NodeState>>`，name/ptr 查找复用 CompiledGraph；
  冷路径（statistics/reset/allQueuesDrained）随之简化为顺序遍历。
  ——已收敛：唯一容器 `vector<unique_ptr<NodeState>>`，新增
  stateByIndex/stateByName/stateByPtr 三个查找辅助（name/ptr 走
  CompiledGraph::indexOf/indexOfPtr）；pushToQueue/getQueueSize 改收
  `NodeState&`（routeToDownstream 顺带改为按边索引直达，免去 ptr 反查）；
  initializeNodeStates 按 CompiledGraph 顺序构建。行为不变，现有全部
  测试为回归网。提交 `refactor: converge engine node lookup on a single
  index-keyed vector (R4.1)`。

- [ ] **R4.2 策略接口摆脱 Graph\* 生命周期**：`ISchedulerStrategy`/
  `ISyncStrategy::initialize(Graph*)` 让策略持有裸指针
  （JoinAwareSyncStrategy 还长期存 `m_graph`），直接用引擎的用户要自己
  保证 Graph 活得比引擎久。CompiledGraph 已持有节点所有权：策略接口改
  以 `const CompiledGraph&`（或专门的拓扑快照视图）初始化，初始化后不
  再依赖可变 Graph。这是公共接口变更，趁无包袱做掉。

- [ ] **R4.3 对齐 gather 骨架统一 + 拆出 alignment 组件**：
  `gatherAlignedInputs`/`gatherStreamAlignedInputs`/
  `gatherTimestampAlignedInputs` 共享同一循环骨架（peek 全部 → 判对齐
  → 弹配对 / 丢滞留 → 循环），`degradeJoinGather` 里还有第四份配对谓词。
  抽成以「对齐谓词 + 滞留选择」为参数的统一实现，从 2400 行的
  execution_engine_impl.cpp 拆出 internal alignment 组件（如
  `src/frame_alignment.hpp`），现有 alignment/join-timeout 测试全绿为
  验收线。

- [ ] **R4.4 wait() 轮询改阻塞**：`Pipeline::Impl::wait()` 是 10ms
  sleep 轮询；引擎已有 completion CV，暴露一个阻塞等待接口
  （如 `ExecutionEngine::waitForIdle()`）供门面使用。

- [ ] **R4.5 批模式完成检查去堆分配**：`checkCompletionAndNotify` 每个
  任务完成都构造 `sink_counts` unordered_map（per-task 堆分配，微秒级
  节点的批模式可见）。sink 索引在 initialize 预计算，快照复用 vector。

## R5. 建设期：验证与冻结（清算完成后启动；承接原 F10–F12）

- [ ] **R5.1（原 F10）真实业务负载验证**：至少一个生产场景 7×24 长期
  运行。框架的下一步考验应来自真实负载而非继续内部打磨。实施中撞到的
  能力墙按需拉动 R6 对应条目（预计首先撞到 R6.1 EOS 与 R6.3 内存策略）。

- [ ] **R5.2（原 F11）ABI/SemVer 政策文档**：1.0 冻结前明确公共头文件
  清单、ABI 兼容承诺与弃用流程。虽无外部用户，「产品起手即顶级框架」
  要求政策先行。

- [ ] **R5.3（原 F12，范围收窄）公共 API 终审**：逐头文件 review。
  异常兼容层去留已由 R2.2 定案、装饰性 API 由 R3 定案；剩余关注点：
  头文件自洽性（compile-check 已有 CI）、命名一致性、`Timestamp` 用
  steady_clock 对跨进程/多机时间戳对齐的语义边界要在文档中明确。

## R6. 建设期：能力边界（产品级特性，一律设计先行）

按「顶级框架」标准衡量出的能力缺口。每项动手前先写设计短文
（docs/design/，含目标、非目标、与现有语义的冲突分析），评审通过再
实现。排序为预估拉动顺序，实际以 R5.1 真实负载的需求为准。

- [ ] **R6.1 流内 EOS / flush 协议**：目前源无法宣告流结束、无 flush
  传播、下游不知道"不会再有帧"。`stopStreaming(wait_for_drain)` 是
  外部整体停机，替代不了流内 EOS——处理有限输入（视频文件）的产品
  第一天就会撞墙。设计要点：EOS 标记如何过 join（多输入端口的 EOS
  合流语义）、如何与对齐/丢弃协调、sink 完成通知。落地后 R3.5 收尾。

- [ ] **R6.2 编译期类型安全的节点层**：数据面目前全是 `std::any` +
  字符串键（DataPacket 参数、context 资源），`portPayloadType` 仅是
  运行时可选检查。F9 否决的是索引化端口的**性能**收益；本条目是
  **类型安全**收益：在 ILogicNode 之上加模板化 TypedNode 糖层
  （声明式输入/输出端口类型），接线错误编译期报错，底层协议不变。
  验收：examples 中至少一个示例全程用 TypedNode 编写。

- [ ] **R6.3 内存策略：packet 池与 allocator 注入点**：每个 packet 都是
  独立堆分配的 shared_ptr，无对象池、无 allocator hook、无设备内存/
  零拷贝概念。30fps × 多路流下分配churn是真实成本。最小落地：
  DataPacket 池化分配接口 + 引擎内部容器的复用审计；设备内存抽象仅做
  设计预留，不过度建设。

- [ ] **R6.4 反馈环（delay-edge）**：纯 DAG 无法表达 tracking、时域
  平滑等"输出回喂上游"拓扑。候选设计：带显式延迟语义的反馈边
  （初始帧注入 + z^-1 语义），保持无环调度性质不变。批模式完成语义
  受影响，需与 F15（微批处理）设计统筹。

- [ ] **R6.5 子图组合**：pipeline 不能作为节点嵌套复用。候选设计：
  `SubgraphNode`（内部持有 CompiledGraph，端口映射到外层）或图级
  compose API。组合性是顶级框架的标志能力，也是 JSON loader 的自然
  延伸（子图引用）。

- [ ] **R6.6 示例体系**：examples/ 目前仅一个 dummy.cpp——顶级个人
  项目一半的说服力来自示例。至少补齐：① 多路摄像头 + 推理 + 对齐
  join 的流式示例（同时是 F5/F6 的活体验证）；② 视频文件批/流处理
  示例（撞 R6.1）；③ TypedNode + JSON 构图 + 插件的组合示例。
  示例纳入 CI 编译。

## 长期观察项（性能报告跟踪，见 docs/Performance_Report.md §5.4）

- [ ] **F13. 高 worker 数轻量帧场景的调度竞争**：worker 数远超有效并行
  度时流式吞吐下降；候选方向是 StreamScheduler 自适应并行度。
- [ ] **F14. 大队列容量的吞吐衰减**：容量增大时环形缓冲缓存局部性变差；
  候选方向是自适应容量或分段缓冲。
- [ ] **F15. 深线性管线微批处理**：线性链数据依赖限制强扩展性；帧级
  流水线重叠可突破，但会改变批模式完成语义，需设计（与 R6.4 统筹）。

## 明确不做（有据可查的裁决，防止重开）

- **PortDataMap 字符串键的性能化替换**：F9 已用基准裁决保留（收益
  < 0.15%，证据见 Performance_Report §7）；重开条件是 R5.1 真实负载
  profile 中 PortDataMap 进入热点前列。注意与 R6.2 的区别：R6.2 是
  类型安全糖层，不改运行时协议，不受此裁决约束。
- **无锁队列 / 线程池唤醒协议 / KeepLatest 契约 / join 看门狗**：本次
  俯瞰确认实现与注释契约一致，是全库质量最高的部分，不动。
- **install/ 目录疑似头文件双份**：已核实未被 git 跟踪（构建产物），
  无漂移风险，非问题。
