# AI Pipe 演进 ROADMAP — TODO 清单

> 来源：2026-07 架构审计（评估报告见 PR 描述 / 会话记录）。
> 原则：先还债（正确性与诚实性），再重构（性能与架构），后增强（特性与 DX）。
> 规则：每完成一项，勾选对应条目并附提交哈希；每阶段结束测试必须全绿。

## 状态图例

- [ ] 未开始　[x] 已完成　（进行中的条目在行尾标注 `(WIP)`）

---

## Phase 0 — 正确性修复与信誉对齐

- [x] **P0.1** 修复 `pushToQueue` 静默丢数据：处理 `push()` 返回值，DropTail 拒绝时向上传播 `QueueRejected`，补计统计与回调
- [x] **P0.2** 修复 `m_currentContext` 数据竞争（随任务值捕获传递或原子化）
- [x] **P0.3** 删除 `ExecutionEngine::Impl` 的移动构造/赋值（`unique_ptr<Impl>` 已提供引擎级移动），消除在途任务悬垂指针风险
- [x] **P0.4** 修复流模式 `execute(wait=true)` 的 push-后即返回竞态
- [x] **P0.5** CI 增加 ASan + TSan 测试 job（先允许失败，修完转门禁）
- [x] **P0.6** 文档诚实化：删除/标注未实现的直方图、percentile、per-node stats、同步丢帧宣称；统一版本号
- [x] **P0.7** 仓库卫生：`3rdparty/logger/` 加入 .gitignore（本地遗留目录未删除，确认无用后可手动删除），清理 CI 中对它的过期 LD_LIBRARY_PATH 引用。（勘误：`install/` 本已在 .gitignore 中且未被跟踪，审计时误判）
- [x] **P0.8** 移除已弃用的 `QueuePushResult` 与遗留 `thread_pool.hpp`（其测试仅覆盖遗留类本身，随之删除；work-stealing 池已有独立测试套件）

## Phase 1 — 图核心重构：CompiledGraph

- [x] **P1.1** 引入 `CompiledGraph`：节点/端口索引化（`NodeId`/`PortId`），预计算邻接表、入度、拓扑序、sink/source 集合、边路由表
- [x] **P1.2** 环检测改迭代实现，`ExecutionEngine::initialize` 强制校验（返回 `GraphCycleDetected`）
- [x] **P1.3** `propagateOutputs`/`routeToDownstream` 改用预编译路由表，消除热路径 O(E) 扫描
- [x] **P1.4** `addEdge` 重复检查改哈希集合（建图从 O(E²) 降为 O(E)）。内部映射改 NodeId key 一项经评估放弃：shared_ptr 哈希即指针哈希，收益甚微，且热路径已由 CompiledGraph 接管
- [x] **P1.5** 新增 propagate/schedule 微基准，Phase 1 前后对比数据写入 `docs/Performance_Report.md`

## Phase 2 — 并发与调度效率

- [x] **P2.1** 线程池空闲挂起：去掉 1ms 轮询，改为纯条件变量唤醒 + 提交侧精确 notify
- [x] **P2.2** 引擎任务提交走轻量路径（`void()` 任务直投，不构造 packaged_task/future）
- [x] **P2.3** `waitForDrain`/`stopStreaming`/`stopExecutionSync` 条件变量化，删除轮询
- [x] **P2.4** `SchedulingContext` 瘦身：端口就绪状态用位掩码，减少热路径字符串拷贝
- [x] **P2.5** 每节点调度状态机审查：单原子 CAS 状态机替代 "atomic + per-node mutex" 双保险
- [x] **P2.6** TSan 转为 CI 门禁。P0.5 摸底的 22 处 data race 全部清零：
  - 线程池 `m_workers.size()` 构造期竞争 → P2.1 已修（改用 `m_queues.size()`）
  - `resetInternalState` 与上次执行尾部线程竞争 → `NodeState::execution_count`/`last_execution` 原子化
  - 性能阈值测试在 sanitizer 构建下自动 SKIP

## Phase 3 — 数据平面：类型安全与所有权

- [x] **P3.1** `DataPacket` v2：扁平存储替代 `std::map`（方法级 API 完全兼容，存储转私有），新增 `TypedParam<T>` 声明式 API（更名自 TypedPort——它绑定的是包内参数而非节点端口，类型化端口声明见 P3.2）
- [x] **P3.2** 端口类型校验：`ILogicNode::portPayloadType()` 可选声明端口负载类型，`addEdge` 期即校验（早于 build），双端声明且不匹配即拒绝，未声明默认放行
- [x] **P3.3** 所有权模型：下游接收 `shared_ptr<const DataPacket>`，提供 COW 逃生门，文档明确约定
- [x] **P3.4** `FrameId/StreamId/timestamp` 内建于 `DataPacket` 头部，引擎为 source 输入自动分配单调 FrameId

## Phase 4 — 同步子系统：接线或裁剪

- [x] **P4.1** 引擎接线：多输入节点 FrameId 对齐（peek 对齐，落后帧判定为永失配对直接丢弃并上报；等待由既有重调度机制承担）；接通 `shouldDrop`（路径节点提前丢弃）与 `markProcessed`（水位线推进）；新增 `ISyncStrategy::tracksNode` 供引擎初始化时缓存成员关系，避免每帧策略锁
- [x] **P4.2** 队列增加 `tryPeek`，设置 `frameIdAccessor`，drop 事件携带真实 frame_id
- [x] **P4.3** 端到端集成测试：fork-join 图 + 注入丢帧，断言 join 帧对齐与兄弟分支同步丢弃
- [x] **P4.4** 裁剪：删除完全无引用的 `coordinated_sync_strategy.hpp`（186 行）；`SyncCoordinator` 保留——其 API 现被 JoinAware 完整行使且有独立测试覆盖
- [x] **P4.5** 流模式节点失败后的恢复语义：当前节点异常后永久停留 FAILED，队列数据滞留（审计 P0.4 期间发现）

## Phase 5 — 可观测性真实化

- [x] **P5.1** 接线 `recordLatency`（sink 端到端延迟直方图）、`total_queue_pops`、`total_wait_time_us`（出队帧龄）、`total_input_frames`、`total_schedule_time_us`（READY→执行延迟）；`queue_full_events` 已在 P0.1 接线
- [x] **P5.2** 启用 `AtomicNodeStatistics`：每 NodeState 持有，快照填充 `node_stats`
- [x] **P5.3** 统一 batch/stream 的 `total_executions` 语义并文档化
- [x] **P5.4** 统计测试补齐：真实引擎跑批/流两模式断言全部接线字段非零且数值精确；`enable_statistics=false` 时门控字段保持为零
- [x] **P5.5** 日志统一：引擎日志经可注入 sink，`PipelineContext` adapter 可接管，废弃双轨

## Phase 6 — 开发者体验与工程化收尾

- [x] **P6.1** 节点注册机制：`AI_PIPE_REGISTER_NODE(_WITH_CONFIG)` 宏 + `NodeRegistry::create(type, name, config)`（PortData 作配置袋，保持零依赖）；JSON 构图加载器留作后续增强
- [x] **P6.2** 节点生命周期：`ILogicNode::setup(context)/teardown()`（默认空实现），引擎在 initialize/reset 调用
- [x] **P6.3** CMake 现代化：库目标 -Wall -Wextra -Wpedantic（修净全部告警）+ AI_PIPE_WERROR 选项、CMakePresets.json（debug/release/asan/tsan/static）、BUILD_SHARED_LIBS 显式化（静态构建验证通过）、CI 扩展 GCC-13+Clang-18 矩阵与 static+Werror job；sanitizer 选项已在 P0.5
- [x] **P6.4** clang-format 入 CI 为硬门禁（pin 22.1.5，全库一次性归一化 146 处漂移）；clang-tidy 入 CI 为咨询 job（发现分诊后转门禁）；补齐全部空 Doxygen @brief。枚举/成员命名统一评估后不做：公共枚举值改名属破坏性变更，收益不抵（记录于此）
- [x] **P6.5** 错误处理单轨收尾：`Graph` 度查询去异常（null 降级为 0+日志）；`DataPacket::param<T>()` 与 `TypedParam::read()` 提供全程无异常的 Result 取参（any_cast 指针形式）；`getParam` 抛异常版保留兼容
- [x] **P6.6** 文档重写：设计文档过时引用清理并指向新文档，新增 `docs/Node_Development_Guide.md` 与 `docs/Migration_Guide.md`，建立 `CHANGELOG.md`，版本升至 v0.4.0

---

## 里程碑

| 版本 | 包含阶段 | 达成标准 | 状态 |
|------|----------|----------|------|
| v0.4 | P0 + P1 | 无已知正确性 bug；热路径无 O(E) 查询；文档与实现一致 | ✅ |
| v0.5 | P2 + P3 | 空闲零 CPU；类型化端口；明确所有权模型 | ✅ |
| v0.6 | P4 + P5 | join 帧对齐生效；全部统计真实可用 | ✅ |
| v1.0 | P6 | 节点注册 + 生命周期 + 工程化门禁齐备；API 冻结 | ✅（工程项齐备；实际发布为 v0.4.0，API 冻结待真实业务验证后宣布） |

> 全部 6 个阶段 35 项任务已完成（2026-07-04）。后续候选增强：JSON 构图加载器（P6.1 注）、clang-tidy 转门禁（P6.4 注）、KeepLatest 语义细化。
