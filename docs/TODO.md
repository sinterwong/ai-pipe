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
- [ ] **P0.5** CI 增加 ASan + TSan 测试 job（先允许失败，修完转门禁）
- [x] **P0.6** 文档诚实化：删除/标注未实现的直方图、percentile、per-node stats、同步丢帧宣称；统一版本号
- [ ] **P0.7** 仓库卫生：`install/` 移出版本控制，处置未跟踪的 `3rdparty/logger`，清理 build 内无关产物
- [ ] **P0.8** 移除已弃用的 `QueuePushResult` 与遗留 `thread_pool.hpp`（迁移其测试）

## Phase 1 — 图核心重构：CompiledGraph

- [ ] **P1.1** 引入 `CompiledGraph`：节点/端口索引化（`NodeId`/`PortId`），预计算邻接表、入度、拓扑序、sink/source 集合、边路由表
- [ ] **P1.2** 环检测改迭代实现，`ExecutionEngine::initialize` 强制校验（返回 `GraphCycleDetected`）
- [ ] **P1.3** `propagateOutputs`/`routeToDownstream` 改用预编译路由表，消除热路径 O(E) 扫描
- [ ] **P1.4** `addEdge` 重复检查改哈希集合；`Graph` 内部映射以 `NodeId` 为 key
- [ ] **P1.5** 新增 propagate/schedule 微基准，Phase 1 前后对比数据写入 `docs/Performance_Report.md`

## Phase 2 — 并发与调度效率

- [ ] **P2.1** 线程池空闲挂起：去掉 1ms 轮询，改为纯条件变量唤醒 + 提交侧精确 notify
- [ ] **P2.2** 引擎任务提交走轻量路径（`void()` 任务直投，不构造 packaged_task/future）
- [ ] **P2.3** `waitForDrain`/`stopStreaming`/`stopExecutionSync` 条件变量化，删除轮询
- [ ] **P2.4** `SchedulingContext` 瘦身：端口就绪状态用位掩码，减少热路径字符串拷贝
- [ ] **P2.5** 每节点调度状态机审查：单原子 CAS 状态机替代 "atomic + per-node mutex" 双保险
- [ ] **P2.6** TSan 转为 CI 门禁

## Phase 3 — 数据平面：类型安全与所有权

- [ ] **P3.1** `DataPacket` v2：扁平存储替代 `std::map`，保留兼容层，新增 `TypedPort<T>` 声明式 API
- [ ] **P3.2** 端口类型校验：节点声明端口类型，`build()` 期校验边两端类型匹配
- [ ] **P3.3** 所有权模型：下游接收 `shared_ptr<const DataPacket>`，提供 COW 逃生门，文档明确约定
- [ ] **P3.4** `FrameId/StreamId/timestamp` 内建于 `DataPacket` 头部，引擎为 source 输入自动分配单调 FrameId

## Phase 4 — 同步子系统：接线或裁剪

- [ ] **P4.1** 引擎接线：多输入节点 FrameId 对齐（peek 对齐 + 落后等待 + 超时降级）；接通 `shouldDrop`/`markProcessed`
- [ ] **P4.2** 队列增加 `tryPeek`，设置 `frameIdAccessor`，drop 事件携带真实 frame_id
- [ ] **P4.3** 端到端集成测试：fork-join 图 + 注入丢帧，断言 join 帧对齐与兄弟分支同步丢弃
- [ ] **P4.4** 裁剪 `CoordinatedSyncStrategy`/`SyncCoordinator` 中接线后仍不可达的死代码
- [ ] **P4.5** 流模式节点失败后的恢复语义：当前节点异常后永久停留 FAILED，队列数据滞留（审计 P0.4 期间发现）

## Phase 5 — 可观测性真实化

- [ ] **P5.1** 接线 `recordLatency`、`total_queue_pops`、`total_wait_time_us`、`queue_full_events`、`total_input_frames`
- [ ] **P5.2** 启用 `AtomicNodeStatistics`：每 NodeState 持有，快照填充 `node_stats`
- [ ] **P5.3** 统一 batch/stream 的 `total_executions` 语义并文档化
- [ ] **P5.4** 统计测试补齐：snapshot 字段非零/单调断言；`enable_statistics=false` 零开销验证
- [ ] **P5.5** 日志统一：引擎日志经可注入 sink，`PipelineContext` adapter 可接管，废弃双轨

## Phase 6 — 开发者体验与工程化收尾

- [ ] **P6.1** 节点注册机制：注册宏 + `NodeRegistry::create(name, config)`，支撑配置驱动构图
- [ ] **P6.2** 节点生命周期：`ILogicNode::setup(context)/teardown()`（默认空实现），引擎在 initialize/reset 调用
- [ ] **P6.3** CMake 现代化：警告全开、sanitizer 选项、CMakePresets、静态库选项、CI 矩阵扩展
- [ ] **P6.4** clang-format/clang-tidy 入 CI；一次性风格统一（枚举/成员命名、Doxygen 补齐）
- [ ] **P6.5** 错误处理单轨收尾：`Graph` 异常改 `Result`，`DataPacket` 提供 `Result` 风格取参
- [ ] **P6.6** 文档重写：设计文档与实现对齐，新增节点开发指南与迁移指南，建立 CHANGELOG

---

## 里程碑

| 版本 | 包含阶段 | 达成标准 |
|------|----------|----------|
| v0.4 | P0 + P1 | 无已知正确性 bug；热路径无 O(E) 查询；文档与实现一致 |
| v0.5 | P2 + P3 | 空闲零 CPU；类型化端口；明确所有权模型 |
| v0.6 | P4 + P5 | join 帧对齐生效；全部统计真实可用 |
| v1.0 | P6 | 节点注册 + 生命周期 + 工程化门禁齐备；API 冻结 |
