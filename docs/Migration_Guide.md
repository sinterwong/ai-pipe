# 迁移指南：v0.3.1 → v0.4.0

v0.4.0 由架构审计路线图驱动（路线图文档 `docs/TODO.md` 已随全部任务完成而移除，可在 git 历史 v0.4.0 tag 中查看），包含若干**破坏性变更**。
本指南按"编译会报错的"与"行为变化的"两类列出，并给出逐项迁移方法。

---

## 一、编译期变更（会直接报错）

### 1. 数据包所有权模型（最重要）

`PortDataPtr` 从 `shared_ptr<PortData>` 变为 `shared_ptr<const PortData>`。
从管道**收到**的数据包一律只读；零拷贝扇出因此天然无数据竞争。

```cpp
// 创建侧（不变）：make_shared 得到可变句柄，填完交给管道
auto packet = std::make_shared<PortData>();   // MutablePortDataPtr
packet->setParam("image", mat);
outputs["output"] = packet;                    // 隐式转为只读

// ❌ 0.3.1 可编译、0.4.0 报错：修改收到的包
inputs.at("input")->setParam("x", 1);

// ✅ 0.4.0 写法：显式写时复制
auto copy = ai_pipe::mutableCopy(inputs.at("input"));
copy->setParam("x", 1);
outputs["output"] = copy;
```

绝大多数节点遵循"创建→填充→交接"模式，无需改动（本仓库全部
内置节点与测试零修改通过编译）。

### 2. `SchedulingContext` 字段（仅影响自定义调度策略）

```cpp
// 0.3.1                                // 0.4.0
context.expected_input_ports.size()  →  context.expected_input_count
context.ready_input_ports.size()     →  context.ready_input_count
context.ready_input_ports.empty()    →  !context.hasReadyInput()
                                        context.ready_port_mask  // 新增位掩码
```

### 3. 已删除的类型

| 删除项 | 替代 |
|--------|------|
| `QueuePushResult` | `Result<PushStatus>`（`pushInput` 的返回类型） |
| `ThreadPool`（`thread_pool.hpp`） | `WorkStealingThreadPool` |
| `CoordinatedSyncStrategy` | `JoinAwareSyncStrategy`（引擎默认） |

### 4. `DataPacket` 存储私有化

直接访问 `.params` 成员的代码改用访问器：`param<T>()`（返回
`Result<T>`，R2.2 起唯一取参通路）/`setParam/has/paramCount/paramKeys`。

---

## 二、行为变更（编译通过但语义不同）

### 1. `initialize()` 校验图

空图返回 `GraphEmpty`，含环图返回 `GraphCycleDetected`（0.3.1 中空图
"成功"初始化，环仅在 `PipelineBuilder::build()` 检测）。

### 2. `id == 0` 表示"未分配帧号"

外部注入且 `id==0` 的包会被引擎盖上单调递增的 FrameId 并写入
`timestamp`；显式设置的 id 保留。节点新建的输出包若未设 id，自动继承
输入的帧标识。**依赖 `id` 恒为 0 的代码需要改用显式 id。**

### 3. 多输入节点按帧对齐取数（流模式）

启用同步策略时（STREAM/HYBRID 默认），join 节点只会收到 FrameId 相等
的输入组合；因兄弟分支丢帧而永失配对的帧会被丢弃并计入统计与 drop
回调。`id==0` 的包视为通配、不参与对齐（保留旧行为）。

### 4. 流模式下节点异常不再瘫痪节点

0.3.1：节点抛异常后永久 FAILED，后续帧滞留。0.4.0：异常是帧级事件，
统计/日志/错误回调照常触发，节点回到服务状态继续处理后续帧。
批模式不变（首个失败即停止本次运行）。

### 5. `total_executions` 语义统一

所有模式下均按**节点执行次数**计数（0.3.1 批模式按 run、流模式按节点，
导致批模式 successRate 可超 100%）。断言具体数值的代码需按
`运行次数 × 节点数` 调整。

### 6. DropTail 拒绝不再静默

队列满且策略为 DropTail 时，`pushInput` 返回 `QueueRejected` 错误
（0.3.1 返回 Enqueued 但数据实际丢失）。检查返回值的代码将开始看到
真实的失败。

---

## 三、推荐的增量采用（非必须）

- 节点重型初始化移入 `setup(context)` / 资源释放移入 `teardown()`。
- 端口声明 `portPayloadType()`，让连接错误在建图期暴露。
- 参数访问换用 `TypedParam<T>` / `param<T>()`，消除运行时字符串与异常。
- 节点类型加 `AI_PIPE_REGISTER_NODE`，为配置驱动构图做准备。
- 用户 logger 通过 `context->attachEngineLogs()` 统一接管框架日志。
