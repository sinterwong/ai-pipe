# R6.1 流内 EOS / flush 协议

> 状态：已实现（2026-08）。本文是 docs/TODO.md R6 要求的设计短文，
> 记录目标、非目标、方案选择与被否决的替代方案。

## 1. 问题

管线此前没有"这一路播完了"的表达方式。

- 源无法宣告流结束。有限输入（播完的视频文件、读到尾的数据集）只能
  沉默地停止 `pushInput`，下游永远不知道不会再有帧。
- 没有 flush 传播。带内部缓冲的节点（攒批推理、时域平滑、滑窗聚合）
  在流末尾会把最后不足一批的数据永久留在自己肚子里。
- `stopStreaming(wait_for_drain)` 是**外部整体停机**：它停的是整条
  管线，且不区分"数据已处理完"与"被强行叫停"。它替代不了流内 EOS。
- `k_end_of_stream_frame_id` / `IFrameMetadata::isEndOfStream()` 已在
  公共头文件里存在，但没有任何引擎路径产生或消费它（R3.5 注释已写明
  这一点）。这是必须收尾的悬空语义。

## 2. 目标

1. 源可以在**某个输入端口**上宣告"此路输入到此为止"。
2. EOS 沿边传播，且严格排在该端口**已入队数据之后**。
3. 节点可以在收到 EOS 时 flush 内部缓冲并产出最后一批输出。
4. join 节点有明确的多输入合流规则。
5. 所有 sink 到达 EOS 后，调用方可被通知/可阻塞等待。
6. 收尾 `k_end_of_stream_frame_id` 的语义。

## 3. 非目标

- **不做**流的重启：EOS 是本次 run 的终态，重新开始须 `reset()`。
- **不改**批模式语义。批模式的完成条件仍是"每个 sink 执行过一次"，
  EOS 只在流式模式下有意义。
- **不做**部分流（per-`StreamId`）EOS。多路复用同一端口的场景下，
  EOS 的粒度是**端口**，不是 `StreamId`。理由见 §6.3。
- **不做**背压意义上的 flush（"把队列催空"）：本协议表达的是
  "不会再有数据"，不是"请赶紧处理"。

## 4. 方案：每端口 EOS 闩（latch），而非入队的 EOS packet

最初的直觉方案是往队列里塞一个 `id == k_end_of_stream_frame_id` 的
in-band packet。**否决**，理由是它和现有的丢弃语义直接冲突：

- 每个输入队列都带丢弃策略。`DropHead` / `KeepLatest` 会在队列满时
  淘汰队头——EOS packet 会被**静默丢掉**，协议随即失效。
- `DropTail` 反过来：队列满时 `push` 直接失败，EOS 根本进不去。
  ingress 是非阻塞的，没有可以等待空位的地方。
- 要让 EOS 免疫丢弃，就得给队列开一条"强制入队"后门；而强制入队要么
  淘汰一帧真实数据（改变了丢弃语义），要么突破容量（破坏无锁环形缓冲
  的容量不变式）。两条路都要动 R5 已冻结的 `lock_free_queue.hpp`。

改用**每个输入端口一个 EOS 闩**（`NodeState` 上的 per-port 原子标志）：

- 闩独立于队列存储，因此**永不被丢弃策略吃掉**，也不占容量。
- 排序语义仍是 in-band 的：端口只有在 `queue.empty() && eos_latched`
  时才算"到达 EOS"，所以闩天然排在所有已入队数据之后。
- 无锁队列一行不改。

代价：闩是布尔而非计数，所以同一端口上"EOS → 再来数据 → 再 EOS"的
循环不被支持。这与"不做流重启"的非目标一致，且 EOS 之后的 `pushInput`
会被显式拒绝（见 §6.1），不会静默乱序。

## 5. 状态机

每个输入端口三态：

```
  OPEN ──signalEndOfStream()/上游传播──► CLOSING ──队列排空──► DRAINED
```

- `OPEN`：正常收数据。
- `CLOSING`：闩已置位，但队列里还有数据；节点继续正常执行。
- `DRAINED`：闩已置位且队列空。该端口不会再有数据。

节点级：**所有**输入端口都 `DRAINED` 时，节点到达 EOS。源节点
（无输入端口）由外部对其入口端口显式 `signalEndOfStream` 驱动。

## 6. 关键规则

### 6.1 Ingress

`ExecutionEngine::signalEndOfStream(node, port)` 置位目标端口的闩并
唤醒调度。EOS 之后再对同一端口 `pushInput` 返回
`ErrorCode::EndOfStreamSignaled` —— 这是调用方的时序 bug，静默接受
会造成 EOS 之后仍有数据的乱序。

### 6.2 join 合流：合取（AND）

节点只有在**全部**输入端口到达 EOS 后才向下游传播 EOS。

否决的替代方案是析取（任一端口 EOS 即传播）：那会在一路先播完时把
整个下游判死，而 join 完全可能还要靠其余分支继续产出。合取规则下
"下游收到 EOS" 严格等价于"上游所有路径都不会再有数据"，这才是下游
真正需要的保证。

### 6.3 EOS 与对齐的交互

多输入节点在对齐模式下按帧号配对。若一路已 `DRAINED` 而其余仍在跑，
继续等待该端口会**永久阻塞** join。

规则：**已 `DRAINED` 的端口退出配对集合**，对齐在剩余活跃端口间进行，
节点收到部分输入执行。这复用了 F6 join 超时已有的
`JoinTimeoutPolicy::PartialInputs` 降级路径——语义一致（"这个端口不会
再有数据了，别等了"），且是已经过测试的代码路径。

per-`StreamId` EOS 被排除也是因为这里：对齐的配对键是
`(stream_id, frame_id)`，若允许单个 stream 单独 EOS，配对集合会变成
随时间变化的动态集合，降级规则要按 stream 分别判定，复杂度远超
本轮收益。端口粒度足够覆盖"一路视频播完"这个真实场景。

### 6.4 flush 钩子

节点到达 EOS 时，引擎**不**用空输入调 `process()`（节点没有理由为
"没有输入"这件事写一个分支），而是调用新的
`ILogicNode::onEndOfStream(outputs, context)`，默认空实现。

- 攒批节点在这里吐出不足一批的残留。
- 钩子的输出走**与 `process()` 完全相同**的传播路径：继承帧身份、
  入下游队列、触发下游调度、sink 则计入结果。
- 钩子抛异常按节点失败处理，但**不阻断 EOS 传播**：下游仍会收到
  EOS，否则一个节点的 flush bug 会让整条管线永远等不到结束。

### 6.5 传播与 sink 完成

节点 EOS 处理完（flush 输出已传播）后，引擎给它每条出边的目标端口
置闩，并调度下游。EOS 因此沿拓扑序自然流到 sink。

所有 sink 节点都到达 EOS 时，引擎：

1. 置 `m_endOfStreamReached`；
2. 唤醒 `waitForEndOfStream()` 的等待者；
3. 触发 `IPipelineObserver::onEndOfStream()`。

引擎**不**自动停机。EOS 是"数据流完了"，停机是调用方的决定——自动
`stop()` 会剥夺调用方读取最终统计、做收尾处理的机会。典型收尾是
`signalEndOfStream() → waitForEndOfStream() → stop()`。

### 6.6 与丢弃协调

EOS 闩不进队列，因此丢弃策略、`SyncCoordinator` 的协同丢弃、
watermark 计算全部不受影响。`k_end_of_stream_frame_id` 保留其
watermark 哨兵用途不变。

## 7. `k_end_of_stream_frame_id` 的收尾

本方案不产生带该 id 的 packet，所以该常量**不会**成为在管线中流动的
标记。它保留两个用途，头文件注释同步更新为最终语义：

1. `SyncCoordinator` watermark 计算的哨兵最大值（既有用途）；
2. `IFrameMetadata::isEndOfStream()` / `createEndOfStream()` 供**节点
   自定义的载荷级**流结束标记使用——引擎不解释它。

引擎级的流结束是端口闩，不是帧 id。R3.5 那条"没有任何引擎路径消费它"
的状态注释因此改写，而不是删除常量。

## 8. 验收

- 线性链：源 EOS → 中间节点 flush → sink 收到 EOS，`waitForEndOfStream`
  返回。
- 攒批节点在 EOS 时吐出残留批次，且该批次到达 sink。
- join：一路 EOS 后另一路仍能产出；两路都 EOS 后下游才收到 EOS。
- 对齐模式下一路 EOS 不阻塞 join。
- EOS 后 `pushInput` 被拒绝。
- flush 钩子抛异常仍不阻断 EOS 传播。
- EOS 闩不受 `DropHead` / `DropTail` 队列压力影响。
