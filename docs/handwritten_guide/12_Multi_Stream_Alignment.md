# 12. 多流对齐取数与滞留帧丢弃 (Multi-Stream Alignment)

在多输入节点（即 Join 合流节点）运行中，即使我们部署了 `ISyncStrategy` 协调丢弃机制，在并发物理环境中，各路数据依旧会因为网络抖动、解码卡顿而无法做到“同时、同步齐整地抵达 Join 节点的各输入端口队列”。

```
Port A Queue:  [Frame 10] [Frame 11] [Frame 12]  <-- Fast
Port B Queue:  [Frame 9 ] [Frame 10]             <-- Slow
```

当 Join 节点试图 Gather 数据时，如果它直接从各自队头盲目 Pop，会导致将 `Port A` 的帧 10 与 `Port B` 的帧 9 配对在一起。
这种“时序漂移（Temporal Drift）”在 AI 场景（例如多摄像头轨迹融合、音画同步）中是灾难性的。

多流对齐引擎负责在 Join 节点的队头，利用 **非占有式 TryPeek 与单调丢弃逻辑** 建立坚不可摧的时序防线。

---

## 1. 核心设计原理

### 1.1 多流对齐核心三策略（Alignment Policies）
为了支持不同的对齐键，AI Pipe 抽象并实现了三种物理对齐算法：
1.  **帧 ID 精确配对（`FrameId`）**：默认策略。只有当各个端口队头数据的 `FrameId` 精确相等时，才视为对齐成功、允许配对打包取出。
2.  **流内帧 ID 配对（`StreamFrameId`）**：适用于各物理流独立编号、可能存在 ID 重合的场景。
3.  **时间戳配对（`Timestamp`）**：完全忽略 FrameId。对齐层动态扫描各端口队头包的物理时间戳，如果它们的极差（Max - Min）小于 `alignment_tolerance`（默认 33ms），则视为时间对齐。

### 1.2 滞留帧丢弃契约（Straggler Drop Contract）
当队头数据不齐时（例如上面的 `Port A` 队头是 10，`Port B` 队头是 9）：
*   **死锁危机**：如果我们不清理队列，`Port A` 就会因为不齐而卡住，不再接收新数据。而 `Port B` 的帧 10 此时正压在帧 9 身后，因为队列满而挤不进来。系统直接陷入死锁。
*   **解决方案（单调丢弃）**：
    *   **在精确对齐（`FrameId` 或 `StreamFrameId`）下**：队头不齐时，找到所有端口队列队头中**最新的 FrameId（即最大值 MaxId）**。
    *   **丢弃滞后者**：对于那些队头 `FrameId` 严格小于 `MaxId` 的落后包（Stragglers），直接将其从队列中弹出（Pop 丢弃）并上报。
    *   **效果**：这样落后的帧 9 瞬间被清除，`Port B` 队头露出了帧 10。在下一轮重试中，`Port A(10)` 与 `Port B(10)` 完美配对，整个管道瞬间顺畅。

---

## 2. 核心巧思与实现细节

### 2.1 依赖非占有式 TryPeek 的两阶段 Gather
如果我们直接调用 `tryPop` 来检查数据是否对齐，一旦发现不对齐，被 Pop 出来的包已经无法放回 Lock-Free 环形数组中，会造成严重的数据丢失。

**实现巧思**：
利用 15 份文档中第 3 篇详述的 `LockFreeNodeQueue::tryPeek` 实现**非占有式两阶段 Gather**：
1.  **第一阶段（Peek 校验）**：
    *   引擎对 Join 节点的所有输入端口队列调用 `tryPeek`，临时取得队头数据的指针副本。
    *   根据选定的 `AlignmentPolicy` 算法，对这些队头指针进行比对。
2.  **第二阶段（Pop 确认）**：
    *   若**比对成功**：说明配对成功。引擎才真正调用无锁的 `tryPop` 将这些包从队列中拔除，打包装入 `PortDataMap`，派发给节点去执行。
    *   若**比对失败**：说明数据不齐。对齐层绝不启动 Pop 确认。而是执行 **滞留帧丢弃契约**，仅对那个被裁定为“Straggler”的落后端口队列，调用一次 `tryPop` 强行清空该落后帧，并立刻向上游和同步策略上报 Drop 事件。

### 2.2 时间戳配对下的单调性保证
在 `Timestamp` 容差对齐策略中，由于不存在 FrameId 指导，如何确定丢弃哪一个队头？

**实现巧思**：
*   **物理依据**：随着时间推演，流入队列的帧其时间戳 `Timestamp` 必然是单调递增、不减的。
*   **丢弃算法**：
    扫描各端口队头时间戳。假设最新的队头时间戳是 `MaxTs`。
    如果发现某端口的队头时间戳 `Ts` 满足 `MaxTs - Ts > alignment_tolerance`，说明该帧已经无望在后续的新数据中配对上（后续进来的帧只会时间戳更晚，差距只会更大）。
    *   **决策**：该落后帧被判定为“无望配对滞留帧”，对其调用 `tryPop` 强行丢弃并上报，推动对齐器单调向前滚动。

---

## 3. 手搓实现参考骨架

你可以根据以下极其严谨的多流对齐核心算法进行手搓：

```cpp
enum class AlignmentPolicy {
    FrameId,
    StreamFrameId,
    Timestamp
};

class MultiStreamAligner {
public:
    MultiStreamAligner(AlignmentPolicy policy, std::chrono::microseconds tolerance)
        : m_policy(policy), m_tolerance(tolerance) {}

    // 核心对齐拉取函数
    bool gatherAlignedInputs(const std::vector<std::string>& ports,
                             std::unordered_map<std::string, std::shared_ptr<LockFreeNodeQueue<DataPacket>>>& queues,
                             PortDataMap& alignedInputs,
                             std::function<void(const std::string& port, std::shared_ptr<DataPacket> droppedPacket)> dropCallback) {

        alignedInputs.clear();
        std::unordered_map<std::string, std::shared_ptr<DataPacket>> peekedPackets;

        // 1. Peek 所有的输入端口队头
        for (const auto& port : ports) {
            std::shared_ptr<DataPacket> pkt;
            if (!queues.at(port)->tryPeek(pkt)) {
                return false; // 只要有任何一个端口队列为空，说明本轮配对绝对无法凑齐，直接返回等待
            }
            peekedPackets[port] = pkt;
        }

        // 2. 根据策略执行比对
        if (m_policy == AlignmentPolicy::FrameId) {
            FrameId maxId = 0;
            for (const auto& pair : peekedPackets) {
                maxId = std::max(maxId, pair.second->id);
            }

            bool allMatched = true;
            std::string stragglerPort;

            for (const auto& pair : peekedPackets) {
                if (pair.second->id < maxId) {
                    allMatched = false;
                    stragglerPort = pair.first;
                    break;
                }
            }

            if (allMatched) {
                // 配对成功：双阶段确认，真正弹出
                for (const auto& port : ports) {
                    std::shared_ptr<DataPacket> realPkt;
                    queues.at(port)->tryPop(realPkt);
                    alignedInputs[port] = realPkt;
                }
                return true;
            } else {
                // 滞留帧单调丢弃：将最落后的那个 stragglerPort 的队头拔除并丢弃
                std::shared_ptr<DataPacket> stragglerPkt;
                queues.at(stragglerPort)->tryPop(stragglerPkt);
                dropCallback(stragglerPort, stragglerPkt);

                return false; // 提示本轮未能拉到对齐数据
            }
        }

        // 3. Timestamp 策略对齐实现 (略...)
        return false;
    }

private:
    AlignmentPolicy m_policy;
    std::chrono::microseconds m_tolerance;
};
```

手搓 `MultiStreamAligner` 期间，深刻感悟“非占有式 TryPeek 与两阶段确认”在高并发高性能队列设计中的绝对统治力。它不仅完全避免了锁竞争，更是完美保证了多核任务调度下，多路数据流交汇处的时序铁律。
