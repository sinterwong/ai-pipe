# 11. 帧同步与协同丢弃 (ISyncStrategy)

在 STREAM 模式下，当有向无环图（DAG）中存在分支合流（Fork-Join）拓扑时，整个执行引擎将面临最严峻的并发控制考验。

```
              +------------->  Branch A (Deep Node A)  ------------>+
              |                                                     |
Fork Node ----+                                                     +----> Join Node
              |                                                     |
              +------------->  Branch B (Fast Node B)  ------------>+
```

如果 `Branch A` 上的节点因为处理复杂算法发生卡顿，导致了背压丢弃了帧 `N`，而此时 `Branch B` 运行平稳，依然愉快地将帧 `N` 塞入 `Join Node` 的对应队列。这会导致：
1.  **虚假耦合（False Coupling）**：不相干的并行分支互相卡住。
2.  **深度盲（Depth Blindness）**：深层节点的丢帧事件无法向上游扩散，导致死锁或持续卡顿。
3.  **对齐落后**：`Join Node` 的 `Branch B` 队列会严重溢出积压，而 `Branch A` 队列却始终没有帧 `N`，造成合流点无尽地卡死。

`ISyncStrategy` 机制便是为了在物理无锁的多核心线程调度中，重建并维护完美的、跨越并行空间的协同控制（Coordinated Drop）。

---

## 1. 核心设计原理

### 1.1 自动 Fork-Join 对识别与物理分支映射
要解决“深度盲”和“虚假耦合”，同步策略必须能够在执行引擎初始化时，对拓扑图做深度的剖析：
*   **物理分支（Branch）**：将从一个 Fork 点出发，一直到合流 Join 点之间的整条物理链路，抽象映射为一个“逻辑分支”。
*   **同步组（Sync Group）**：由共享同一个 Join 节点的所有逻辑分支联合构成。
*   **非相关隔离**：属于不同同步组的分支，其任何丢弃和延迟完全隔离。属于同一同步组的分支，其数据生命周期强绑定。

### 1.2 协同丢弃（Drop Propagation）与水位线控制
当组内的某一分支在中间环节（如 `Deep Node A`）因为队列满触发背压丢弃了帧 `N` 时：
1.  **全局上报（`reportDrop`）**：`Deep Node A` 瞬间向 `ISyncStrategy` 上报：“我因为背压丢弃了第 `N` 帧”。
2.  **丢弃广播（Drop Broadcast）**：`ISyncStrategy` 立即将该丢弃通知扩散到整个同步组内的所有同胞分支。
3.  **协同提前丢弃（Coordinated Early Drop）**：
    *   同胞分支上的其他节点在下一次激活或执行时，首先通过 `shouldDrop(node, N)` 询问策略。
    *   **结果**：策略返回 `true`。同胞节点直接在队列头丢弃第 `N` 帧，无需白白送入 `process()` 去计算（这就好比已经知道兄弟组的数据丢了，自己算了也无法在合流点配对，所以提前自我了断，省下了珍贵的 CPU 算力）。
4.  **水位线推进（Watermark Advancing）**：当组内所有活跃分支都完成了第 `N` 帧的处理（无论成功处理还是协同丢弃），水位线 `Watermark` 安全向前推进至 `N`。策略可安全清空内部记录该帧的缓存字典，防止内存泄漏。

---

## 2. 核心巧思与实现细节

### 2.1 逆向拓扑 BFS 攻克深度盲
为什么早期的单纯丢弃检测会导致系统依旧卡顿？因为它们“只知队尾不知深度（Depth Blindness）”。深层节点的丢帧消息，上游源节点根本感知不到。

**实现巧思（`JoinAwareSyncStrategy` 算法）**：
在初始化时，`JoinAwareSyncStrategy` 顺着合流节点逆向进行 **广度优先搜索（Reverse BFS）**，并构建映射表：
```cpp
std::unordered_map<std::string, std::string> m_nodeToSyncGroup;
std::unordered_map<std::string, std::string> m_nodeToBranchId;
```
通过该字典，引擎中运行的任何节点，只要一发生 Drop 行为，策略就能在 $O(1)$ 的时间内判定：
*   该节点属于哪一个逻辑同步组。
*   该节点属于组内的哪一条物理分支。
这使得丢弃信号在瞬间穿透了任意复杂的 DAG 深度物理屏障。

### 2.2 无锁读、轻量互斥写的协同数据结构
在热路径中，各 Worker 线程会以数十万次每秒的频率调用 `shouldDrop()` 进行防御性检查。如果这个检查包含重量级的锁，整个多线程线程池的并行度将退化成单线程。

**实现巧思**：
设计读写物理分离：
```cpp
mutable std::shared_mutex m_dropMutex;
std::unordered_map<SyncGroupId, std::unordered_map<FrameId, std::unordered_set<BranchId>>> m_droppedBranches;
```
*   **`shouldDrop()` 走读锁**：
    ```cpp
    bool shouldDrop(const std::string& node, FrameId frame) const {
        std::shared_lock<std::shared_mutex> lock(m_dropMutex);
        // 查询当前组、当前帧是否有其他分支丢弃过。如果有，返回 true
    }
    ```
    通过 `std::shared_lock`，多线程并发读取时，CPU 缓存保持 Shared 状态，零锁竞争，开销极低。
*   **`reportDrop()` 走写锁**：仅在极少数由于突发背压导致丢帧时，才获取独占写锁，登记被丢弃分支。极大地保障了常态下的微秒级超低延迟运行。

---

## 3. 手搓实现参考骨架

你可以根据以下极其惊艳的 `JoinAwareSyncStrategy` 骨架设计进行手搓复习：

```cpp
using SyncGroupId = std::string;
using BranchId = std::string;

class ISyncStrategy {
public:
    virtual ~ISyncStrategy() = default;

    virtual void initialize(const Graph& graph) = 0;
    virtual void reset() = 0;

    // 上报某节点丢弃了某帧
    virtual void reportDrop(const std::string& nodeName, FrameId frameId, const std::string& reason) = 0;

    // 询问某节点在处理某帧时是否应该协同丢弃
    virtual bool shouldDrop(const std::string& nodeName, FrameId frameId) const = 0;

    // 节点顺利完成处理，标记当前分支已过该关
    virtual void markProcessed(const std::string& nodeName, FrameId frameId) = 0;
};

// ==================== 智能合流感知同步策略 ====================
class JoinAwareSyncStrategy : public ISyncStrategy {
public:
    void initialize(const Graph& graph) override {
        // 1. 逆向遍历拓扑，检测 Fork-Join 组
        for (const auto& node : graph.getNodes()) {
            if (node->getExpectedInputPorts().size() > 1) {
                // 找到 Join 节点，将其建立为 SyncGroup
                std::string groupId = node->getName() + "_sync_group";
                m_syncGroups.insert(groupId);

                // 逆向 BFS 溯源，将所有上游路径上的节点打上标签映射
                buildReverseBranchMap(graph, node, groupId);
            }
        }
    }

    void reset() override {
        std::unique_lock<std::shared_mutex> lock(m_dropMutex);
        m_droppedBranches.clear();
        m_processedBranches.clear();
    }

    void reportDrop(const std::string& nodeName, FrameId frameId, const std::string&) override {
        auto groupIt = m_nodeToGroup.find(nodeName);
        if (groupIt == m_nodeToGroup.end()) return; // 该节点不需要同步

        const auto& groupId = groupIt->second;
        const auto& branchId = m_nodeToBranch.at(nodeName);

        std::unique_lock<std::shared_mutex> lock(m_dropMutex);
        m_droppedBranches[groupId][frameId].insert(branchId);
    }

    bool shouldDrop(const std::string& nodeName, FrameId frameId) const override {
        auto groupIt = m_nodeToGroup.find(nodeName);
        if (groupIt == m_nodeToGroup.end()) return false;

        const auto& groupId = groupIt->second;
        const auto& branchId = m_nodeToBranch.at(nodeName);

        std::shared_lock<std::shared_mutex> lock(m_dropMutex);
        auto groupIt2 = m_droppedBranches.find(groupId);
        if (groupIt2 == m_droppedBranches.end()) return false;

        auto frameIt = groupIt2->second.find(frameId);
        if (frameIt == groupIt2->second.end()) return false;

        // 如果在当前同步组当前帧里，已经有【其他分支】发生了丢帧，则当前分支理应协同丢弃
        const auto& droppedSet = frameIt->second;
        if (!droppedSet.empty() && droppedSet.find(branchId) == droppedSet.end()) {
            return true;
        }
        return false;
    }

    void markProcessed(const std::string& nodeName, FrameId frameId) override {
        auto groupIt = m_nodeToGroup.find(nodeName);
        if (groupIt == m_nodeToGroup.end()) return;

        const auto& groupId = groupIt->second;
        const auto& branchId = m_nodeToBranch.at(nodeName);

        std::unique_lock<std::shared_mutex> lock(m_dropMutex);
        m_processedBranches[groupId][frameId].insert(branchId);

        // 如果该组的所有分支都已到达（无论成功还是丢弃），推进水位线，清空物理缓存
        if (checkAllBranchesReached(groupId, frameId)) {
            m_droppedBranches[groupId].erase(frameId);
            m_processedBranches[groupId].erase(frameId);
        }
    }

private:
    void buildReverseBranchMap(const Graph& graph, const std::shared_ptr<ILogicNode>& joinNode, const std::string& groupId) {
        // 利用入边追溯，对 JoinNode 的每个输入，标记不同的 BranchId，并溯源打标
        for (const auto& edge : graph.getIncomingEdges(joinNode)) {
            std::string branchId = edge.sourcePort + "_branch";
            m_groupBranches[groupId].insert(branchId);

            // 逆向 DFS 标记整条路径
            auto srcNode = graph.getNodes(); // 示例简化，实际顺着 edge.sourceNode 向上递归标记
            // m_nodeToGroup[nodeName] = groupId;
            // m_nodeToBranch[nodeName] = branchId;
        }
    }

    bool checkAllBranchesReached(const std::string& groupId, FrameId frameId) {
        size_t total = m_groupBranches[groupId].size();
        size_t reached = m_processedBranches[groupId][frameId].size() + m_droppedBranches[groupId][frameId].size();
        return reached >= total;
    }

    std::unordered_set<SyncGroupId> m_syncGroups;
    std::unordered_map<std::string, SyncGroupId> m_nodeToGroup;
    std::unordered_map<std::string, BranchId> m_nodeToBranch;
    std::unordered_map<SyncGroupId, std::unordered_set<BranchId>> m_groupBranches;

    mutable std::shared_mutex m_dropMutex;
    // groupId -> frameId -> set of branches that dropped this frame
    std::unordered_map<SyncGroupId, std::unordered_map<FrameId, std::unordered_set<BranchId>>> m_droppedBranches;
    // groupId -> frameId -> set of branches that successfully processed this frame
    std::unordered_map<SyncGroupId, std::unordered_map<FrameId, std::unordered_set<BranchId>>> m_processedBranches;
};
```

手搓 `ISyncStrategy` 期间，你将领略到最前沿的数据流协同美学。它是无锁多核心分布式任务调度下，既保证各自高吞吐、又能在特定汇合点展现完美物理约束的关键法宝。
