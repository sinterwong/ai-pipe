# 09. 调度策略系统 (ISchedulerStrategy)

在绝大多数 DAG（有向无环图）管线中，调度器（Scheduler）往往是一团纠缠不清的业务代码。它需要同时应付“离线单次 BATCH 执行”和“实时持续高频 STREAM 执行”。这导致代码膨胀、难以扩展，且无法针对特定运行场景进行算法级的优化。

AI Pipe 运用 **策略模式（Strategy Pattern）** 设计了 `ISchedulerStrategy` 调度策略接口。它将调度控制逻辑提升到引擎外部，实现了核心引擎与激活/完成评估逻辑的完美解耦。

---

## 1. 核心设计原理

### 1.1 调度决策机制：三方解耦结构
```
                  +--------------------------+
                  |     ExecutionEngine      |
                  +--------------------------+
                     |                    ^
                     | (Provides State)   | (Dispatches Task)
                     v                    |
                  +--------------------------+
                  |    ISchedulerStrategy    |
                  +--------------------------+
                               |
            +------------------+------------------+
            |                                     |
            v                                     v
+-----------------------+             +-----------------------+
| BatchSchedulerStrategy|             |StreamSchedulerStrategy|
| (All Inputs Ready)    |             | (Partial / Keepalive) |
+-----------------------+             +-----------------------+
```

1.  **数据提供方（ExecutionEngine）**：只负责维护物理队列的状态、执行具体任务，在事件发生时向策略汇报。
2.  **决策方（ISchedulerStrategy）**：不持有任何物理线程与队列。它只在决策点被动地接收一个只读的 `SchedulingContext`（调度上下文快照），并以极快的速度返回一个决定：`ScheduleNow`（立即激活运行）、`WaitForInputs`（继续等待）或 `SkipExecution`（不执行）。
3.  **调度上下文（`SchedulingContext`）**：
    ```cpp
    struct SchedulingContext {
        std::shared_ptr<ILogicNode> node;
        std::vector<std::string> expected_input_ports; // 节点声明的全部输入端口
        std::vector<std::string> ready_input_ports;    // 当前队列中确实拥有数据的端口
        size_t pending_predecessor_count;              // 待运行的前驱节点个数（限 Batch 模式）
        bool is_source_node;                           // 是否为源（叶子）节点
        bool is_sink_node;                             // 是否为汇（叶子）节点
        uint64_t execution_count;                      // 该节点已成功被执行的次数
    };
    ```

---

## 2. 核心巧思与实现细节

### 2.1 批模式调度（`BatchSchedulerStrategy`）契约与无堆分配优化
*   **激活契约**：在 `BATCH` 模式中，一个节点只有当它的**所有输入端口全都有数据**、且**所有上游前驱节点都已经彻底执行完毕**时，才能被调度一次。
*   **完成语义（`SinglePass`）**：当 DAG 中所有的 Sink 节点（没有后继的叶子节点）都成功执行并产生了一次输出，整趟管线的 `run()` 即宣告圆满完成，退回控制权。
*   **实现巧思（避免频繁分配堆内存）**：
    在 Batch 运行中，引擎每完成一个节点任务，都需要调用 `checkCompletion` 快速评估全局是否完成。如果每次评估都去动态构造一个 `unordered_map` 或高频 `new` 内存来传递各节点执行计数，会在热路径上频繁引发内核级内存分配器锁，产生几十微秒的随机延迟。
    *   **优化方案**：在 `checkCompletion` 接口中，要求传入复用的、预先分配好物理空间的 `std::vector<SinkExecutionCount>`。通过极高局部性的连续内存扫描，在 O(S)（S 为 Sink 节点数，通常极小）的时间内无锁、零堆分配地完成判定，控制调度耗时在 50ns 以内。

### 2.2 流模式调度（`StreamSchedulerStrategy`）重调度与保活（Keepalive）
与 `BATCH` 模式一次性通过即销毁控制流不同，`STREAM` 模式管线是常驻内存的、连续推流的。
*   **重调度逻辑（`onNodeComplete`）**：
    当一个节点 `Node A` 在 Worker 线程中运行完毕并输出了数据。
    *   在 `BATCH` 中，`onNodeComplete` 返回 `false`（不自重调度，依赖数据向前推进激活下游）。
    *   在 `STREAM` 中，一个节点在消费完数据并输出后，其对应的输入队列中可能仍有高频推入的后续帧积压。
    *   **保活策略**：如果节点输入队列依然不空，`StreamScheduler` 的 `onNodeComplete` 返回 `true`，促使执行引擎**立刻将该节点重新提交回线程池（Self-Reschedule）**，实现不间断的流式处理，避免因为缺少事件触发而导致数据永远滞留在队列中。

---

## 3. 手搓实现参考骨架

你可以根据以下完美解耦的 C++20 调度系统骨架进行手搓：

```cpp
enum class ScheduleDecision {
    ScheduleNow,
    WaitForInputs,
    SkipExecution
};

struct ScheduleResult {
    ScheduleDecision decision;
    std::string reason;

    static ScheduleResult scheduleNow(std::string r) { return {ScheduleDecision::ScheduleNow, std::move(r)}; }
    static ScheduleResult waitForInputs(std::string r) { return {ScheduleDecision::WaitForInputs, std::move(r)}; }
    static ScheduleResult skipExecution(std::string r) { return {ScheduleDecision::SkipExecution, std::move(r)}; }
};

class ISchedulerStrategy {
public:
    virtual ~ISchedulerStrategy() = default;

    // 核心决策：是否可以调度当前节点
    virtual ScheduleResult shouldSchedule(const SchedulingContext& ctx) const = 0;

    // 节点执行完毕后，是否允许其自动向线程池自重调度
    virtual bool onNodeComplete(const std::shared_ptr<ILogicNode>& node, bool success) = 0;

    // 检查管线全局完成状态
    virtual bool isCompleted(const std::vector<std::shared_ptr<ILogicNode>>& sinkNodes,
                             const std::unordered_map<std::string, uint64_t>& executionCounts) const = 0;
};

// ==================== Batch 调度策略 ====================
class BatchSchedulerStrategy : public ISchedulerStrategy {
public:
    ScheduleResult shouldSchedule(const SchedulingContext& ctx) const override {
        // Batch 模式必须等待所有声明的输入端口就绪
        if (ctx.ready_input_ports.size() < ctx.expected_input_ports.size()) {
            return ScheduleResult::waitForInputs("Waiting for all ports: " +
                std::to_string(ctx.ready_input_ports.size()) + "/" +
                std::to_string(ctx.expected_input_ports.size()));
        }
        // 前驱节点必须全数彻底运行完
        if (ctx.pending_predecessor_count > 0) {
            return ScheduleResult::waitForInputs("Waiting for predecessors: " +
                std::to_string(ctx.pending_predecessor_count));
        }
        return ScheduleResult::scheduleNow("All inputs and predecessors ready");
    }

    bool onNodeComplete(const std::shared_ptr<ILogicNode>&, bool) override {
        return false; // Batch 模式依靠前驱推进，决不重调度自己
    }

    bool isCompleted(const std::vector<std::shared_ptr<ILogicNode>>& sinkNodes,
                             const std::unordered_map<std::string, uint64_t>& executionCounts) const override {
        for (const auto& sink : sinkNodes) {
            auto it = executionCounts.find(sink->getName());
            if (it == executionCounts.end() || it->second == 0) {
                return false; // 只要有任何一个叶子节点未运行过，代表 Batch 未结束
            }
        }
        return true;
    }
};

// ==================== Stream 调度策略 ====================
class StreamSchedulerStrategy : public ISchedulerStrategy {
public:
    ScheduleResult shouldSchedule(const SchedulingContext& ctx) const override {
        // Stream 模式下，对于 Source 节点，只要有初始数据直接调度
        if (ctx.is_source_node) {
            if (!ctx.ready_input_ports.empty()) {
                return ScheduleResult::scheduleNow("Source node has input");
            }
            return ScheduleResult::waitForInputs("Source waiting for pushInput");
        }

        // 对于普通节点，如果支持 partial inputs（部分就绪即调度，可选配置），只需至少有一个端口有数据
        if (ctx.ready_input_ports.empty()) {
            return ScheduleResult::waitForInputs("Waiting for at least one port");
        }

        // 默认依然等待全端口就绪对齐
        if (ctx.ready_input_ports.size() < ctx.expected_input_ports.size()) {
            return ScheduleResult::waitForInputs("Streaming waiting for all alignment ports");
        }

        return ScheduleResult::scheduleNow("Ports aligned and ready");
    }

    bool onNodeComplete(const std::shared_ptr<ILogicNode>&, bool success) override {
        return success; // 只要刚才执行成功，且队列中仍有后续积压数据，应持续重调度自己（保活）
    }

    bool isCompleted(const std::vector<std::shared_ptr<ILogicNode>>&,
                     const std::unordered_map<std::string, uint64_t>&) const override {
        return false; // Stream 模式常驻常开，永不自动判定为 Completed
    }
};
```

手搓 `ISchedulerStrategy` 期间，细细体会“纯算法状态层与执行控制物理层的解耦设计”。正是这种极其干净的解耦，为我们后续在阶段三手搓超高吞吐的“多流对齐”和“协同丢弃”打下了无可挑剔的基础。
