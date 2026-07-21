# 08. 执行引擎骨架与 PIMPL 模式 (ExecutionEngine)

`ExecutionEngine` 是 AI Pipe 的核心“发动机”。它承载了高并发执行中所有的脏活累活：调度线程、驱逐背压、检测死锁、路由输出、以及维持极其复杂的内部状态一致性。

然而，作为底层引擎，它不仅需要极致的执行性能，还必须拥有优雅、高内聚且具备 **二进制稳定性（ABI Stability）** 的公开发布接口。这就引入了 C++ 架构设计中的至高巧思 —— **PIMPL（Private Implementation，指针指向实现）** 模式。

---

## 1. 核心设计原理

### 1.1 二进制兼容性（ABI Stability）与 PIMPL 模式
在传统的 C++ 头文件中，如果我们在 `ExecutionEngine.hpp` 中直接声明私有的成员变量（如 `std::vector<std::thread>`、`std::mutex`、`WorkStealingThreadPool` 等）：
*   **编译漂移（Compilation Spill）**：只要我们对任何内部变量做微调，或者升级了底层线程池，依赖于公共头文件的所有业务模块（甚至包括动态插件）都**必须重新编译（Header pollution）**。
*   **内存布局漂移（ABI Break）**：由于不同编译器、或者不同编译选项（如是否开启 DEBUG）下 STL 容器的大小不一致，公共类的 `sizeof` 会在运行时发生严重的内存越界崩溃（Heap corruption）。

**实现巧思**：
利用 PIMPL 模式，在公共头文件 `execution_engine.hpp` 中，仅声明一个**不透明的（Incomplete Type）**内部前置声明和一个独占智能指针：
```cpp
class ExecutionEngine {
    // ... 公共接口
private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};
```
在对应的 `execution_engine_impl.cpp` 中，再完整定义 `class ExecutionEngine::Impl`。
由于不透明指针在任何平台上的大小恒为 8 字节，这彻底将公共 API 与复杂的底层变化隔绝。用户代码只需引入轻量级的公共接口，而内部实现的千军万马（如 ThreadPool 锁配置、内部队列大小）即使被改写一万遍，用户的二进制连接也依旧稳如泰山。

```
+---------------------------------------+
|        execution_engine.hpp           |
| (Public API - Clean, Fixed size)      |
+---------------------------------------+
                   |
                   | (Holds unique_ptr to Impl)
                   v
+---------------------------------------+
|      execution_engine_impl.hpp/.cpp   |
| (Private Implementation - Heavyweight) |
| Contains Deques, Threads, Mutexes...  |
+---------------------------------------+
```

### 1.2 内核执行节点状态机（NodeState）
执行引擎必须精细维护 DAG 拓扑中每个节点在运行时的各项并发事务，这就引入了 `NodeState` 结构。
对于每一个 `ILogicNode`，引擎都在内部为其绑定并管理一个专属的 `NodeState` 副本：
```cpp
struct NodeState {
    std::shared_ptr<ILogicNode> node;
    // 为节点的每一个输入端口配备一个独立的 LockFreeNodeQueue 队列
    std::unordered_map<std::string, std::shared_ptr<LockFreeNodeQueue>> inputQueues;
    std::atomic<size_t> activeTasks{0}; // 当前并发执行该节点的 Worker 数量
    std::atomic<uint64_t> executionCount{0}; // 累积执行成功次数
};
```

---

## 2. 核心巧思与实现细节

### 2.1 核心 executeNodeTask 运行双环（Execution Loop）
当一个节点被调度器激活，执行引擎提交一个物理任务到线程池， Worker 线程在执行该任务时（`executeNodeTask`），流程如下：

1.  **输入拉取与对齐（Gather Input Ports）**：
    *   引擎扫描该 `NodeState` 的所有 `inputQueues`。
    *   **对齐校验**：如果是多输入节点，必须满足特定的对齐策略（如帧号相同）。若对齐失败，执行落后帧的单调丢弃并上报，继续循环拉取，直至各端口数据齐备或判定为不齐（返回失败）。
2.  **节点处理（Invoke Node Process）**：
    *   调用 `node->process(inputs, outputs, context)`。
    *   由于用户编写的 `process` 可能由于各种运行时资源原因失败，引擎必须使用 **C++20 Result 异常拦截与错误码划分契约**（见 17 份文档之 17 篇详解）保护自己。
3.  **下游输出分发（Propagate Outputs）**：
    *   如果 `process` 成功，产生了 `outputs`。
    *   引擎读取拓扑图，寻找该节点向外拉出的所有 `Edge`。
    *   **零拷贝路由**：获取下游目标节点的 `NodeState`，将对应的 `outputs[src_port]` 包通过 `tryPush` 推入下游节点的 `dest_port` 的 `LockFreeNodeQueue` 队列中。
4.  **下游重调度触发（OnNodeComplete Trigger）**：
    *   每成功路由一个包，意味着下游节点具备了新的输入数据。
    *   调用调度策略的 `onNodeComplete()` 决定是否重调度。如果重调度，将下游节点的任务提交回线程池（形成了整个管线数据滚滚向前的核心驱动力）。

---

## 3. 手搓实现参考骨架

以下是手搓 `ExecutionEngine` 最硬核的 PIMPL 骨架定义：

```cpp
// ==================== execution_engine.hpp (公共头) ====================
namespace ai_pipe {

class Graph;
class PipelineContext;

class ExecutionEngine {
public:
    explicit ExecutionEngine();
    ~ExecutionEngine(); // 析构必须在 cpp 中实现，否则 unique_ptr 找不到 Impl 完整定义

    // 禁用拷贝，支持移动
    ExecutionEngine(const ExecutionEngine&) = delete;
    ExecutionEngine& operator=(const ExecutionEngine&) = delete;
    ExecutionEngine(ExecutionEngine&&) noexcept;
    ExecutionEngine& operator=(ExecutionEngine&&) noexcept;

    bool initialize(Graph* graph, uint8_t numWorkers);
    bool execute(const PortDataMap& initialInputs, std::shared_ptr<PipelineContext> ctx);
    void stop();

private:
    class Impl;
    std::unique_ptr<Impl> m_impl;
};

} // namespace ai_pipe


// ==================== execution_engine_impl.cpp (内部实现) ====================
#include "execution_engine.hpp"
#include "lock_free_queue.hpp"
#include "work_stealing_thread_pool.hpp"

namespace ai_pipe {

struct NodeState {
    std::shared_ptr<ILogicNode> node;
    std::unordered_map<std::string, std::shared_ptr<LockFreeNodeQueue<DataPacket>>> inputQueues;
    std::atomic<size_t> activeTasks{0};
    std::atomic<uint64_t> executionCount{0};
};

class ExecutionEngine::Impl {
public:
    Impl() = default;

    bool initialize(Graph* graph, uint8_t numWorkers) {
        m_graph = graph;
        m_threadPool = std::make_unique<WorkStealingThreadPool>(numWorkers);

        // 1. 初始化各节点的 NodeState 及其输入 LockFree 队列
        for (const auto& node : m_graph->getNodes()) {
            auto state = std::make_shared<NodeState>();
            state->node = node;
            for (const auto& port : node->getExpectedInputPorts()) {
                // 初始化有界队列，容量来自配置
                state->inputQueues[port] = std::make_shared<LockFreeNodeQueue<DataPacket>>(16);
            }
            m_nodeStates[node->getName()] = state;
        }
        return true;
    }

    bool execute(const PortDataMap& initialInputs, std::shared_ptr<PipelineContext> ctx) {
        // 2. 将初始输入推入 Source 节点的对应队列
        for (const auto& pair : initialInputs) {
            // ... 路由数据并提交首批就绪任务
        }
        return true;
    }

    void executeNodeTask(std::shared_ptr<NodeState> state, std::shared_ptr<PipelineContext> ctx) {
        // 3. 执行核心循环：拉取数据
        PortDataMap inputs;
        if (!gatherInputs(state, inputs)) return;

        PortDataMap outputs;
        try {
            // 4. 调用 process
            state->node->process(inputs, outputs, ctx);

            // 5. 将输出 Propagate 到下游
            propagateOutputs(state, outputs, ctx);

            state->executionCount.fetch_add(1, std::memory_order_relaxed);
        } catch (...) {
            // 异常兜底，通知管线进入 ERROR
        }
    }

private:
    bool gatherInputs(std::shared_ptr<NodeState> state, PortDataMap& inputs) {
        // 单个或多个输入槽位 Pop，执行对齐逻辑
        return true;
    }

    void propagateOutputs(std::shared_ptr<NodeState> state, const PortDataMap& outputs, std::shared_ptr<PipelineContext> ctx) {
        auto outgoingEdges = m_graph->getOutgoingEdges(state->node);
        for (const auto& edge : outgoingEdges) {
            auto outData = outputs.at(edge.sourcePort);
            auto targetState = m_nodeStates.at(edge.destNode);
            targetState->inputQueues.at(edge.destPort)->tryPush(outData);

            // 触发下游节点的就绪评估与重调度
            triggerReschedule(targetState, ctx);
        }
    }

    void triggerReschedule(std::shared_ptr<NodeState> state, std::shared_ptr<PipelineContext> ctx) {
        // 依据 ISchedulerStrategy 评估是否提交任务
        m_threadPool->submit([this, state, ctx]() {
            executeNodeTask(state, ctx);
        });
    }

    Graph* m_graph{nullptr};
    std::unique_ptr<WorkStealingThreadPool> m_threadPool;
    std::unordered_map<std::string, std::shared_ptr<NodeState>> m_nodeStates;
};

// 桥接公有类与 Impl
ExecutionEngine::ExecutionEngine() : m_impl(std::make_unique<Impl>()) {}
ExecutionEngine::~ExecutionEngine() = default;
bool ExecutionEngine::initialize(Graph* graph, uint8_t numWorkers) { return m_impl->initialize(graph, numWorkers); }
bool ExecutionEngine::execute(const PortDataMap& inputs, std::shared_ptr<PipelineContext> ctx) { return m_impl->execute(inputs, ctx); }
void ExecutionEngine::stop() { m_impl->stop(); }

} // namespace ai_pipe
```

在手搓 `ExecutionEngine` 期间，你将亲身体会“二进制 PIMPL 屏障”的威力。接口干净清爽，实现惊涛骇浪，这是真正的顶尖工业级 C++ 并发框架所必须跨越的 ABI 境界。
