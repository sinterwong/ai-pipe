# 07. DAG 拓扑定义与合法性校验 (Graph)

AI Pipe 框架的运行核心是**有向无环图（DAG, Directed Acyclic Graph）**。在框架设计中，图不仅仅是数据流连接的“配置项”，更是指导执行引擎分配线程任务、激活调度状态、建立帧同步以及多路合并对齐的“物理交通图”。

一个错误、带有环路、或者类型不匹配的拓扑图，会导致执行引擎陷入死锁或不确定性的死循环。因此，`Graph` 模块的首要任务是在**编译期/初始化期**对图结构进行彻底、严苛的合法性校验。

---

## 1. 核心设计原理

### 1.1 节点命名端口设计（Named Ports Model）
传统 DAG 引擎往往使用“点对点”的粗粒度连接，即：`NodeA -> NodeB`。
这种设计无法满足复杂的 AI 流媒体处理场景（例如 `PreprocessNode` 输出 `image` 发给 `InferenceNode`，输出 `metadata` 发给 `RenderNode`）。

**解决方案**：
引入**命名端口（Named Ports）**模型：
```
+--------------------------+               +--------------------------+
|       Source Node        |               |     Destination Node     |
|                          |               |                          |
|                  [portA] |=====(Edge)===>| [portB]                  |
+--------------------------+               +--------------------------+
```
*   `Edge` 被定义为一个四元组：`(source_node_name, source_port, dest_node_name, dest_port)`。
*   每个 `ILogicNode` 节点在编写时，必须实现：
    *   `getExpectedInputPorts()`：明确声明该节点期望获得哪些命名的输入数据。
    *   `getExpectedOutputPorts()`：明确声明该节点在执行后会产出哪些命名的输出数据。
*   **物理意义**：该设计极大地方便了执行引擎在将输出路由传导（Propagate）给下游节点时，能精准地锁定具体的物理队列槽位。

### 1.2 O(1) 拓扑关系表达
为了使执行引擎在热路径上能够无锁、瞬时地进行前驱（Predecessors）与后继（Successors）节点查询，`Graph` 必须在内部维护以下三张核心邻接表：
1.  **后继邻接表（`m_adjListOut`）**：`std::unordered_map<std::shared_ptr<ILogicNode>, std::vector<Edge>>`。方便节点完成后，快速获知数据应当推送给哪些下游节点的哪些端口。
2.  **前驱邻接表（`m_adjListIn`）**：`std::unordered_map<std::shared_ptr<ILogicNode>, std::vector<Edge>>`。方便调度器追溯某个节点的数据来源。
3.  **入度表（`m_inDegree`）**：记录每个节点的前驱连接边数量。入度为 `0` 的节点自动被识别为**源节点（Source Node）**。

---

## 2. 核心巧思与实现细节

### 2.1 基于 DFS 变种的三色标记环检测算法（Cycle Detection）
在 DAG 中，环路（Cycle）是致命的，它意味着前驱节点依赖于后继节点的输出，会导致在调度时节点入度永远无法清零，管线直接死锁。

**实现巧思**：
利用经典的**三色 DFS 深度优先遍历算法**，在图构建完成后（初始化阶段）进行环检测。
*   **白色（未访问，0）**：节点还未开始被 DFS 遍历。
*   **灰色（正在访问，1）**：节点已经开始被遍历，但其所有的后继分支还未完全回溯完毕。说明该节点处于当前的 DFS “调用栈（Call Stack）”路径中。
*   **黑色（访问完毕，2）**：节点及其所有的下游子孙节点已经全部安全地遍历、回溯完毕，说明不存环。
*   **冲突断定**：如果在深度搜索的过程中，碰到了一个**灰色**节点，说明我们顺着子孙节点的连接边，又指回了调用栈里的上游祖先。这就坐实了**环路的存在（Cycle Detected）**。

```
DFS Visit:
 [White] -> [Grey] (Push Call Stack) -> Visit Successors...
                         |
      If successor is [Grey] =====> CYCLE DETECTED!
                         |
 Visit finished -> [Black] (Pop Call Stack)
```

### 2.2 命名端口强契约校验（Payload Port Validation）
虽然 C++ 属于强类型语言，但是在高灵活性 DAG 中，节点间传递的数据包 `DataPacket` 经过了 `std::any` 的类型擦除。
如果我们在 `NodeA` 期望输出 `float` 数据的 `portA`，与 `NodeB` 期望读取 `int` 数据的 `portB` 之间拉起一条 `Edge`。这会导致运行时 `std::any_cast` 的类型崩溃。

**实现巧思**：
在 `Graph` 中支持数据类型签名校验（非强求，但非常建议手搓）。或者在 `addEdge` 时，强行校验：
1.  `source_node` 的 `getExpectedOutputPorts()` 列表中，必须包含 `source_port`。
2.  `dest_node` 的 `getExpectedInputPorts()` 列表中，必须包含 `dest_port`。
如果缺少，立即拦截并返回 `InvalidArgument` 错误，阻止错误的拓扑流入执行引擎，降低运行时的不确定性。

---

## 3. 手搓实现参考骨架

你可以根据以下极其健壮的 C++20 `Graph` 骨架进行实现：

```cpp
struct Edge {
    std::string sourceNode;
    std::string sourcePort;
    std::string destNode;
    std::string destPort;
};

class Graph {
public:
    Graph() = default;

    // 禁止拷贝，保证所有权清晰
    Graph(const Graph&) = delete;
    Graph& operator=(const Graph&) = delete;
    Graph(Graph&&) noexcept = default;
    Graph& operator=(Graph&&) noexcept = default;

    bool addNode(std::shared_ptr<ILogicNode> node) {
        if (!node) return false;
        if (m_nodes.find(node->getName()) != m_nodes.end()) {
            return false; // 重名节点拒绝
        }
        m_nodes[node->getName()] = node;
        m_nodeList.push_back(node);
        return true;
    }

    bool addEdge(const std::string& src, const std::string& srcPort,
                 const std::string& dest, const std::string& destPort) {
        auto srcIt = m_nodes.find(src);
        auto destIt = m_nodes.find(dest);
        if (srcIt == m_nodes.end() || destIt == m_nodes.end()) {
            return false; // 节点不存在
        }

        // 强校验端口声明
        auto srcNode = srcIt->second;
        auto destNode = destIt->second;
        if (!hasPort(srcNode->getExpectedOutputPorts(), srcPort) ||
            !hasPort(destNode->getExpectedInputPorts(), destPort)) {
            return false; // 端口声明未找到
        }

        Edge edge{src, srcPort, dest, destPort};
        m_edges.push_back(edge);

        m_adjListOut[srcNode].push_back(edge);
        m_adjListIn[destNode].push_back(edge);
        m_inDegree[destNode]++;

        return true;
    }

    bool hasCycle() const {
        std::unordered_map<std::shared_ptr<ILogicNode>, int> visited; // 0=White, 1=Grey, 2=Black
        for (const auto& pair : m_nodes) {
            visited[pair.second] = 0;
        }

        for (const auto& pair : m_nodes) {
            if (visited[pair.second] == 0) {
                if (dfsCheckCycle(pair.second, visited)) {
                    return true; // 存在环
                }
            }
        }
        return false;
    }

    const std::vector<std::shared_ptr<ILogicNode>>& getNodes() const { return m_nodeList; }
    const std::vector<Edge>& getEdges() const { return m_edges; }

    std::vector<Edge> getOutgoingEdges(const std::shared_ptr<ILogicNode>& node) const {
        auto it = m_adjListOut.find(node);
        if (it == m_adjListOut.end()) return {};
        return it->second;
    }

    std::vector<Edge> getIncomingEdges(const std::shared_ptr<ILogicNode>& node) const {
        auto it = m_adjListIn.find(node);
        if (it == m_adjListIn.end()) return {};
        return it->second;
    }

    int getInDegree(const std::shared_ptr<ILogicNode>& node) const {
        auto it = m_inDegree.find(node);
        if (it == m_inDegree.end()) return 0;
        return it->second;
    }

private:
    bool hasPort(const std::vector<std::string>& ports, const std::string& target) const {
        return std::find(ports.begin(), ports.end(), target) != ports.end();
    }

    bool dfsCheckCycle(const std::shared_ptr<ILogicNode>& node,
                       std::unordered_map<std::shared_ptr<ILogicNode>, int>& visited) const {
        visited[node] = 1; // Mark Grey

        auto it = m_adjListOut.find(node);
        if (it != m_adjListOut.end()) {
            for (const auto& edge : it->second) {
                auto childNode = m_nodes.at(edge.destNode);
                if (visited[childNode] == 1) {
                    return true; // Grey to Grey, cycle!
                }
                if (visited[childNode] == 0) {
                    if (dfsCheckCycle(childNode, visited)) {
                        return true;
                    }
                }
            }
        }

        visited[node] = 2; // Mark Black
        return false;
    }

    std::unordered_map<std::string, std::shared_ptr<ILogicNode>> m_nodes;
    std::vector<std::shared_ptr<ILogicNode>> m_nodeList;
    std::vector<Edge> m_edges;

    std::unordered_map<std::shared_ptr<ILogicNode>, std::vector<Edge>> m_adjListOut;
    std::unordered_map<std::shared_ptr<ILogicNode>, std::vector<Edge>> m_adjListIn;
    std::unordered_map<std::shared_ptr<ILogicNode>, int> m_inDegree;
};
```

手搓 `Graph` 期间，深刻体会其作为整个框架控制流的基础。当你确保环检测和端口声明能在最前端把一切非法流控拦住时，后端的执行引擎将运行得极其省心、极其纯粹。
