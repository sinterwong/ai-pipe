#ifndef AI_PIPE_COMPILED_GRAPH_HPP
#define AI_PIPE_COMPILED_GRAPH_HPP

#include "ai_pipe/error.hpp"
#include "ai_pipe/graph.hpp"
#include <algorithm>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace ai_pipe {

class CompiledGraph {
public:
  using NodeIndex = std::uint32_t;
  using NodePtr = std::shared_ptr<ILogicNode>;

  static constexpr NodeIndex k_invalid_index =
      std::numeric_limits<NodeIndex>::max();

  static constexpr std::uint32_t k_invalid_port =
      std::numeric_limits<std::uint32_t>::max();

  /**
   * @brief One outgoing connection of a node, precomputed for routing
   */
  struct OutEdge {
    std::string source_port;
    NodeIndex dest_node{k_invalid_index};
    std::string dest_port;

    /// Position of dest_port within the destination node's declared
    /// input ports; lets the engine index straight into its per-port
    /// queue array without hashing the port name.
    std::uint32_t dest_port_index{k_invalid_port};
  };

  /**
   * @brief Compile a Graph into an immutable indexed view
   * @return CompiledGraph on success, or Error with:
   *   - GraphEmpty: graph has no nodes
   *   - GraphCycleDetected: graph contains a cycle
   *   - InvalidConfiguration: a non-source node declares an input port
   *     with no incoming edge (it could never become schedulable)
   */
  static Result<CompiledGraph> compile(const Graph &graph) {
    CompiledGraph cg;

    const auto &nodes = graph.getNodes();
    if (nodes.empty()) {
      return Result<CompiledGraph>::err(ErrorCode::GraphEmpty,
                                        "Graph has no nodes");
    }

    const auto node_count = static_cast<NodeIndex>(nodes.size());
    cg.m_nodes = nodes;
    cg.m_outEdges.resize(node_count);
    cg.m_successors.resize(node_count);
    cg.m_predecessors.resize(node_count);
    cg.m_inDegree.assign(node_count, 0);

    std::vector<std::vector<std::string>> input_ports(node_count);
    for (NodeIndex i = 0; i < node_count; ++i) {
      cg.m_nameToIndex[nodes[i]->getName()] = i;
      cg.m_ptrToIndex[nodes[i].get()] = i;
      input_ports[i] = nodes[i]->getExpectedInputPorts();
    }

    // Which declared input ports receive at least one edge; filled in
    // the single edge pass below and used for connectivity validation.
    std::vector<std::vector<bool>> port_covered(node_count);
    for (NodeIndex i = 0; i < node_count; ++i) {
      port_covered[i].assign(input_ports[i].size(), false);
    }

    // Edge-count in-degrees drive Kahn's algorithm; adjacency lists are
    // deduplicated separately for scheduling fan-out.
    for (const auto &edge : graph.getEdges()) {
      const NodeIndex src = cg.indexOfPtr(edge.source_node.get());
      const NodeIndex dst = cg.indexOfPtr(edge.dest_node.get());
      if (src == k_invalid_index || dst == k_invalid_index) {
        return Result<CompiledGraph>::err(
            ErrorCode::InternalError,
            "Edge references a node that is not in the graph");
      }

      std::uint32_t dest_port_index = k_invalid_port;
      for (std::uint32_t pi = 0;
           pi < static_cast<std::uint32_t>(input_ports[dst].size()); ++pi) {
        if (input_ports[dst][pi] == edge.dest_port) {
          dest_port_index = pi;
          port_covered[dst][pi] = true;
          break;
        }
      }
      cg.m_outEdges[src].push_back(
          {edge.source_port, dst, edge.dest_port, dest_port_index});
      cg.m_inDegree[dst]++;

      auto &succ = cg.m_successors[src];
      if (std::find(succ.begin(), succ.end(), dst) == succ.end()) {
        succ.push_back(dst);
      }
      auto &pred = cg.m_predecessors[dst];
      if (std::find(pred.begin(), pred.end(), src) == pred.end()) {
        pred.push_back(src);
      }
    }

    // Iterative Kahn's algorithm: topological order + cycle detection
    std::vector<int> remaining = cg.m_inDegree;
    std::deque<NodeIndex> ready;
    for (NodeIndex i = 0; i < node_count; ++i) {
      if (remaining[i] == 0) {
        ready.push_back(i);
      }
    }

    cg.m_topoOrder.reserve(node_count);
    while (!ready.empty()) {
      NodeIndex current = ready.front();
      ready.pop_front();
      cg.m_topoOrder.push_back(current);

      for (const auto &out : cg.m_outEdges[current]) {
        if (--remaining[out.dest_node] == 0) {
          ready.push_back(out.dest_node);
        }
      }
    }

    if (cg.m_topoOrder.size() != node_count) {
      return Result<CompiledGraph>::err(ErrorCode::GraphCycleDetected,
                                        "Graph contains a cycle");
    }

    for (NodeIndex i = 0; i < node_count; ++i) {
      if (cg.m_inDegree[i] == 0) {
        cg.m_sourceNodes.push_back(i);
      }
      if (cg.m_outEdges[i].empty()) {
        cg.m_sinkNodes.push_back(i);
      }
    }

    // Connectivity validation: a node that has upstream edges only
    // executes when ALL of its declared input ports hold data, so any
    // unconnected declared port would starve it forever. Source nodes
    // (in-degree 0) are exempt - their ports are fed externally via
    // pushInput / initial inputs. Coverage was collected during the
    // single edge pass above, keeping compilation O(V + E).
    for (NodeIndex i = 0; i < node_count; ++i) {
      if (cg.m_inDegree[i] == 0) {
        continue;
      }
      for (std::size_t pi = 0; pi < input_ports[i].size(); ++pi) {
        if (!port_covered[i][pi]) {
          return Result<CompiledGraph>::err(
              ErrorCode::InvalidConfiguration,
              "Input port '" + input_ports[i][pi] + "' of node '" +
                  nodes[i]->getName() +
                  "' has no incoming edge; the node could never execute",
              nodes[i]->getName());
        }
      }
    }

    return cg;
  }

  // Lookup

  [[nodiscard]] NodeIndex indexOf(const std::string &name) const {
    auto it = m_nameToIndex.find(name);
    return it != m_nameToIndex.end() ? it->second : k_invalid_index;
  }

  [[nodiscard]] NodeIndex indexOfPtr(const ILogicNode *node) const {
    auto it = m_ptrToIndex.find(node);
    return it != m_ptrToIndex.end() ? it->second : k_invalid_index;
  }

  [[nodiscard]] const NodePtr &node(NodeIndex index) const {
    return m_nodes[index];
  }

  [[nodiscard]] std::size_t nodeCount() const { return m_nodes.size(); }

  // Topology (all O(1) per node)

  /** @brief Outgoing edges of a node, for output routing */
  [[nodiscard]] const std::vector<OutEdge> &outEdges(NodeIndex index) const {
    return m_outEdges[index];
  }

  /** @brief Deduplicated successor node indices */
  [[nodiscard]] const std::vector<NodeIndex> &
  successors(NodeIndex index) const {
    return m_successors[index];
  }

  /** @brief Deduplicated predecessor node indices */
  [[nodiscard]] const std::vector<NodeIndex> &
  predecessors(NodeIndex index) const {
    return m_predecessors[index];
  }

  /** @brief Edge-count in-degree (parallel edges counted individually) */
  [[nodiscard]] int inDegree(NodeIndex index) const {
    return m_inDegree[index];
  }

  [[nodiscard]] bool isSource(NodeIndex index) const {
    return m_inDegree[index] == 0;
  }

  [[nodiscard]] bool isSink(NodeIndex index) const {
    return m_outEdges[index].empty();
  }

  /** @brief Node indices in a valid topological order */
  [[nodiscard]] const std::vector<NodeIndex> &topologicalOrder() const {
    return m_topoOrder;
  }

  [[nodiscard]] const std::vector<NodeIndex> &sourceNodes() const {
    return m_sourceNodes;
  }

  [[nodiscard]] const std::vector<NodeIndex> &sinkNodes() const {
    return m_sinkNodes;
  }

private:
  CompiledGraph() = default;

  std::vector<NodePtr> m_nodes;
  std::unordered_map<std::string, NodeIndex> m_nameToIndex;
  std::unordered_map<const ILogicNode *, NodeIndex> m_ptrToIndex;

  std::vector<std::vector<OutEdge>> m_outEdges;
  std::vector<std::vector<NodeIndex>> m_successors;
  std::vector<std::vector<NodeIndex>> m_predecessors;
  std::vector<int> m_inDegree;

  std::vector<NodeIndex> m_topoOrder;
  std::vector<NodeIndex> m_sourceNodes;
  std::vector<NodeIndex> m_sinkNodes;
};

} // namespace ai_pipe

#endif // AI_PIPE_COMPILED_GRAPH_HPP
