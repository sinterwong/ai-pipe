#ifndef AI_PIPE_GRAPH_HPP
#define AI_PIPE_GRAPH_HPP

#include "ai_pipe/edge.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include <unordered_map>
#include <unordered_set>

namespace ai_pipe {
class Graph {
public:
  Graph() = default;
  ~Graph() = default;

  // Allow move, disable copy
  Graph(const Graph &) = delete;
  Graph &operator=(const Graph &) = delete;
  Graph(Graph &&) = default;
  Graph &operator=(Graph &&) = default;

  bool addNode(const std::shared_ptr<ILogicNode> &node);

  std::shared_ptr<ILogicNode> getNode(const std::string &name) const;

  const std::vector<std::shared_ptr<ILogicNode>> &getNodes() const;

  bool addEdge(const std::string &source_node_name,
               const std::string &source_port,
               const std::string &dest_node_name, const std::string &dest_port);

  const std::vector<Edge> &getEdges() const;

  int getInDegree(const std::shared_ptr<ILogicNode> &node) const;

  int getOutDegree(const std::shared_ptr<ILogicNode> &node) const;

  const std::vector<std::shared_ptr<ILogicNode>> &
  getOutgoingNeighbors(const std::shared_ptr<ILogicNode> &node) const;

  const std::vector<std::shared_ptr<ILogicNode>> &
  getIncomingNeighbors(const std::shared_ptr<ILogicNode> &node) const;

  std::vector<Edge>
  getIncomingEdges(const std::shared_ptr<ILogicNode> &dest_node) const;

  std::vector<Edge>
  getOutgoingEdges(const std::shared_ptr<ILogicNode> &source_node) const;

  bool hasCycle() const;

  void clear();

private:
  std::vector<std::shared_ptr<ILogicNode>> m_nodes;
  std::vector<Edge> m_edges;

  std::unordered_map<std::string, std::shared_ptr<ILogicNode>> m_nodeMap;

  std::unordered_map<std::shared_ptr<ILogicNode>,
                     std::vector<std::shared_ptr<ILogicNode>>>
      m_adjListOut;
  std::unordered_map<std::shared_ptr<ILogicNode>,
                     std::vector<std::shared_ptr<ILogicNode>>>
      m_adjListIn;
  std::unordered_map<std::shared_ptr<ILogicNode>, int> m_inDegree;

  // O(1) duplicate-edge detection; keys are
  // "src\0sport\0dst\0dport" (node names + port names).
  std::unordered_set<std::string> m_edgeKeys;
};

} // namespace ai_pipe
#endif
