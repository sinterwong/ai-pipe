/**
 * @file graph.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-05-16
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_GRAPH_HPP
#define AI_PIPE_GRAPH_HPP

#include "ai_pipe/edge.hpp"
#include "ai_pipe/node_base.hpp"
#include <unordered_map>

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

  bool addEdge(const std::string &source_node_name, const std::string &source_port,
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
  bool hasCycleDFS(
      const std::shared_ptr<ILogicNode> &node,
      std::unordered_map<std::shared_ptr<ILogicNode>, int> &visit_status) const;

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
};

} // namespace ai_pipe
#endif