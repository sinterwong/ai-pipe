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

  bool addEdge(const std::string &sourceNodeName, const std::string &sourcePort,
               const std::string &destNodeName, const std::string &destPort);

  const std::vector<Edge> &getEdges() const;

  int getInDegree(const std::shared_ptr<ILogicNode> &node) const;

  int getOutDegree(const std::shared_ptr<ILogicNode> &node) const;

  const std::vector<std::shared_ptr<ILogicNode>> &
  getOutgoingNeighbors(const std::shared_ptr<ILogicNode> &node) const;

  const std::vector<std::shared_ptr<ILogicNode>> &
  getIncomingNeighbors(const std::shared_ptr<ILogicNode> &node) const;

  std::vector<Edge>
  getIncomingEdges(const std::shared_ptr<ILogicNode> &destNode) const;

  std::vector<Edge>
  getOutgoingEdges(const std::shared_ptr<ILogicNode> &sourceNode) const;

  bool hasCycle() const;

  void clear();

private:
  bool hasCycleDFS(
      const std::shared_ptr<ILogicNode> &node,
      std::unordered_map<std::shared_ptr<ILogicNode>, int> &visitStatus) const;

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