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
#ifndef __PIPE_GRAPH_HPP__
#define __PIPE_GRAPH_HPP__

#include "ai_pipe/edge.hpp"
#include "ai_pipe/node_base.hpp"
#include <unordered_map>

namespace ai_pipe {
class Graph {
public:
  Graph() = default;
  ~Graph() = default;

  // 允许 move 但禁止 copy
  Graph(const Graph &) = delete;
  Graph &operator=(const Graph &) = delete;
  Graph(Graph &&) = default;
  Graph &operator=(Graph &&) = default;

  bool addNode(const std::shared_ptr<NodeBase> &node);

  std::shared_ptr<NodeBase> getNode(const std::string &name) const;

  const std::vector<std::shared_ptr<NodeBase>> &getNodes() const;

  bool addEdge(const std::string &sourceNodeName, const std::string &sourcePort,
               const std::string &destNodeName, const std::string &destPort);

  const std::vector<Edge> &getEdges() const;

  int getInDegree(const std::shared_ptr<NodeBase> &node) const;

  int getOutDegree(const std::shared_ptr<NodeBase> &node) const;

  const std::vector<std::shared_ptr<NodeBase>> &
  getOutgoingNeighbors(const std::shared_ptr<NodeBase> &node) const;

  const std::vector<std::shared_ptr<NodeBase>> &
  getIncomingNeighbors(const std::shared_ptr<NodeBase> &node) const;

  std::vector<Edge>
  getIncomingEdges(const std::shared_ptr<NodeBase> &destNode) const;

  std::vector<Edge>
  getOutgoingEdges(const std::shared_ptr<NodeBase> &sourceNode) const;

  bool hasCycle() const;

  void clear();

private:
  bool hasCycleDFS(
      const std::shared_ptr<NodeBase> &node,
      std::unordered_map<std::shared_ptr<NodeBase>, int> &visitStatus) const;

private:
  std::vector<std::shared_ptr<NodeBase>> nodes_;
  std::vector<Edge> edges_;

  std::unordered_map<std::string, std::shared_ptr<NodeBase>> nodeMap_;

  std::unordered_map<std::shared_ptr<NodeBase>,
                     std::vector<std::shared_ptr<NodeBase>>>
      adjListOut_;
  std::unordered_map<std::shared_ptr<NodeBase>,
                     std::vector<std::shared_ptr<NodeBase>>>
      adjListIn_;
  std::unordered_map<std::shared_ptr<NodeBase>, int> inDegree_;
};

} // namespace ai_pipe
#endif