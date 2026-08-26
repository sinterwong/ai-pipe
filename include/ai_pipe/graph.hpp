#ifndef AI_PIPE_GRAPH_HPP
#define AI_PIPE_GRAPH_HPP

#include "ai_pipe/edge.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include <unordered_map>
#include <unordered_set>

namespace ai_pipe {

/**
 * Owns the nodes and directed edges that define a pipeline.
 *
 * Node names must be unique. Edges are validated against each node's declared
 * ports and optional payload types when they are added. Cycles are permitted
 * during construction and reported by `hasCycle()` or engine initialization.
 *
 * `Graph` is not thread-safe. Build or query it from one thread before passing
 * it to an execution engine.
 */
class Graph {
public:
  Graph() = default;
  ~Graph() = default;

  Graph(const Graph &) = delete;
  Graph &operator=(const Graph &) = delete;
  Graph(Graph &&) = default;
  Graph &operator=(Graph &&) = default;

  /** Adds a non-null node whose name is not already present. */
  bool addNode(const std::shared_ptr<ILogicNode> &node);

  /** Returns the named node, or `nullptr` when it is not present. */
  std::shared_ptr<ILogicNode> getNode(const std::string &name) const;

  /**
   * Returns nodes in insertion order.
   *
   * The reference remains valid until the graph is mutated or destroyed.
   */
  const std::vector<std::shared_ptr<ILogicNode>> &getNodes() const;

  /**
   * Adds a validated edge between existing nodes.
   *
   * Returns `false` for unknown nodes or ports, incompatible declared payload
   * types, or an identical existing edge. An empty port name selects the
   * framework's unnamed/default port convention.
   */
  bool addEdge(const std::string &source_node_name,
               const std::string &source_port,
               const std::string &dest_node_name, const std::string &dest_port);

  /** Returns edges in insertion order; invalidated by graph mutation. */
  const std::vector<Edge> &getEdges() const;

  /** Returns the number of incoming edges, or zero for a null/unknown node. */
  int getInDegree(const std::shared_ptr<ILogicNode> &node) const;

  /** Returns the number of outgoing edges, or zero for a null/unknown node. */
  int getOutDegree(const std::shared_ptr<ILogicNode> &node) const;

  /** Returns outgoing neighbors; invalidated by graph mutation. */
  const std::vector<std::shared_ptr<ILogicNode>> &
  getOutgoingNeighbors(const std::shared_ptr<ILogicNode> &node) const;

  /** Returns incoming neighbors; invalidated by graph mutation. */
  const std::vector<std::shared_ptr<ILogicNode>> &
  getIncomingNeighbors(const std::shared_ptr<ILogicNode> &node) const;

  /** Returns copies of all edges entering `dest_node`. */
  std::vector<Edge>
  getIncomingEdges(const std::shared_ptr<ILogicNode> &dest_node) const;

  /** Returns copies of all edges leaving `source_node`. */
  std::vector<Edge>
  getOutgoingEdges(const std::shared_ptr<ILogicNode> &source_node) const;

  /** Returns whether the current directed graph contains a cycle. */
  bool hasCycle() const;

  /** Removes every node and edge. */
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

  // NUL separators make the composite key unambiguous without allocating a
  // tuple object for every duplicate-edge lookup.
  std::unordered_set<std::string> m_edgeKeys;
};

} // namespace ai_pipe
#endif
