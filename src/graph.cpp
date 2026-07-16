/**
 * @file graph.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Mutable DAG construction API implementation
 * @version 0.1
 * @date 2025-05-16
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "ai_pipe/graph.hpp"
#include "logger.hpp"
#include <algorithm>
#include <deque>

namespace ai_pipe {
bool Graph::addNode(const std::shared_ptr<ILogicNode> &node) {
  if (!node) {
    LOG_ERROR_S << "Tried to add a null node to the graph";
    return false;
  }
  if (m_nodeMap.find(node->getName()) != m_nodeMap.end()) {
    LOG_ERROR_S << "Node with name " << node->getName()
                << " already exists in the graph";
    return false;
  }
  m_nodes.push_back(node);
  m_nodeMap[node->getName()] = node;

  // init adj and indegree
  m_adjListOut[node] = {};
  m_adjListIn[node] = {};
  m_inDegree[node] = 0;
  return true;
}

std::shared_ptr<ILogicNode> Graph::getNode(const std::string &name) const {
  auto it = m_nodeMap.find(name);
  if (it != m_nodeMap.end()) {
    return it->second;
  }
  return nullptr;
}

const std::vector<std::shared_ptr<ILogicNode>> &Graph::getNodes() const {
  return m_nodes;
}

bool Graph::addEdge(const std::string &source_node_name,
                    const std::string &source_port_name,
                    const std::string &dest_node_name,
                    const std::string &dest_port_name) {
  auto source_node = getNode(source_node_name);
  auto dest_node = getNode(dest_node_name);

  if (!source_node) {
    LOG_ERROR_S << "Source node " << source_node_name << " not found";
    return false;
  }
  if (!dest_node) {
    LOG_ERROR_S << "Destination node " << dest_node_name << " not found";
    return false;
  }

  const auto &expected_output_ports = source_node->getExpectedOutputPorts();
  if (expected_output_ports.empty() && !source_port_name.empty()) {
    LOG_ERROR_S
        << "Source node '" << source_node_name
        << "' declares no output ports, but tried to connect from port '"
        << source_port_name << "'.";
    return false;
  }
  if (!source_port_name.empty()) {
    if (std::find(expected_output_ports.begin(), expected_output_ports.end(),
                  source_port_name) == expected_output_ports.end()) {
      LOG_ERROR_S << "Source port '" << source_port_name
                  << "' is not a declared output port for node '"
                  << source_node_name << "'.";
      return false;
    }
  }

  // Validate destination node port
  const auto &expected_input_ports = dest_node->getExpectedInputPorts();
  if (expected_input_ports.empty() && !dest_port_name.empty()) {
    LOG_ERROR_S << "Destination node '" << dest_node_name
                << "' declares no input ports, but tried to connect to port '"
                << dest_port_name << "'.";
    return false;
  }
  // Similarly, only search for non-empty port names
  if (!dest_port_name.empty()) {
    if (std::find(expected_input_ports.begin(), expected_input_ports.end(),
                  dest_port_name) == expected_input_ports.end()) {
      LOG_ERROR_S << "Destination port '" << dest_port_name
                  << "' is not a declared input port for node '"
                  << dest_node_name << "'.";
      return false;
    }
  }
  // Payload type validation: reject the edge when both endpoints declare
  // a concrete payload type and the types disagree. typeid(void) means
  // "untyped" and always passes.
  if (!source_port_name.empty() && !dest_port_name.empty()) {
    const std::type_index source_type =
        source_node->portPayloadType(source_port_name);
    const std::type_index dest_type =
        dest_node->portPayloadType(dest_port_name);
    if (source_type != typeid(void) && dest_type != typeid(void) &&
        source_type != dest_type) {
      LOG_ERROR_S << "Payload type mismatch on edge " << source_node_name << ":"
                  << source_port_name << " (" << source_type.name() << ") -> "
                  << dest_node_name << ":" << dest_port_name << " ("
                  << dest_type.name() << ")";
      return false;
    }
  }

  // Reject an identical edge (same source node/port and dest node/port)
  // via a hash-set membership test instead of scanning every edge.
  std::string edge_key;
  edge_key.reserve(source_node_name.size() + source_port_name.size() +
                   dest_node_name.size() + dest_port_name.size() + 3);
  edge_key.append(source_node_name).push_back('\0');
  edge_key.append(source_port_name).push_back('\0');
  edge_key.append(dest_node_name).push_back('\0');
  edge_key.append(dest_port_name);
  if (!m_edgeKeys.insert(std::move(edge_key)).second) {
    LOG_WARNING_S << "Edge from " << source_node_name << ":" << source_port_name
                  << " to " << dest_node_name << ":" << dest_port_name
                  << " already exists. Skipping.";
    return false;
  }

  m_edges.emplace_back(
      Edge{source_node, source_port_name, dest_node, dest_port_name});

  // update adj
  m_adjListOut[source_node].push_back(dest_node);
  m_adjListIn[dest_node].push_back(source_node);
  m_inDegree[dest_node]++;

  // Cycle detection will be performed after the entire graph is constructed
  return true;
}

const std::vector<Edge> &Graph::getEdges() const { return m_edges; }

int Graph::getInDegree(const std::shared_ptr<ILogicNode> &node) const {
  if (!node) {
    LOG_ERROR_S << "getInDegree called with null node";
    return 0;
  }
  auto it = m_inDegree.find(node);
  if (it != m_inDegree.end()) {
    return it->second;
  }
  LOG_WARNING_S << "Node " << node->getName() << " not found in inDegree map";
  return 0;
}

int Graph::getOutDegree(const std::shared_ptr<ILogicNode> &node) const {
  if (!node) {
    LOG_ERROR_S << "getOutDegree called with null node";
    return 0;
  }
  auto it = m_adjListOut.find(node);
  if (it != m_adjListOut.end()) {
    return static_cast<int>(it->second.size());
  }
  LOG_WARNING_S << "Node " << node->getName()
                << " not found in m_adjListOut map";
  return 0;
}

const std::vector<std::shared_ptr<ILogicNode>> &
Graph::getOutgoingNeighbors(const std::shared_ptr<ILogicNode> &node) const {
  static const std::vector<std::shared_ptr<ILogicNode>> empty_neighbors;
  if (!node) {
    return empty_neighbors;
  }
  auto it = m_adjListOut.find(node);
  if (it != m_adjListOut.end()) {
    return it->second;
  }
  return empty_neighbors;
}

const std::vector<std::shared_ptr<ILogicNode>> &
Graph::getIncomingNeighbors(const std::shared_ptr<ILogicNode> &node) const {
  static const std::vector<std::shared_ptr<ILogicNode>> empty_neighbors;
  if (!node) {
    return empty_neighbors;
  }
  auto it = m_adjListIn.find(node);
  if (it != m_adjListIn.end()) {
    return it->second;
  }
  return empty_neighbors;
}

std::vector<Edge>
Graph::getIncomingEdges(const std::shared_ptr<ILogicNode> &dest_node) const {
  std::vector<Edge> incoming_edges;
  for (const auto &edge : m_edges) {
    if (edge.dest_node == dest_node) {
      incoming_edges.push_back(edge);
    }
  }
  return incoming_edges;
}

std::vector<Edge>
Graph::getOutgoingEdges(const std::shared_ptr<ILogicNode> &source_node) const {
  std::vector<Edge> outgoing_edges;
  for (const auto &edge : m_edges) {
    if (edge.source_node == source_node) {
      outgoing_edges.push_back(edge);
    }
  }
  return outgoing_edges;
}

bool Graph::hasCycle() const {
  // Iterative Kahn's algorithm: repeatedly peel zero-in-degree nodes.
  // If not every node gets peeled, the remainder contains a cycle.
  // No recursion, so arbitrarily deep graphs cannot overflow the stack.
  std::unordered_map<std::shared_ptr<ILogicNode>, int> remaining = m_inDegree;

  std::deque<std::shared_ptr<ILogicNode>> ready;
  for (const auto &node : m_nodes) {
    auto it = remaining.find(node);
    if (it == remaining.end() || it->second == 0) {
      ready.push_back(node);
    }
  }

  std::size_t processed = 0;
  while (!ready.empty()) {
    auto current = ready.front();
    ready.pop_front();
    processed++;

    auto adj_it = m_adjListOut.find(current);
    if (adj_it == m_adjListOut.end()) {
      continue;
    }
    for (const auto &neighbor : adj_it->second) {
      if (--remaining[neighbor] == 0) {
        ready.push_back(neighbor);
      }
    }
  }

  return processed != m_nodes.size();
}

void Graph::clear() {
  m_edgeKeys.clear();
  m_nodes.clear();
  m_edges.clear();
  m_nodeMap.clear();
  m_adjListOut.clear();
  m_adjListIn.clear();
  m_inDegree.clear();
}

} // namespace ai_pipe