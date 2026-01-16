/**
 * @file graph.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-05-16
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "ai_pipe/graph.hpp"
#include "logger.hpp"
#include <algorithm>

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
  // Check if an identical edge already exists (same source node, source port,
  // destination node, and destination port)
  for (const auto &existing_edge : m_edges) {
    if (existing_edge.source_node == source_node &&
        existing_edge.source_port == source_port_name &&
        existing_edge.dest_node == dest_node &&
        existing_edge.dest_port == dest_port_name) {
      LOG_WARNING_S << "Edge from " << source_node_name << ":"
                    << source_port_name << " to " << dest_node_name << ":"
                    << dest_port_name << "already exists. Skipping.";
      return false;
    }
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
    LOG_ERROR_S << "Node is null";
    throw std::runtime_error("Node is null");
  }
  auto it = m_inDegree.find(node);
  if (it != m_inDegree.end()) {
    return it->second;
  } else {
    LOG_WARNING_S << "Node " << node->getName() << " not found in inDegree map";
    return 0;
  }
  return 0;
}

int Graph::getOutDegree(const std::shared_ptr<ILogicNode> &node) const {
  if (!node) {
    LOG_ERROR_S << "Node is null";
    throw std::runtime_error("Node is null");
  }
  auto it = m_adjListOut.find(node);
  if (it != m_adjListOut.end()) {
    return it->second.size();
  } else {
    LOG_WARNING_S << "Node " << node->getName()
                  << " not found in m_adjListOut map";
    return 0;
  }
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
  // 0: unvisited, 1: visiting (in recursion stack), 2: visited
  std::unordered_map<std::shared_ptr<ILogicNode>, int> visit_status;
  for (const auto &node_sp : m_nodes) {
    if (visit_status[node_sp] == 0) {
      if (hasCycleDFS(node_sp, visit_status)) {
        return true;
      }
    }
  }
  return false;
}

void Graph::clear() {
  m_nodes.clear();
  m_edges.clear();
  m_nodeMap.clear();
  m_adjListOut.clear();
  m_adjListIn.clear();
  m_inDegree.clear();
}

bool Graph::hasCycleDFS(
    const std::shared_ptr<ILogicNode> &node,
    std::unordered_map<std::shared_ptr<ILogicNode>, int> &visit_status) const {
  // Mark as visiting (in recursion stack)
  visit_status[node] = 1;

  auto it_adj = m_adjListOut.find(node);
  if (it_adj != m_adjListOut.end()) {
    for (auto v : it_adj->second) {
      if (visit_status[v] == 1) {
        return true;
      }
      if (visit_status[v] == 0) {
        if (hasCycleDFS(v, visit_status)) {
          return true;
        }
      }
    }
  }
  // Mark as visited (finished processing)
  visit_status[node] = 2;
  return false;
}

} // namespace ai_pipe