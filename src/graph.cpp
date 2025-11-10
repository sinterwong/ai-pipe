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
#include "graph.hpp"
#include <algorithm>
#include <logger.hpp>

namespace ai_pipe {
bool Graph::addNode(const std::shared_ptr<ILogicNode> &node) {
  if (!node) {
    LOG_ERRORS << "Tried to add a null node to the graph";
    return false;
  }
  if (m_nodeMap.find(node->getName()) != m_nodeMap.end()) {
    LOG_ERRORS << "Node with name " << node->getName()
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

bool Graph::addEdge(const std::string &sourceNodeName,
                    const std::string &sourcePortName,
                    const std::string &destNodeName,
                    const std::string &destPortName) {
  auto sourceNode = getNode(sourceNodeName);
  auto destNode = getNode(destNodeName);

  if (!sourceNode) {
    LOG_ERRORS << "Source node " << sourceNodeName << " not found";
    return false;
  }
  if (!destNode) {
    LOG_ERRORS << "Destination node " << destNodeName << " not found";
    return false;
  }

  const auto &expectedOutputPorts = sourceNode->getExpectedOutputPorts();
  if (expectedOutputPorts.empty() && !sourcePortName.empty()) {
    LOG_ERRORS << "Source node '" << sourceNodeName
               << "' declares no output ports, but tried to connect from port '"
               << sourcePortName << "'.";
    return false;
  }
  if (!sourcePortName.empty()) {
    if (std::find(expectedOutputPorts.begin(), expectedOutputPorts.end(),
                  sourcePortName) == expectedOutputPorts.end()) {
      LOG_ERRORS << "Source port '" << sourcePortName
                 << "' is not a declared output port for node '"
                 << sourceNodeName << "'.";
      return false;
    }
  }

  // Validate destination node port
  const auto &expectedInputPorts = destNode->getExpectedInputPorts();
  if (expectedInputPorts.empty() && !destPortName.empty()) {
    LOG_ERRORS << "Destination node '" << destNodeName
               << "' declares no input ports, but tried to connect to port '"
               << destPortName << "'.";
    return false;
  }
  // Similarly, only search for non-empty port names
  if (!destPortName.empty()) {
    if (std::find(expectedInputPorts.begin(), expectedInputPorts.end(),
                  destPortName) == expectedInputPorts.end()) {
      LOG_ERRORS << "Destination port '" << destPortName
                 << "' is not a declared input port for node '" << destNodeName
                 << "'.";
      return false;
    }
  }
  // Check if an identical edge already exists (same source node, source port,
  // destination node, and destination port)
  for (const auto &existingEdge : m_edges) {
    if (existingEdge.sourceNode == sourceNode &&
        existingEdge.sourcePort == sourcePortName &&
        existingEdge.destNode == destNode &&
        existingEdge.destPort == destPortName) {
      LOG_WARNINGS << "Edge from " << sourceNodeName << ":" << sourcePortName
                   << " to " << destNodeName << ":" << destPortName
                   << "already exists. Skipping.";
      return false;
    }
  }

  m_edges.emplace_back(
      Edge{sourceNode, sourcePortName, destNode, destPortName});

  // update adj
  m_adjListOut[sourceNode].push_back(destNode);
  m_adjListIn[destNode].push_back(sourceNode);
  m_inDegree[destNode]++;

  // Cycle detection will be performed after the entire graph is constructed
  return true;
}

const std::vector<Edge> &Graph::getEdges() const { return m_edges; }

int Graph::getInDegree(const std::shared_ptr<ILogicNode> &node) const {
  if (!node) {
    LOG_ERRORS << "Node is null";
    throw std::runtime_error("Node is null");
  }
  auto it = m_inDegree.find(node);
  if (it != m_inDegree.end()) {
    return it->second;
  } else {
    LOG_WARNINGS << "Node " << node->getName() << " not found in inDegree map";
    return 0;
  }
  return 0;
}

int Graph::getOutDegree(const std::shared_ptr<ILogicNode> &node) const {
  if (!node) {
    LOG_ERRORS << "Node is null";
    throw std::runtime_error("Node is null");
  }
  auto it = m_adjListOut.find(node);
  if (it != m_adjListOut.end()) {
    return it->second.size();
  } else {
    LOG_WARNINGS << "Node " << node->getName()
                 << " not found in m_adjListOut map";
    return 0;
  }
}

const std::vector<std::shared_ptr<ILogicNode>> &
Graph::getOutgoingNeighbors(const std::shared_ptr<ILogicNode> &node) const {
  static const std::vector<std::shared_ptr<ILogicNode>> emptyNeighbors;
  if (!node) {
    return emptyNeighbors;
  }
  auto it = m_adjListOut.find(node);
  if (it != m_adjListOut.end()) {
    return it->second;
  }
  return emptyNeighbors;
}

const std::vector<std::shared_ptr<ILogicNode>> &
Graph::getIncomingNeighbors(const std::shared_ptr<ILogicNode> &node) const {
  static const std::vector<std::shared_ptr<ILogicNode>> emptyNeighbors;
  if (!node) {
    return emptyNeighbors;
  }
  auto it = m_adjListIn.find(node);
  if (it != m_adjListIn.end()) {
    return it->second;
  }
  return emptyNeighbors;
}

std::vector<Edge>
Graph::getIncomingEdges(const std::shared_ptr<ILogicNode> &destNode) const {
  std::vector<Edge> incomingEdges;
  for (const auto &edge : m_edges) {
    if (edge.destNode == destNode) {
      incomingEdges.push_back(edge);
    }
  }
  return incomingEdges;
}

std::vector<Edge>
Graph::getOutgoingEdges(const std::shared_ptr<ILogicNode> &sourceNode) const {
  std::vector<Edge> outgoingEdges;
  for (const auto &edge : m_edges) {
    if (edge.sourceNode == sourceNode) {
      outgoingEdges.push_back(edge);
    }
  }
  return outgoingEdges;
}

bool Graph::hasCycle() const {
  // 0: unvisited, 1: visiting (in recursion stack), 2: visited
  std::unordered_map<std::shared_ptr<ILogicNode>, int> visitStatus;
  for (const auto &nodeSp : m_nodes) {
    if (visitStatus[nodeSp] == 0) {
      if (hasCycleDFS(nodeSp, visitStatus)) {
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
    std::unordered_map<std::shared_ptr<ILogicNode>, int> &visitStatus) const {
  // Mark as visiting (in recursion stack)
  visitStatus[node] = 1;

  auto itAdj = m_adjListOut.find(node);
  if (itAdj != m_adjListOut.end()) {
    for (auto v : itAdj->second) {
      if (visitStatus[v] == 1) {
        return true;
      }
      if (visitStatus[v] == 0) {
        if (hasCycleDFS(v, visitStatus)) {
          return true;
        }
      }
    }
  }
  // Mark as visited (finished processing)
  visitStatus[node] = 2;
  return false;
}

} // namespace ai_pipe