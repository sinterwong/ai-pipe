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
  if (nodeMap_.find(node->getName()) != nodeMap_.end()) {
    LOG_ERRORS << "Node with name " << node->getName()
               << " already exists in the graph";
    return false;
  }
  nodes_.push_back(node);
  nodeMap_[node->getName()] = node;

  // init adj and indegree
  adjListOut_[node] = {};
  adjListIn_[node] = {};
  inDegree_[node] = 0;
  return true;
}

std::shared_ptr<ILogicNode> Graph::getNode(const std::string &name) const {
  auto it = nodeMap_.find(name);
  if (it != nodeMap_.end()) {
    return it->second;
  }
  return nullptr;
}

const std::vector<std::shared_ptr<ILogicNode>> &Graph::getNodes() const {
  return nodes_;
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

  // 校验目标节点端口
  const auto &expectedInputPorts = destNode->getExpectedInputPorts();
  if (expectedInputPorts.empty() && !destPortName.empty()) {
    LOG_ERRORS << "Destination node '" << destNodeName
               << "' declares no input ports, but tried to connect to port '"
               << destPortName << "'.";
    return false;
  }
  if (!destPortName.empty()) { // 同理，只对非空端口名进行查找
    if (std::find(expectedInputPorts.begin(), expectedInputPorts.end(),
                  destPortName) == expectedInputPorts.end()) {
      LOG_ERRORS << "Destination port '" << destPortName
                 << "' is not a declared input port for node '" << destNodeName
                 << "'.";
      return false;
    }
  }
  // 检查是否已经存在完全相同的边(源节点、源端口、目标节点、目标端口都相同)
  for (const auto &existingEdge : edges_) {
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

  edges_.emplace_back(Edge{sourceNode, sourcePortName, destNode, destPortName});

  // update adj
  adjListOut_[sourceNode].push_back(destNode);
  adjListIn_[destNode].push_back(sourceNode);
  inDegree_[destNode]++;

  // 环路检测将会在整个graph构建完成之后检查
  return true;
}

const std::vector<Edge> &Graph::getEdges() const { return edges_; }

int Graph::getInDegree(const std::shared_ptr<ILogicNode> &node) const {
  if (!node) {
    LOG_ERRORS << "Node is null";
    throw std::runtime_error("Node is null");
  }
  auto it = inDegree_.find(node);
  if (it != inDegree_.end()) {
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
  auto it = adjListOut_.find(node);
  if (it != adjListOut_.end()) {
    return it->second.size();
  } else {
    LOG_WARNINGS << "Node " << node->getName()
                 << " not found in adjListOut_ map";
    return 0;
  }
}

const std::vector<std::shared_ptr<ILogicNode>> &
Graph::getOutgoingNeighbors(const std::shared_ptr<ILogicNode> &node) const {
  static const std::vector<std::shared_ptr<ILogicNode>> emptyNeighbors;
  if (!node) {
    return emptyNeighbors;
  }
  auto it = adjListOut_.find(node);
  if (it != adjListOut_.end()) {
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
  auto it = adjListIn_.find(node);
  if (it != adjListIn_.end()) {
    return it->second;
  }
  return emptyNeighbors;
}

std::vector<Edge>
Graph::getIncomingEdges(const std::shared_ptr<ILogicNode> &destNode) const {
  std::vector<Edge> incomingEdges;
  for (const auto &edge : edges_) {
    if (edge.destNode == destNode) {
      incomingEdges.push_back(edge);
    }
  }
  return incomingEdges;
}

std::vector<Edge>
Graph::getOutgoingEdges(const std::shared_ptr<ILogicNode> &sourceNode) const {
  std::vector<Edge> outgoingEdges;
  for (const auto &edge : edges_) {
    if (edge.sourceNode == sourceNode) {
      outgoingEdges.push_back(edge);
    }
  }
  return outgoingEdges;
}

bool Graph::hasCycle() const {
  // 0: unvisited, 1: visiting (in recursion stack), 2: visited
  std::unordered_map<std::shared_ptr<ILogicNode>, int> visitStatus;
  for (const auto &node_sp : nodes_) {
    if (visitStatus[node_sp] == 0) {
      if (hasCycleDFS(node_sp, visitStatus)) {
        return true;
      }
    }
  }
  return false;
}

void Graph::clear() {
  nodes_.clear();
  edges_.clear();
  nodeMap_.clear();
  adjListOut_.clear();
  adjListIn_.clear();
  inDegree_.clear();
}

bool Graph::hasCycleDFS(
    const std::shared_ptr<ILogicNode> &node,
    std::unordered_map<std::shared_ptr<ILogicNode>, int> &visitStatus) const {
  // Mark as visiting (in recursion stack)
  visitStatus[node] = 1;

  auto it_adj = adjListOut_.find(node);
  if (it_adj != adjListOut_.end()) {
    for (auto v : it_adj->second) {
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