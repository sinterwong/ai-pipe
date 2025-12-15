/**
 * @file edge.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_EDGE_HPP
#define AI_PIPE_EDGE_HPP

#include "ai_pipe/node_base.hpp"
#include <memory>

namespace ai_pipe {
struct Edge {
  std::shared_ptr<ILogicNode> sourceNode;
  std::string sourcePort;
  std::shared_ptr<ILogicNode> destNode;
  std::string destPort;

  Edge(std::shared_ptr<ILogicNode> sourceNode, std::string sourcePort,
       std::shared_ptr<ILogicNode> destNode, std::string destPort)
      : sourceNode(std::move(sourceNode)), sourcePort(std::move(sourcePort)),
        destNode(std::move(destNode)), destPort(std::move(destPort)) {}
};
} // namespace ai_pipe

#endif
