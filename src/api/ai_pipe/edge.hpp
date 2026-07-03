/**
 * @file edge.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Directed edge connecting a source node port to a destination node port
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_EDGE_HPP
#define AI_PIPE_EDGE_HPP

#include "ai_pipe/i_logic_node.hpp"
#include <memory>

namespace ai_pipe {
struct Edge {
  std::shared_ptr<ILogicNode> source_node;
  std::string source_port;
  std::shared_ptr<ILogicNode> dest_node;
  std::string dest_port;

  Edge(std::shared_ptr<ILogicNode> source_node, std::string source_port,
       std::shared_ptr<ILogicNode> dest_node, std::string dest_port)
      : source_node(std::move(source_node)),
        source_port(std::move(source_port)), dest_node(std::move(dest_node)),
        dest_port(std::move(dest_port)) {}
};
} // namespace ai_pipe

#endif
