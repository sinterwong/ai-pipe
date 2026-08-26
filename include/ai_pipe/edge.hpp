#ifndef AI_PIPE_EDGE_HPP
#define AI_PIPE_EDGE_HPP

#include "ai_pipe/i_logic_node.hpp"
#include <memory>

namespace ai_pipe {

/** A directed connection between two named node ports. */
struct Edge {
  std::shared_ptr<ILogicNode> source_node; ///< Keeps the source node alive.
  std::string source_port;                 ///< Declared source output port.
  std::shared_ptr<ILogicNode> dest_node; ///< Keeps the destination node alive.
  std::string dest_port;                 ///< Declared destination input port.

  Edge(std::shared_ptr<ILogicNode> source_node, std::string source_port,
       std::shared_ptr<ILogicNode> dest_node, std::string dest_port)
      : source_node(std::move(source_node)),
        source_port(std::move(source_port)), dest_node(std::move(dest_node)),
        dest_port(std::move(dest_port)) {}
};
} // namespace ai_pipe

#endif
