#ifndef __NODE_REGISTRAR_HPP__
#define __NODE_REGISTRAR_HPP__

#include "node_base.hpp"

#include <type_safe_factory.hpp>

namespace ai_pipe {
using NodeFactory = common_utils::Factory<NodeBase>;

class NodeRegistrar {
public:
  static NodeRegistrar &getInstance() {
    static NodeRegistrar instance;
    return instance;
  }

  NodeRegistrar(const NodeRegistrar &) = delete;
  NodeRegistrar &operator=(const NodeRegistrar &) = delete;
  NodeRegistrar(NodeRegistrar &&) = delete;
  NodeRegistrar &operator=(NodeRegistrar &&) = delete;

private:
  NodeRegistrar();
};

[[maybe_unused]] inline const static NodeRegistrar &node_registrar =
    NodeRegistrar::getInstance();
} // namespace ai_pipe

#endif // __NODE_REGISTRAR_HPP__
