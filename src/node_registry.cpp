#include "ai_pipe/node_registry.hpp"

namespace ai_pipe {

NodeRegistry &NodeRegistry::instance() {
  static NodeRegistry registry;
  return registry;
}

} // namespace ai_pipe
