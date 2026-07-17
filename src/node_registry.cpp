/**
 * @file node_registry.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Out-of-line home for the NodeRegistry singleton
 *
 * The only member defined here is instance(). See the declaration in
 * node_registry.hpp for why it must not be inline: the registry has to
 * be one object per process, and only a definition living inside the
 * library guarantees that across dlopen and static-link boundaries.
 *
 * @copyright Copyright (c) 2026
 */

#include "ai_pipe/node_registry.hpp"

namespace ai_pipe {

NodeRegistry &NodeRegistry::instance() {
  static NodeRegistry registry;
  return registry;
}

} // namespace ai_pipe
