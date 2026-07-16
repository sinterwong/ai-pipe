/**
 * @file test_plugin_badversion.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Plugin declaring an incompatible plugin ABI revision. The
 *        loader must reject it and roll back its registration (F8).
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/plugin.hpp"

namespace ai_pipe_test_plugin {

class BadVersionNode : public ai_pipe::ILogicNode {
public:
  explicit BadVersionNode(const std::string &name) : ILogicNode(name) {}

  void process(const ai_pipe::PortDataMap &,
               ai_pipe::PortDataMap &,
               std::shared_ptr<ai_pipe::PipelineContext>) override {}
};

AI_PIPE_REGISTER_NODE(BadVersionNode);

} // namespace ai_pipe_test_plugin

// Hand-rolled descriptor (instead of AI_PIPE_PLUGIN) with a plugin ABI
// revision the host does not speak.
extern "C" const ::ai_pipe::PluginDescriptor *ai_pipe_plugin_descriptor() {
  static const ::ai_pipe::PluginDescriptor descriptor{
      ::ai_pipe::k_plugin_abi_version + 999,
      static_cast<std::uint32_t>(AI_PIPE_VERSION), "bad_version"};
  return &descriptor;
}
