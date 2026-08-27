#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/plugin.hpp"

namespace {

class RejectedNode final : public ai_pipe::ILogicNode {
public:
  explicit RejectedNode(const std::string &name) : ILogicNode(name) {}

  void process(const ai_pipe::PortDataMap &, ai_pipe::PortDataMap &,
               std::shared_ptr<ai_pipe::PipelineContext>) override {}
};

} // namespace

AI_PIPE_PLUGIN("rejected_plugin");

extern "C" AI_PIPE_PLUGIN_EXPORT bool
ai_pipe_register_plugin_v1(ai_pipe::NodeRegistry &registry) {
  (void)registry.registerNode<RejectedNode>("test.rejected");
  return false;
}
