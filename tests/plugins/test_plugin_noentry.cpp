#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/node_registry.hpp"

namespace ai_pipe_test_plugin {

class OrphanPluginNode : public ai_pipe::ILogicNode {
public:
  explicit OrphanPluginNode(const std::string &name) : ILogicNode(name) {}

  void process(const ai_pipe::PortDataMap &, ai_pipe::PortDataMap &,
               std::shared_ptr<ai_pipe::PipelineContext>) override {}
};

AI_PIPE_REGISTER_NODE(OrphanPluginNode);

} // namespace ai_pipe_test_plugin
