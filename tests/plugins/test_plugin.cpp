#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/node_registry.hpp"
#include "ai_pipe/plugin.hpp"

namespace ai_pipe_test_plugin {

// Trivial pass-through node contributed by the plugin
class PluginEchoNode : public ai_pipe::ILogicNode {
public:
  explicit PluginEchoNode(const std::string &name) : ILogicNode(name) {}

  void process(const ai_pipe::PortDataMap &inputs,
               ai_pipe::PortDataMap &outputs,
               std::shared_ptr<ai_pipe::PipelineContext>) override {
    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }
};

} // namespace ai_pipe_test_plugin

AI_PIPE_PLUGIN("test_plugin");

extern "C" AI_PIPE_PLUGIN_EXPORT bool
ai_pipe_register_plugin_v1(ai_pipe::NodeRegistry &registry) {
  return registry.registerNode<ai_pipe_test_plugin::PluginEchoNode>("test.echo")
      .isOk();
}
