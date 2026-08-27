#include <ai_pipe/i_logic_node.hpp>
#include <ai_pipe/node_registry.hpp>
#include <ai_pipe/plugin.hpp>

namespace {

class PackageEchoNode final : public ai_pipe::ILogicNode {
public:
  explicit PackageEchoNode(const std::string &name) : ILogicNode(name) {}

  void process(const ai_pipe::PortDataMap &inputs,
               ai_pipe::PortDataMap &outputs,
               std::shared_ptr<ai_pipe::PipelineContext>) override {
    outputs["output"] = inputs.at("input");
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }
};

} // namespace

AI_PIPE_PLUGIN("package_consumer.echo");

extern "C" AI_PIPE_PLUGIN_EXPORT bool
ai_pipe_register_plugin_v1(ai_pipe::NodeRegistry &registry) {
  return registry.registerNode<PackageEchoNode>("package.echo").isOk();
}
