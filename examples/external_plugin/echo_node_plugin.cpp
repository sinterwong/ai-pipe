#include <ai_pipe/i_logic_node.hpp>
#include <ai_pipe/node_registry.hpp>
#include <ai_pipe/plugin.hpp>

#include <memory>
#include <string>
#include <vector>

namespace {

/** Passes the `input` packet to `output` without changing its storage. */
class EchoNode final : public ai_pipe::ILogicNode {
public:
  explicit EchoNode(const std::string &name) : ILogicNode(name) {}

  void process(const ai_pipe::PortDataMap &inputs,
               ai_pipe::PortDataMap &outputs,
               std::shared_ptr<ai_pipe::PipelineContext>) override {
    outputs["output"] = inputs.at("input");
  }

  [[nodiscard]] std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  [[nodiscard]] std::vector<std::string>
  getExpectedOutputPorts() const override {
    return {"output"};
  }
};

AI_PIPE_REGISTER_NODE(EchoNode);

} // namespace

AI_PIPE_PLUGIN("example_echo_node");
