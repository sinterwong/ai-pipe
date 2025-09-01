
#include "through_pass_node.hpp"
#include "ai_pipe/types.hpp"
#include "exception.hpp"
#include "node_registrar.hpp"
#include <logger.hpp>
#include <nlohmann/json.hpp>

using namespace ai_pipe::exception;
namespace ai_pipe::examples {

void from_json(const nlohmann::json &j, ThroughPassNodeParams &p) {
  j.at("name").get_to(p.name);
}

ThroughPassNode::ThroughPassNode(const std::string &name,
                                 const ThroughPassNodeParams &params)
    : NodeBase(name), mParams(params) {}

void ThroughPassNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                              std::shared_ptr<PipelineContext> context) {
  const std::string inputPortName = getExpectedInputPorts()[0];
  const std::string outputPortName = getExpectedOutputPorts()[0];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "ThroughPassNode: Missing '" << inputPortName << "' input.";
    throw InvalidValueException("ThroughPassNode: Missing '" + inputPortName +
                                "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);

  // print info
  LOG_INFOS << "ThroughPassNode: Processing data from input port '"
            << inputPortName << "' to output port '" << outputPortName << "'.";

  auto outputDataPacket = std::make_shared<PortData>();
  *outputDataPacket = *inputDataPacket;
  outputs[outputPortName] = outputDataPacket;
}

std::vector<std::string> ThroughPassNode::getExpectedInputPorts() const {
  return {"input"};
}

std::vector<std::string> ThroughPassNode::getExpectedOutputPorts() const {
  return {"output"};
}

AI_PIPE_REGISTER_NODE(ThroughPassNode, ThroughPassNodeParams);
} // namespace ai_pipe::examples
