
#include "through_pass_node.hpp"
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/logger.hpp"
#include "exception.hpp"
#include "node_registrar.hpp"
#include <nlohmann/json.hpp>

using namespace ai_pipe::exception;
namespace ai_pipe::examples {

void from_json(const nlohmann::json &j, ThroughPassNodeParams &p) {
  j.at("name").get_to(p.name);
}

ThroughPassNode::ThroughPassNode(const std::string &name,
                                 const ThroughPassNodeParams &params)
    : ILogicNode(name), m_params(params) {}

void ThroughPassNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                              std::shared_ptr<PipelineContext> context) {
  const std::string input_port_name = getExpectedInputPorts()[0];
  const std::string output_port_name = getExpectedOutputPorts()[0];

  if (inputs.find(input_port_name) == inputs.end()) {
    LOG_ERROR_S << "ThroughPassNode: Missing '" << input_port_name
                << "' input.";
    throw InvalidValueException("ThroughPassNode: Missing '" + input_port_name +
                                "' input.");
  }

  const auto &input_data_packet = inputs.at(input_port_name);

  // print info
  LOG_TRACE_S << "ThroughPassNode: Processing data from input port '"
              << input_port_name << "' to output port '" << output_port_name
              << "'.";

  auto output_data_packet = std::make_shared<PortData>();
  *output_data_packet = *input_data_packet;
  outputs[output_port_name] = output_data_packet;
}

std::vector<std::string> ThroughPassNode::getExpectedInputPorts() const {
  return {"input"};
}

std::vector<std::string> ThroughPassNode::getExpectedOutputPorts() const {
  return {"output"};
}

AI_PIPE_REGISTER_NODE(ThroughPassNode, ThroughPassNodeParams);
} // namespace ai_pipe::examples
