#include "result_saver_node.hpp"

#include <logger.hpp>
#include <mexception.hpp>

namespace ai_pipe {
using namespace common_utils::exception;

ResultSaverNode::ResultSaverNode(const std::string &name,
                                 const ResultSaverNodeParams &params)
    : NodeBase(name), params_(params) {
  if (params_.outputDir.empty()) {
    LOG_ERRORS << "ResultSaverNode: Missing 'output_dir' parameter.";
    throw InvalidValueException(
        "ResultSaverNode: Missing 'output_dir' parameter.");
  }
  if (!std::filesystem::exists(params_.outputDir)) {
    std::filesystem::create_directories(params_.outputDir);
  }
}

void ResultSaverNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                              std::shared_ptr<PipelineContext> context) {
  const std::string inputPortName = getExpectedInputPorts()[0];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "ResultSaverNode: Missing '" << inputPortName << "' input.";
    throw InvalidValueException("ResultSaverNode: Missing '" + inputPortName +
                                "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);
  if (!inputDataPacket->has<ai_core::AlgoOutput>("infer_result")) {
    LOG_ERRORS << "ResultSaverNode: '" << inputPortName
               << "' input is not of type InferenceResult.";
    throw InvalidValueException("ResultSaverNode: '" + inputPortName +
                                "' input is not of type InferenceResult.");
  }

  auto result = inputDataPacket->getParam<ai_core::AlgoOutput>("infer_result");
  const auto &detResults = result.getParams<ai_core::DetRet>();

  // Just print results
  LOG_INFOS << "Inference Results:";
  for (const auto &box : detResults->bboxes) {
    LOG_INFOS << "  Label: " << box.label << ", Score: " << box.score
              << ", BBox: [" << box.rect->x << ", " << box.rect->y << ", "
              << box.rect->width << ", " << box.rect->height << "]";
  }
}

std::vector<std::string> ResultSaverNode::getExpectedInputPorts() const {
  return {"inference_result_input"};
}

} // namespace ai_pipe
