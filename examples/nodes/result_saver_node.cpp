#include "result_saver_node.hpp"
#include "ai_pipe/exception.hpp"
#include "ai_pipe/node_registrar.hpp"
#include "ai_pipe/types.hpp"
#include <ai_core/algo_data_types.hpp>
#include <filesystem>
#include <logger.hpp>
#include <nlohmann/json.hpp>
#include <opencv2/opencv.hpp>

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, ResultSaverNodeParams &p) {
  if (j.contains("output_dir")) {
    j.at("output_dir").get_to(p.outputDir);
  } else {
    throw std::runtime_error(
        "Missing 'output_dir' in ResultSaverNodeParams JSON");
  }
}

ResultSaverNode::ResultSaverNode(const std::string &name,
                                 const ResultSaverNodeParams &params)
    : NodeBase(name), mParams(params) {
  if (mParams.outputDir.empty()) {
    LOG_ERRORS << "ResultSaverNode: Missing 'output_dir' parameter.";
    throw InvalidValueException(
        "ResultSaverNode: Missing 'output_dir' parameter.");
  }
  if (!std::filesystem::exists(mParams.outputDir)) {
    std::filesystem::create_directories(mParams.outputDir);
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

AI_PIPE_REGISTER_NODE(ResultSaverNode, ResultSaverNodeParams);
} // namespace ai_pipe::examples
