#include <filesystem>

#include "types/pipe_data_types.hpp"
#include "visualization_node.hpp"

#include <mexception.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

namespace ai_pipe {
using namespace common_utils::exception;

VisualizationNode::VisualizationNode(const std::string &name,
                                     const VisualizationNodeParams &params)
    : NodeBase(name), params_(params) {

  if (params_.outputDir.empty()) {
    LOG_ERRORS << "VisualizationNode: Missing 'output_dir' parameter.";
    throw InvalidValueException(
        "VisualizationNode: Missing 'output_dir' parameter.");
  }

  if (!std::filesystem::exists(params_.outputDir)) {
    std::filesystem::create_directories(params_.outputDir);
  }

  outputDir_ = params_.outputDir;
}

void VisualizationNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                                std::shared_ptr<PipelineContext> context) {
  const std::string rawImageInputPort = getExpectedInputPorts()[0];
  const std::string inferRetInputPort = getExpectedInputPorts()[1];
  const std::string outputPortName = getExpectedOutputPorts()[0];

  if (inputs.find(rawImageInputPort) == inputs.end() ||
      inputs.find(inferRetInputPort) == inputs.end()) {
    throw InvalidValueException("VisualizationNode: Missing '" +
                                rawImageInputPort + "' or '" +
                                inferRetInputPort + "' input.");
  }

  const auto &rawImagePacket = inputs.at(rawImageInputPort);
  if (!rawImagePacket->has<ImageFramePtr>("image_data") ||
      !rawImagePacket->has<std::string>("image_path")) {
    throw InvalidValueException("VisualizationNode: '" + rawImageInputPort +
                                "' input is not of type ImageFrame.");
  }
  const ImageFramePtr &imageData =
      rawImagePacket->getParam<ImageFramePtr>("image_data");

  const auto &inferRetPacket = inputs.at(inferRetInputPort);
  if (!inferRetPacket->has<ai_core::AlgoOutput>("infer_result")) {
    throw InvalidValueException("VisualizationNode: '" + inferRetInputPort +
                                "' input is not of type InferenceResult.");
  }
  auto algoOutput =
      inferRetPacket->getParam<ai_core::AlgoOutput>("infer_result");

  const auto &inferRet = algoOutput.getParams<ai_core::DetRet>();
  if (!inferRet) {
    throw InvalidValueException(
        "VisualizationNode: Inference result is not of type DetRet.");
  }

  cv::Mat visualizedImage = imageData->data.clone();

  for (const auto &box : inferRet->bboxes) {
    cv::rectangle(
        visualizedImage, cv::Point(box.rect.x, box.rect.y),
        cv::Point(box.rect.x + box.rect.width, box.rect.y + box.rect.height),
        cv::Scalar(0, 255, 0), 2);
    std::string label_text =
        "Label: " + std::to_string(box.label) +
        " Score: " + std::to_string(box.score).substr(0, 4);
    cv::putText(visualizedImage, label_text,
                cv::Point(box.rect.x, box.rect.y - 10),
                cv::FONT_HERSHEY_SIMPLEX, 0.5, cv::Scalar(0, 255, 0), 2);
  }

  auto outputName = rawImagePacket->getParam<std::string>("image_path");
  std::filesystem::path originalPath(outputName);
  std::string filenameStem = originalPath.stem().string();
  std::string filenameExt = originalPath.extension().string();
  std::string outputFileName = filenameStem + "_" +
                               std::to_string(imageData->frameId) +
                               "_visualized" + filenameExt;
  std::string outputPathStr = (outputDir_ / outputFileName).string();

  if (!cv::imwrite(outputPathStr, visualizedImage)) {
    throw FileOperationException(
        "VisualizationNode: Failed to save visualized image to " +
        outputPathStr);
  }

  auto visualizedDataPacket = std::make_shared<PortData>();
  visualizedDataPacket->setParam<cv::Mat>("visualized_image", visualizedImage);
  outputs[outputPortName] = visualizedDataPacket;
}

std::vector<std::string> VisualizationNode::getExpectedInputPorts() const {
  return {"raw_image_viz_input", "infer_ret_viz_input"};
}

std::vector<std::string> VisualizationNode::getExpectedOutputPorts() const {
  return {"visualized_image_output_data"};
}
} // namespace ai_pipe
