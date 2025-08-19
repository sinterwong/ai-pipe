#include "visualization_node.hpp"
#include "ai_pipe/exception.hpp"
#include "ai_pipe/node_registrar.hpp"
#include "data_types.hpp"
#include <filesystem>
#include <logger.hpp>
#include <nlohmann/json.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/imgproc.hpp>

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, VisualizationNodeParams &p) {
  if (j.contains("output_dir")) {
    j.at("output_dir").get_to(p.outputDir);
  } else {
    throw std::runtime_error(
        "Missing 'output_dir' in VisualizationNodeParams JSON");
  }
  if (j.contains("prefix_name")) {
    j.at("prefix_name").get_to(p.prefixName);
  } else {
    LOG_WARNINGS << "Missing 'prefix_name' in VisualizationNodeParams JSON. "
                    "Defaulting to empty string.";
    p.prefixName = "";
  }
}

VisualizationNode::VisualizationNode(const std::string &name,
                                     const VisualizationNodeParams &params)
    : NodeBase(name), mParams(params) {

  if (!mParams.outputDir.empty()) {
    if (!std::filesystem::exists(mParams.outputDir)) {
      std::filesystem::create_directories(mParams.outputDir);
    }
    LOG_INFOS << "VisualizationNode: Output directory set to: "
              << mParams.outputDir;
    mOutputDirPath = mParams.outputDir;
  }
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
  if (!rawImagePacket->has<ImageFramePtr>("image_data")) {
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

  cv::Mat visualizedImage = visualizeAccordOutput(algoOutput, imageData->data);

  if (!mParams.outputDir.empty()) {
    std::string filename;
    filename =
        mParams.prefixName + "_" + std::to_string(imageData->frameId) + ".jpg";

    std::string outputPathStr = (mOutputDirPath / filename).string();
    if (!cv::imwrite(outputPathStr, visualizedImage)) {
      throw FileOperationException(
          "VisualizationNode: Failed to save visualized image to " +
          outputPathStr);
    }
  }

  auto visualizedDataPacket = std::make_shared<PortData>();
  visualizedDataPacket->setParam<cv::Mat>("visualized_image", visualizedImage);
  outputs[outputPortName] = visualizedDataPacket;
}

cv::Mat
VisualizationNode::visualizeAccordOutput(ai_core::AlgoOutput &algoOutput,
                                         const cv::Mat &originalImage) const {
  // TODO: use different visualization schemes according to different types of
  // outputs
  cv::Mat visualizedImage = originalImage.clone();
  if (const auto &inferRet = algoOutput.getParams<ai_core::DetRet>();
      inferRet != nullptr) {
    for (const auto &box : inferRet->bboxes) {
      cv::rectangle(visualizedImage, cv::Point(box.rect->x, box.rect->y),
                    cv::Point(box.rect->x + box.rect->width,
                              box.rect->y + box.rect->height),
                    cv::Scalar(0, 255, 0), 2);
      std::string label_text =
          "Label: " + std::to_string(box.label) +
          " Score: " + std::to_string(box.score).substr(0, 4);
      cv::putText(visualizedImage, label_text,
                  cv::Point(box.rect->x, box.rect->y - 10),
                  cv::FONT_HERSHEY_SIMPLEX, 0.5, cv::Scalar(0, 255, 0), 2);
    }
  } else {
    LOG_WARNINGS
        << "VisualizationNode: Unsupported AlgoOutput type for visualization.";
  }
  return visualizedImage;
}

std::vector<std::string> VisualizationNode::getExpectedInputPorts() const {
  return {"raw_image_viz_input", "infer_ret_viz_input"};
}

std::vector<std::string> VisualizationNode::getExpectedOutputPorts() const {
  return {"visualized_image_output_data"};
}

AI_PIPE_REGISTER_NODE(VisualizationNode, VisualizationNodeParams);
} // namespace ai_pipe::examples
