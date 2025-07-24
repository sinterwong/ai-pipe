/**
 * @file gen_frame_node.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "gen_frame_node.hpp"
#include "logic_types/pipe_data_types.hpp"
#include <logger.hpp>
#include <mexception.hpp>

namespace ai_pipe {
using namespace common_utils::exception;

GenFrameNode::GenFrameNode(const std::string &name,
                           const GenFrameNodeParams &params)
    : NodeBase(name), mParams(params) {}

void GenFrameNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                           std::shared_ptr<PipelineContext> context) {

  const std::string inputPortName = getExpectedInputPorts()[0];
  const std::string outputPortName = getExpectedOutputPorts()[0];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "GenFrameNode: Missing '" << inputPortName << "' input.";
    throw InvalidValueException("GenFrameNode: Missing '" + inputPortName +
                                "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);
  if (!inputDataPacket->has<ImageFramePtr>("image_data")) {
    LOG_ERRORS << "GenFrameNode: '" << inputPortName
               << "' input does not contain 'image_data'.";
    throw InvalidValueException("GenFrameNode: '" + inputPortName +
                                "' input does not contain 'image_data'.");
  }

  auto imageFramePtr = inputDataPacket->getParam<ImageFramePtr>("image_data");

  ai_core::AlgoInput algoInput;

  // FIXME: set temp roi
  auto roi = std::make_shared<cv::Rect>(0, 0, imageFramePtr->data.cols,
                                        imageFramePtr->data.rows);
  algoInput.setParams<ai_core::FrameInput>(
      ai_core::FrameInput{std::make_shared<cv::Mat>(imageFramePtr->data), roi});
  auto outputDataPacket = std::make_shared<PortData>();
  outputDataPacket->setParam<ai_core::AlgoInput>("input_data", algoInput);
  outputs[outputPortName] = outputDataPacket;
}

std::vector<std::string> GenFrameNode::getExpectedInputPorts() const {
  return {"image_input_data"};
}

std::vector<std::string> GenFrameNode::getExpectedOutputPorts() const {
  return {"algo_input"};
}
} // namespace ai_pipe