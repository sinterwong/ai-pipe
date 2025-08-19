/**
 * @file make_frame_input_node.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "make_frame_input_node.hpp"
#include "ai_pipe/node_registrar.hpp"
#include "data_types.hpp"
#include "exception.hpp"
#include <ai_core/algo_data_types.hpp>
#include <ai_core/algo_input_types.hpp>
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, MakeFrameInputNodeParams &p) {
  // MakeFrameInputNodeParams currently has no parameters, so this function is
  // empty. If parameters are added in the future, they should be parsed here.
  (void)j; // Suppress unused variable warning
  (void)p; // Suppress unused variable warning
}

MakeFrameInputNode::MakeFrameInputNode(const std::string &name,
                                       const MakeFrameInputNodeParams &params)
    : NodeBase(name), mParams(params) {}

void MakeFrameInputNode::process(const PortDataMap &inputs,
                                 PortDataMap &outputs,
                                 std::shared_ptr<PipelineContext> context) {

  const std::string inputPortName = getExpectedInputPorts()[0];
  const std::string outputPortName = getExpectedOutputPorts()[0];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "MakeFrameInputNode: Missing '" << inputPortName
               << "' input.";
    throw InvalidValueException("MakeFrameInputNode: Missing '" +
                                inputPortName + "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);
  if (!inputDataPacket->has<ImageFramePtr>("image_data")) {
    LOG_ERRORS << "MakeFrameInputNode: '" << inputPortName
               << "' input does not contain 'image_data'.";
    throw InvalidValueException("MakeFrameInputNode: '" + inputPortName +
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

std::vector<std::string> MakeFrameInputNode::getExpectedInputPorts() const {
  return {"image_input_data"};
}

std::vector<std::string> MakeFrameInputNode::getExpectedOutputPorts() const {
  return {"algo_input"};
}

AI_PIPE_REGISTER_NODE(MakeFrameInputNode, MakeFrameInputNodeParams);
} // namespace ai_pipe::examples