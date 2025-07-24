/**
 * @file infer_engine_node.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "infer_engine_node.hpp"
#include <logger.hpp>
#include <mexception.hpp>

namespace ai_pipe {
using namespace common_utils::exception;

InferEngineNode::InferEngineNode(const std::string &name,
                                 const InferEngineNodeParams &params)
    : NodeBase(name), mEngine(std::make_unique<ai_core::dnn::AlgoInferEngine>(
                          params.moduleName, params.inferParams)),
      mParams(params) {
  if (mEngine->initialize() != ai_core::InferErrorCode::SUCCESS) {
    throw ExecutionException("Failed to initialize inference engine module: " +
                             params.moduleName);
  }
}

void InferEngineNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                              std::shared_ptr<PipelineContext> context) {
  const std::string inputPortName = getExpectedInputPorts()[0];
  const std::string outputPortName = getExpectedOutputPorts()[0];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "InferEngineNode: Missing '" << inputPortName << "' input.";
    throw InvalidValueException("InferEngineNode: Missing '" + inputPortName +
                                "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);
  if (!inputDataPacket->has<ai_core::TensorData>("model_input_tensor")) {
    LOG_ERRORS << "InferEngineNode: '" << inputPortName
               << "' input does not contain 'model_input_tensor'.";
    throw InvalidValueException(
        "InferEngineNode: '" + inputPortName +
        "' input does not contain 'model_input_tensor'.");
  }

  auto modelInput =
      inputDataPacket->getParam<ai_core::TensorData>("model_input_tensor");

  ai_core::TensorData modelOutput;
  if (mEngine->infer(modelInput, modelOutput) !=
      ai_core::InferErrorCode::SUCCESS) {
    throw ExecutionException("InferEngineNode: Failed to perform inference.");
  }

  auto outputDataPacket = std::make_shared<PortData>();
  outputDataPacket->setParam<ai_core::TensorData>("model_output_tensor",
                                                  modelOutput);
  outputs[outputPortName] = outputDataPacket;
}

std::vector<std::string> InferEngineNode::getExpectedInputPorts() const {
  return {"model_input_tensor"};
}

std::vector<std::string> InferEngineNode::getExpectedOutputPorts() const {
  return {"model_output_tensor"};
}
} // namespace ai_pipe