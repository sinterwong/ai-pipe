/**
 * @file preprocssor_node.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "preprocssor_node.hpp"
#include <logger.hpp>
#include <mexception.hpp>

namespace ai_pipe {
using namespace common_utils::exception;

PreprocessorNode::PreprocessorNode(const std::string &name,
                                   const PreprocessorNodeParams &params)
    : NodeBase(name), mPreprocessor(std::make_unique<ai_core::dnn::AlgoPreproc>(
                          params.moduleName)),
      mParams(params) {
  if (mPreprocessor->initialize() != ai_core::InferErrorCode::SUCCESS) {
    throw ExecutionException("Failed to initialize preprocessor module: " +
                             params.moduleName);
  }
}

void PreprocessorNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                               std::shared_ptr<PipelineContext> context) {
  const std::string inputPortName = getExpectedInputPorts()[0];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "PreprocessorNode: Missing '" << inputPortName << "' input.";
    throw InvalidValueException("PreprocessorNode: Missing '" + inputPortName +
                                "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);
  if (!inputDataPacket->has<ai_core::AlgoInput>("input_data")) {
    LOG_ERRORS << "PreprocessorNode: '" << inputPortName
               << "' input does not contain 'image_data'.";
    throw InvalidValueException("PreprocessorNode: '" + inputPortName +
                                "' input does not contain 'image_data'.");
  }

  auto algoInput = inputDataPacket->getParam<ai_core::AlgoInput>("input_data");

  ai_core::TensorData modelInput;
  if (mPreprocessor->process(algoInput, mParams.preprocParams, modelInput) !=
      ai_core::InferErrorCode::SUCCESS) {
    throw ExecutionException("PreprocessorNode: Failed to process image.");
  }

  auto outputDataPacket = std::make_shared<PortData>();
  outputDataPacket->setParam<ai_core::TensorData>("model_input_tensor",
                                                  modelInput);
  outputs[getExpectedOutputPorts()[0]] = outputDataPacket;

  auto preprocParamsPacket = std::make_shared<PortData>();
  preprocParamsPacket->setParam<ai_core::AlgoPreprocParams>(
      "preproc_params", mParams.preprocParams);
  outputs[getExpectedOutputPorts()[1]] = preprocParamsPacket;
}

std::vector<std::string> PreprocessorNode::getExpectedInputPorts() const {
  return {"algo_input"};
}

std::vector<std::string> PreprocessorNode::getExpectedOutputPorts() const {
  return {"model_input_tensor", "final_preproc_params"};
}
} // namespace ai_pipe