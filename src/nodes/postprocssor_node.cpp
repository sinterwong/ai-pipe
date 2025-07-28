/**
 * @file postprocssor_node.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "postprocssor_node.hpp"
#include <logger.hpp>
#include <mexception.hpp>

namespace ai_pipe {
using namespace common_utils::exception;

PostprocessorNode::PostprocessorNode(const std::string &name,
                                     const PostprocessorNodeParams &params)
    : NodeBase(name),
      mPostprocessor(
          std::make_unique<ai_core::dnn::AlgoPostproc>(params.moduleName)),
      mParams(params) {
  if (mPostprocessor->initialize() != ai_core::InferErrorCode::SUCCESS) {
    throw ExecutionException("Failed to initialize postprocessor module: " +
                             params.moduleName);
  }
}

void PostprocessorNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                                std::shared_ptr<PipelineContext> context) {

  const std::string preprocParamsPortName = getExpectedInputPorts()[0];
  const std::string modelOutputPortName = getExpectedInputPorts()[1];
  const std::string outputPortName = getExpectedOutputPorts()[0];

  if (inputs.find(preprocParamsPortName) == inputs.end()) {
    LOG_ERRORS << "PostprocessorNode: Missing '" << preprocParamsPortName
               << "' input.";
    throw InvalidValueException("PostprocessorNode: Missing '" +
                                preprocParamsPortName + "' input.");
  }

  const auto &preprocParamsPacket = inputs.at(preprocParamsPortName);
  if (!preprocParamsPacket->has<ai_core::AlgoPreprocParams>("preproc_params")) {
    LOG_ERRORS << "PostprocessorNode: '" << preprocParamsPortName
               << "' input does not contain 'preproc_params'.";
    throw InvalidValueException("PostprocessorNode: '" + preprocParamsPortName +
                                "' input does not contain 'preproc_params'.");
  }
  auto preprocParams =
      preprocParamsPacket->getParam<ai_core::AlgoPreprocParams>(
          "preproc_params");

  if (inputs.find(modelOutputPortName) == inputs.end()) {
    LOG_ERRORS << "PostprocessorNode: Missing '" << modelOutputPortName
               << "' input.";
    throw InvalidValueException("PostprocessorNode: Missing '" +
                                modelOutputPortName + "' input.");
  }

  const auto &inputDataPacket = inputs.at(modelOutputPortName);
  if (!inputDataPacket->has<ai_core::TensorData>("model_output_tensor")) {
    LOG_ERRORS << "PostprocessorNode: '" << modelOutputPortName
               << "' input does not contain 'model_output_tensor'.";
    throw InvalidValueException(
        "PostprocessorNode: '" + modelOutputPortName +
        "' input does not contain 'model_output_tensor'.");
  }
  auto modelOutput =
      inputDataPacket->getParam<ai_core::TensorData>("model_output_tensor");

  ai_core::AlgoOutput algoOutput;
  if (mPostprocessor->process(modelOutput, preprocParams, algoOutput,
                              mParams.postprocParams) !=
      ai_core::InferErrorCode::SUCCESS) {
    throw ExecutionException("PostprocessorNode: Failed to process output.");
  }

  auto outputDataPacket = std::make_shared<PortData>();
  outputDataPacket->setParam<ai_core::AlgoOutput>("infer_result", algoOutput);
  outputs[outputPortName] = outputDataPacket;
}

std::vector<std::string> PostprocessorNode::getExpectedInputPorts() const {
  return {"final_preproc_params", "model_output_tensor"};
}

std::vector<std::string> PostprocessorNode::getExpectedOutputPorts() const {
  return {"algo_output"};
}
} // namespace ai_pipe