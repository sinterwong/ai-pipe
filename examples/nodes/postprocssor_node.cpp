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
#include "ai_pipe/exception.hpp"
#include "ai_pipe/node_registrar.hpp"
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace ai_core {
void from_json(const nlohmann::json &j, AnchorDetParams &p) {
  if (j.contains("cond_thre")) {
    j.at("cond_thre").get_to(p.condThre);
  } else {
    throw std::runtime_error("Missing 'cond_thre' in AnchorDetParams JSON");
  }
  if (j.contains("nms_thre")) {
    j.at("nms_thre").get_to(p.nmsThre);
  } else {
    throw std::runtime_error("Missing 'nms_thre' in AnchorDetParams JSON");
  }
  if (j.contains("det_alog_type")) {
    p.detAlogType = static_cast<AnchorDetParams::AnchorDetAlogType>(
        j.at("det_alog_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'det_alog_type' in AnchorDetParams JSON");
  }
  if (j.contains("output_names")) {
    j.at("output_names").get_to(p.outputNames);
  } else {
    throw std::runtime_error("Missing 'output_names' in AnchorDetParams JSON");
  }
}

void from_json(const nlohmann::json &j, GenericPostParams &p) {
  if (j.contains("postproc_type")) {
    p.postprocType = static_cast<GenericPostParams::GenericAlgoType>(
        j.at("postproc_type").get<int>());
  } else {
    throw std::runtime_error(
        "Missing 'postproc_type' in GenericPostParams JSON");
  }
  if (j.contains("output_names")) {
    j.at("output_names").get_to(p.outputNames);
  } else {
    throw std::runtime_error(
        "Missing 'output_names' in GenericPostParams JSON");
  }
}
} // namespace ai_core

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, PostprocessorNodeParams &p) {
  if (j.contains("module_name")) {
    j.at("module_name").get_to(p.moduleName);
  } else {
    throw std::runtime_error(
        "Missing 'module_name' in PostprocessorNodeParams JSON");
  }

  if (p.moduleName == "GenericPostproc") {
    if (j.contains("postproc_params")) {
      ai_core::GenericPostParams arg;
      j.at("postproc_params").get_to(arg);
      p.postprocParams.setParams<ai_core::GenericPostParams>(arg);
    } else {
      throw std::runtime_error("Unsupport moduleName " + p.moduleName +
                               " has no 'postproc_params' block.");
    }
  } else if (p.moduleName == "AnchorDetPostproc") {
    if (j.contains("postproc_params")) {
      ai_core::AnchorDetParams arg;
      j.at("postproc_params").get_to(arg);
      p.postprocParams.setParams<ai_core::AnchorDetParams>(arg);
    } else {
      throw std::runtime_error("Unsupport moduleName " + p.moduleName +
                               " has no 'postproc_params' block.");
    }
  } else {
    throw std::runtime_error("Unknown postprocessor module name: " +
                             p.moduleName);
  }
}

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

AI_PIPE_REGISTER_NODE(PostprocessorNode, PostprocessorNodeParams);
} // namespace ai_pipe::examples
