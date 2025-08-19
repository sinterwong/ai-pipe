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
#include "ai_pipe/exception.hpp"
#include "ai_pipe/node_registrar.hpp"
#include <logger.hpp>
#include <nlohmann/json.hpp>
#include <opencv2/opencv.hpp>

namespace ai_core {
void from_json(const nlohmann::json &j, FramePreprocessArg &p) {
  if (j.contains("preproc_task_type")) {
    p.preprocTaskType = static_cast<FramePreprocessArg::FramePreprocType>(
        j.at("preproc_task_type").get<int>());
  } else {
    throw std::runtime_error(
        "Missing 'preproc_task_type' in FramePreprocessArg JSON");
  }
  if (j.contains("model_input_shape")) {
    auto shape_arr = j.at("model_input_shape");
    p.modelInputShape.w = j.at("model_input_shape").at("w").get<int>();
    p.modelInputShape.h = j.at("model_input_shape").at("h").get<int>();
    p.modelInputShape.c = j.at("model_input_shape").at("c").get<int>();
  } else {
    throw std::runtime_error(
        "Missing 'model_input_shape' in FramePreprocessArg JSON");
  }
  if (j.contains("need_resize")) {
    j.at("need_resize").get_to(p.needResize);
  } else {
    throw std::runtime_error(
        "Missing 'need_resize' in FramePreprocessArg JSON");
  }
  if (j.contains("is_equal_scale")) {
    j.at("is_equal_scale").get_to(p.isEqualScale);
  } else {
    throw std::runtime_error(
        "Missing 'is_equal_scale' in FramePreprocessArg JSON");
  }
  if (j.contains("hwc2chw")) {
    j.at("hwc2chw").get_to(p.hwc2chw);
  } else {
    throw std::runtime_error("Missing 'hwc2chw' in FramePreprocessArg JSON");
  }
  if (j.contains("data_type")) {
    p.dataType = static_cast<DataType>(j.at("data_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'data_type' in FramePreprocessArg JSON");
  }
  if (j.contains("buffer_location")) {
    p.outputLocation =
        static_cast<BufferLocation>(j.at("buffer_location").get<int>());
  } else {
    throw std::runtime_error(
        "Missing 'output_location' in FramePreprocessArg JSON");
  }
  if (j.contains("input_names")) {
    j.at("input_names").get_to(p.inputNames);
  } else {
    p.inputNames = {};
  }
  if (j.contains("pad")) {
    j.at("pad").get_to(p.pad);
  } else {
    p.pad = {};
  }
  if (j.contains("mean_vals")) {
    j.at("mean_vals").get_to(p.meanVals);
  } else {
    p.meanVals = {};
  }
  if (j.contains("norm_vals")) {
    j.at("norm_vals").get_to(p.normVals);
  } else {
    p.normVals = {};
  }
}
} // namespace ai_core

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, PreprocessorNodeParams &p) {
  if (j.contains("module_name")) {
    j.at("module_name").get_to(p.moduleName);
  } else {
    throw std::runtime_error(
        "Missing 'module_name' in PreprocessorNodeParams JSON");
  }

  if (p.moduleName == "FramePreprocess") {
    if (j.contains("preproc_params")) {
      ai_core::FramePreprocessArg arg;
      j.at("preproc_params").get_to(arg);
      p.preprocParams.setParams<ai_core::FramePreprocessArg>(arg);
    } else {
      throw std::runtime_error("Unsupport moduleName " + p.moduleName +
                               " has no 'preproc_params' block.");
    }
  } else {
    throw std::runtime_error("Unknown preprocessor module name: " +
                             p.moduleName);
  }
}

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

AI_PIPE_REGISTER_NODE(PreprocessorNode, PreprocessorNodeParams);
} // namespace ai_pipe::examples