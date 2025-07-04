#include <vector>

#include "logic_types/pipe_data_types.hpp"
#include "vision_inference_node.hpp"

#include <logger.hpp>
#include <mexception.hpp>

namespace ai_pipe {
using namespace common_utils::exception;
VisionInferenceNode::VisionInferenceNode(
    const std::string &name, const VisionInferenceNodeParams &params)
    : NodeBase(name), params_(params) {

  if (params_.modelName.empty()) {
    LOG_ERRORS << "VisionInferenceNode: Missing 'model_name' parameter.";
    throw InvalidValueException(
        "VisionInferenceNode: Missing 'model_name' parameter.");
  }
}

void VisionInferenceNode::process(const PortDataMap &inputs,
                                  PortDataMap &outputs,
                                  std::shared_ptr<PipelineContext> context) {
  const std::string inputPortName = getExpectedInputPorts()[0];
  const std::string ouputPortName = getExpectedOutputPorts()[0];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "VisionInferenceNode: Missing '" << inputPortName
               << "' input.";
    throw InvalidValueException("VisionInferenceNode: Missing '" +
                                inputPortName + "' input.");
  }

  if (!context->isValid()) {
    LOG_ERRORS << "VisionInferenceNode: Pipeline context is invalid.";
    throw InvalidValueException(
        "VisionInferenceNode: Pipeline context is invalid.");
  }

  const auto &algoManager = context->getAlgoManager();
  if (!algoManager) {
    LOG_ERRORS
        << "VisionInferenceNode: AlgoManager is not set in pipeline context.";
    throw InvalidValueException(
        "VisionInferenceNode: AlgoManager is not set in pipeline context.");
  }

  if (!algoManager->hasAlgo(params_.modelName)) {
    LOG_ERRORS << "VisionInferenceNode: Model '" << params_.modelName
               << "' not registered with AlgoManager.";
    throw InvalidValueException("VisionInferenceNode: Model '" +
                                params_.modelName +
                                "' not registered with AlgoManager.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);
  if (!inputDataPacket->has<ImageFramePtr>("image_data")) {
    LOG_ERRORS << "VisionInferenceNode: '" << inputPortName
               << "' input is not of type ImageFrame.";
    throw InvalidValueException("VisionInferenceNode: '" + inputPortName +
                                "' input is not of type ImageFrame.");
  }

  const ImageFramePtr &imageData =
      inputDataPacket->getParam<ImageFramePtr>("image_data");

  // make input data
  // TODO: maybe a dedicated node can be set up later to complete this step
  ai_core::AlgoPreprocParams preprocParams;
  ai_core::FramePreprocessArg framePreprocArgs;
  framePreprocArgs.roi = {0, 0, imageData->data.cols, imageData->data.rows};
  framePreprocArgs.originShape = {imageData->data.cols, imageData->data.rows,
                                  imageData->data.channels()};
  framePreprocArgs.modelInputShape = {640, 640, 3};
  framePreprocArgs.isEqualScale = true;
  framePreprocArgs.needResize = true;
  framePreprocArgs.pad = {0, 0, 0};
  framePreprocArgs.meanVals = {0, 0, 0};
  framePreprocArgs.normVals = {255.f, 255.f, 255.f};
  framePreprocArgs.dataType = ai_core::DataType::FLOAT16;
  framePreprocArgs.hwc2chw = true;
  framePreprocArgs.inputName = "images";
  preprocParams.setParams(framePreprocArgs);

  ai_core::AlgoInput algoInput;
  ai_core::FrameInput frameInput;
  frameInput.image = imageData->data;
  algoInput.setParams(frameInput);

  ai_core::AlgoPostprocParams postprocParams;
  ai_core::AnchorDetParams anchorDetParams;
  anchorDetParams.condThre = 0.35;
  anchorDetParams.nmsThre = 0.5;
  anchorDetParams.outputNames = {"output0"};
  postprocParams.setParams(anchorDetParams);

  ai_core::AlgoOutput algoOutput;

  ai_core::InferErrorCode inferRet = algoManager->infer(
      params_.modelName, algoInput, preprocParams, algoOutput, postprocParams);
  if (inferRet != ai_core::InferErrorCode::SUCCESS) {
    LOG_ERRORS << "VisionInferenceNode: Inference failed for model '"
               << params_.modelName << "'. Error: " << (int)inferRet;
    throw ExecutionException(
        "VisionInferenceNode: Inference failed for model '" +
        params_.modelName + "'.");
  }

  auto inference_result_data_packet = std::make_shared<PortData>();
  inference_result_data_packet->setParam<ai_core::AlgoOutput>("infer_result",
                                                              algoOutput);
  outputs[ouputPortName] = inference_result_data_packet;
}

std::vector<std::string> VisionInferenceNode::getExpectedInputPorts() const {
  return {"raw_image_input"};
}

std::vector<std::string> VisionInferenceNode::getExpectedOutputPorts() const {
  return {"inference_output_result"};
}
} // namespace ai_pipe
