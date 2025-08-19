#include "vision_inference_node.hpp"
#include "ai_pipe/exception.hpp"
#include "ai_pipe/node_registrar.hpp"
#include "data_types.hpp"
#include <ai_core/algo_manager.hpp>
#include <logger.hpp>
#include <nlohmann/json.hpp>
#include <vector>

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, VisionInferenceNodeParams &p) {
  if (j.contains("model_name")) {
    j.at("model_name").get_to(p.modelName);
  } else {
    throw std::runtime_error(
        "Missing 'model_name' in VisionInferenceNodeParams JSON");
  }
}

VisionInferenceNode::VisionInferenceNode(
    const std::string &name, const VisionInferenceNodeParams &params)
    : NodeBase(name), mParams(params) {

  if (mParams.modelName.empty()) {
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

  const auto &algoManager =
      context->getResource<ai_core::dnn::AlgoManager>("algo_manager");
  if (!algoManager) {
    LOG_ERRORS
        << "VisionInferenceNode: AlgoManager is not set in pipeline context.";
    throw InvalidValueException(
        "VisionInferenceNode: AlgoManager is not set in pipeline context.");
  }

  if (!algoManager->hasAlgo(mParams.modelName)) {
    LOG_ERRORS << "VisionInferenceNode: Model '" << mParams.modelName
               << "' not registered with AlgoManager.";
    throw InvalidValueException("VisionInferenceNode: Model '" +
                                mParams.modelName +
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
  framePreprocArgs.modelInputShape = {640, 640, 3};
  framePreprocArgs.isEqualScale = true;
  framePreprocArgs.needResize = true;
  framePreprocArgs.pad = {0, 0, 0};
  framePreprocArgs.meanVals = {0, 0, 0};
  framePreprocArgs.normVals = {255.f, 255.f, 255.f};
  framePreprocArgs.dataType = ai_core::DataType::FLOAT16;
  framePreprocArgs.hwc2chw = true;
  framePreprocArgs.inputNames = {"images"};
  preprocParams.setParams(framePreprocArgs);

  ai_core::AlgoInput algoInput;
  ai_core::FrameInput frameInput;
  frameInput.image = std::make_shared<cv::Mat>(imageData->data);
  frameInput.inputRoi = std::make_shared<cv::Rect>(0, 0, imageData->data.cols,
                                                   imageData->data.rows);
  algoInput.setParams(frameInput);

  ai_core::AlgoPostprocParams postprocParams;
  ai_core::AnchorDetParams anchorDetParams;
  anchorDetParams.detAlogType =
      ai_core::AnchorDetParams::AnchorDetAlogType::YOLO_DET_V11;
  anchorDetParams.condThre = 0.35;
  anchorDetParams.nmsThre = 0.5;
  anchorDetParams.outputNames = {"output0"};
  postprocParams.setParams(anchorDetParams);

  ai_core::AlgoOutput algoOutput;

  ai_core::InferErrorCode inferRet = algoManager->infer(
      mParams.modelName, algoInput, preprocParams, algoOutput, postprocParams);
  if (inferRet != ai_core::InferErrorCode::SUCCESS) {
    LOG_ERRORS << "VisionInferenceNode: Inference failed for model '"
               << mParams.modelName << "'. Error: " << (int)inferRet;
    throw ExecutionException(
        "VisionInferenceNode: Inference failed for model '" +
        mParams.modelName + "'.");
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

AI_PIPE_REGISTER_NODE(VisionInferenceNode, VisionInferenceNodeParams);
} // namespace ai_pipe::examples
