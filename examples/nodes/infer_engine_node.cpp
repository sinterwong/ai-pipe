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
#include "ai_pipe/exception.hpp"
#include "ai_pipe/node_registrar.hpp"
#include <logger.hpp>

namespace ai_core {
void from_json(const nlohmann::json &j, AlgoInferParams &p) {
  if (j.contains("name")) {
    j.at("name").get_to(p.name);
  } else {
    throw std::runtime_error("Missing 'name' in AlgoInferParams JSON");
  }
  if (j.contains("model_path")) {
    j.at("model_path").get_to(p.modelPath);
  } else {
    throw std::runtime_error("Missing 'model_path' in AlgoInferParams JSON");
  }
  if (j.contains("need_decrypt")) {
    j.at("need_decrypt").get_to(p.needDecrypt);
  } else {
    throw std::runtime_error("Missing 'need_decrypt' in AlgoInferParams JSON");
  }
  if (j.contains("device_type")) {
    p.deviceType = static_cast<DeviceType>(j.at("device_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'device_type' in AlgoInferParams JSON");
  }
  if (j.contains("data_type")) {
    p.dataType = static_cast<DataType>(j.at("data_type").get<int>());
  } else {
    throw std::runtime_error("Missing 'data_type' in AlgoInferParams JSON");
  }
}
} // namespace ai_core

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, InferEngineNodeParams &p) {
  if (j.contains("module_name")) {
    j.at("module_name").get_to(p.moduleName);
  } else {
    throw std::runtime_error(
        "Missing 'module_name' in InferEngineNodeParams JSON");
  }

  if (j.contains("infer_params")) {
    j.at("infer_params").get_to(p.inferParams);
  } else {
    throw std::runtime_error(
        "Missing 'infer_params' in InferEngineNodeParams JSON");
  }
}

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

AI_PIPE_REGISTER_NODE(InferEngineNode, InferEngineNodeParams);
} // namespace ai_pipe::examples