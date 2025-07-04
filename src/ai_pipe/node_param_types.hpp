/**
 * @file node_param_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-07
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __AI_NODE_PARAM_TYPES_HPP__
#define __AI_NODE_PARAM_TYPES_HPP__

#include <variant>

#include "logic_types/pipe_common_types.hpp"

#include <ai_core/algo_data_types.hpp>
#include <data_packet.hpp>
#include <logger.hpp>
#include <nlohmann/json.hpp>
#include <param_center.hpp>

namespace ai_pipe {
struct DemoSourceNodeParams {
  int source_id;
};

struct DemoProcessingNodeParams {
  float processing_threshold;
};

struct DemoSinkNodeParams {
  std::string output_path;
};

struct ImageReaderNodeParams {
  ColorType colorType;
};

struct VisionInferenceNodeParams {
  std::string modelName;
};

struct ResultSaverNodeParams {
  std::string outputDir;
};

struct VisualizationNodeParams {
  std::string outputDir;
};

using NodeParams = common_utils::ParamCenter<
    std::variant<std::monostate, DemoSourceNodeParams, DemoProcessingNodeParams,
                 DemoSinkNodeParams>>;

using NodeConstructParams = common_utils::DataPacket;

void from_json(const nlohmann::json &j, DemoSourceNodeParams &p);

void from_json(const nlohmann::json &j, DemoProcessingNodeParams &p);

void from_json(const nlohmann::json &j, DemoSinkNodeParams &p);

void from_json(const nlohmann::json &j, ImageReaderNodeParams &p);

void from_json(const nlohmann::json &j, VisionInferenceNodeParams &p);

void from_json(const nlohmann::json &j, ResultSaverNodeParams &p);

void from_json(const nlohmann::json &j, VisualizationNodeParams &p);

template <typename ParamsType>
void handleNodeParams(const nlohmann::json &nodeConfig,
                      NodeConstructParams &creationParams,
                      const std::string &name, const std::string &type) {
  ParamsType specificParams;
  if (nodeConfig.contains("params")) {
    nodeConfig.at("params").get_to(specificParams);
  } else {
    LOG_ERRORS << "Node " << name << " of type " << type
               << " has no 'params' block. Using default parameters.";
    throw std::runtime_error("Missing 'params' block for node " + name +
                             " of type " + type);
  }
  creationParams.setParam("node_specific_params", specificParams);
}

using NodeParamHandler = void (*)(const nlohmann::json &, NodeConstructParams &,
                                  const std::string &, const std::string &);

// convert logic to data to simplify code
static const std::unordered_map<std::string, NodeParamHandler> s_paramHandlers =
    {{"DemoSourceNode", &handleNodeParams<DemoSourceNodeParams>},
     {"DemoProcessingNode", &handleNodeParams<DemoProcessingNodeParams>},
     {"DemoSinkNode", &handleNodeParams<DemoSinkNodeParams>},
     {"ImageReaderNode", &handleNodeParams<ImageReaderNodeParams>},
     {"VisionInferenceNode", &handleNodeParams<VisionInferenceNodeParams>},
     {"ResultSaverNode", &handleNodeParams<ResultSaverNodeParams>},
     {"VisualizationNode", &handleNodeParams<VisualizationNodeParams>}};

} // namespace ai_pipe

#endif
