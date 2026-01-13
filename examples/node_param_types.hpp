/**
 * @file param_parser.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-08-19
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_PARAM_PARSER_HPP
#define AI_PIPE_PARAM_PARSER_HPP

#include "ai_pipe/data_packet.hpp"
#include <nlohmann/json.hpp>

namespace ai_pipe {

using NodeConstructParams = common_utils::DataPacket;

class ParamParserBase {
public:
  virtual ~ParamParserBase() = default;
  virtual void parse(const nlohmann::json &nodeConfig,
                     NodeConstructParams &creationParams,
                     const std::string &name,
                     const std::string &type) const = 0;
};

template <typename ParamsType>
class ParamParser final : public ParamParserBase {
public:
  void parse(const nlohmann::json &nodeConfig,
             NodeConstructParams &creationParams, const std::string &name,
             const std::string &type) const override {
    if (nodeConfig.contains("params")) {
      try {
        ParamsType specificParams = nodeConfig.at("params").get<ParamsType>();
        creationParams.setParam("node_specific_params", specificParams);
      } catch (const nlohmann::json::exception &e) {
        throw std::runtime_error("Error parsing 'params' for node '" + name +
                                 "' (type: " + type + "): " + e.what());
      }
    } else {
      // Check if the parameter type is default-constructible
      if constexpr (std::is_default_constructible_v<ParamsType>) {
        creationParams.setParam("node_specific_params", ParamsType{});
      } else {
        throw std::runtime_error("Missing 'params' block for node " + name +
                                 " of type " + type);
      }
    }
  }
};
} // namespace ai_pipe

#endif