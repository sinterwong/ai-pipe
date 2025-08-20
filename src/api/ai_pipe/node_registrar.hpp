/**
 * @file node_registrar.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-08-18
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __NODE_REGISTRAR_HPP__
#define __NODE_REGISTRAR_HPP__

#include "ai_pipe/node_base.hpp"
#include "node_param_types.hpp"
#include "type_safe_factory.hpp"

namespace ai_pipe {
namespace common_utils {
template <class BaseClass> class Factory;
}

using NodeParamParserFactory = common_utils::Factory<ParamParserBase>;
using NodeCreatorFactory = common_utils::Factory<NodeBase>;

#define AI_PIPE_REGISTER_NODE(NodeType, NodeParamType)                         \
  [[maybe_unused]]                                                             \
  const auto ___##NodeType##Temp__ =                                           \
      NodeCreatorFactory::instance().registerCreator(                          \
          #NodeType,                                                           \
          [](const ai_pipe::NodeConstructParams &params)                       \
              -> std::shared_ptr<ai_pipe::NodeBase> {                          \
            auto nodeName = params.getParam<std::string>("name");              \
            auto nodeSpecificParams =                                          \
                params.getParam<NodeParamType>("node_specific_params");        \
            return std::make_shared<NodeType>(nodeName, nodeSpecificParams);   \
          });                                                                  \
  [[maybe_unused]]                                                             \
  const auto ___##NodeType##ParamParserTemp__ =                                \
      NodeParamParserFactory::instance().registerCreator(                      \
          #NodeType,                                                           \
          [](const ai_pipe::NodeConstructParams &)                             \
              -> std::shared_ptr<ai_pipe::ParamParserBase> {                   \
            return std::make_shared<ai_pipe::ParamParser<NodeParamType>>();    \
          });

} // namespace ai_pipe

#endif // __AI_PIPE_PLUGIN_REGISTRATION_HPP__
