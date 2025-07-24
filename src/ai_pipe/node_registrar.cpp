

#include "node_registrar.hpp"
#include "node_base.hpp"
#include "node_param_types.hpp"
#include "nodes/gen_frame_node.hpp"
#include "nodes/image_reader_node.hpp"
#include "nodes/infer_engine_node.hpp"
#include "nodes/postprocssor_node.hpp"
#include "nodes/preprocssor_node.hpp"
#include "nodes/result_saver_node.hpp"
#include "nodes/vision_inference_node.hpp"
#include "nodes/visualization_node.hpp"

#include <type_safe_factory.hpp>

namespace ai_pipe {

#define REGISTER_NODE_TYPE(NodeType, NodeParamType)                            \
  NodeFactory::instance().registerCreator(                                     \
      #NodeType,                                                               \
      [](const NodeConstructParams &params) -> std::shared_ptr<NodeBase> {     \
        auto node_name = params.getParam<std::string>("name");                 \
        auto node_specific_params =                                            \
            params.getParam<NodeParamType>("node_specific_params");            \
        return std::make_shared<NodeType>(node_name, node_specific_params);    \
      });                                                                      \
  LOG_INFOS << "Registered " #NodeType " creator.";

NodeRegistrar::NodeRegistrar() {
  REGISTER_NODE_TYPE(ImageReaderNode, ImageReaderNodeParams);
  REGISTER_NODE_TYPE(VisionInferenceNode, VisionInferenceNodeParams);
  REGISTER_NODE_TYPE(ResultSaverNode, ResultSaverNodeParams);
  REGISTER_NODE_TYPE(VisualizationNode, VisualizationNodeParams);
  REGISTER_NODE_TYPE(PreprocessorNode, PreprocessorNodeParams);
  REGISTER_NODE_TYPE(InferEngineNode, InferEngineNodeParams);
  REGISTER_NODE_TYPE(PostprocessorNode, PostprocessorNodeParams);
  REGISTER_NODE_TYPE(GenFrameNode, GenFrameNodeParams);
}

} // namespace ai_pipe
