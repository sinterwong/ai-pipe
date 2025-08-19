/**
 * @file vision_inference_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-22
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __INFERENCE_NODE_HPP__
#define __INFERENCE_NODE_HPP__

#include "ai_pipe/node_base.hpp"
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {
struct VisionInferenceNodeParams {
  std::string modelName;
};

void from_json(const nlohmann::json &j, VisionInferenceNodeParams &p);

class VisionInferenceNode : public NodeBase {
public:
  VisionInferenceNode(const std::string &name,
                      const VisionInferenceNodeParams &params);
  ~VisionInferenceNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  VisionInferenceNodeParams mParams;
};

} // namespace ai_pipe::examples

#endif // __INFERENCE_NODE_HPP__
