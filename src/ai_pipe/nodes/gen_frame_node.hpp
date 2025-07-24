/**
 * @file gen_frame_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __GEN_FRAME_NODE_HPP__
#define __GEN_FRAME_NODE_HPP__

#include "node_base.hpp"
#include "node_param_types.hpp"
#include <ai_core/algo_input_types.hpp>
#include <opencv2/opencv.hpp>

namespace ai_pipe {

class GenFrameNode : public NodeBase {
public:
  GenFrameNode(const std::string &name, const GenFrameNodeParams &params);
  ~GenFrameNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  GenFrameNodeParams mParams;
};

} // namespace ai_pipe

#endif // __GEN_FRAME_NODE_HPP__