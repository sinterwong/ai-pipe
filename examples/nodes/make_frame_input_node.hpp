/**
 * @file make_frame_input_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __MAKE_FRAME_INPUT_NODE_HPP__
#define __MAKE_FRAME_INPUT_NODE_HPP__

#include "ai_pipe/node_base.hpp"
#include <nlohmann/json.hpp>
#include <opencv2/opencv.hpp>

namespace ai_pipe::examples {

struct MakeFrameInputNodeParams {};

void from_json(const nlohmann::json &j, MakeFrameInputNodeParams &p);

class MakeFrameInputNode : public NodeBase {
public:
  MakeFrameInputNode(const std::string &name,
                     const MakeFrameInputNodeParams &params);
  ~MakeFrameInputNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  MakeFrameInputNodeParams mParams;
};

} // namespace ai_pipe::examples

#endif
