/**
 * @file result_saver_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-22
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __RESULT_SAVER_NODE_HPP__
#define __RESULT_SAVER_NODE_HPP__

#include "node_base.hpp"
#include "node_param_types.hpp"
#include "pipe_types.hpp"

namespace ai_pipe {

class ResultSaverNode : public NodeBase {
public:
  ResultSaverNode(const std::string &name, const ResultSaverNodeParams &params);
  ~ResultSaverNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

private:
  ResultSaverNodeParams params_;
};

} // namespace ai_pipe

#endif // __RESULT_SAVER_NODE_HPP__
