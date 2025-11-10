/**
 * @file through_pass_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-08-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_EXAMPLES_THROUGH_PASS_NODE_HPP
#define AI_PIPE_EXAMPLES_THROUGH_PASS_NODE_HPP

#include "ai_pipe/node_base.hpp"
#include "ai_pipe/types.hpp"
#include <nlohmann/json.hpp>
#include <string>

namespace ai_pipe::examples {

struct ThroughPassNodeParams {
  std::string name;
};

void from_json(const nlohmann::json &j, ThroughPassNodeParams &p);

class ThroughPassNode : public ILogicNode {
public:
  ThroughPassNode(const std::string &name, const ThroughPassNodeParams &params);
  ~ThroughPassNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  ThroughPassNodeParams m_params;
};
} // namespace ai_pipe::examples

#endif // __IMAGE_READER_NODE_HPP__
