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

#include "ai_pipe/node_base.hpp"
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {

struct ResultSaverNodeParams {
  std::string outputDir;
};

void from_json(const nlohmann::json &j, ResultSaverNodeParams &p);

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
  ResultSaverNodeParams mParams;
};

} // namespace ai_pipe::examples

#endif // __RESULT_SAVER_NODE_HPP__
