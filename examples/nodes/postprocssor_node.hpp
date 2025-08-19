/**
 * @file postprocssor_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef __POSTPROCESSOR_NODE_HPP__
#define __POSTPROCESSOR_NODE_HPP__

#include "ai_core/algo_data_types.hpp"
#include "ai_pipe/node_base.hpp"
#include "ai_pipe/types.hpp"
#include <ai_core/algo_postproc.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {

struct PostprocessorNodeParams {
  std::string moduleName;
  ai_core::AlgoPostprocParams postprocParams;
};

void from_json(const nlohmann::json &j, PostprocessorNodeParams &p);

class PostprocessorNode : public NodeBase {
public:
  PostprocessorNode(const std::string &name,
                    const PostprocessorNodeParams &params);
  ~PostprocessorNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  std::unique_ptr<ai_core::dnn::AlgoPostproc> mPostprocessor;
  PostprocessorNodeParams mParams;
};
} // namespace ai_pipe::examples
#endif