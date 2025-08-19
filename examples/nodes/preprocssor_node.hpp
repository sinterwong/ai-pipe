/**
 * @file preprocssor_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef __PREPROCESSOR_NODE_HPP__
#define __PREPROCESSOR_NODE_HPP__

#include "ai_pipe/node_base.hpp"
#include <ai_core/algo_preproc.hpp>
#include <ai_core/preproc_types.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {

struct PreprocessorNodeParams {
  std::string moduleName;
  ai_core::AlgoPreprocParams preprocParams;
};

void from_json(const nlohmann::json &j, PreprocessorNodeParams &p);

class PreprocessorNode : public NodeBase {
public:
  PreprocessorNode(const std::string &name,
                   const PreprocessorNodeParams &params);
  ~PreprocessorNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  std::unique_ptr<ai_core::dnn::AlgoPreproc> mPreprocessor;
  PreprocessorNodeParams mParams;
};
} // namespace ai_pipe::examples

#endif