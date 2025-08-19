/**
 * @file infer_engine_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-23
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef __INFER_ENGINE_NODE_HPP__
#define __INFER_ENGINE_NODE_HPP__

#include "ai_pipe/node_base.hpp"
#include <ai_core/algo_infer_engine.hpp>
#include <ai_core/infer_params_types.hpp>
#include <nlohmann/json.hpp>

namespace ai_pipe::examples {
struct InferEngineNodeParams {
  std::string moduleName;
  ai_core::AlgoInferParams inferParams;
};

void from_json(const nlohmann::json &j, InferEngineNodeParams &p);

class InferEngineNode : public NodeBase {
public:
  InferEngineNode(const std::string &name, const InferEngineNodeParams &params);
  ~InferEngineNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  std::unique_ptr<ai_core::dnn::AlgoInferEngine> mEngine;
  InferEngineNodeParams mParams;
};
} // namespace ai_pipe::examples

#endif