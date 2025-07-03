/**
 * @file pipeline_context.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "pipeline_context.hpp"

#include <ai_core/algo_manager.hpp>
namespace ai_pipe {

void PipelineContext::setAlgoManager(
    std::shared_ptr<ai_core::dnn::AlgoManager> manager) {
  algoManager_ = std::move(manager);
}

std::shared_ptr<ai_core::dnn::AlgoManager>
PipelineContext::getAlgoManager() const {
  return algoManager_;
}

bool PipelineContext::isValid() const { return algoManager_ != nullptr; }

} // namespace ai_pipe
