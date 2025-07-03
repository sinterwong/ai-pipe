/**
 * @file pipeline_context.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __PIPE_PIPELINE_CONTEXT_HPP__
#define __PIPE_PIPELINE_CONTEXT_HPP__

#include <ai_core/algo_manager.hpp>

namespace ai_pipe {
class PipelineContext : public std::enable_shared_from_this<PipelineContext> {
public:
  PipelineContext() = default;
  ~PipelineContext() = default;

  PipelineContext(const PipelineContext &) = delete;
  PipelineContext &operator=(const PipelineContext &) = delete;
  PipelineContext(PipelineContext &&) = default;
  PipelineContext &operator=(PipelineContext &&) = default;

  void setAlgoManager(std::shared_ptr<ai_core::dnn::AlgoManager> manager);

  std::shared_ptr<ai_core::dnn::AlgoManager> getAlgoManager() const;

  bool isValid() const;

private:
  // 所有成员内部线程安全
  std::shared_ptr<ai_core::dnn::AlgoManager> algoManager_;

  // TODO: 后续实现数据生产者和自定义共享资源
  // std::unordered_map<std::string, std::any> customResources_;

  // TODO: 暂时不需要锁，现有资源都是内部线程安全的
  // std::mutex mutex_;
};
} // namespace ai_pipe

#endif
