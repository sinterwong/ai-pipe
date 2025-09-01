/**
 * @file pipeline_impl.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-07-04
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __PIPE_PIPELINE_IMPL_HPP__
#define __PIPE_PIPELINE_IMPL_HPP__

#include "execution_engine.hpp"
#include "pipeline.hpp"

namespace ai_pipe {
class Pipeline::Impl {
public:
  Impl();
  ~Impl();

  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;

  Impl(Impl &&other) noexcept;
  Impl &operator=(Impl &&other) noexcept;

  bool initialize(Graph &&graph, std::shared_ptr<PipelineContext> context,
                  uint8_t numWorkers = 1);

  bool start();

  bool stop();

  void reset();

  bool feedDataAsync(const PortDataMap &initialInputs);

  std::future<bool>
  feedDataAndGetResultFuture(const PortDataMap &initialInputs);

  PipelineState getState() const;

  std::unordered_map<std::string, NodeExecutionState> getNodeStates() const;

  void setPipelineResultCallback(
      std::function<void(const PortDataMap &finalResults)> callback);

  void setPipelineErrorCallback(std::function<void(const std::string &errorMsg,
                                                   const std::string &nodeName)>
                                    callback);

  const Graph &getGraph() const { return *mGraph; }

  PipelineContext &getContext() { return *mContext; }

private:
  std::unique_ptr<Graph> mGraph;
  std::unique_ptr<ExecutionEngine> mExecutionEngine;
  std::atomic<PipelineState> mState;

  std::shared_ptr<PipelineContext> mContext;

  std::function<void(const std::string &errorMsg, const std::string &nodeName)>
      mOnPipelineError;
  std::function<void(const PortDataMap &finalResults)> mOnPipelineResult;
};
} // namespace ai_pipe

#endif
