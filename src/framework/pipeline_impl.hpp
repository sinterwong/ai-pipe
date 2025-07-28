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

  bool initialize(const PipelineConfig &config,
                  std::shared_ptr<PipelineContext> context_);

  bool initializeWithGraph(Graph &&graph,
                           std::shared_ptr<PipelineContext> context,
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

  const Graph &getGraph() const { return *graph_; }

  PipelineContext &getContext() { return *context_; }

private:
  Graph buildGraphFromConfig(const std::string &configPath);

private:
  std::unique_ptr<Graph> graph_;
  std::unique_ptr<ExecutionEngine> executionEngine_;
  std::atomic<PipelineState> state_;

  std::shared_ptr<PipelineContext> context_;

  std::function<void(const std::string &errorMsg, const std::string &nodeName)>
      onPipelineError_;
  std::function<void(const PortDataMap &finalResults)> onPipelineResult_;
};
} // namespace ai_pipe

#endif
