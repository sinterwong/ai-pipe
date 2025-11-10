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

#include "execution_engine_base.hpp"
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
                  const PipelineConfig &config);

  bool start();

  bool stop();

  void reset();

  bool feedDataAsync(const PortDataMap &initialInputs);

  std::future<bool>
  feedDataAndGetResultFuture(const PortDataMap &initialInputs);

  PipelineState getState() const;

  EngineState getEngineState() const;

  std::unordered_map<std::string, NodeExecutionState> getNodeStates() const;

  void setPipelineResultCallback(
      std::function<void(const PortDataMap &finalResults)> callback);

  void setPipelineErrorCallback(std::function<void(const std::string &errorMsg,
                                                   const std::string &nodeName)>
                                    callback);

  const Graph &getGraph() const { return *m_graph; }

  PipelineContext &getContext() { return *m_context; }

private:
  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<IExecutionEngine> m_executionEngine;
  std::atomic<PipelineState> m_state;

  std::shared_ptr<PipelineContext> m_context;

  std::function<void(const std::string &errorMsg, const std::string &nodeName)>
      m_onPipelineError;
  std::function<void(const PortDataMap &finalResults)> m_onPipelineResult;
};
} // namespace ai_pipe

#endif
