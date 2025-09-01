/**
 * @file pipeline.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __PIPE_PIPELINE_HPP__
#define __PIPE_PIPELINE_HPP__
#include "ai_pipe/config.hpp"
#include "ai_pipe/context.hpp"
#include "ai_pipe/graph.hpp"
#include <functional>
#include <future>

namespace ai_pipe {
class Pipeline {
public:
  Pipeline();
  ~Pipeline();

  Pipeline(const Pipeline &) = delete;
  Pipeline &operator=(const Pipeline &) = delete;

  Pipeline(Pipeline &&other) noexcept;
  Pipeline &operator=(Pipeline &&other) noexcept;

  bool initialize(const std::string &graphConfigPath,
                  std::shared_ptr<PipelineContext> context,
                  uint8_t numWorkers = 1);

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

  const Graph &getGraph() const;

  PipelineContext &getContext();

private:
  class Impl;
  std::unique_ptr<Impl> pImpl_;
};
} // namespace ai_pipe

#endif