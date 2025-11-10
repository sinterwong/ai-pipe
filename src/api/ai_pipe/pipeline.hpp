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

  /**
   * @brief Initialize the pipeline with a graph and configuration
   * @param graph The computation graph
   * @param context Pipeline context for shared state
   * @param config Pipeline configuration including engine type and workers
   * @return true if initialization succeeds, false otherwise
   */
  bool initialize(Graph &&graph, std::shared_ptr<PipelineContext> context,
                  const PipelineConfig &config = PipelineConfig());

  /**
   * @brief Initialize the pipeline (legacy interface)
   * @param graph The computation graph
   * @param context Pipeline context for shared state
   * @param numWorkers Number of worker threads (defaults to 1)
   * @return true if initialization succeeds, false otherwise
   * @note This is a legacy interface. Use the PipelineConfig version for new
   * code.
   */
  bool initialize(Graph &&graph, std::shared_ptr<PipelineContext> context,
                  uint8_t numWorkers);

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

  const Graph &getGraph() const;

  PipelineContext &getContext();

private:
  class Impl;
  std::unique_ptr<Impl> m_pImpl;
};
} // namespace ai_pipe

#endif