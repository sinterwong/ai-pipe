/**
 * @file pipeline_impl.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Pipeline PIMPL implementation (internal)
 * @version 1.0
 * @date 2025-12-24
 *
 * This is an INTERNAL header file. Users should not include this directly.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_INTERNAL_PIPELINE_IMPL_HPP
#define AI_PIPE_INTERNAL_PIPELINE_IMPL_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_scheduler_strategy.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include "ai_pipe/pipeline.hpp"

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

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
                  const PipelineOptions &options,
                  std::unique_ptr<ISchedulerStrategy> scheduler,
                  std::unique_ptr<ISyncStrategy> sync);

  ExecutionResult run(const PortDataMap &inputs,
                      std::optional<std::chrono::milliseconds> timeout);
  std::future<ExecutionResult> runAsync(const PortDataMap &inputs);
  bool submit(const PortDataMap &inputs);

  bool start(std::shared_ptr<PipelineContext> context);
  void stop(bool wait_for_drain);

  QueuePushResult pushInput(const std::string &source_node,
                            const std::string &port_name, PortDataPtr data);

  [[nodiscard]] bool isStreaming() const;
  [[nodiscard]] std::size_t queueDepth(const std::string &node_name) const;
  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name) const;
  bool waitForDrain(std::size_t max_depth, std::chrono::milliseconds timeout);

  void cancel();
  void wait();
  void reset();

  [[nodiscard]] bool isReady() const;
  [[nodiscard]] bool isRunning() const;
  [[nodiscard]] bool hasError() const;
  [[nodiscard]] PipelineState state() const;
  [[nodiscard]] EngineState engineState() const;
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  nodeStates() const;
  [[nodiscard]] ExecutionMode mode() const;
  [[nodiscard]] EngineStatisticsSnapshot statistics() const;

  [[nodiscard]] const Graph &graph() const;
  [[nodiscard]] PipelineContext &context();
  [[nodiscard]] const PipelineContext &context() const;
  [[nodiscard]] std::string info() const;

  void addObserver(std::shared_ptr<IPipelineObserver> observer);
  void removeObserver(const std::shared_ptr<IPipelineObserver> &observer);

private:
  [[nodiscard]] bool validateState(const char *operation) const;
  void transitionTo(PipelineState new_state);

  [[nodiscard]] EngineConfig buildEngineConfig() const;
  void setupEngineCallbacks();

  void notifyExecutionStarted();
  void notifyExecutionCompleted(const PortDataMap &results);
  void notifyExecutionFailed(const std::string &error,
                             const std::string &node_name);
  void notifyFrameDropped(const std::string &node_name, std::uint64_t frame_id,
                          const std::string &reason);

  [[nodiscard]] ExecutionResult
  createSuccessResult(const PortDataMap &outputs,
                      std::chrono::milliseconds elapsed) const;
  [[nodiscard]] ExecutionResult
  createErrorResult(const std::string &message) const;

private:
  std::unique_ptr<Graph> m_graph;
  std::unique_ptr<ExecutionEngine> m_engine;
  std::shared_ptr<PipelineContext> m_context;

  PipelineOptions m_options;

  std::unique_ptr<ISchedulerStrategy> m_customScheduler;
  std::unique_ptr<ISyncStrategy> m_customSync;

  std::atomic<PipelineState> m_state{PipelineState::UNINITIALIZED};
  mutable std::mutex m_stateMutex;

  std::vector<std::shared_ptr<IPipelineObserver>> m_observers;
  mutable std::mutex m_observersMutex;

  PortDataMap m_lastResults;
  std::string m_lastError;
  std::string m_lastErrorNode;
  mutable std::mutex m_resultsMutex;

  std::chrono::steady_clock::time_point m_executionStart;
};

} // namespace ai_pipe

#endif // AI_PIPE_INTERNAL_PIPELINE_IMPL_HPP
