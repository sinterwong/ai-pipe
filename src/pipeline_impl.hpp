/**
 * @file pipeline_impl.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Pipeline PIMPL implementation (internal)
 * @version 2.0
 * @date 2025-12-24
 *
 * This is an INTERNAL header file. Users should not include this directly.
 *
 * v2.0: Unified error handling with Result<T>.
 *       - initialize() returns Result<void> instead of bool
 *       - run() returns Result<ExecutionOutput> instead of ExecutionResult
 *       - submit()/start() return Result<void> instead of bool
 *       - pushInput() returns Result<PushStatus> instead of QueuePushResult
 *       - waitForDrain() returns Result<void> instead of bool
 *       - Observer notifications use Error type
 *       - PipelineBuilder::build() returns Result<Pipeline> (never throws)
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
#include <optional>
#include <vector>

namespace ai_pipe {

class Pipeline::Impl {
public:
  Impl();
  ~Impl();

  // Neither copyable nor movable: the engine callbacks installed by
  // setupEngineCallbacks() capture `this`, so the Impl address must stay
  // stable for its lifetime. Pipeline's move semantics come from moving
  // the unique_ptr<Impl>, which never relocates the Impl object itself
  // (same rule as ExecutionEngine::Impl).
  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;
  Impl(Impl &&) = delete;
  Impl &operator=(Impl &&) = delete;

  // ---- Initialization ----

  Result<void> initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          const PipelineOptions &options,
                          std::unique_ptr<ISchedulerStrategy> scheduler,
                          std::unique_ptr<ISyncStrategy> sync);

  // ---- Batch execution ----

  Result<ExecutionOutput> run(const PortDataMap &inputs,
                              std::optional<std::chrono::milliseconds> timeout);
  std::future<Result<ExecutionOutput>> runAsync(const PortDataMap &inputs);
  Result<void> submit(const PortDataMap &inputs);

  // ---- Streaming interface ----

  Result<void> start(std::shared_ptr<PipelineContext> context);
  void stop(bool wait_for_drain);

  Result<PushStatus> pushInput(const std::string &source_node,
                               const std::string &port_name, PortDataPtr data);

  [[nodiscard]] bool isStreaming() const;
  [[nodiscard]] std::size_t queueDepth(const std::string &node_name) const;
  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name) const;
  Result<void> waitForDrain(std::size_t max_depth,
                            std::chrono::milliseconds timeout);

  // ---- Control ----

  void cancel();
  void wait();
  void reset();

  // ---- Status ----

  [[nodiscard]] bool isReady() const;
  [[nodiscard]] bool isRunning() const;
  [[nodiscard]] bool hasError() const;
  [[nodiscard]] PipelineState state() const;
  [[nodiscard]] EngineState engineState() const;
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  nodeStates() const;
  [[nodiscard]] ExecutionMode mode() const;
  [[nodiscard]] EngineStatisticsSnapshot statistics() const;

  Result<void> setTraceSink(std::shared_ptr<ITraceSink> sink);

  // ---- Accessors ----

  [[nodiscard]] const Graph &graph() const;
  [[nodiscard]] PipelineContext &context();
  [[nodiscard]] const PipelineContext &context() const;
  [[nodiscard]] std::string info() const;

  // ---- Observer management ----

  void addObserver(std::shared_ptr<IPipelineObserver> observer);
  void removeObserver(const std::shared_ptr<IPipelineObserver> &observer);

private:
  /**
   * @brief Validate pipeline state for an operation
   * @return Result<void> - ok if state is valid, Error with context otherwise
   */
  [[nodiscard]] Result<void> validateState(const char *operation) const;

  void transitionTo(PipelineState new_state);

  [[nodiscard]] EngineConfig buildEngineConfig() const;
  void setupEngineCallbacks();

  // Observer notifications (now using Error type for failures)
  void notifyExecutionStarted();
  void notifyExecutionCompleted(const PortDataMap &results);
  void notifyExecutionFailed(const Error &error);
  void notifyFrameDropped(const std::string &node_name, std::uint64_t frame_id,
                          const std::string &reason);

private:
  std::unique_ptr<Graph> m_graph;
  std::unique_ptr<ExecutionEngine> m_engine;
  std::shared_ptr<PipelineContext> m_context;

  PipelineOptions m_options;

  std::unique_ptr<ISchedulerStrategy> m_customScheduler;
  std::unique_ptr<ISyncStrategy> m_customSync;

  // Deliberately self-maintained rather than derived from EngineState:
  // run()/submit() claim RUNNING synchronously at submission, before the
  // engine transitions, so a concurrent second submission fails
  // AlreadyRunning instead of slipping through the engine's IDLE window.
  // The cost of the duplicate is drift, handled at the callback boundary:
  // per-frame streaming errors must not latch ERROR here (see
  // setupEngineCallbacks).
  std::atomic<PipelineState> m_state{PipelineState::UNINITIALIZED};
  mutable std::mutex m_stateMutex;

  std::vector<std::shared_ptr<IPipelineObserver>> m_observers;
  mutable std::mutex m_observersMutex;

  // Error state: stores the last error from async execution
  PortDataMap m_lastResults;
  std::optional<Error> m_lastError;
  mutable std::mutex m_resultsMutex;

  std::chrono::steady_clock::time_point m_executionStart;
};

} // namespace ai_pipe

#endif // AI_PIPE_INTERNAL_PIPELINE_IMPL_HPP
