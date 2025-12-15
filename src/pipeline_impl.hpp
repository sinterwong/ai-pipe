/**
 * @file pipeline_impl.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Pipeline implementation details (internal)
 * @version 0.3
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_PIPELINE_IMPL_HPP
#define AI_PIPE_PIPELINE_IMPL_HPP

#include "execution_engine_base.hpp"
#include "pipeline.hpp"
#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>

namespace ai_pipe {

/**
 * @brief Pipeline implementation using PIMPL idiom
 */
class Pipeline::Impl {
public:
  Impl();
  ~Impl();

  // Non-copyable
  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;

  // Movable
  Impl(Impl &&other) noexcept;
  Impl &operator=(Impl &&other) noexcept;

  // -------------------------------------------------------------------------
  // Initialization
  // -------------------------------------------------------------------------

  bool initialize(Graph &&graph, std::shared_ptr<PipelineContext> context,
                  const PipelineOptions &options);

  // -------------------------------------------------------------------------
  // Execution
  // -------------------------------------------------------------------------

  ExecutionResult run(const PortDataMap &inputs,
                      std::optional<std::chrono::milliseconds> timeout);

  std::future<ExecutionResult> runAsync(const PortDataMap &inputs);

  bool submit(const PortDataMap &inputs);

  // -------------------------------------------------------------------------
  // Control
  // -------------------------------------------------------------------------

  void cancel();
  void wait();
  void reset();

  // -------------------------------------------------------------------------
  // Status
  // -------------------------------------------------------------------------

  [[nodiscard]] bool isReady() const;
  [[nodiscard]] bool isRunning() const;
  [[nodiscard]] bool hasError() const;
  [[nodiscard]] PipelineState state() const;
  [[nodiscard]] EngineState engineState() const;
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  nodeStates() const;

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  [[nodiscard]] const Graph &graph() const;
  [[nodiscard]] PipelineContext &context();
  [[nodiscard]] const PipelineContext &context() const;

  // -------------------------------------------------------------------------
  // Observer Management
  // -------------------------------------------------------------------------

  void addObserver(std::shared_ptr<IPipelineObserver> observer);
  void removeObserver(const std::shared_ptr<IPipelineObserver> &observer);

private:
  // -------------------------------------------------------------------------
  // Internal Helpers
  // -------------------------------------------------------------------------

  void setupEngineCallbacks();
  void notifyExecutionStarted();
  void notifyExecutionCompleted(const PortDataMap &results);
  void notifyExecutionFailed(const std::string &error,
                             const std::string &node_name);

  bool validateState(const char *operation) const;
  void transitionTo(PipelineState new_state);

  ExecutionResult createSuccessResult(const PortDataMap &outputs,
                                      std::chrono::milliseconds elapsed) const;
  ExecutionResult createErrorResult(const std::string &message) const;

private:
  // Core components
  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<IExecutionEngine> m_engine;
  std::shared_ptr<PipelineContext> m_context;
  PipelineOptions m_options;

  // State
  std::atomic<PipelineState> m_state{PipelineState::UNINITIALIZED};
  mutable std::mutex m_stateMutex;

  // Observers
  std::vector<std::shared_ptr<IPipelineObserver>> m_observers;
  mutable std::mutex m_observersMutex;

  // Execution tracking
  std::chrono::steady_clock::time_point m_executionStart;

  // Result storage for async execution
  PortDataMap m_lastResults;
  std::string m_lastError;
  std::string m_lastErrorNode;
  std::mutex m_resultsMutex;
};

} // namespace ai_pipe

#endif // AI_PIPE_PIPELINE_IMPL_HPP