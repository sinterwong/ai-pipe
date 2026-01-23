/**
 * @file execution_engine.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Execution Engine with Strategy-Based Architecture
 * @version 1.0
 * @date 2025-12-24
 *
 * This file provides the execution engine interface using PIMPL pattern
 * to hide implementation details from users.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_EXECUTION_ENGINE_HPP
#define AI_PIPE_EXECUTION_ENGINE_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/enum.hpp"
#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_scheduler_strategy.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

namespace ai_pipe {

/**
 * @brief Execution engine with strategy-based architecture
 *
 * This is the single execution engine class that supports all execution modes
 * through pluggable strategies:
 * - ISchedulerStrategy: Controls node scheduling behavior
 * - ISyncStrategy: Controls frame synchronization across branches
 *
 * Users should not subclass this engine. Instead, they should implement
 * custom strategies to achieve desired behaviors.
 */
class ExecutionEngine {
public:
  using SchedulerStrategyPtr = std::unique_ptr<ISchedulerStrategy>;
  using SyncStrategyPtr = std::unique_ptr<ISyncStrategy>;

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  static std::unique_ptr<ExecutionEngine>
  create(const EngineConfig &config = {});

  ExecutionEngine();
  explicit ExecutionEngine(const EngineConfig &config);
  ~ExecutionEngine();

  // Non-copyable
  ExecutionEngine(const ExecutionEngine &) = delete;
  ExecutionEngine &operator=(const ExecutionEngine &) = delete;

  // Movable
  ExecutionEngine(ExecutionEngine &&) noexcept;
  ExecutionEngine &operator=(ExecutionEngine &&) noexcept;

  // -------------------------------------------------------------------------
  // Strategy Injection
  // -------------------------------------------------------------------------

  void setSchedulerStrategy(SchedulerStrategyPtr strategy);
  void setSyncStrategy(SyncStrategyPtr strategy);
  void configureForMode(ExecutionMode mode);

  // -------------------------------------------------------------------------
  // Core Interface
  // -------------------------------------------------------------------------

  bool initialize(Graph *graph, std::uint8_t num_workers = 0);

  bool execute(const PortDataMap &initial_inputs,
               bool wait_for_completion = true,
               std::shared_ptr<PipelineContext> context = nullptr);

  void stopExecutionAsync();
  void stopExecutionSync();
  void reset();

  [[nodiscard]] EngineState getState() const;
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  getNodeStates() const;

  // -------------------------------------------------------------------------
  // Callback Registration
  // -------------------------------------------------------------------------

  void
  setPipelineResultCallback(std::function<void(const PortDataMap &)> callback);

  void setPipelineErrorCallback(
      std::function<void(const std::string &, const std::string &)> callback);

  void setDropCallback(
      std::function<void(const std::string &node, std::uint64_t frame_id,
                         const std::string &reason)>
          callback);

  // -------------------------------------------------------------------------
  // Streaming Interface
  // -------------------------------------------------------------------------

  bool startStreaming(std::shared_ptr<PipelineContext> context = nullptr);
  void stopStreaming(bool wait_for_drain = true);
  [[nodiscard]] bool isStreaming() const;

  [[nodiscard]] QueuePushResult pushInput(const std::string &source_node,
                                          const std::string &port_name,
                                          PortDataPtr data);

  [[nodiscard]] QueuePushResult pushInput(const std::string &source_node,
                                          PortDataPtr data);

  // -------------------------------------------------------------------------
  // State & Monitoring
  // -------------------------------------------------------------------------

  [[nodiscard]] EngineStatisticsSnapshot statistics() const;

  [[nodiscard]] std::size_t queueDepth(const std::string &node_name,
                                       const std::string &port_name = "") const;

  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name,
                                      const std::string &port_name = "") const;

  bool waitForDrain(
      std::size_t max_depth = 0,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

  // -------------------------------------------------------------------------
  // Configuration
  // -------------------------------------------------------------------------

  void setNodeQueueConfig(const std::string &node_name,
                          const QueueConfig &config);

  [[nodiscard]] const EngineConfig &config() const;

  // -------------------------------------------------------------------------
  // Information
  // -------------------------------------------------------------------------

  [[nodiscard]] std::string info() const;
  [[nodiscard]] std::string strategyInfo() const;

private:
  // PIMPL idiom to hide implementation details
  class Impl;
  std::unique_ptr<Impl> m_impl;
};

// =============================================================================
// Convenience Factory Functions
// =============================================================================

inline std::unique_ptr<ExecutionEngine>
createBatchEngine(std::uint8_t workers = 4) {
  return ExecutionEngine::create(EngineConfig::batch(workers));
}

inline std::unique_ptr<ExecutionEngine>
createStreamEngine(std::uint8_t workers = 4, std::size_t queue_capacity = 16) {
  return ExecutionEngine::create(EngineConfig::stream(workers, queue_capacity));
}

inline std::unique_ptr<ExecutionEngine>
createHybridEngine(std::uint8_t workers = 4, std::size_t queue_capacity = 16) {
  return ExecutionEngine::create(EngineConfig::hybrid(workers, queue_capacity));
}

} // namespace ai_pipe

#endif // AI_PIPE_EXECUTION_ENGINE_HPP
