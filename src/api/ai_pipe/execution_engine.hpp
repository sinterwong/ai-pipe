/**
 * @file execution_engine.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Execution Engine with Strategy-Based Architecture
 * @version 2.0
 * @date 2025-12-24
 *
 * v2.0: Unified error handling with Result<T>.
 *       All fallible operations return Result<void> or Result<PushStatus>
 *       instead of bare bool or ad-hoc types.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_EXECUTION_ENGINE_HPP
#define AI_PIPE_EXECUTION_ENGINE_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/enum.hpp"
#include "ai_pipe/error.hpp"
#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_scheduler_strategy.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace ai_pipe {

class ITraceSink;

/**
 * @brief Execution engine with strategy-based architecture
 *
 * This is the single execution engine class that supports all execution modes
 * through pluggable strategies:
 * - ISchedulerStrategy: Controls node scheduling behavior
 * - ISyncStrategy: Controls frame synchronization across branches
 *
 * All fallible operations return Result<T> for unified error handling:
 * - Result<void> for operations with no return value
 * - Result<PushStatus> for queue push operations
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

  Result<void> setSchedulerStrategy(SchedulerStrategyPtr strategy);
  Result<void> setSyncStrategy(SyncStrategyPtr strategy);
  void configureForMode(ExecutionMode mode);

  /**
   * @brief Install a trace sink receiving per-frame span events (F7)
   *
   * See ai_pipe/trace.hpp. Pass nullptr to disable tracing. Like the
   * strategies, the sink can only change while the engine is idle.
   * @return Result<void> - success or InvalidState while running
   */
  Result<void> setTraceSink(std::shared_ptr<ITraceSink> sink);

  // -------------------------------------------------------------------------
  // Core Interface
  // -------------------------------------------------------------------------

  /**
   * @brief Initialize the engine with a graph and worker count
   * @return Result<void> - success or Error with:
   *   - InvalidArgument: null graph pointer
   *   - AlreadyRunning: engine is currently running
   */
  Result<void> initialize(Graph *graph, std::uint8_t num_workers = 0);

  /**
   * @brief Execute the pipeline with initial inputs
   *
   * When `timeout` is set (and wait_for_completion is true), the wait is
   * bounded: on expiry the engine requests cooperative cancellation on
   * the context's CancellationToken, triggers the stop protocol, and
   * returns ExecutionTimeout immediately. An in-flight node keeps
   * running until its next cancellation point (or until it returns);
   * the engine is left STOPPED with possible queue residue - call
   * reset() before the next execution. `timeout` is ignored in
   * streaming mode and when wait_for_completion is false.
   *
   * @return Result<void> - success or Error with:
   *   - AlreadyRunning: engine already executing
   *   - NotInitialized: engine not initialized
   *   - ExecutionFailed: input distribution or execution failed
   *   - ExecutionStopped: execution was stopped externally
   *   - ExecutionTimeout: bounded wait expired (see above)
   */
  Result<void>
  execute(const PortDataMap &initial_inputs, bool wait_for_completion = true,
          std::shared_ptr<PipelineContext> context = nullptr,
          std::optional<std::chrono::milliseconds> timeout = std::nullopt);

  void stopExecutionAsync();
  void stopExecutionSync();
  void reset();

  [[nodiscard]] EngineState getState() const;
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  getNodeStates() const;

  // -------------------------------------------------------------------------
  // Callback Registration (for async event notifications)
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

  /**
   * @brief Start streaming mode
   * @return Result<void> - success or Error with:
   *   - InvalidState: engine not idle
   *   - StreamingNotSupported: scheduler doesn't support streaming
   */
  Result<void>
  startStreaming(std::shared_ptr<PipelineContext> context = nullptr);
  void stopStreaming(bool wait_for_drain = true);
  [[nodiscard]] bool isStreaming() const;

  /**
   * @brief Push input data into a source node's queue
   * @return Result<PushStatus> - PushStatus on success, or Error with:
   *   - NotStreaming: not in streaming/running mode
   *   - NodeNotFound: unknown node name
   *   - PortNotFound: no valid port on node
   *   - QueueRejected: push was rejected
   */
  [[nodiscard]] Result<PushStatus> pushInput(const std::string &source_node,
                                             const std::string &port_name,
                                             PortDataPtr data);

  [[nodiscard]] Result<PushStatus> pushInput(const std::string &source_node,
                                             PortDataPtr data);

  // -------------------------------------------------------------------------
  // State & Monitoring
  // -------------------------------------------------------------------------

  [[nodiscard]] EngineStatisticsSnapshot statistics() const;

  [[nodiscard]] std::size_t queueDepth(const std::string &node_name,
                                       const std::string &port_name = "") const;

  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name,
                                      const std::string &port_name = "") const;

  Result<void> waitForDrain(
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

} // namespace ai_pipe

#endif // AI_PIPE_EXECUTION_ENGINE_HPP
