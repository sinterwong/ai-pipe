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

  // Construction

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

  // Strategy Injection

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

  // Core Interface

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

  // Callback Registration (for async event notifications)

  void
  setPipelineResultCallback(std::function<void(const PortDataMap &)> callback);

  void setPipelineErrorCallback(
      std::function<void(const std::string &, const std::string &)> callback);

  void setDropCallback(
      std::function<void(const std::string &node, std::uint64_t frame_id,
                         const std::string &reason)>
          callback);

  /**
   * @brief Called once end of stream has reached every sink (R6.1)
   * @see signalEndOfStream
   */
  void setEndOfStreamCallback(std::function<void()> callback);

  // Streaming Interface

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

  // End of stream (R6.1) - see docs/design/eos_flush.md

  /**
   * @brief Declare that no more data will arrive on an input port
   *
   * Latches end-of-stream on the port. Already-queued packets are still
   * processed; once the port drains, and once every other input port of
   * the node has likewise drained, the engine calls the node's
   * onEndOfStream() flush hook, propagates its output, and latches EOS
   * on the node's downstream ports. EOS therefore walks the graph in
   * topological order and reaches the sinks last.
   *
   * The latch lives beside the queue rather than inside it, so it can
   * never be evicted by a drop policy or blocked by a full queue.
   *
   * Streaming mode only, and only for nodes that have at least one
   * input port (a node with no inputs has nothing to close).
   *
   * @param source_node Node whose input port is closing
   * @param port_name   Input port; empty selects the node's first
   * @return Ok, or Error with:
   *   - NotStreaming: engine is not streaming
   *   - NodeNotFound / PortNotFound: unknown node, or no input port
   *   - EndOfStreamSignaled: the port was already closed
   */
  Result<void> signalEndOfStream(const std::string &source_node,
                                 const std::string &port_name);

  Result<void> signalEndOfStream(const std::string &source_node);

  /**
   * @brief Block until EOS has reached every sink
   *
   * Returns Ok once the last sink has run its flush hook, meaning the
   * pipeline has produced everything it ever will. Does NOT stop the
   * engine - reading final statistics and deciding when to shut down
   * stay with the caller. The usual shutdown is
   * signalEndOfStream() -> waitForEndOfStream() -> stopStreaming().
   *
   * @param timeout Maximum wait; <= 0 waits indefinitely
   * @return Ok on EOS, ExecutionTimeout on expiry, or NotStreaming
   */
  Result<void> waitForEndOfStream(
      std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

  /** @brief Has EOS reached every sink? */
  [[nodiscard]] bool isEndOfStreamReached() const;

  // State & Monitoring

  [[nodiscard]] EngineStatisticsSnapshot statistics() const;

  [[nodiscard]] std::size_t queueDepth(const std::string &node_name,
                                       const std::string &port_name = "") const;

  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name,
                                      const std::string &port_name = "") const;

  Result<void> waitForDrain(
      std::size_t max_depth = 0,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

  /**
   * @brief Block until the engine leaves the RUNNING state (R4.4)
   *
   * A condition-variable wait on the engine's completion signal - no
   * polling. Covers batch completion, stop/cancel, errors, and
   * streaming shutdown. Returns immediately when not RUNNING.
   */
  void waitForIdle();

  // Configuration

  void setNodeQueueConfig(const std::string &node_name,
                          const QueueConfig &config);

  [[nodiscard]] const EngineConfig &config() const;

  // Information

  [[nodiscard]] std::string info() const;
  [[nodiscard]] std::string strategyInfo() const;

private:
  // PIMPL idiom to hide implementation details
  class Impl;
  std::unique_ptr<Impl> m_impl;
};

// Convenience Factory Functions

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
