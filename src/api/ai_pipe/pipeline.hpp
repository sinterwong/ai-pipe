/**
 * @file pipeline.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief High-level Pipeline API for AI Pipe library
 * @version 2.0
 * @date 2025-12-24
 *
 * v2.0: Unified error handling with Result<T>.
 *       Pipeline::run() returns Result<ExecutionOutput> instead of
 *       ExecutionResult. All fallible operations use Result<void>.
 *       PipelineBuilder::build() returns Result<Pipeline> instead of throwing.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_PIPELINE_HPP
#define AI_PIPE_PIPELINE_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/enum.hpp"
#include "ai_pipe/error.hpp"
#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_scheduler_strategy.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>

namespace ai_pipe {

class PipelineBuilder;
class ITraceSink;

struct PipelineOptions {
  ExecutionMode mode = ExecutionMode::BATCH;
  std::uint8_t num_workers = 4;
  std::chrono::milliseconds execution_timeout{0};

  std::size_t queue_capacity = 0;
  std::string drop_strategy = "DropHead";
  bool enable_sync_coordination = false;
  bool enable_statistics = true;

  /// Multi-input alignment key; see AlignmentPolicy (execution_types.hpp)
  AlignmentPolicy alignment_policy = AlignmentPolicy::FrameId;
  /// Pairing tolerance for AlignmentPolicy::Timestamp
  std::chrono::microseconds alignment_tolerance{33000};

  /// Join wait cap before degradation; 0 = wait indefinitely (default)
  std::chrono::milliseconds join_wait_timeout{0};
  /// Degradation on join timeout; see JoinTimeoutPolicy
  JoinTimeoutPolicy join_timeout_policy = JoinTimeoutPolicy::PartialInputs;

  static PipelineOptions batch(std::uint8_t workers = 4) {
    PipelineOptions opts;
    opts.mode = ExecutionMode::BATCH;
    opts.num_workers = workers;
    opts.queue_capacity = 0;
    opts.enable_sync_coordination = false;
    return opts;
  }

  static PipelineOptions stream(std::uint8_t workers = 4,
                                std::size_t queue_cap = 16) {
    PipelineOptions opts;
    opts.mode = ExecutionMode::STREAM;
    opts.num_workers = workers;
    opts.queue_capacity = queue_cap;
    opts.enable_sync_coordination = true;
    return opts;
  }
};

// =============================================================================
// Execution Output (success payload for Pipeline::run())
// =============================================================================

/**
 * @brief Output from a successful pipeline execution
 *
 * This replaces the old ExecutionResult which mixed success data with error
 * info. In the new model, Pipeline::run() returns Result<ExecutionOutput>:
 *   - On success: contains ExecutionOutput with outputs and timing
 *   - On failure: contains Error with code, message, and node context
 */
struct ExecutionOutput {
  PortDataMap outputs;
  std::chrono::milliseconds elapsed{0};
};

// =============================================================================
// Pipeline Observer (retained for async event notifications)
// =============================================================================

class IPipelineObserver {
public:
  virtual ~IPipelineObserver() = default;

  virtual void onExecutionStarted() {}
  virtual void onExecutionCompleted(const PortDataMap &) {}

  /**
   * @brief Called when a node execution fails asynchronously
   * @param error The Error object with full context
   */
  virtual void onExecutionFailed(const Error &error) {
    // Default: delegate to legacy signature for backward compatibility
    onExecutionFailedLegacy(error.message(), error.nodeName());
  }

  virtual void onFrameDropped(const std::string &, std::uint64_t,
                              const std::string &) {}

  /**
   * @brief End of stream has reached every sink (R6.1)
   *
   * Fired once per run, after the last sink's flush hook. The pipeline
   * is still running at this point - see Pipeline::signalEndOfStream().
   */
  virtual void onEndOfStream() {}

protected:
  /**
   * @brief Legacy signature for backward compatibility.
   *        Override onExecutionFailed(const Error&) instead.
   */
  virtual void onExecutionFailedLegacy(const std::string &,
                                       const std::string &) {}
};

class CallbackObserver : public IPipelineObserver {
public:
  CallbackObserver &onStart(std::function<void()> cb) {
    m_start = std::move(cb);
    return *this;
  }
  CallbackObserver &onResult(std::function<void(const PortDataMap &)> cb) {
    m_result = std::move(cb);
    return *this;
  }
  CallbackObserver &onError(std::function<void(const Error &)> cb) {
    m_error = std::move(cb);
    return *this;
  }
  CallbackObserver &
  onDrop(std::function<void(const std::string &, std::uint64_t,
                            const std::string &)>
             cb) {
    m_drop = std::move(cb);
    return *this;
  }

  void onExecutionStarted() override {
    if (m_start)
      m_start();
  }
  void onExecutionCompleted(const PortDataMap &r) override {
    if (m_result)
      m_result(r);
  }
  void onExecutionFailed(const Error &e) override {
    if (m_error)
      m_error(e);
  }
  void onFrameDropped(const std::string &n, std::uint64_t f,
                      const std::string &r) override {
    if (m_drop)
      m_drop(n, f, r);
  }

private:
  std::function<void()> m_start;
  std::function<void(const PortDataMap &)> m_result;
  std::function<void(const Error &)> m_error;
  std::function<void(const std::string &, std::uint64_t, const std::string &)>
      m_drop;
};

// =============================================================================
// Pipeline
// =============================================================================

class Pipeline {
public:
  Pipeline();
  ~Pipeline();

  Pipeline(const Pipeline &) = delete;
  Pipeline &operator=(const Pipeline &) = delete;

  Pipeline(Pipeline &&) noexcept;
  Pipeline &operator=(Pipeline &&) noexcept;

  static PipelineBuilder create();

  // ---- Batch execution ----

  /**
   * @brief Run the pipeline synchronously
   * @return Result<ExecutionOutput> - outputs+timing on success, Error on
   * failure
   */
  Result<ExecutionOutput> run(const PortDataMap &inputs);

  /**
   * @brief Run the pipeline synchronously with a bounded wait (R1.3)
   *
   * The timeout is real, not an after-the-fact check: when it expires
   * the call returns ExecutionTimeout immediately, even if a node is
   * hung. Contract on expiry:
   * - the context's CancellationToken is cancelled (a cooperative node
   *   can observe it and bail out mid-process) and the engine stop
   *   protocol fires; an uncooperative node keeps running until its
   *   process() returns, but nothing new is scheduled;
   * - input queues may hold undelivered frames (no automatic drain);
   * - the pipeline enters ERROR - call reset() before the next run,
   *   which clears queue residue and returns the engine to IDLE.
   * A timeout <= 0ms means unbounded (same as the overload above).
   */
  Result<ExecutionOutput> run(const PortDataMap &inputs,
                              std::chrono::milliseconds timeout);

  /**
   * @brief Run the pipeline asynchronously
   * @return future that resolves to Result<ExecutionOutput>
   */
  std::future<Result<ExecutionOutput>> runAsync(const PortDataMap &inputs);

  /**
   * @brief Submit inputs for fire-and-forget execution
   * @return Result<void>
   */
  Result<void> submit(const PortDataMap &inputs);

  // ---- Streaming interface ----

  /**
   * @brief Start streaming mode
   * @return Result<void> - success or Error with appropriate code
   */
  Result<void> start();
  Result<void> start(std::shared_ptr<PipelineContext> context);
  void stop(bool wait_for_drain = true);

  /**
   * @brief Push input data for streaming processing
   * @return Result<PushStatus> - push status on success, Error on rejection
   */
  [[nodiscard]] Result<PushStatus> pushInput(const std::string &source_node,
                                             PortDataPtr data);
  [[nodiscard]] Result<PushStatus> pushInput(const std::string &source_node,
                                             const std::string &port_name,
                                             PortDataPtr data);

  /**
   * @brief Declare that no more data will arrive on an input port (R6.1)
   *
   * The graceful counterpart to stop(): stop() tears the pipeline down,
   * whereas this says "this input is finished" and lets the remaining
   * data flow through. Queued packets are still processed; when the port
   * drains (and every other input port of the node has too) the node's
   * ILogicNode::onEndOfStream() flush hook runs, its output is
   * propagated, and EOS is latched on the node's downstream ports. EOS
   * thus walks the graph in topological order and reaches sinks last.
   *
   * A join forwards EOS only once ALL of its input ports have finished,
   * so "downstream saw EOS" means every upstream path is done.
   *
   * Streaming mode only. Pushing to a port after closing it returns
   * ErrorCode::EndOfStreamSignaled.
   *
   * Typical finite-source shutdown:
   * @code
   *   pipeline.signalEndOfStream("decoder");
   *   pipeline.waitForEndOfStream();
   *   pipeline.stop();
   * @endcode
   *
   * @param node_name Node whose input port is closing
   * @param port_name Input port; empty selects the node's first
   *
   * @see docs/design/eos_flush.md
   */
  Result<void> signalEndOfStream(const std::string &node_name);
  Result<void> signalEndOfStream(const std::string &node_name,
                                 const std::string &port_name);

  /**
   * @brief Block until EOS has reached every sink
   *
   * Returns Ok once the pipeline has produced everything it ever will.
   * Does not stop the pipeline - that stays the caller's decision.
   *
   * @param timeout Maximum wait; <= 0 waits indefinitely
   * @return Ok on EOS, ExecutionTimeout on expiry, NotStreaming if the
   *         pipeline is not streaming, ExecutionStopped if it was
   *         stopped before EOS arrived
   */
  Result<void> waitForEndOfStream(
      std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

  /** @brief Has EOS reached every sink? */
  [[nodiscard]] bool isEndOfStreamReached() const;

  [[nodiscard]] bool isStreaming() const;
  [[nodiscard]] std::size_t queueDepth(const std::string &node_name) const;
  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name) const;
  Result<void> waitForDrain(
      std::size_t max_depth = 0,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

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

  /**
   * @brief Install a trace sink on the underlying engine (F7)
   *
   * See ai_pipe/trace.hpp. Only allowed while the engine is idle.
   */
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
  friend class PipelineBuilder;

  Result<void> initialize(Graph &&graph,
                          std::shared_ptr<PipelineContext> context,
                          const PipelineOptions &options,
                          std::unique_ptr<ISchedulerStrategy> scheduler,
                          std::unique_ptr<ISyncStrategy> sync);

  class Impl;
  std::unique_ptr<Impl> m_impl;
};

// =============================================================================
// Pipeline Builder
// =============================================================================

class PipelineBuilder {
public:
  PipelineBuilder();
  ~PipelineBuilder();

  PipelineBuilder(PipelineBuilder &&) noexcept;
  PipelineBuilder &operator=(PipelineBuilder &&) noexcept;

  PipelineBuilder(const PipelineBuilder &) = delete;
  PipelineBuilder &operator=(const PipelineBuilder &) = delete;

  // Core configuration
  PipelineBuilder &withGraph(Graph graph);
  PipelineBuilder &withContext(std::shared_ptr<PipelineContext> context);
  PipelineBuilder &withMode(ExecutionMode mode);
  PipelineBuilder &withWorkers(std::uint8_t count);
  PipelineBuilder &withTimeout(std::chrono::milliseconds timeout);
  PipelineBuilder &withOptions(PipelineOptions options);

  // Streaming configuration
  PipelineBuilder &withQueueCapacity(std::size_t capacity);
  PipelineBuilder &withDropStrategy(std::string strategy);
  PipelineBuilder &withSyncCoordination(bool enable);

  // Strategy configuration
  PipelineBuilder &
  withSchedulerStrategy(std::unique_ptr<ISchedulerStrategy> strategy);
  PipelineBuilder &withSyncStrategy(std::unique_ptr<ISyncStrategy> strategy);

  // Callbacks (now using Error-aware signatures)
  PipelineBuilder &onResult(std::function<void(const PortDataMap &)> callback);
  PipelineBuilder &onError(std::function<void(const Error &)> callback);
  PipelineBuilder &onDrop(std::function<void(const std::string &, std::uint64_t,
                                             const std::string &)>
                              callback);
  PipelineBuilder &withObserver(std::shared_ptr<IPipelineObserver> observer);

  // Build

  /**
   * @brief Build the pipeline
   * @return Result<Pipeline> - configured Pipeline on success, Error on failure
   *
   * Unlike the old build() which threw exceptions, this never throws.
   */
  Result<Pipeline> build();

private:
  struct BuilderState;
  std::unique_ptr<BuilderState> m_state;
};

// =============================================================================
// Convenience Factory Functions
// =============================================================================

inline Result<Pipeline> makeBatchPipeline(Graph graph,
                                          std::uint8_t workers = 4) {
  return Pipeline::create()
      .withGraph(std::move(graph))
      .withMode(ExecutionMode::BATCH)
      .withWorkers(workers)
      .build();
}

inline Result<Pipeline> makeStreamPipeline(Graph graph,
                                           std::uint8_t workers = 4,
                                           std::size_t queue_capacity = 16) {
  return Pipeline::create()
      .withGraph(std::move(graph))
      .withMode(ExecutionMode::STREAM)
      .withWorkers(workers)
      .withQueueCapacity(queue_capacity)
      .withSyncCoordination(true)
      .build();
}

} // namespace ai_pipe

#endif // AI_PIPE_PIPELINE_HPP
