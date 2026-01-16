/**
 * @file pipeline.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief High-level Pipeline API for AI Pipe library
 * @version 1.0
 * @date 2025-12-24
 *
 * This file provides the main user-facing API for building and executing
 * computation pipelines. It supports both batch and streaming execution modes.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_PIPELINE_HPP
#define AI_PIPE_PIPELINE_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/enum.hpp"
#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_scheduler_strategy.hpp"
#include "ai_pipe/i_sync_strategy.hpp"

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace ai_pipe {

class PipelineBuilder;

struct PipelineOptions {
  ExecutionMode mode = ExecutionMode::BATCH;
  std::uint8_t num_workers = 4;
  std::chrono::milliseconds execution_timeout{0};

  std::size_t queue_capacity = 0;
  std::string drop_strategy = "DropHead";
  bool enable_sync_coordination = false;
  bool enable_statistics = true;

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

struct ExecutionResult {
  bool success = false;
  PortDataMap outputs;
  std::string error_message;
  std::chrono::milliseconds elapsed{0};
  explicit operator bool() const { return success; }
};

// =============================================================================
// Pipeline Observer
// =============================================================================

class IPipelineObserver {
public:
  virtual ~IPipelineObserver() = default;

  virtual void onExecutionStarted() {}
  virtual void onExecutionCompleted(const PortDataMap &) {}
  virtual void onExecutionFailed(const std::string &, const std::string &) {}
  virtual void onFrameDropped(const std::string &, std::uint64_t,
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
  CallbackObserver &
  onError(std::function<void(const std::string &, const std::string &)> cb) {
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
  void onExecutionFailed(const std::string &e, const std::string &n) override {
    if (m_error)
      m_error(e, n);
  }
  void onFrameDropped(const std::string &n, std::uint64_t f,
                      const std::string &r) override {
    if (m_drop)
      m_drop(n, f, r);
  }

private:
  std::function<void()> m_start;
  std::function<void(const PortDataMap &)> m_result;
  std::function<void(const std::string &, const std::string &)> m_error;
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

  // Batch execution
  ExecutionResult run(const PortDataMap &inputs);
  ExecutionResult run(const PortDataMap &inputs,
                      std::chrono::milliseconds timeout);
  std::future<ExecutionResult> runAsync(const PortDataMap &inputs);
  bool submit(const PortDataMap &inputs);

  // Streaming interface
  bool start();
  bool start(std::shared_ptr<PipelineContext> context);
  void stop(bool wait_for_drain = true);

  [[nodiscard]] QueuePushResult pushInput(const std::string &source_node,
                                          PortDataPtr data);
  [[nodiscard]] QueuePushResult pushInput(const std::string &source_node,
                                          const std::string &port_name,
                                          PortDataPtr data);

  [[nodiscard]] bool isStreaming() const;
  [[nodiscard]] std::size_t queueDepth(const std::string &node_name) const;
  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name) const;
  bool waitForDrain(
      std::size_t max_depth = 0,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

  // Control
  void cancel();
  void wait();
  void reset();

  // Status
  [[nodiscard]] bool isReady() const;
  [[nodiscard]] bool isRunning() const;
  [[nodiscard]] bool hasError() const;
  [[nodiscard]] PipelineState state() const;
  [[nodiscard]] EngineState engineState() const;
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  nodeStates() const;
  [[nodiscard]] ExecutionMode mode() const;
  [[nodiscard]] EngineStatisticsSnapshot statistics() const;

  // Accessors
  [[nodiscard]] const Graph &graph() const;
  [[nodiscard]] PipelineContext &context();
  [[nodiscard]] const PipelineContext &context() const;
  [[nodiscard]] std::string info() const;

  // Observer management
  void addObserver(std::shared_ptr<IPipelineObserver> observer);
  void removeObserver(const std::shared_ptr<IPipelineObserver> &observer);

private:
  friend class PipelineBuilder;

  bool initialize(Graph &&graph, std::shared_ptr<PipelineContext> context,
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

  // Callbacks
  PipelineBuilder &onResult(std::function<void(const PortDataMap &)> callback);
  PipelineBuilder &onError(
      std::function<void(const std::string &, const std::string &)> callback);
  PipelineBuilder &onDrop(std::function<void(const std::string &, std::uint64_t,
                                             const std::string &)>
                              callback);
  PipelineBuilder &withObserver(std::shared_ptr<IPipelineObserver> observer);

  // Build
  Pipeline build();
  std::optional<Pipeline> tryBuild();

private:
  struct BuilderState;
  std::unique_ptr<BuilderState> m_state;
};

// =============================================================================
// Convenience Factory Functions
// =============================================================================

inline Pipeline makeBatchPipeline(Graph graph, std::uint8_t workers = 4) {
  return Pipeline::create()
      .withGraph(std::move(graph))
      .withMode(ExecutionMode::BATCH)
      .withWorkers(workers)
      .build();
}

inline Pipeline makeStreamPipeline(Graph graph, std::uint8_t workers = 4,
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
