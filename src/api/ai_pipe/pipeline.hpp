/**
 * @file pipeline.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Modern pipeline interface with fluent API
 * @version 0.3
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_PIPELINE_HPP
#define AI_PIPE_PIPELINE_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/enum.hpp"
#include "ai_pipe/graph.hpp"
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>

namespace ai_pipe {

// Forward declarations
class Pipeline;
class PipelineBuilder;

// =============================================================================
// Result Types
// =============================================================================

/**
 * @brief Execution result containing output data or error information
 */
struct ExecutionResult {
  bool success{false};
  PortDataMap outputs;
  std::string error_message;
  std::chrono::milliseconds elapsed{0};

  explicit operator bool() const { return success; }
};

/**
 * @brief Pipeline event listener interface
 */
class IPipelineObserver {
public:
  virtual ~IPipelineObserver() = default;

  virtual void onExecutionStarted() {}
  virtual void onExecutionCompleted(const PortDataMap &results) {}
  virtual void onExecutionFailed(const std::string &error,
                                 const std::string &node_name) {}
  virtual void onNodeStateChanged(const std::string &node_name,
                                  NodeExecutionState state) {}
};

/**
 * @brief Simple callback-based observer adapter
 */
class CallbackObserver : public IPipelineObserver {
public:
  using ResultCallback = std::function<void(const PortDataMap &)>;
  using ErrorCallback =
      std::function<void(const std::string &, const std::string &)>;

  CallbackObserver &onResult(ResultCallback cb) {
    m_resultCallback = std::move(cb);
    return *this;
  }

  CallbackObserver &onError(ErrorCallback cb) {
    m_errorCallback = std::move(cb);
    return *this;
  }

  void onExecutionCompleted(const PortDataMap &results) override {
    if (m_resultCallback)
      m_resultCallback(results);
  }

  void onExecutionFailed(const std::string &error,
                         const std::string &node_name) override {
    if (m_errorCallback)
      m_errorCallback(error, node_name);
  }

private:
  ResultCallback m_resultCallback;
  ErrorCallback m_errorCallback;
};

// =============================================================================
// Pipeline Configuration
// =============================================================================

/**
 * @brief Pipeline configuration with sensible defaults
 */
struct PipelineOptions {
  std::string engine_type = "DefaultExecutionEngine";
  std::uint8_t num_workers = 4;
  std::chrono::milliseconds execution_timeout{0}; // 0 = no timeout
  bool auto_reset_on_error = false;
};

// =============================================================================
// Pipeline Class
// =============================================================================

/**
 * @brief High-level pipeline for graph-based computation
 *
 * Usage:
 * @code
 *   auto pipeline = Pipeline::create()
 *       .withGraph(std::move(graph))
 *       .withWorkers(4)
 *       .onResult([](const PortDataMap& r) { process(r); })
 *       .onError([](auto& err, auto& node) { log(err); })
 *       .build();
 *
 *   // Synchronous execution
 *   auto result = pipeline.run(inputs);
 *
 *   // Asynchronous execution
 *   auto future = pipeline.runAsync(inputs);
 * @endcode
 */
class Pipeline {
public:
  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  Pipeline();
  ~Pipeline();

  // Non-copyable
  Pipeline(const Pipeline &) = delete;
  Pipeline &operator=(const Pipeline &) = delete;

  // Movable
  Pipeline(Pipeline &&other) noexcept;
  Pipeline &operator=(Pipeline &&other) noexcept;

  /**
   * @brief Create a pipeline builder for fluent construction
   */
  static PipelineBuilder create();

  // -------------------------------------------------------------------------
  // Execution Interface
  // -------------------------------------------------------------------------

  /**
   * @brief Execute pipeline synchronously
   * @param inputs Initial input data for source nodes
   * @return Execution result with outputs or error
   */
  [[nodiscard]] ExecutionResult run(const PortDataMap &inputs);

  /**
   * @brief Execute pipeline synchronously with timeout
   * @param inputs Initial input data
   * @param timeout Maximum execution time
   * @return Execution result (may indicate timeout)
   */
  [[nodiscard]] ExecutionResult run(const PortDataMap &inputs,
                                    std::chrono::milliseconds timeout);

  /**
   * @brief Execute pipeline asynchronously
   * @param inputs Initial input data
   * @return Future that resolves to execution result
   */
  [[nodiscard]] std::future<ExecutionResult>
  runAsync(const PortDataMap &inputs);

  /**
   * @brief Execute with callback notification (fire-and-forget)
   * @param inputs Initial input data
   * @return true if execution was started successfully
   */
  bool submit(const PortDataMap &inputs);

  // -------------------------------------------------------------------------
  // Control Interface
  // -------------------------------------------------------------------------

  /**
   * @brief Cancel ongoing execution
   */
  void cancel();

  /**
   * @brief Wait for any ongoing execution to complete
   */
  void wait();

  /**
   * @brief Reset pipeline to initial state (clears any error state)
   */
  void reset();

  // -------------------------------------------------------------------------
  // Status Interface
  // -------------------------------------------------------------------------

  /**
   * @brief Check if pipeline is ready for execution
   */
  [[nodiscard]] bool isReady() const;

  /**
   * @brief Check if pipeline is currently executing
   */
  [[nodiscard]] bool isRunning() const;

  /**
   * @brief Check if pipeline is in error state
   */
  [[nodiscard]] bool hasError() const;

  /**
   * @brief Get current pipeline state
   */
  [[nodiscard]] PipelineState state() const;

  /**
   * @brief Get execution engine state
   */
  [[nodiscard]] EngineState engineState() const;

  /**
   * @brief Get all node states
   */
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  nodeStates() const;

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /**
   * @brief Get the computation graph (const)
   */
  [[nodiscard]] const Graph &graph() const;

  /**
   * @brief Get pipeline context for shared state
   */
  [[nodiscard]] PipelineContext &context();
  [[nodiscard]] const PipelineContext &context() const;

  // -------------------------------------------------------------------------
  // Observer Management
  // -------------------------------------------------------------------------

  /**
   * @brief Add an observer for pipeline events
   */
  void addObserver(std::shared_ptr<IPipelineObserver> observer);

  /**
   * @brief Remove an observer
   */
  void removeObserver(const std::shared_ptr<IPipelineObserver> &observer);

private:
  friend class PipelineBuilder;

  // Private initialization (used by builder)
  bool initialize(Graph &&graph, std::shared_ptr<PipelineContext> context,
                  const PipelineOptions &options);

  class Impl;
  std::unique_ptr<Impl> m_impl;
};

// =============================================================================
// Pipeline Builder
// =============================================================================

/**
 * @brief Fluent builder for Pipeline construction
 */
class PipelineBuilder {
public:
  PipelineBuilder();
  ~PipelineBuilder();

  PipelineBuilder(PipelineBuilder &&) noexcept;
  PipelineBuilder &operator=(PipelineBuilder &&) noexcept;

  /**
   * @brief Set the computation graph
   */
  PipelineBuilder &withGraph(Graph graph);

  /**
   * @brief Set shared context
   */
  PipelineBuilder &withContext(std::shared_ptr<PipelineContext> context);

  /**
   * @brief Set number of worker threads
   */
  PipelineBuilder &withWorkers(std::uint8_t count);

  /**
   * @brief Set execution engine type
   */
  PipelineBuilder &withEngine(std::string engine_type);

  /**
   * @brief Set execution timeout
   */
  PipelineBuilder &withTimeout(std::chrono::milliseconds timeout);

  /**
   * @brief Set full options
   */
  PipelineBuilder &withOptions(PipelineOptions options);

  /**
   * @brief Add result callback (convenience method)
   */
  PipelineBuilder &onResult(std::function<void(const PortDataMap &)> callback);

  /**
   * @brief Add error callback (convenience method)
   */
  PipelineBuilder &onError(
      std::function<void(const std::string &, const std::string &)> callback);

  /**
   * @brief Add a full observer
   */
  PipelineBuilder &withObserver(std::shared_ptr<IPipelineObserver> observer);

  /**
   * @brief Build the pipeline
   * @return Configured Pipeline instance
   * @throws std::runtime_error if configuration is invalid
   */
  [[nodiscard]] Pipeline build();

  /**
   * @brief Try to build the pipeline
   * @return Pipeline if successful, nullopt otherwise
   */
  [[nodiscard]] std::optional<Pipeline> tryBuild();

private:
  struct BuilderState;
  std::unique_ptr<BuilderState> m_state;
};

// =============================================================================
// Convenience Factory Functions
// =============================================================================

/**
 * @brief Create a simple pipeline with minimal configuration
 * @param graph Computation graph
 * @param num_workers Number of worker threads
 * @return Configured Pipeline
 */
inline Pipeline makePipeline(Graph graph, std::uint8_t num_workers = 4) {
  return Pipeline::create()
      .withGraph(std::move(graph))
      .withWorkers(num_workers)
      .build();
}

} // namespace ai_pipe

#endif // AI_PIPE_PIPELINE_HPP