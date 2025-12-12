/**
 * @file context.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Comprehensive pipeline execution context with thread-safe resource
 *        management, metrics collection, cancellation support, and more.
 * @version 0.2
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_PIPELINE_CONTEXT_HPP
#define AI_PIPE_PIPELINE_CONTEXT_HPP

#include <any>
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <vector>

namespace ai_pipe {

// Forward declarations
class PipelineContext;
class ScopedContext;

// =============================================================================
// Supporting Types
// =============================================================================

/**
 * @brief Unique identifier for pipeline executions
 */
struct ExecutionId {
  std::uint64_t value{0};

  bool operator==(const ExecutionId &other) const {
    return value == other.value;
  }
  bool operator!=(const ExecutionId &other) const {
    return value != other.value;
  }

  static ExecutionId generate();
  [[nodiscard]] std::string toString() const;
};

/**
 * @brief Metrics for a single node execution
 */
struct NodeMetrics {
  std::string node_name;
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point end_time;
  std::chrono::microseconds duration{0};
  bool success{false};
  std::string error_message;
  std::size_t input_size{0};
  std::size_t output_size{0};

  [[nodiscard]] double durationMs() const {
    return static_cast<double>(duration.count()) / 1000.0;
  }
};

/**
 * @brief Aggregated metrics for an entire pipeline execution
 */
struct ExecutionMetrics {
  ExecutionId execution_id;
  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point end_time;
  std::chrono::milliseconds total_duration{0};
  std::size_t nodes_executed{0};
  std::size_t nodes_failed{0};
  std::vector<NodeMetrics> node_metrics;

  [[nodiscard]] double successRate() const {
    if (nodes_executed == 0)
      return 0.0;
    return static_cast<double>(nodes_executed - nodes_failed) /
           static_cast<double>(nodes_executed) * 100.0;
  }
};

/**
 * @brief Log entry for pipeline execution
 */
struct LogEntry {
  enum class Level { kDebug, kInfo, kWarning, kError };

  Level level{Level::kInfo};
  std::chrono::system_clock::time_point timestamp;
  std::string node_name;
  std::string message;

  [[nodiscard]] std::string levelString() const;
  [[nodiscard]] std::string format() const;
};

/**
 * @brief Cancellation token for cooperative cancellation
 */
class CancellationToken {
public:
  CancellationToken() = default;

  void cancel() { m_cancelled.store(true, std::memory_order_release); }
  void reset() { m_cancelled.store(false, std::memory_order_release); }

  [[nodiscard]] bool isCancelled() const {
    return m_cancelled.load(std::memory_order_acquire);
  }

  /**
   * @brief Throw if cancelled (for convenient cancellation point)
   * @throws std::runtime_error if cancelled
   */
  void throwIfCancelled() const {
    if (isCancelled()) {
      throw std::runtime_error("Operation cancelled");
    }
  }

private:
  std::atomic<bool> m_cancelled{false};
};

/**
 * @brief Progress reporter for long-running nodes
 */
class ProgressReporter {
public:
  using ProgressCallback =
      std::function<void(double progress, const std::string &message)>;

  explicit ProgressReporter(ProgressCallback callback = nullptr)
      : m_callback(std::move(callback)) {}

  ProgressReporter(ProgressReporter &&other) noexcept
      : m_callback(std::move(other.m_callback)),
        // atomic 不能移动，只能读取旧值来初始化新值
        m_progress(other.m_progress.load(std::memory_order_relaxed)),
        m_message(std::move(other.m_message)) {}

  ProgressReporter &operator=(ProgressReporter &&other) noexcept {
    if (this != &other) {
      m_callback = std::move(other.m_callback);
      // atomic 不能移动赋值，只能存入新值
      m_progress.store(other.m_progress.load(std::memory_order_relaxed),
                       std::memory_order_relaxed);
      m_message = std::move(other.m_message);
    }
    return *this;
  }

  ProgressReporter(const ProgressReporter &) = delete;
  ProgressReporter &operator=(const ProgressReporter &) = delete;

  void report(double progress, const std::string &message = "") {
    m_progress.store(progress, std::memory_order_release);
    m_message = message;
    if (m_callback) {
      m_callback(progress, message);
    }
  }

  [[nodiscard]] double progress() const {
    return m_progress.load(std::memory_order_acquire);
  }

  [[nodiscard]] const std::string &message() const { return m_message; }

private:
  ProgressCallback m_callback;
  std::atomic<double> m_progress{0.0};
  std::string m_message;
};

// =============================================================================
// Pipeline Context
// =============================================================================

/**
 * @brief Thread-safe execution context for pipeline
 *
 * Provides:
 * - Thread-safe resource/service management
 * - Execution tracking and metrics collection
 * - Cancellation support
 * - Logging infrastructure
 * - Configuration management
 * - Progress reporting
 *
 * Usage:
 * @code
 *   auto ctx = std::make_shared<PipelineContext>();
 *
 *   // Register resources
 *   ctx->setResource("model", model_ptr);
 *   ctx->setService<IInferenceEngine>(engine_ptr);
 *
 *   // In node processing
 *   void process(const PortDataMap& in, PortDataMap& out,
 *                std::shared_ptr<PipelineContext> ctx) {
 *     ctx->cancellation().throwIfCancelled();
 *     auto model = ctx->getResource<Model>("model");
 *     ctx->log(LogEntry::Level::kInfo, name(), "Processing...");
 *   }
 * @endcode
 */
class PipelineContext : public std::enable_shared_from_this<PipelineContext> {
public:
  PipelineContext();
  ~PipelineContext() = default;

  // Non-copyable, movable
  PipelineContext(const PipelineContext &) = delete;
  PipelineContext &operator=(const PipelineContext &) = delete;
  PipelineContext(PipelineContext &&) noexcept;
  PipelineContext &operator=(PipelineContext &&) noexcept;

  // -------------------------------------------------------------------------
  // Resource Management (Thread-Safe)
  // -------------------------------------------------------------------------

  /**
   * @brief Store a named resource
   * @tparam T Resource type
   * @param name Resource name
   * @param resource Shared pointer to resource
   */
  template <typename T>
  void setResource(const std::string &name, std::shared_ptr<T> resource) {
    std::unique_lock lock(m_resourceMutex);
    m_resources[name] = std::move(resource);
  }

  /**
   * @brief Retrieve a named resource
   * @tparam T Expected resource type
   * @param name Resource name
   * @return Shared pointer to resource, or nullptr if not found/wrong type
   */
  template <typename T>
  [[nodiscard]] std::shared_ptr<T> getResource(const std::string &name) const {
    std::shared_lock lock(m_resourceMutex);
    auto it = m_resources.find(name);
    if (it == m_resources.end()) {
      return nullptr;
    }
    try {
      return std::any_cast<std::shared_ptr<T>>(it->second);
    } catch (const std::bad_any_cast &) {
      return nullptr;
    }
  }

  /**
   * @brief Check if a resource exists
   */
  [[nodiscard]] bool hasResource(const std::string &name) const {
    std::shared_lock lock(m_resourceMutex);
    return m_resources.count(name) > 0;
  }

  /**
   * @brief Remove a resource
   * @return true if resource was removed
   */
  bool removeResource(const std::string &name) {
    std::unique_lock lock(m_resourceMutex);
    return m_resources.erase(name) > 0;
  }

  /**
   * @brief Get all resource names
   */
  [[nodiscard]] std::vector<std::string> resourceNames() const {
    std::shared_lock lock(m_resourceMutex);
    std::vector<std::string> names;
    names.reserve(m_resources.size());
    for (const auto &[name, _] : m_resources) {
      names.push_back(name);
    }
    return names;
  }

  // -------------------------------------------------------------------------
  // Service Locator (Type-Based)
  // -------------------------------------------------------------------------

  /**
   * @brief Register a service by interface type
   * @tparam Interface Service interface type
   * @param service Service implementation
   */
  template <typename Interface>
  void setService(std::shared_ptr<Interface> service) {
    std::unique_lock lock(m_serviceMutex);
    m_services[std::type_index(typeid(Interface))] = std::move(service);
  }

  /**
   * @brief Retrieve a service by interface type
   * @tparam Interface Service interface type
   * @return Service implementation or nullptr
   */
  template <typename Interface>
  [[nodiscard]] std::shared_ptr<Interface> getService() const {
    std::shared_lock lock(m_serviceMutex);
    auto it = m_services.find(std::type_index(typeid(Interface)));
    if (it == m_services.end()) {
      return nullptr;
    }
    try {
      return std::any_cast<std::shared_ptr<Interface>>(it->second);
    } catch (const std::bad_any_cast &) {
      return nullptr;
    }
  }

  /**
   * @brief Check if a service is registered
   */
  template <typename Interface> [[nodiscard]] bool hasService() const {
    std::shared_lock lock(m_serviceMutex);
    return m_services.count(std::type_index(typeid(Interface))) > 0;
  }

  // -------------------------------------------------------------------------
  // Configuration Management
  // -------------------------------------------------------------------------

  /**
   * @brief Set a configuration value
   */
  template <typename T> void setConfig(const std::string &key, T value) {
    std::unique_lock lock(m_configMutex);
    m_config[key] = std::move(value);
  }

  /**
   * @brief Get a configuration value
   * @return Value if exists and correct type, default_value otherwise
   */
  template <typename T>
  [[nodiscard]] T getConfig(const std::string &key,
                            T default_value = T{}) const {
    std::shared_lock lock(m_configMutex);
    auto it = m_config.find(key);
    if (it == m_config.end()) {
      return default_value;
    }
    try {
      return std::any_cast<T>(it->second);
    } catch (const std::bad_any_cast &) {
      return default_value;
    }
  }

  /**
   * @brief Check if configuration key exists
   */
  [[nodiscard]] bool hasConfig(const std::string &key) const {
    std::shared_lock lock(m_configMutex);
    return m_config.count(key) > 0;
  }

  // -------------------------------------------------------------------------
  // Execution Tracking
  // -------------------------------------------------------------------------

  /**
   * @brief Start a new execution (resets metrics, generates new ID)
   * @return The new execution ID
   */
  ExecutionId beginExecution();

  /**
   * @brief End the current execution
   */
  void endExecution();

  /**
   * @brief Get current execution ID
   */
  [[nodiscard]] ExecutionId executionId() const { return m_executionId; }

  /**
   * @brief Check if an execution is in progress
   */
  [[nodiscard]] bool isExecuting() const {
    return m_isExecuting.load(std::memory_order_acquire);
  }

  // -------------------------------------------------------------------------
  // Node Metrics
  // -------------------------------------------------------------------------

  /**
   * @brief Begin tracking metrics for a node
   */
  void beginNodeExecution(const std::string &node_name);

  /**
   * @brief End tracking metrics for a node
   * @param success Whether the node succeeded
   * @param error_message Error message if failed
   */
  void endNodeExecution(const std::string &node_name, bool success = true,
                        const std::string &error_message = "");

  /**
   * @brief Get metrics for the current/last execution
   */
  [[nodiscard]] ExecutionMetrics executionMetrics() const;

  /**
   * @brief Get metrics for a specific node
   */
  [[nodiscard]] std::optional<NodeMetrics>
  nodeMetrics(const std::string &node_name) const;

  // -------------------------------------------------------------------------
  // Cancellation Support
  // -------------------------------------------------------------------------

  /**
   * @brief Get the cancellation token
   */
  [[nodiscard]] CancellationToken &cancellation() {
    return m_cancellationToken;
  }
  [[nodiscard]] const CancellationToken &cancellation() const {
    return m_cancellationToken;
  }

  /**
   * @brief Request cancellation of current execution
   */
  void requestCancellation() { m_cancellationToken.cancel(); }

  /**
   * @brief Check if cancellation was requested
   */
  [[nodiscard]] bool isCancellationRequested() const {
    return m_cancellationToken.isCancelled();
  }

  // -------------------------------------------------------------------------
  // Logging
  // -------------------------------------------------------------------------

  /**
   * @brief Log a message
   */
  void log(LogEntry::Level level, const std::string &node_name,
           const std::string &message);

  /**
   * @brief Convenience logging methods
   */
  void logDebug(const std::string &node_name, const std::string &message) {
    log(LogEntry::Level::kDebug, node_name, message);
  }
  void logInfo(const std::string &node_name, const std::string &message) {
    log(LogEntry::Level::kInfo, node_name, message);
  }
  void logWarning(const std::string &node_name, const std::string &message) {
    log(LogEntry::Level::kWarning, node_name, message);
  }
  void logError(const std::string &node_name, const std::string &message) {
    log(LogEntry::Level::kError, node_name, message);
  }

  /**
   * @brief Get all log entries for current execution
   */
  [[nodiscard]] std::vector<LogEntry> logs() const;

  /**
   * @brief Get log entries filtered by level
   */
  [[nodiscard]] std::vector<LogEntry> logs(LogEntry::Level min_level) const;

  /**
   * @brief Clear log entries
   */
  void clearLogs();

  /**
   * @brief Set maximum number of log entries to keep (0 = unlimited)
   */
  void setMaxLogEntries(std::size_t max_entries) {
    m_maxLogEntries = max_entries;
  }

  // -------------------------------------------------------------------------
  // Progress Reporting
  // -------------------------------------------------------------------------

  /**
   * @brief Get progress reporter for a node
   */
  [[nodiscard]] ProgressReporter &
  progressReporter(const std::string &node_name);

  /**
   * @brief Report overall pipeline progress
   */
  void reportProgress(double progress, const std::string &message = "");

  /**
   * @brief Get overall pipeline progress
   */
  [[nodiscard]] double overallProgress() const {
    return m_overallProgress.load(std::memory_order_acquire);
  }

  /**
   * @brief Set progress callback
   */
  void setProgressCallback(ProgressReporter::ProgressCallback callback) {
    m_progressCallback = std::move(callback);
  }

  // -------------------------------------------------------------------------
  // User Data (Arbitrary Key-Value Storage)
  // -------------------------------------------------------------------------

  /**
   * @brief Set arbitrary user data
   */
  template <typename T> void setUserData(const std::string &key, T value) {
    std::unique_lock lock(m_userDataMutex);
    m_userData[key] = std::move(value);
  }

  /**
   * @brief Get arbitrary user data
   */
  template <typename T>
  [[nodiscard]] std::optional<T> getUserData(const std::string &key) const {
    std::shared_lock lock(m_userDataMutex);
    auto it = m_userData.find(key);
    if (it == m_userData.end()) {
      return std::nullopt;
    }
    try {
      return std::any_cast<T>(it->second);
    } catch (const std::bad_any_cast &) {
      return std::nullopt;
    }
  }

  /**
   * @brief Remove user data
   */
  bool removeUserData(const std::string &key) {
    std::unique_lock lock(m_userDataMutex);
    return m_userData.erase(key) > 0;
  }

  // -------------------------------------------------------------------------
  // Reset / Clear
  // -------------------------------------------------------------------------

  /**
   * @brief Reset context for new execution (clears metrics, logs, progress)
   * @note Does NOT clear resources, services, or config
   */
  void resetExecution();

  /**
   * @brief Full reset (clears everything including resources)
   */
  void reset();

private:
  // Resources (named, any type)
  mutable std::shared_mutex m_resourceMutex;
  std::unordered_map<std::string, std::any> m_resources;

  // Services (type-indexed)
  mutable std::shared_mutex m_serviceMutex;
  std::unordered_map<std::type_index, std::any> m_services;

  // Configuration
  mutable std::shared_mutex m_configMutex;
  std::unordered_map<std::string, std::any> m_config;

  // User data
  mutable std::shared_mutex m_userDataMutex;
  std::unordered_map<std::string, std::any> m_userData;

  // Execution tracking
  ExecutionId m_executionId;
  std::atomic<bool> m_isExecuting{false};
  std::chrono::steady_clock::time_point m_executionStartTime;

  // Metrics
  mutable std::mutex m_metricsMutex;
  std::unordered_map<std::string, NodeMetrics> m_nodeMetrics;
  std::size_t m_nodesExecuted{0};
  std::size_t m_nodesFailed{0};

  // Cancellation
  CancellationToken m_cancellationToken;

  // Logging
  mutable std::mutex m_logMutex;
  std::vector<LogEntry> m_logs;
  std::size_t m_maxLogEntries{1000};

  // Progress
  std::atomic<double> m_overallProgress{0.0};
  ProgressReporter::ProgressCallback m_progressCallback;
  mutable std::mutex m_progressMutex;
  std::unordered_map<std::string, ProgressReporter> m_nodeProgressReporters;
};

// =============================================================================
// Scoped Context Helper
// =============================================================================

/**
 * @brief RAII helper for node execution context
 *
 * Automatically tracks node execution metrics and handles errors.
 *
 * Usage:
 * @code
 *   void MyNode::process(..., std::shared_ptr<PipelineContext> ctx) {
 *     ScopedNodeExecution scope(ctx, getName());
 *     // ... node logic ...
 *     // Metrics automatically recorded on scope exit
 *   }
 * @endcode
 */
class ScopedNodeExecution {
public:
  ScopedNodeExecution(std::shared_ptr<PipelineContext> ctx,
                      const std::string &node_name)
      : m_ctx(std::move(ctx)), m_nodeName(node_name), m_success(true) {
    if (m_ctx) {
      m_ctx->beginNodeExecution(m_nodeName);
    }
  }

  ~ScopedNodeExecution() {
    if (m_ctx) {
      m_ctx->endNodeExecution(m_nodeName, m_success, m_errorMessage);
    }
  }

  // Non-copyable, non-movable
  ScopedNodeExecution(const ScopedNodeExecution &) = delete;
  ScopedNodeExecution &operator=(const ScopedNodeExecution &) = delete;
  ScopedNodeExecution(ScopedNodeExecution &&) = delete;
  ScopedNodeExecution &operator=(ScopedNodeExecution &&) = delete;

  /**
   * @brief Mark execution as failed
   */
  void setFailed(const std::string &error_message = "") {
    m_success = false;
    m_errorMessage = error_message;
  }

  /**
   * @brief Check cancellation and throw if requested
   */
  void checkCancellation() const {
    if (m_ctx) {
      m_ctx->cancellation().throwIfCancelled();
    }
  }

  /**
   * @brief Report progress for this node
   */
  void reportProgress(double progress, const std::string &message = "") {
    if (m_ctx) {
      m_ctx->progressReporter(m_nodeName).report(progress, message);
    }
  }

  /**
   * @brief Log a message
   */
  void log(LogEntry::Level level, const std::string &message) {
    if (m_ctx) {
      m_ctx->log(level, m_nodeName, message);
    }
  }

private:
  std::shared_ptr<PipelineContext> m_ctx;
  std::string m_nodeName;
  bool m_success;
  std::string m_errorMessage;
};

} // namespace ai_pipe

#endif // AI_PIPE_PIPELINE_CONTEXT_HPP