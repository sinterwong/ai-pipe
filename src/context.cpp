/**
 * @file context.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief PipelineContext implementation
 * @version 0.2
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#include "ai_pipe/context.hpp"
#include <iomanip>
#include <iostream>
#include <sstream>

namespace ai_pipe {

// =============================================================================
// ExecutionId Implementation
// =============================================================================

ExecutionId ExecutionId::generate() {
  static std::atomic<std::uint64_t> counter{0};
  static const std::uint64_t base =
      static_cast<std::uint64_t>(
          std::chrono::steady_clock::now().time_since_epoch().count()) &
      0xFFFFFFFF00000000ULL;

  return ExecutionId{base | (++counter)};
}

std::string ExecutionId::toString() const {
  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::setw(16) << value;
  return oss.str();
}

// =============================================================================
// ConsoleLoggerAdapter Implementation
// =============================================================================

void ConsoleLoggerAdapter::log(PipeLogLevel level, const std::string &node_name,
                               const std::string &message) {
  const char *level_str = nullptr;
  switch (level) {
  case PipeLogLevel::KDebug:
    level_str = "DEBUG";
    break;
  case PipeLogLevel::KInfo:
    level_str = "INFO";
    break;
  case PipeLogLevel::KWarning:
    level_str = "WARN";
    break;
  case PipeLogLevel::KError:
    level_str = "ERROR";
    break;
  default:
    level_str = "???";
    break;
  }

  std::ostringstream oss;
  oss << "[" << level_str << "]";
  if (!node_name.empty()) {
    oss << " [" << node_name << "]";
  }
  oss << " " << message;

  // Thread-safe output
  static std::mutex cout_mutex;
  std::lock_guard<std::mutex> lock(cout_mutex);
  std::cout << oss.str() << std::endl;
}

// =============================================================================
// PipelineContext Implementation
// =============================================================================

PipelineContext::PipelineContext() : m_executionId{0} {}

PipelineContext::PipelineContext(PipelineContext &&other) noexcept {
  // Lock all mutexes - use unique_lock for write access
  std::unique_lock resource_lock(other.m_resourceMutex);
  std::unique_lock service_lock(other.m_serviceMutex);
  std::unique_lock config_lock(other.m_configMutex);
  std::unique_lock user_data_lock(other.m_userDataMutex);
  std::lock_guard metrics_lock(other.m_metricsMutex);
  std::lock_guard logger_lock(other.m_loggerMutex);
  std::lock_guard progress_lock(other.m_progressMutex);

  m_resources = std::move(other.m_resources);
  m_services = std::move(other.m_services);
  m_config = std::move(other.m_config);
  m_userData = std::move(other.m_userData);
  m_executionId = other.m_executionId;
  m_isExecuting.store(other.m_isExecuting.load());
  m_executionStartTime = other.m_executionStartTime;
  m_nodeMetrics = std::move(other.m_nodeMetrics);
  m_nodesExecuted = other.m_nodesExecuted;
  m_nodesFailed = other.m_nodesFailed;
  m_loggerAdapter = std::move(other.m_loggerAdapter);
  m_overallProgress.store(other.m_overallProgress.load());
  m_progressCallback = std::move(other.m_progressCallback);
  m_nodeProgressReporters = std::move(other.m_nodeProgressReporters);
}

PipelineContext &PipelineContext::operator=(PipelineContext &&other) noexcept {
  if (this == &other) {
    return *this;
  }

  // Lock all mutexes - use unique_lock for write access on both
  std::unique_lock self_resource_lock(m_resourceMutex, std::defer_lock);
  std::unique_lock other_resource_lock(other.m_resourceMutex, std::defer_lock);
  std::unique_lock self_service_lock(m_serviceMutex, std::defer_lock);
  std::unique_lock other_service_lock(other.m_serviceMutex, std::defer_lock);
  std::unique_lock self_config_lock(m_configMutex, std::defer_lock);
  std::unique_lock other_config_lock(other.m_configMutex, std::defer_lock);
  std::unique_lock self_user_data_lock(m_userDataMutex, std::defer_lock);
  std::unique_lock other_user_data_lock(other.m_userDataMutex, std::defer_lock);

  std::lock(self_resource_lock, other_resource_lock, self_service_lock,
            other_service_lock, self_config_lock, other_config_lock,
            self_user_data_lock, other_user_data_lock);

  std::scoped_lock other_locks(m_metricsMutex, other.m_metricsMutex,
                               m_loggerMutex, other.m_loggerMutex,
                               m_progressMutex, other.m_progressMutex);

  m_resources = std::move(other.m_resources);
  m_services = std::move(other.m_services);
  m_config = std::move(other.m_config);
  m_userData = std::move(other.m_userData);
  m_executionId = other.m_executionId;
  m_isExecuting.store(other.m_isExecuting.load());
  m_executionStartTime = other.m_executionStartTime;
  m_nodeMetrics = std::move(other.m_nodeMetrics);
  m_nodesExecuted = other.m_nodesExecuted;
  m_nodesFailed = other.m_nodesFailed;
  m_loggerAdapter = std::move(other.m_loggerAdapter);
  m_overallProgress.store(other.m_overallProgress.load());
  m_progressCallback = std::move(other.m_progressCallback);
  m_nodeProgressReporters = std::move(other.m_nodeProgressReporters);

  return *this;
}

// -------------------------------------------------------------------------
// Execution Tracking
// -------------------------------------------------------------------------

ExecutionId PipelineContext::beginExecution() {
  resetExecution();

  m_executionId = ExecutionId::generate();
  m_executionStartTime = std::chrono::steady_clock::now();
  m_isExecuting.store(true, std::memory_order_release);

  return m_executionId;
}

void PipelineContext::endExecution() {
  m_isExecuting.store(false, std::memory_order_release);
}

// -------------------------------------------------------------------------
// Node Metrics
// -------------------------------------------------------------------------

void PipelineContext::beginNodeExecution(const std::string &node_name) {
  std::lock_guard<std::mutex> lock(m_metricsMutex);

  NodeMetrics metrics;
  metrics.node_name = node_name;
  metrics.start_time = std::chrono::steady_clock::now();
  metrics.success = false;

  m_nodeMetrics[node_name] = metrics;
}

void PipelineContext::endNodeExecution(const std::string &node_name,
                                       bool success,
                                       const std::string &error_message) {
  std::lock_guard<std::mutex> lock(m_metricsMutex);

  auto it = m_nodeMetrics.find(node_name);
  if (it == m_nodeMetrics.end()) {
    NodeMetrics metrics;
    metrics.node_name = node_name;
    metrics.start_time = std::chrono::steady_clock::now();
    it = m_nodeMetrics.emplace(node_name, metrics).first;
  }

  auto &metrics = it->second;
  metrics.end_time = std::chrono::steady_clock::now();
  metrics.duration = std::chrono::duration_cast<std::chrono::microseconds>(
      metrics.end_time - metrics.start_time);
  metrics.success = success;
  metrics.error_message = error_message;

  ++m_nodesExecuted;
  if (!success) {
    ++m_nodesFailed;
  }
}

ExecutionMetrics PipelineContext::executionMetrics() const {
  std::lock_guard<std::mutex> lock(m_metricsMutex);

  ExecutionMetrics result;
  result.execution_id = m_executionId;
  result.start_time = m_executionStartTime;
  result.end_time = std::chrono::steady_clock::now();
  result.total_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      result.end_time - result.start_time);
  result.nodes_executed = m_nodesExecuted;
  result.nodes_failed = m_nodesFailed;

  result.node_metrics.reserve(m_nodeMetrics.size());
  for (const auto &[name, metrics] : m_nodeMetrics) {
    result.node_metrics.push_back(metrics);
  }

  return result;
}

std::optional<NodeMetrics>
PipelineContext::nodeMetrics(const std::string &node_name) const {
  std::lock_guard<std::mutex> lock(m_metricsMutex);

  auto it = m_nodeMetrics.find(node_name);
  if (it == m_nodeMetrics.end()) {
    return std::nullopt;
  }
  return it->second;
}

// -------------------------------------------------------------------------
// Progress Reporting
// -------------------------------------------------------------------------

ProgressReporter &
PipelineContext::progressReporter(const std::string &node_name) {
  std::lock_guard<std::mutex> lock(m_progressMutex);

  auto it = m_nodeProgressReporters.find(node_name);
  if (it == m_nodeProgressReporters.end()) {
    it = m_nodeProgressReporters
             .emplace(node_name, std::make_unique<ProgressReporter>())
             .first;
  }
  return *it->second;
}

void PipelineContext::reportProgress(double progress,
                                     const std::string &message) {
  m_overallProgress.store(progress, std::memory_order_release);
  if (m_progressCallback) {
    m_progressCallback(progress, message);
  }
}

// -------------------------------------------------------------------------
// Reset
// -------------------------------------------------------------------------

void PipelineContext::resetExecution() {
  // Reset cancellation
  m_cancellationToken.reset();

  // Reset metrics
  {
    std::lock_guard<std::mutex> lock(m_metricsMutex);
    m_nodeMetrics.clear();
    m_nodesExecuted = 0;
    m_nodesFailed = 0;
  }

  // Reset progress
  m_overallProgress.store(0.0, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lock(m_progressMutex);
    m_nodeProgressReporters.clear();
  }

  // Reset execution state
  m_executionId = ExecutionId{0};
  m_isExecuting.store(false, std::memory_order_release);
}

void PipelineContext::reset() {
  resetExecution();

  // Clear resources
  {
    std::unique_lock lock(m_resourceMutex);
    m_resources.clear();
  }

  // Clear services
  {
    std::unique_lock lock(m_serviceMutex);
    m_services.clear();
  }

  // Clear config
  {
    std::unique_lock lock(m_configMutex);
    m_config.clear();
  }

  // Clear user data
  {
    std::unique_lock lock(m_userDataMutex);
    m_userData.clear();
  }

  // Note: Logger adapter is intentionally NOT cleared
}

} // namespace ai_pipe
