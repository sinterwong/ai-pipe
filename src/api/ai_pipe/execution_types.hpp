/**
 * @file execution_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Execution engine types and configurations
 * @version 1.0
 * @date 2025-12-24
 *
 * This file defines common types, enums, and configurations used by
 * the execution engine and pipeline components.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_EXECUTION_TYPES_HPP
#define AI_PIPE_EXECUTION_TYPES_HPP

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace ai_pipe {

/**
 * @brief Execution mode for the pipeline
 */
enum class ExecutionMode {
  BATCH,  ///< Traditional batch processing
  STREAM, ///< Continuous streaming with backpressure
  HYBRID  ///< Mixed batch/stream behavior
};

/**
 * @brief Convert execution mode to string
 */
inline std::string executionModeToString(ExecutionMode mode) {
  switch (mode) {
  case ExecutionMode::BATCH:
    return "BATCH";
  case ExecutionMode::STREAM:
    return "STREAM";
  case ExecutionMode::HYBRID:
    return "HYBRID";
  }
  return "UNKNOWN";
}

/**
 * @brief Configuration for node input queues
 */
struct QueueConfig {
  std::size_t capacity = 0;               ///< 0 = unbounded
  std::string drop_strategy = "DropHead"; ///< DropHead, DropTail, KeepLatest
  std::size_t keep_latest_n = 1;          ///< For KeepLatest strategy
  bool track_statistics = true;
};

/**
 * @brief Result of a queue push operation
 */
struct QueuePushResult {
  enum class Status { Enqueued, Dropped, Rejected };

  Status status = Status::Rejected;

  std::string message;
  std::size_t queue_size = 0;

  bool isOk() const { return status != Status::Rejected; }
  bool isDropped() const { return status == Status::Dropped; }

  explicit operator bool() const { return isOk(); }

  static QueuePushResult success(std::size_t size) {
    return {Status::Enqueued, "success", size};
  }

  static QueuePushResult dropped(const std::string &reason, std::size_t size) {
    return {Status::Dropped, reason, size};
  }

  static QueuePushResult rejected(const std::string &reason, std::size_t size) {
    return {Status::Rejected, reason, size};
  }
};

/**
 * @brief Configuration for the execution engine
 */
struct EngineConfig {
  ExecutionMode mode = ExecutionMode::BATCH;
  std::uint8_t num_workers = 4;

  // Queue settings
  std::size_t default_queue_capacity = 0; ///< 0 = unbounded (batch mode)
  std::string default_drop_strategy = "DropHead";

  // Streaming settings
  bool enable_sync_coordination = false;
  bool allow_partial_inputs = false;
  std::chrono::milliseconds min_execution_interval{0};

  // Monitoring
  bool enable_statistics = true;
  bool enable_drop_logging = true;

  /**
   * @brief Create batch processing configuration
   */
  static EngineConfig batch(std::uint8_t workers = 4) {
    EngineConfig config;
    config.mode = ExecutionMode::BATCH;
    config.num_workers = workers;
    config.default_queue_capacity = 0; // Unbounded
    config.enable_sync_coordination = false;
    return config;
  }

  /**
   * @brief Create stream processing configuration
   */
  static EngineConfig stream(std::uint8_t workers = 4,
                             std::size_t queue_capacity = 16) {
    EngineConfig config;
    config.mode = ExecutionMode::STREAM;
    config.num_workers = workers;
    config.default_queue_capacity = queue_capacity;
    config.enable_sync_coordination = true;
    return config;
  }

  /**
   * @brief Create hybrid processing configuration
   */
  static EngineConfig hybrid(std::uint8_t workers = 4,
                             std::size_t queue_capacity = 16) {
    EngineConfig config;
    config.mode = ExecutionMode::HYBRID;
    config.num_workers = workers;
    config.default_queue_capacity = queue_capacity;
    config.enable_sync_coordination = true;
    return config;
  }
};

/**
 * @brief Atomic statistics for the execution engine
 */
struct EngineStatistics {
  std::atomic<std::uint64_t> total_executions{0};
  std::atomic<std::uint64_t> successful_executions{0};
  std::atomic<std::uint64_t> failed_executions{0};
  std::atomic<std::uint64_t> total_frames_processed{0};
  std::atomic<std::uint64_t> total_frames_dropped{0};
  std::atomic<std::uint64_t> total_queue_pushes{0};
  std::atomic<std::uint64_t> total_processing_time_us{0};

  std::chrono::steady_clock::time_point start_time;

  void reset() {
    total_executions.store(0, std::memory_order_relaxed);
    successful_executions.store(0, std::memory_order_relaxed);
    failed_executions.store(0, std::memory_order_relaxed);
    total_frames_processed.store(0, std::memory_order_relaxed);
    total_frames_dropped.store(0, std::memory_order_relaxed);
    total_queue_pushes.store(0, std::memory_order_relaxed);
    total_processing_time_us.store(0, std::memory_order_relaxed);
    start_time = std::chrono::steady_clock::now();
  }

  [[nodiscard]] double successRate() const {
    auto total = total_executions.load(std::memory_order_relaxed);
    if (total == 0)
      return 100.0;
    auto success = successful_executions.load(std::memory_order_relaxed);
    return 100.0 * static_cast<double>(success) / static_cast<double>(total);
  }

  [[nodiscard]] double dropRate() const {
    auto processed = total_frames_processed.load(std::memory_order_relaxed);
    auto dropped = total_frames_dropped.load(std::memory_order_relaxed);
    auto total = processed + dropped;
    if (total == 0)
      return 0.0;
    return 100.0 * static_cast<double>(dropped) / static_cast<double>(total);
  }

  [[nodiscard]] double throughput() const {
    auto elapsed = std::chrono::steady_clock::now() - start_time;
    auto seconds =
        std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
    if (seconds == 0)
      return 0.0;
    auto processed = total_frames_processed.load(std::memory_order_relaxed);
    return static_cast<double>(processed) / static_cast<double>(seconds);
  }
};

/**
 * @brief Copyable statistics snapshot
 */
struct EngineStatisticsSnapshot {
  std::uint64_t total_executions{0};
  std::uint64_t successful_executions{0};
  std::uint64_t failed_executions{0};
  std::uint64_t total_frames_processed{0};
  std::uint64_t total_frames_dropped{0};
  std::uint64_t total_queue_pushes{0};
  std::uint64_t total_processing_time_us{0};
  double success_rate{0.0};
  double drop_rate{0.0};
  double throughput{0.0};

  EngineStatisticsSnapshot() = default;

  explicit EngineStatisticsSnapshot(const EngineStatistics &stats) {
    total_executions = stats.total_executions.load(std::memory_order_relaxed);
    successful_executions =
        stats.successful_executions.load(std::memory_order_relaxed);
    failed_executions = stats.failed_executions.load(std::memory_order_relaxed);
    total_frames_processed =
        stats.total_frames_processed.load(std::memory_order_relaxed);
    total_frames_dropped =
        stats.total_frames_dropped.load(std::memory_order_relaxed);
    total_queue_pushes =
        stats.total_queue_pushes.load(std::memory_order_relaxed);
    total_processing_time_us =
        stats.total_processing_time_us.load(std::memory_order_relaxed);
    success_rate = stats.successRate();
    drop_rate = stats.dropRate();
    throughput = stats.throughput();
  }
};

} // namespace ai_pipe

#endif // AI_PIPE_EXECUTION_TYPES_HPP
