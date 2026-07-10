/**
 * @file execution_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Execution engine types and configurations
 * @version 2.0
 * @date 2025-12-24
 *
 * v2.0: Unified error handling - replaced QueuePushResult with PushStatus
 *       used inside Result<PushStatus>. The deprecated QueuePushResult
 *       wrapper was removed after the migration completed.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_EXECUTION_TYPES_HPP
#define AI_PIPE_EXECUTION_TYPES_HPP

#include "ai_pipe/error.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

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
  /**
   * Window size N for KeepLatest. "At most N" is strict for a single
   * producer per queue; with concurrent producers the bound is
   * eventual (transient overshoot up to N + producers - 1). See the
   * KeepLatest contract in the framework design doc, section 7.1.
   */
  std::size_t keep_latest_n = 1;
  bool track_statistics = true;
};

// =============================================================================
// Push Status (replaces QueuePushResult)
// =============================================================================

/**
 * @brief Outcome of a successful queue push operation
 *
 * This type represents the two non-error outcomes of pushing data:
 *   - Enqueued: data was added to the queue normally
 *   - Dropped:  data was accepted but an older frame was evicted (backpressure)
 *
 * Rejection (node not found, not streaming, etc.) is represented by
 * returning Result<PushStatus> with an Error, not by PushStatus itself.
 */
struct PushStatus {
  enum class Outcome : std::uint8_t {
    Enqueued, ///< Data successfully added to queue
    Dropped   ///< Data accepted, but a frame was evicted due to backpressure
  };

  Outcome outcome = Outcome::Enqueued;
  std::string detail;         ///< Human-readable info (e.g., drop reason)
  std::size_t queue_size = 0; ///< Queue size after the operation

  [[nodiscard]] bool isDropped() const { return outcome == Outcome::Dropped; }

  static PushStatus enqueued(std::size_t size) {
    return {Outcome::Enqueued, "success", size};
  }

  static PushStatus dropped(const std::string &reason, std::size_t size) {
    return {Outcome::Dropped, reason, size};
  }
};

/**
 * @brief Configuration for the execution engine
 */
struct EngineConfig {
  ExecutionMode mode = ExecutionMode::BATCH;
  std::uint8_t num_workers = 4;

  std::size_t default_queue_capacity = 0;
  std::string default_drop_strategy = "DropHead";

  bool enable_sync_coordination = false;
  bool allow_partial_inputs = false;
  std::chrono::milliseconds min_execution_interval{0};

  bool enable_statistics = true;
  bool enable_drop_logging = true;

  static EngineConfig batch(std::uint8_t workers = 4) {
    EngineConfig config;
    config.mode = ExecutionMode::BATCH;
    config.num_workers = workers;
    config.default_queue_capacity = 0;
    config.enable_sync_coordination = false;
    return config;
  }

  static EngineConfig stream(std::uint8_t workers = 4,
                             std::size_t queue_capacity = 16) {
    EngineConfig config;
    config.mode = ExecutionMode::STREAM;
    config.num_workers = workers;
    config.default_queue_capacity = queue_capacity;
    config.enable_sync_coordination = true;
    return config;
  }

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

// ============================================================================
// Latency Histogram
// ============================================================================

/**
 * @brief Histogram bucket boundaries in microseconds
 *
 * 16 buckets: <10us, <25us, <50us, <100us, <250us, <500us, <1ms, <2.5ms,
 *            <5ms, <10ms, <25ms, <50ms, <100ms, <250ms, <500ms, >=500ms
 */
struct LatencyHistogram {
  static constexpr std::size_t k_num_buckets = 16;

  static constexpr std::array<std::uint64_t, k_num_buckets - 1>
      k_bucket_bounds = {10,   25,    50,    100,   250,    500,    1000,  2500,
                         5000, 10000, 25000, 50000, 100000, 250000, 500000};

  std::array<std::atomic<std::uint64_t>, k_num_buckets> buckets{};

  LatencyHistogram() {
    for (auto &bucket : buckets) {
      bucket.store(0, std::memory_order_relaxed);
    }
  }

  static std::size_t getBucketIndex(std::uint64_t latency_us) {
    for (std::size_t i = 0; i < k_bucket_bounds.size(); ++i) {
      if (latency_us < k_bucket_bounds[i]) {
        return i;
      }
    }
    return k_num_buckets - 1;
  }

  void record(std::uint64_t latency_us) {
    std::size_t idx = getBucketIndex(latency_us);
    buckets[idx].fetch_add(1, std::memory_order_relaxed);
  }

  void reset() {
    for (auto &bucket : buckets) {
      bucket.store(0, std::memory_order_relaxed);
    }
  }

  [[nodiscard]] std::uint64_t totalCount() const {
    std::uint64_t total = 0;
    for (const auto &bucket : buckets) {
      total += bucket.load(std::memory_order_relaxed);
    }
    return total;
  }

  static std::string getBucketLabel(std::size_t index) {
    if (index == 0)
      return "<10us";
    if (index >= k_num_buckets - 1)
      return ">=500ms";

    auto bound = k_bucket_bounds[index];
    if (bound < 1000) {
      return "<" + std::to_string(bound) + "us";
    } else if (bound < 1000000) {
      if (bound % 1000 == 0) {
        return "<" + std::to_string(bound / 1000) + "ms";
      }
      double ms = static_cast<double>(bound) / 1000.0;
      std::string result = "<" + std::to_string(static_cast<int>(ms));
      int decimal = static_cast<int>((ms - static_cast<int>(ms)) * 10);
      if (decimal > 0) {
        result += "." + std::to_string(decimal);
      }
      result += "ms";
      return result;
    }
    return "<" + std::to_string(bound / 1000) + "ms";
  }
};

// ============================================================================
// Per-Node Statistics
// ============================================================================

struct NodeStatistics {
  std::string node_name;
  std::uint64_t execution_count{0};
  std::uint64_t success_count{0};
  std::uint64_t failure_count{0};
  std::uint64_t total_processing_us{0};
  std::uint64_t min_processing_us{std::numeric_limits<std::uint64_t>::max()};
  std::uint64_t max_processing_us{0};
  std::uint64_t total_input_count{0};
  std::uint64_t total_output_count{0};
  std::size_t current_queue_depth{0};

  NodeStatistics() = default;
  explicit NodeStatistics(const std::string &name) : node_name(name) {}

  [[nodiscard]] double avgProcessingTimeUs() const {
    if (success_count == 0)
      return 0.0;
    return static_cast<double>(total_processing_us) /
           static_cast<double>(success_count);
  }

  [[nodiscard]] double successRate() const {
    if (execution_count == 0)
      return 100.0;
    return 100.0 * static_cast<double>(success_count) /
           static_cast<double>(execution_count);
  }
};

struct AtomicNodeStatistics {
  std::atomic<std::uint64_t> execution_count{0};
  std::atomic<std::uint64_t> success_count{0};
  std::atomic<std::uint64_t> failure_count{0};
  std::atomic<std::uint64_t> total_processing_us{0};
  std::atomic<std::uint64_t> min_processing_us{
      std::numeric_limits<std::uint64_t>::max()};
  std::atomic<std::uint64_t> max_processing_us{0};
  std::atomic<std::uint64_t> total_input_count{0};
  std::atomic<std::uint64_t> total_output_count{0};

  void recordIo(std::uint64_t inputs, std::uint64_t outputs) {
    total_input_count.fetch_add(inputs, std::memory_order_relaxed);
    total_output_count.fetch_add(outputs, std::memory_order_relaxed);
  }

  void recordExecution(bool success, std::uint64_t processing_us) {
    execution_count.fetch_add(1, std::memory_order_relaxed);
    if (success) {
      success_count.fetch_add(1, std::memory_order_relaxed);
    } else {
      failure_count.fetch_add(1, std::memory_order_relaxed);
    }
    total_processing_us.fetch_add(processing_us, std::memory_order_relaxed);

    // Update min/max (relaxed, approximate is fine for stats)
    auto current_min = min_processing_us.load(std::memory_order_relaxed);
    while (processing_us < current_min &&
           !min_processing_us.compare_exchange_weak(
               current_min, processing_us, std::memory_order_relaxed)) {
    }

    auto current_max = max_processing_us.load(std::memory_order_relaxed);
    while (processing_us > current_max &&
           !max_processing_us.compare_exchange_weak(
               current_max, processing_us, std::memory_order_relaxed)) {
    }
  }

  [[nodiscard]] NodeStatistics snapshot(const std::string &name) const {
    NodeStatistics stats(name);
    stats.execution_count = execution_count.load(std::memory_order_relaxed);
    stats.success_count = success_count.load(std::memory_order_relaxed);
    stats.failure_count = failure_count.load(std::memory_order_relaxed);
    stats.total_processing_us =
        total_processing_us.load(std::memory_order_relaxed);
    stats.min_processing_us = min_processing_us.load(std::memory_order_relaxed);
    stats.max_processing_us = max_processing_us.load(std::memory_order_relaxed);
    stats.total_input_count = total_input_count.load(std::memory_order_relaxed);
    stats.total_output_count =
        total_output_count.load(std::memory_order_relaxed);
    return stats;
  }

  void reset() {
    execution_count.store(0, std::memory_order_relaxed);
    success_count.store(0, std::memory_order_relaxed);
    failure_count.store(0, std::memory_order_relaxed);
    total_processing_us.store(0, std::memory_order_relaxed);
    min_processing_us.store(std::numeric_limits<std::uint64_t>::max(),
                            std::memory_order_relaxed);
    max_processing_us.store(0, std::memory_order_relaxed);
    total_input_count.store(0, std::memory_order_relaxed);
    total_output_count.store(0, std::memory_order_relaxed);
  }
};

// ============================================================================
// Engine Statistics
// ============================================================================

struct EngineStatistics {
  // Execution counts. Unit: one NODE execution attempt (unified across
  // batch/stream/hybrid since P5.3; previously batch counted pipeline
  // runs while stream counted node schedules, which broke successRate).
  // total_executions ~= successful_executions + failed_executions,
  // modulo attempts still in flight at snapshot time.
  std::atomic<std::uint64_t> total_executions{0};
  std::atomic<std::uint64_t> successful_executions{0};
  std::atomic<std::uint64_t> failed_executions{0};

  // Frame counts
  std::atomic<std::uint64_t> total_input_frames{0};
  std::atomic<std::uint64_t> total_output_frames{0};
  std::atomic<std::uint64_t> total_dropped_frames{0};

  // Queue stats
  std::atomic<std::uint64_t> total_queue_pushes{0};
  std::atomic<std::uint64_t> total_queue_pops{0};
  std::atomic<std::uint64_t> queue_full_events{0};

  // Timing
  std::atomic<std::uint64_t> total_processing_time_us{0};
  std::atomic<std::uint64_t> total_wait_time_us{0};
  std::atomic<std::uint64_t> total_schedule_time_us{0};

  // Latency distribution
  LatencyHistogram latency_histogram;

  // Start time for throughput calculation
  std::chrono::steady_clock::time_point start_time;

  EngineStatistics() : start_time(std::chrono::steady_clock::now()) {}

  void reset() {
    total_executions.store(0, std::memory_order_relaxed);
    successful_executions.store(0, std::memory_order_relaxed);
    failed_executions.store(0, std::memory_order_relaxed);
    total_input_frames.store(0, std::memory_order_relaxed);
    total_output_frames.store(0, std::memory_order_relaxed);
    total_dropped_frames.store(0, std::memory_order_relaxed);
    total_queue_pushes.store(0, std::memory_order_relaxed);
    total_queue_pops.store(0, std::memory_order_relaxed);
    queue_full_events.store(0, std::memory_order_relaxed);
    total_processing_time_us.store(0, std::memory_order_relaxed);
    total_wait_time_us.store(0, std::memory_order_relaxed);
    total_schedule_time_us.store(0, std::memory_order_relaxed);
    latency_histogram.reset();
    start_time = std::chrono::steady_clock::now();
  }

  void recordLatency(std::uint64_t latency_us) {
    latency_histogram.record(latency_us);
  }

  [[nodiscard]] double successRate() const {
    auto total = total_executions.load(std::memory_order_relaxed);
    if (total == 0)
      return 100.0;
    auto success = successful_executions.load(std::memory_order_relaxed);
    return 100.0 * static_cast<double>(success) / static_cast<double>(total);
  }

  [[nodiscard]] double dropRate() const {
    auto output = total_output_frames.load(std::memory_order_relaxed);
    auto dropped = total_dropped_frames.load(std::memory_order_relaxed);
    auto total = output + dropped;
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
    auto output = total_output_frames.load(std::memory_order_relaxed);
    return static_cast<double>(output) / static_cast<double>(seconds);
  }

  [[nodiscard]] double inputOutputRatio() const {
    auto input = total_input_frames.load(std::memory_order_relaxed);
    auto output = total_output_frames.load(std::memory_order_relaxed);
    if (output == 0)
      return 0.0;
    return static_cast<double>(input) / static_cast<double>(output);
  }

  [[nodiscard]] double avgProcessingTimeUs() const {
    auto success = successful_executions.load(std::memory_order_relaxed);
    if (success == 0)
      return 0.0;
    auto total_time = total_processing_time_us.load(std::memory_order_relaxed);
    return static_cast<double>(total_time) / static_cast<double>(success);
  }

  [[nodiscard]] double avgWaitTimeUs() const {
    auto pops = total_queue_pops.load(std::memory_order_relaxed);
    if (pops == 0)
      return 0.0;
    auto wait_time = total_wait_time_us.load(std::memory_order_relaxed);
    return static_cast<double>(wait_time) / static_cast<double>(pops);
  }

  [[nodiscard]] double avgScheduleTimeUs() const {
    auto execs = total_executions.load(std::memory_order_relaxed);
    if (execs == 0)
      return 0.0;
    auto sched_time = total_schedule_time_us.load(std::memory_order_relaxed);
    return static_cast<double>(sched_time) / static_cast<double>(execs);
  }

  [[nodiscard]] std::uint64_t totalFramesProcessed() const {
    return total_output_frames.load(std::memory_order_relaxed);
  }

  [[nodiscard]] std::uint64_t totalFramesDropped() const {
    return total_dropped_frames.load(std::memory_order_relaxed);
  }
};

// ============================================================================
// Engine Statistics Snapshot
// ============================================================================

struct EngineStatisticsSnapshot {
  std::uint64_t total_executions{0};
  std::uint64_t successful_executions{0};
  std::uint64_t failed_executions{0};

  std::uint64_t total_input_frames{0};
  std::uint64_t total_output_frames{0};
  std::uint64_t total_dropped_frames{0};

  std::uint64_t total_queue_pushes{0};
  std::uint64_t total_queue_pops{0};
  std::uint64_t queue_full_events{0};

  std::uint64_t total_processing_time_us{0};
  std::uint64_t total_wait_time_us{0};
  std::uint64_t total_schedule_time_us{0};

  std::array<std::uint64_t, LatencyHistogram::k_num_buckets>
      latency_histogram_buckets{};

  double success_rate{0.0};
  double drop_rate{0.0};
  double throughput{0.0};

  std::chrono::steady_clock::time_point start_time;
  std::chrono::steady_clock::time_point snapshot_time;

  std::vector<NodeStatistics> node_stats;

  EngineStatisticsSnapshot() {
    snapshot_time = std::chrono::steady_clock::now();
    start_time = snapshot_time;
    latency_histogram_buckets.fill(0);
  }

  explicit EngineStatisticsSnapshot(const EngineStatistics &stats) {
    total_executions = stats.total_executions.load(std::memory_order_relaxed);
    successful_executions =
        stats.successful_executions.load(std::memory_order_relaxed);
    failed_executions = stats.failed_executions.load(std::memory_order_relaxed);

    total_input_frames =
        stats.total_input_frames.load(std::memory_order_relaxed);
    total_output_frames =
        stats.total_output_frames.load(std::memory_order_relaxed);
    total_dropped_frames =
        stats.total_dropped_frames.load(std::memory_order_relaxed);

    total_queue_pushes =
        stats.total_queue_pushes.load(std::memory_order_relaxed);
    total_queue_pops = stats.total_queue_pops.load(std::memory_order_relaxed);
    queue_full_events = stats.queue_full_events.load(std::memory_order_relaxed);

    total_processing_time_us =
        stats.total_processing_time_us.load(std::memory_order_relaxed);
    total_wait_time_us =
        stats.total_wait_time_us.load(std::memory_order_relaxed);
    total_schedule_time_us =
        stats.total_schedule_time_us.load(std::memory_order_relaxed);

    for (std::size_t i = 0; i < LatencyHistogram::k_num_buckets; ++i) {
      latency_histogram_buckets[i] =
          stats.latency_histogram.buckets[i].load(std::memory_order_relaxed);
    }

    start_time = stats.start_time;
    snapshot_time = std::chrono::steady_clock::now();

    success_rate = stats.successRate();
    drop_rate = stats.dropRate();
    throughput = stats.throughput();
  }

  [[nodiscard]] std::uint64_t totalFramesProcessed() const {
    return total_output_frames;
  }

  [[nodiscard]] std::uint64_t totalFramesDropped() const {
    return total_dropped_frames;
  }

  [[nodiscard]] double inputOutputRatio() const {
    if (total_output_frames == 0)
      return 0.0;
    return static_cast<double>(total_input_frames) /
           static_cast<double>(total_output_frames);
  }

  [[nodiscard]] double avgProcessingTimeUs() const {
    if (successful_executions == 0)
      return 0.0;
    return static_cast<double>(total_processing_time_us) /
           static_cast<double>(successful_executions);
  }

  [[nodiscard]] double avgWaitTimeUs() const {
    if (total_queue_pops == 0)
      return 0.0;
    return static_cast<double>(total_wait_time_us) /
           static_cast<double>(total_queue_pops);
  }

  [[nodiscard]] double avgScheduleTimeUs() const {
    if (total_executions == 0)
      return 0.0;
    return static_cast<double>(total_schedule_time_us) /
           static_cast<double>(total_executions);
  }

  [[nodiscard]] double elapsedSeconds() const {
    auto elapsed = snapshot_time - start_time;
    return std::chrono::duration<double>(elapsed).count();
  }

  [[nodiscard]] std::vector<std::pair<std::string, double>>
  latencyPercentiles() const {
    std::vector<std::pair<std::string, double>> result;

    std::uint64_t total = 0;
    for (auto count : latency_histogram_buckets) {
      total += count;
    }

    if (total == 0) {
      result.emplace_back("p50", 0.0);
      result.emplace_back("p90", 0.0);
      result.emplace_back("p95", 0.0);
      result.emplace_back("p99", 0.0);
      result.emplace_back("p99.9", 0.0);
      return result;
    }

    auto getPercentile = [&](double percentile) -> double {
      std::uint64_t target = static_cast<std::uint64_t>(
          static_cast<double>(total) * percentile / 100.0);
      std::uint64_t cumulative = 0;

      for (std::size_t i = 0; i < LatencyHistogram::k_num_buckets; ++i) {
        cumulative += latency_histogram_buckets[i];
        if (cumulative >= target) {
          if (i < LatencyHistogram::k_bucket_bounds.size()) {
            return static_cast<double>(LatencyHistogram::k_bucket_bounds[i]);
          }
          return 500000.0;
        }
      }
      return 500000.0;
    };

    result.emplace_back("p50", getPercentile(50.0));
    result.emplace_back("p90", getPercentile(90.0));
    result.emplace_back("p95", getPercentile(95.0));
    result.emplace_back("p99", getPercentile(99.0));
    result.emplace_back("p99.9", getPercentile(99.9));

    return result;
  }

  [[nodiscard]] std::vector<std::pair<std::string, std::uint64_t>>
  histogramData() const {
    std::vector<std::pair<std::string, std::uint64_t>> result;
    result.reserve(LatencyHistogram::k_num_buckets);

    for (std::size_t i = 0; i < LatencyHistogram::k_num_buckets; ++i) {
      result.emplace_back(LatencyHistogram::getBucketLabel(i),
                          latency_histogram_buckets[i]);
    }

    return result;
  }
};

} // namespace ai_pipe

#endif // AI_PIPE_EXECUTION_TYPES_HPP
