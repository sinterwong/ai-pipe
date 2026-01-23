/**
 * @file bounded_drop_queue.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-12-24
 *
 *  This queue implementation provides:
 * - Configurable capacity limits
 * - Pluggable drop strategies for overflow handling
 * - Drop event callbacks for monitoring
 * - Statistics tracking
 * - Thread-safe operations
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_BOUNDED_DROP_QUEUE_HPP
#define AI_PIPE_BOUNDED_DROP_QUEUE_HPP

#include "ai_pipe/frame_metadata.hpp"
#include "drop_strategy.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace ai_pipe {

// =============================================================================
// Queue Statistics
// =============================================================================

/**
 * @brief Statistics for queue monitoring
 */
struct QueueStatistics {
  std::atomic<std::uint64_t> total_pushed{0};
  std::atomic<std::uint64_t> total_popped{0};
  std::atomic<std::uint64_t> total_dropped{0};
  std::atomic<std::uint64_t> total_rejected{0};
  std::atomic<std::uint64_t> peak_size{0};
  std::chrono::steady_clock::time_point created_time{
      std::chrono::steady_clock::now()};

  void reset() {
    total_pushed.store(0, std::memory_order_relaxed);
    total_popped.store(0, std::memory_order_relaxed);
    total_dropped.store(0, std::memory_order_relaxed);
    total_rejected.store(0, std::memory_order_relaxed);
    peak_size.store(0, std::memory_order_relaxed);
    created_time = std::chrono::steady_clock::now();
  }

  [[nodiscard]] double dropRate() const {
    auto pushed = total_pushed.load(std::memory_order_relaxed);
    auto dropped = total_dropped.load(std::memory_order_relaxed);
    if (pushed == 0)
      return 0.0;
    return static_cast<double>(dropped) / static_cast<double>(pushed) * 100.0;
  }

  [[nodiscard]] double throughput() const {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - created_time)
                       .count();
    if (elapsed == 0)
      return 0.0;
    return static_cast<double>(total_popped.load(std::memory_order_relaxed)) /
           static_cast<double>(elapsed);
  }
};

/**
 * @brief Copyable snapshot of queue statistics
 */
struct QueueStatisticsSnapshot {
  std::uint64_t total_pushed{0};
  std::uint64_t total_popped{0};
  std::uint64_t total_dropped{0};
  std::uint64_t total_rejected{0};
  std::uint64_t peak_size{0};
  std::chrono::steady_clock::time_point created_time;

  QueueStatisticsSnapshot() = default;

  explicit QueueStatisticsSnapshot(const QueueStatistics &stats)
      : total_pushed(stats.total_pushed.load(std::memory_order_relaxed)),
        total_popped(stats.total_popped.load(std::memory_order_relaxed)),
        total_dropped(stats.total_dropped.load(std::memory_order_relaxed)),
        total_rejected(stats.total_rejected.load(std::memory_order_relaxed)),
        peak_size(stats.peak_size.load(std::memory_order_relaxed)),
        created_time(stats.created_time) {}

  [[nodiscard]] double dropRate() const {
    if (total_pushed == 0)
      return 0.0;
    return static_cast<double>(total_dropped) /
           static_cast<double>(total_pushed) * 100.0;
  }

  [[nodiscard]] double throughput() const {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - created_time)
                       .count();
    if (elapsed == 0)
      return 0.0;
    return static_cast<double>(total_popped) / static_cast<double>(elapsed);
  }
};

// =============================================================================
// Bounded Drop Queue Configuration
// =============================================================================

/**
 * @brief Configuration for BoundedDropQueue
 */
struct BoundedDropQueueConfig {
  std::size_t capacity = 16;    ///< Maximum queue size
  std::size_t target_size = 0;  ///< Target size after dropping (0 = capacity-1)
  bool drop_on_full = true;     ///< Drop old items when full (vs reject new)
  bool track_statistics = true; ///< Enable statistics collection
  std::string node_name;        ///< Owner node name (for logging)
  std::string port_name;        ///< Port name (for logging)
};

// =============================================================================
// Bounded Drop Queue
// =============================================================================

/**
 * @brief Thread-safe bounded queue with configurable drop strategy
 *
 * This queue is designed for backpressure scenarios where:
 * - Producers may be faster than consumers
 * - Old data should be dropped to prevent memory exhaustion
 * - Drop events need to be tracked and potentially coordinated
 *
 * @tparam T The type of items stored in the queue
 */
template <typename T> class BoundedDropQueue {
public:
  using value_type = T;
  using size_type = std::size_t;
  using StrategyPtr = std::unique_ptr<IDropStrategy<T>>;

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  /**
   * @brief Construct with capacity and default DropHead strategy
   */
  explicit BoundedDropQueue(size_type capacity)
      : m_config{.capacity = capacity},
        m_strategy(std::make_unique<DropHeadStrategy<T>>()) {}

  /**
   * @brief Construct with full configuration
   */
  explicit BoundedDropQueue(BoundedDropQueueConfig config)
      : m_config(std::move(config)),
        m_strategy(std::make_unique<DropHeadStrategy<T>>()) {
    if (m_config.target_size == 0) {
      m_config.target_size = m_config.capacity > 0 ? m_config.capacity - 1 : 0;
    }
  }

  /**
   * @brief Construct with capacity and custom strategy
   */
  BoundedDropQueue(size_type capacity, StrategyPtr strategy)
      : m_config{.capacity = capacity}, m_strategy(std::move(strategy)) {
    if (!m_strategy) {
      m_strategy = std::make_unique<DropHeadStrategy<T>>();
    }
  }

  ~BoundedDropQueue() = default;

  // Non-copyable
  BoundedDropQueue(const BoundedDropQueue &) = delete;
  BoundedDropQueue &operator=(const BoundedDropQueue &) = delete;

  // Movable
  BoundedDropQueue(BoundedDropQueue &&other) noexcept {
    std::lock_guard<std::mutex> lock(other.m_mutex);
    m_queue = std::move(other.m_queue);
    m_config = std::move(other.m_config);
    m_strategy = std::move(other.m_strategy);
    m_dropCallback = std::move(other.m_dropCallback);
    // Note: Statistics are not moved (start fresh)
  }

  BoundedDropQueue &operator=(BoundedDropQueue &&other) noexcept {
    if (this != &other) {
      std::scoped_lock lock(m_mutex, other.m_mutex);
      m_queue = std::move(other.m_queue);
      m_config = std::move(other.m_config);
      m_strategy = std::move(other.m_strategy);
      m_dropCallback = std::move(other.m_dropCallback);
    }
    return *this;
  }

  // -------------------------------------------------------------------------
  // Strategy Management
  // -------------------------------------------------------------------------

  /**
   * @brief Set the drop strategy
   */
  void setStrategy(StrategyPtr strategy) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_strategy = std::move(strategy);
    if (!m_strategy) {
      m_strategy = std::make_unique<DropHeadStrategy<T>>();
    }
  }

  /**
   * @brief Get strategy name
   */
  [[nodiscard]] std::string strategyName() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_strategy ? m_strategy->name() : "None";
  }

  // -------------------------------------------------------------------------
  // Callback Management
  // -------------------------------------------------------------------------

  /**
   * @brief Set callback for drop events
   */
  void setDropCallback(DropEventCallback callback) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_dropCallback = std::move(callback);
  }

  /**
   * @brief Set callback for frame ID extraction (used for drop events)
   *
   * Accessor can return either:
   * - std::optional<FrameId> directly
   * - std::shared_ptr<IFrameMetadata> from which frameId() is called
   */
  template <typename Accessor> void setFrameIdAccessor(Accessor accessor) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_frameIdAccessor = [accessor](const T &item) -> std::optional<FrameId> {
      auto result = accessor(item);
      // Check if result is already optional<FrameId>
      if constexpr (std::is_same_v<decltype(result), std::optional<FrameId>>) {
        return result;
      } else if constexpr (std::is_same_v<decltype(result), FrameId>) {
        return result;
      } else {
        // Assume it's a pointer-like type with frameId() method
        if (result) {
          return result->frameId();
        }
        return std::nullopt;
      }
    };
  }

  // -------------------------------------------------------------------------
  // Queue Operations
  // -------------------------------------------------------------------------

  /**
   * @brief Push an item to the queue
   *
   * If the queue is at capacity, the drop strategy determines which
   * items (if any) should be dropped to make room.
   *
   * @param item The item to push
   * @return true if item was accepted, false if rejected
   */
  bool push(T item) {
    std::vector<FrameId> dropped_frame_ids;
    bool accepted = false;

    {
      std::lock_guard<std::mutex> lock(m_mutex);

      // Track statistics
      if (m_config.track_statistics) {
        m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
      }

      // Check if we need to apply drop strategy
      if (m_queue.size() >= m_config.capacity) {
        // Ask strategy if we should accept the incoming item
        if (!m_strategy->shouldAcceptIncoming(m_queue, item,
                                              m_config.capacity)) {
          if (m_config.track_statistics) {
            m_stats.total_rejected.fetch_add(1, std::memory_order_relaxed);
          }
          // Don't call callback while holding lock
          return false;
        }

        // Get indices to drop
        auto drop_indices = m_strategy->selectDropIndices(
            m_queue, item, m_config.capacity, m_config.target_size);

        if (!drop_indices.empty()) {
          // Collect frame IDs for callback before dropping
          dropped_frame_ids = collectFrameIds(drop_indices);

          // Sort indices in descending order to remove from back to front
          std::sort(drop_indices.begin(), drop_indices.end(),
                    std::greater<size_type>());

          // Remove items at specified indices
          for (auto idx : drop_indices) {
            if (idx < m_queue.size()) {
              m_queue.erase(m_queue.begin() + static_cast<std::ptrdiff_t>(idx));
            }
          }

          if (m_config.track_statistics) {
            m_stats.total_dropped.fetch_add(drop_indices.size(),
                                            std::memory_order_relaxed);
          }
        }
      }

      // Add the new item
      m_queue.push_back(std::move(item));
      accepted = true;

      // Update peak size
      if (m_config.track_statistics) {
        size_type current_size = m_queue.size();
        size_type peak = m_stats.peak_size.load(std::memory_order_relaxed);
        while (current_size > peak &&
               !m_stats.peak_size.compare_exchange_weak(
                   peak, current_size, std::memory_order_relaxed)) {
        }
      }
    }

    // Notify consumers
    m_condition.notify_one();

    // Call drop callbacks outside the lock
    if (!dropped_frame_ids.empty() && m_dropCallback) {
      for (FrameId frame_id : dropped_frame_ids) {
        DropEvent event{
            .frame_id = frame_id,
            .stream_id = frame_constants::k_default_stream_id,
            .drop_time = std::chrono::steady_clock::now(),
            .node_name = m_config.node_name,
            .port_name = m_config.port_name,
            .reason = m_strategy->name() + " overflow",
            .queue_size_before = m_config.capacity,
            .queue_size_after = m_queue.size(),
            .total_drops =
                m_stats.total_dropped.load(std::memory_order_relaxed),
        };
        m_dropCallback(event);
      }
    }

    return accepted;
  }

  /**
   * @brief Push with explicit frame ID (convenience method)
   */
  bool push(T item, FrameId frame_id) {
    (void)frame_id; // Frame ID is stored in item's metadata
    return push(std::move(item));
  }

  /**
   * @brief Try to pop an item without blocking
   * @return The item, or std::nullopt if queue is empty
   */
  [[nodiscard]] std::optional<T> tryPop() {
    std::lock_guard<std::mutex> lock(m_mutex);

    if (m_queue.empty()) {
      return std::nullopt;
    }

    T item = std::move(m_queue.front());
    m_queue.pop_front();

    if (m_config.track_statistics) {
      m_stats.total_popped.fetch_add(1, std::memory_order_relaxed);
    }

    return item;
  }

  /**
   * @brief Wait for an item with timeout
   * @param timeout Maximum time to wait
   * @return The item, or std::nullopt on timeout
   */
  [[nodiscard]] std::optional<T> waitPop(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(m_mutex);

    if (!m_condition.wait_for(lock, timeout,
                              [this] { return !m_queue.empty(); })) {
      return std::nullopt;
    }

    T item = std::move(m_queue.front());
    m_queue.pop_front();

    if (m_config.track_statistics) {
      m_stats.total_popped.fetch_add(1, std::memory_order_relaxed);
    }

    return item;
  }

  /**
   * @brief Block until an item is available
   * @return The popped item
   */
  [[nodiscard]] T waitPop() {
    std::unique_lock<std::mutex> lock(m_mutex);

    m_condition.wait(lock, [this] { return !m_queue.empty(); });

    T item = std::move(m_queue.front());
    m_queue.pop_front();

    if (m_config.track_statistics) {
      m_stats.total_popped.fetch_add(1, std::memory_order_relaxed);
    }

    return item;
  }

  /**
   * @brief Peek at the front item without removing it
   * @return Pointer to front item, or nullptr if empty
   */
  [[nodiscard]] const T *peek() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_queue.empty()) {
      return nullptr;
    }
    return &m_queue.front();
  }

  /**
   * @brief Get frame ID of front item (if available)
   */
  [[nodiscard]] std::optional<FrameId> peekFrameId() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_queue.empty() || !m_frameIdAccessor) {
      return std::nullopt;
    }
    return m_frameIdAccessor(m_queue.front());
  }

  // -------------------------------------------------------------------------
  // Batch Operations
  // -------------------------------------------------------------------------

  /**
   * @brief Pop multiple items at once
   * @param max_count Maximum number of items to pop
   * @return Vector of popped items
   */
  [[nodiscard]] std::vector<T> popBatch(size_type max_count) {
    std::lock_guard<std::mutex> lock(m_mutex);

    std::vector<T> result;
    size_type count = std::min(max_count, m_queue.size());
    result.reserve(count);

    for (size_type i = 0; i < count; ++i) {
      result.push_back(std::move(m_queue.front()));
      m_queue.pop_front();
    }

    if (m_config.track_statistics) {
      m_stats.total_popped.fetch_add(count, std::memory_order_relaxed);
    }

    return result;
  }

  /**
   * @brief Drop all items with frame ID less than the specified value
   * @param min_frame_id Minimum frame ID to keep
   * @return Number of items dropped
   */
  size_type dropBefore(FrameId min_frame_id) {
    if (!m_frameIdAccessor) {
      return 0;
    }

    std::vector<FrameId> dropped_ids;
    size_type drop_count = 0;

    {
      std::lock_guard<std::mutex> lock(m_mutex);

      auto new_end =
          std::remove_if(m_queue.begin(), m_queue.end(), [&](const T &item) {
            auto frame_id = m_frameIdAccessor(item);
            if (frame_id.has_value() && frame_id.value() < min_frame_id) {
              dropped_ids.push_back(frame_id.value());
              return true;
            }
            return false;
          });

      drop_count =
          static_cast<size_type>(std::distance(new_end, m_queue.end()));
      m_queue.erase(new_end, m_queue.end());

      if (m_config.track_statistics) {
        m_stats.total_dropped.fetch_add(drop_count, std::memory_order_relaxed);
      }
    }

    // Callbacks outside lock
    if (m_dropCallback) {
      for (FrameId frame_id : dropped_ids) {
        DropEvent event{
            .frame_id = frame_id,
            .drop_time = std::chrono::steady_clock::now(),
            .node_name = m_config.node_name,
            .port_name = m_config.port_name,
            .reason =
                "Sync drop (before frame " + std::to_string(min_frame_id) + ")",
            .total_drops =
                m_stats.total_dropped.load(std::memory_order_relaxed),
        };
        m_dropCallback(event);
      }
    }

    return drop_count;
  }

  /**
   * @brief Drop items matching a predicate
   * @param predicate Function returning true for items to drop
   * @return Number of items dropped
   */
  template <typename Predicate> size_type dropIf(Predicate predicate) {
    size_type drop_count = 0;

    {
      std::lock_guard<std::mutex> lock(m_mutex);

      auto new_end = std::remove_if(m_queue.begin(), m_queue.end(), predicate);
      drop_count =
          static_cast<size_type>(std::distance(new_end, m_queue.end()));
      m_queue.erase(new_end, m_queue.end());

      if (m_config.track_statistics) {
        m_stats.total_dropped.fetch_add(drop_count, std::memory_order_relaxed);
      }
    }

    return drop_count;
  }

  // -------------------------------------------------------------------------
  // Query Operations
  // -------------------------------------------------------------------------

  /**
   * @brief Check if queue is empty
   */
  [[nodiscard]] bool empty() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.empty();
  }

  /**
   * @brief Get current queue size
   */
  [[nodiscard]] size_type size() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.size();
  }

  /**
   * @brief Get queue capacity
   */
  [[nodiscard]] size_type capacity() const { return m_config.capacity; }

  /**
   * @brief Get fill ratio (size / capacity)
   */
  [[nodiscard]] double fillRatio() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_config.capacity == 0)
      return 0.0;
    return static_cast<double>(m_queue.size()) /
           static_cast<double>(m_config.capacity);
  }

  /**
   * @brief Check if queue is at or over capacity
   */
  [[nodiscard]] bool isFull() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.size() >= m_config.capacity;
  }

  // -------------------------------------------------------------------------
  // Configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Update capacity
   */
  void setCapacity(size_type new_capacity) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_config.capacity = new_capacity;
    if (m_config.target_size == 0 || m_config.target_size >= new_capacity) {
      m_config.target_size = new_capacity > 0 ? new_capacity - 1 : 0;
    }
  }

  /**
   * @brief Get configuration
   */
  [[nodiscard]] const BoundedDropQueueConfig &config() const {
    return m_config;
  }

  // -------------------------------------------------------------------------
  // Statistics
  // -------------------------------------------------------------------------

  /**
   * @brief Get queue statistics
   */
  [[nodiscard]] const QueueStatistics &statistics() const { return m_stats; }

  /**
   * @brief Get copyable statistics snapshot
   */
  [[nodiscard]] QueueStatisticsSnapshot statisticsSnapshot() const {
    return QueueStatisticsSnapshot(m_stats);
  }

  /**
   * @brief Reset statistics
   */
  void resetStatistics() { m_stats.reset(); }

  // -------------------------------------------------------------------------
  // Clear
  // -------------------------------------------------------------------------

  /**
   * @brief Remove all items from the queue
   */
  void clear() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_queue.clear();
  }

private:
  /**
   * @brief Collect frame IDs from items at given indices
   */
  std::vector<FrameId>
  collectFrameIds(const std::vector<size_type> &indices) const {
    std::vector<FrameId> ids;
    if (!m_frameIdAccessor) {
      return ids;
    }

    ids.reserve(indices.size());
    for (auto idx : indices) {
      if (idx < m_queue.size()) {
        auto frame_id = m_frameIdAccessor(m_queue[idx]);
        if (frame_id.has_value()) {
          ids.push_back(frame_id.value());
        }
      }
    }
    return ids;
  }

private:
  // Queue storage
  std::deque<T> m_queue;

  // Configuration
  BoundedDropQueueConfig m_config;

  // Drop strategy
  StrategyPtr m_strategy;

  // Callbacks
  DropEventCallback m_dropCallback;
  std::function<std::optional<FrameId>(const T &)> m_frameIdAccessor;

  // Statistics
  QueueStatistics m_stats;

  // Thread safety
  mutable std::mutex m_mutex;
  std::condition_variable m_condition;
};

// =============================================================================
// Queue Factory
// =============================================================================

/**
 * @brief Factory for creating bounded drop queues with common configurations
 */
template <typename T> class BoundedDropQueueFactory {
public:
  /**
   * @brief Create queue with DropHead strategy (keep latest)
   */
  static BoundedDropQueue<T> createDropHead(std::size_t capacity) {
    return BoundedDropQueue<T>(capacity,
                               std::make_unique<DropHeadStrategy<T>>());
  }

  /**
   * @brief Create queue with DropTail strategy (reject new)
   */
  static BoundedDropQueue<T> createDropTail(std::size_t capacity) {
    return BoundedDropQueue<T>(capacity,
                               std::make_unique<DropTailStrategy<T>>());
  }

  /**
   * @brief Create queue with KeepLatestN strategy
   */
  static BoundedDropQueue<T> createKeepLatest(std::size_t capacity,
                                              std::size_t keep_n) {
    return BoundedDropQueue<T>(
        capacity, std::make_unique<KeepLatestNStrategy<T>>(keep_n));
  }

  /**
   * @brief Create queue with Adaptive strategy
   */
  static BoundedDropQueue<T> createAdaptive(std::size_t capacity) {
    return BoundedDropQueue<T>(capacity,
                               std::make_unique<AdaptiveDropStrategy<T>>());
  }
};

} // namespace ai_pipe

#endif // AI_PIPE_BOUNDED_DROP_QUEUE_HPP