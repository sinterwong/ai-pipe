/**
 * @file drip_strategy.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-12-24
 *
 * This file defines the strategy pattern for queue overflow handling.
 * Different strategies can be used based on application requirements:
 * - DropHead: Drop oldest items (keep latest N)
 * - DropTail: Reject newest items (keep oldest N)
 * - DropByPriority: Drop based on priority scores
 * - Custom strategies via the IDropStrategy interface
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_DROP_STRATEGY_HPP
#define AI_PIPE_DROP_STRATEGY_HPP

#include "frame_metadata.hpp"
#include <algorithm>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace ai_pipe {

// Forward declarations
template <typename T> class BoundedDropQueue;

// =============================================================================
// Drop Event Information
// =============================================================================

/**
 * @brief Information about a drop event for callbacks/logging
 */
struct DropEvent {
  FrameId frame_id{frame_constants::k_invalid_frame_id};
  StreamId stream_id{frame_constants::k_default_stream_id};
  Timestamp drop_time{std::chrono::steady_clock::now()};
  std::string node_name;
  std::string port_name;
  std::string reason;
  std::size_t queue_size_before{0};
  std::size_t queue_size_after{0};
  std::size_t total_drops{0};

  [[nodiscard]] std::string toString() const {
    return "DropEvent{frame=" + std::to_string(frame_id) +
           ", stream=" + std::to_string(stream_id) + ", node=" + node_name +
           ", port=" + port_name + ", reason=" + reason + "}";
  }
};

/**
 * @brief Callback type for drop event notifications
 */
using DropEventCallback = std::function<void(const DropEvent &)>;

// =============================================================================
// Drop Strategy Interface
// =============================================================================

/**
 * @brief Abstract interface for queue drop strategies
 *
 * Implementations define how items are selected for dropping when a
 * bounded queue reaches capacity. The strategy receives information
 * about both the incoming item and the current queue state.
 *
 * @tparam T The type of items in the queue
 */
template <typename T> class IDropStrategy {
public:
  virtual ~IDropStrategy() = default;

  /**
   * @brief Determine items to drop when queue is at capacity
   *
   * @param queue Current queue contents (read-only view)
   * @param incoming The new item attempting to enter the queue
   * @param capacity Maximum queue capacity
   * @param target_size Target size after dropping (usually capacity - 1)
   * @return Indices of items to drop (from queue), or empty to reject incoming
   */
  [[nodiscard]] virtual std::vector<std::size_t>
  selectDropIndices(const std::deque<T> &queue, const T &incoming,
                    std::size_t capacity, std::size_t target_size) const = 0;

  /**
   * @brief Check if incoming item should be accepted when queue is full
   *
   * Called before selectDropIndices to allow immediate rejection.
   *
   * @param queue Current queue contents
   * @param incoming The new item
   * @param capacity Maximum queue capacity
   * @return true if the item should be considered for insertion
   */
  [[nodiscard]] virtual bool shouldAcceptIncoming(const std::deque<T> &queue,
                                                  const T &incoming,
                                                  std::size_t capacity) const {
    (void)queue;
    (void)incoming;
    (void)capacity;
    return true; // Default: always consider incoming
  }

  /**
   * @brief Get strategy name for logging
   */
  [[nodiscard]] virtual std::string name() const = 0;

  /**
   * @brief Create a clone of this strategy
   */
  [[nodiscard]] virtual std::unique_ptr<IDropStrategy<T>> clone() const = 0;
};

// =============================================================================
// Drop Head Strategy (Keep Latest N)
// =============================================================================

/**
 * @brief Drop oldest items to make room for new ones
 *
 * This is the most common strategy for real-time streaming applications
 * where the latest data is most valuable. When the queue reaches capacity,
 * the oldest items at the head of the queue are dropped.
 *
 * @tparam T The type of items in the queue
 */
template <typename T> class DropHeadStrategy final : public IDropStrategy<T> {
public:
  /**
   * @brief Number of items to keep when dropping
   * @param keep_count Number of items to retain (default: capacity - 1)
   */
  explicit DropHeadStrategy(
      std::optional<std::size_t> keep_count = std::nullopt)
      : m_keepCount(keep_count) {}

  [[nodiscard]] std::vector<std::size_t>
  selectDropIndices(const std::deque<T> &queue, const T &incoming,
                    std::size_t capacity,
                    std::size_t target_size) const override {
    (void)incoming;

    std::vector<std::size_t> indices;

    // Calculate how many to drop
    std::size_t keep = m_keepCount.value_or(target_size);
    keep = std::min(keep, capacity - 1); // Always leave room for incoming

    if (queue.size() <= keep) {
      return indices; // Nothing to drop
    }

    std::size_t drop_count = queue.size() - keep;

    // Drop from the head (oldest items)
    indices.reserve(drop_count);
    for (std::size_t i = 0; i < drop_count; ++i) {
      indices.push_back(i);
    }

    return indices;
  }

  [[nodiscard]] std::string name() const override { return "DropHead"; }

  [[nodiscard]] std::unique_ptr<IDropStrategy<T>> clone() const override {
    return std::make_unique<DropHeadStrategy<T>>(m_keepCount);
  }

private:
  std::optional<std::size_t> m_keepCount;
};

// =============================================================================
// Drop Tail Strategy (Reject New)
// =============================================================================

/**
 * @brief Reject new items when queue is full
 *
 * Preserves older data at the cost of potentially losing newer updates.
 * Useful when all data must be processed in order.
 *
 * @tparam T The type of items in the queue
 */
template <typename T> class DropTailStrategy final : public IDropStrategy<T> {
public:
  [[nodiscard]] std::vector<std::size_t>
  selectDropIndices(const std::deque<T> &queue, const T &incoming,
                    std::size_t capacity,
                    std::size_t target_size) const override {
    (void)incoming;
    (void)capacity;
    (void)target_size;
    (void)queue;

    // Return empty to indicate incoming should be rejected
    return {};
  }

  [[nodiscard]] bool shouldAcceptIncoming(const std::deque<T> &queue,
                                          const T &incoming,
                                          std::size_t capacity) const override {
    (void)incoming;
    // Reject when at or over capacity
    return queue.size() < capacity;
  }

  [[nodiscard]] std::string name() const override { return "DropTail"; }

  [[nodiscard]] std::unique_ptr<IDropStrategy<T>> clone() const override {
    return std::make_unique<DropTailStrategy<T>>();
  }
};

// =============================================================================
// Keep Latest N Strategy
// =============================================================================

/**
 * @brief Keep only the latest N items
 *
 * More aggressive than DropHead - ensures queue never exceeds N items
 * regardless of how fast items arrive.
 *
 * @tparam T The type of items in the queue
 */
template <typename T>
class KeepLatestNStrategy final : public IDropStrategy<T> {
public:
  explicit KeepLatestNStrategy(std::size_t n) : m_keepCount(n) {
    if (n == 0) {
      throw std::invalid_argument("KeepLatestNStrategy: N must be > 0");
    }
  }

  [[nodiscard]] std::vector<std::size_t>
  selectDropIndices(const std::deque<T> &queue, const T &incoming,
                    std::size_t capacity,
                    std::size_t target_size) const override {
    (void)incoming;
    (void)capacity;
    (void)target_size;

    std::vector<std::size_t> indices;

    // Keep at most m_keepCount - 1 to make room for incoming
    std::size_t keep = m_keepCount > 0 ? m_keepCount - 1 : 0;

    if (queue.size() <= keep) {
      return indices;
    }

    std::size_t drop_count = queue.size() - keep;
    indices.reserve(drop_count);

    for (std::size_t i = 0; i < drop_count; ++i) {
      indices.push_back(i);
    }

    return indices;
  }

  [[nodiscard]] std::string name() const override {
    return "KeepLatest" + std::to_string(m_keepCount);
  }

  [[nodiscard]] std::unique_ptr<IDropStrategy<T>> clone() const override {
    return std::make_unique<KeepLatestNStrategy<T>>(m_keepCount);
  }

private:
  std::size_t m_keepCount;
};

// =============================================================================
// Frame-Aware Drop Strategy
// =============================================================================

/**
 * @brief Drop strategy that uses frame metadata for intelligent dropping
 *
 * This strategy accesses frame metadata from queue items to make
 * drop decisions based on frame IDs and timestamps.
 *
 * @tparam T The type of items in the queue (must have frame metadata accessor)
 * @tparam MetadataAccessor Functor to extract IFrameMetadata from T
 */
template <typename T, typename MetadataAccessor>
class FrameAwareDropStrategy final : public IDropStrategy<T> {
public:
  /**
   * @brief Drop mode enumeration
   */
  enum class Mode {
    DropOldestFrames, ///< Drop items with lowest frame IDs
    DropByTimestamp,  ///< Drop items with oldest timestamps
    DropDuplicates    ///< Drop items with duplicate frame IDs (keep latest)
  };

  explicit FrameAwareDropStrategy(Mode mode, MetadataAccessor accessor,
                                  std::size_t keep_count = 1)
      : m_mode(mode), m_accessor(std::move(accessor)), m_keepCount(keep_count) {
  }

  [[nodiscard]] std::vector<std::size_t>
  selectDropIndices(const std::deque<T> &queue, const T &incoming,
                    std::size_t capacity,
                    std::size_t target_size) const override {
    (void)capacity;

    std::vector<std::size_t> indices;

    if (queue.empty()) {
      return indices;
    }

    switch (m_mode) {
    case Mode::DropOldestFrames:
      return selectByOldestFrames(queue, incoming, target_size);

    case Mode::DropByTimestamp:
      return selectByOldestTimestamps(queue, incoming, target_size);

    case Mode::DropDuplicates:
      return selectDuplicates(queue, incoming);

    default:
      return indices;
    }
  }

  [[nodiscard]] std::string name() const override {
    switch (m_mode) {
    case Mode::DropOldestFrames:
      return "FrameAware:DropOldestFrames";
    case Mode::DropByTimestamp:
      return "FrameAware:DropByTimestamp";
    case Mode::DropDuplicates:
      return "FrameAware:DropDuplicates";
    default:
      return "FrameAware:Unknown";
    }
  }

  [[nodiscard]] std::unique_ptr<IDropStrategy<T>> clone() const override {
    return std::make_unique<FrameAwareDropStrategy<T, MetadataAccessor>>(
        m_mode, m_accessor, m_keepCount);
  }

private:
  std::vector<std::size_t> selectByOldestFrames(const std::deque<T> &queue,
                                                const T &incoming,
                                                std::size_t target_size) const {
    (void)incoming;

    std::vector<std::pair<std::size_t, FrameId>> indexed_frames;
    indexed_frames.reserve(queue.size());

    for (std::size_t i = 0; i < queue.size(); ++i) {
      auto metadata = m_accessor(queue[i]);
      if (metadata) {
        indexed_frames.emplace_back(i, metadata->frameId());
      }
    }

    // Sort by frame ID (ascending)
    std::sort(indexed_frames.begin(), indexed_frames.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });

    // Select oldest frames to drop
    std::vector<std::size_t> indices;
    std::size_t keep = std::min(m_keepCount, target_size);
    std::size_t drop_count = queue.size() > keep ? queue.size() - keep : 0;

    for (std::size_t i = 0; i < drop_count && i < indexed_frames.size(); ++i) {
      indices.push_back(indexed_frames[i].first);
    }

    return indices;
  }

  std::vector<std::size_t>
  selectByOldestTimestamps(const std::deque<T> &queue, const T &incoming,
                           std::size_t target_size) const {
    (void)incoming;

    std::vector<std::pair<std::size_t, Timestamp>> indexed_times;
    indexed_times.reserve(queue.size());

    for (std::size_t i = 0; i < queue.size(); ++i) {
      auto metadata = m_accessor(queue[i]);
      if (metadata) {
        indexed_times.emplace_back(i, metadata->timestamp());
      }
    }

    // Sort by timestamp (ascending = oldest first)
    std::sort(indexed_times.begin(), indexed_times.end(),
              [](const auto &a, const auto &b) { return a.second < b.second; });

    std::vector<std::size_t> indices;
    std::size_t keep = std::min(m_keepCount, target_size);
    std::size_t drop_count = queue.size() > keep ? queue.size() - keep : 0;

    for (std::size_t i = 0; i < drop_count && i < indexed_times.size(); ++i) {
      indices.push_back(indexed_times[i].first);
    }

    return indices;
  }

  std::vector<std::size_t> selectDuplicates(const std::deque<T> &queue,
                                            const T &incoming) const {
    std::vector<std::size_t> indices;

    // Get incoming frame ID
    auto incoming_meta = m_accessor(incoming);
    if (!incoming_meta) {
      return indices;
    }

    FrameId incoming_frame = incoming_meta->frameId();

    // Find all items with the same frame ID (duplicates)
    for (std::size_t i = 0; i < queue.size(); ++i) {
      auto metadata = m_accessor(queue[i]);
      if (metadata && metadata->frameId() == incoming_frame) {
        indices.push_back(i);
      }
    }

    return indices;
  }

  Mode m_mode;
  MetadataAccessor m_accessor;
  std::size_t m_keepCount;
};

// =============================================================================
// Adaptive Drop Strategy
// =============================================================================

/**
 * @brief Adaptive strategy that adjusts behavior based on queue pressure
 *
 * Provides different drop behaviors based on queue fill level:
 * - Low pressure (<50%): No drops
 * - Medium pressure (50-80%): Gentle dropping
 * - High pressure (>80%): Aggressive dropping
 *
 * @tparam T The type of items in the queue
 */
template <typename T>
class AdaptiveDropStrategy final : public IDropStrategy<T> {
public:
  /**
   * @brief Configure pressure thresholds (as fractions of capacity)
   */
  struct Config {
    double medium_threshold = 0.5; ///< Start gentle dropping
    double high_threshold = 0.8;   ///< Start aggressive dropping
    std::size_t gentle_keep =
        0; ///< Items to keep in medium pressure (0 = auto)
    std::size_t aggressive_keep = 1; ///< Items to keep in high pressure
  };

  explicit AdaptiveDropStrategy(Config config = {}) : m_config(config) {}

  [[nodiscard]] std::vector<std::size_t>
  selectDropIndices(const std::deque<T> &queue, const T &incoming,
                    std::size_t capacity,
                    std::size_t target_size) const override {
    (void)incoming;

    std::vector<std::size_t> indices;

    if (queue.empty() || capacity == 0) {
      return indices;
    }

    double fill_ratio =
        static_cast<double>(queue.size()) / static_cast<double>(capacity);

    std::size_t keep_count;

    if (fill_ratio >= m_config.high_threshold) {
      // High pressure: aggressive dropping
      keep_count = m_config.aggressive_keep;
    } else if (fill_ratio >= m_config.medium_threshold) {
      // Medium pressure: gentle dropping
      keep_count = m_config.gentle_keep > 0 ? m_config.gentle_keep
                                            : (target_size * 3 / 4); // Keep 75%
    } else {
      // Low pressure: no dropping
      return indices;
    }

    keep_count = std::min(keep_count, target_size);

    if (queue.size() <= keep_count) {
      return indices;
    }

    std::size_t drop_count = queue.size() - keep_count;
    indices.reserve(drop_count);

    // Drop from head (oldest first)
    for (std::size_t i = 0; i < drop_count; ++i) {
      indices.push_back(i);
    }

    return indices;
  }

  [[nodiscard]] std::string name() const override { return "Adaptive"; }

  [[nodiscard]] std::unique_ptr<IDropStrategy<T>> clone() const override {
    return std::make_unique<AdaptiveDropStrategy<T>>(m_config);
  }

private:
  Config m_config;
};

// =============================================================================
// Composite Drop Strategy
// =============================================================================

/**
 * @brief Combines multiple strategies with fallback logic
 *
 * Tries strategies in order until one returns non-empty drop indices.
 *
 * @tparam T The type of items in the queue
 */
template <typename T>
class CompositeDropStrategy final : public IDropStrategy<T> {
public:
  void addStrategy(std::unique_ptr<IDropStrategy<T>> strategy) {
    m_strategies.push_back(std::move(strategy));
  }

  [[nodiscard]] std::vector<std::size_t>
  selectDropIndices(const std::deque<T> &queue, const T &incoming,
                    std::size_t capacity,
                    std::size_t target_size) const override {
    for (const auto &strategy : m_strategies) {
      auto indices =
          strategy->selectDropIndices(queue, incoming, capacity, target_size);
      if (!indices.empty()) {
        return indices;
      }
    }
    return {};
  }

  [[nodiscard]] bool shouldAcceptIncoming(const std::deque<T> &queue,
                                          const T &incoming,
                                          std::size_t capacity) const override {
    // All strategies must accept
    for (const auto &strategy : m_strategies) {
      if (!strategy->shouldAcceptIncoming(queue, incoming, capacity)) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] std::string name() const override {
    std::string result = "Composite[";
    for (std::size_t i = 0; i < m_strategies.size(); ++i) {
      if (i > 0)
        result += ", ";
      result += m_strategies[i]->name();
    }
    result += "]";
    return result;
  }

  [[nodiscard]] std::unique_ptr<IDropStrategy<T>> clone() const override {
    auto cloned = std::make_unique<CompositeDropStrategy<T>>();
    for (const auto &strategy : m_strategies) {
      cloned->addStrategy(strategy->clone());
    }
    return cloned;
  }

private:
  std::vector<std::unique_ptr<IDropStrategy<T>>> m_strategies;
};

// =============================================================================
// Strategy Factory
// =============================================================================

/**
 * @brief Factory for creating common drop strategies
 */
template <typename T> class DropStrategyFactory {
public:
  /**
   * @brief Create a DropHead strategy (most common for real-time)
   */
  static std::unique_ptr<IDropStrategy<T>> createDropHead() {
    return std::make_unique<DropHeadStrategy<T>>();
  }

  /**
   * @brief Create a DropTail strategy (reject new when full)
   */
  static std::unique_ptr<IDropStrategy<T>> createDropTail() {
    return std::make_unique<DropTailStrategy<T>>();
  }

  /**
   * @brief Create a KeepLatestN strategy
   */
  static std::unique_ptr<IDropStrategy<T>> createKeepLatest(std::size_t n) {
    return std::make_unique<KeepLatestNStrategy<T>>(n);
  }

  /**
   * @brief Create an Adaptive strategy with default config
   */
  static std::unique_ptr<IDropStrategy<T>> createAdaptive() {
    return std::make_unique<AdaptiveDropStrategy<T>>();
  }

  /**
   * @brief Create strategy by name
   */
  static std::unique_ptr<IDropStrategy<T>> createByName(const std::string &name,
                                                        std::size_t param = 1) {
    if (name == "DropHead" || name == "drop_head") {
      return createDropHead();
    } else if (name == "DropTail" || name == "drop_tail") {
      return createDropTail();
    } else if (name == "KeepLatest" || name == "keep_latest") {
      return createKeepLatest(param);
    } else if (name == "Adaptive" || name == "adaptive") {
      return createAdaptive();
    }
    throw std::invalid_argument("Unknown drop strategy: " + name);
  }
};

} // namespace ai_pipe

#endif // AI_PIPE_DROP_STRATEGY_HPP