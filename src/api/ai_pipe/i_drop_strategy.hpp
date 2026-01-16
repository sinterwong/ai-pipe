#ifndef AI_PIPE_I_DROP_STRATEGY_HPP
#define AI_PIPE_I_DROP_STRATEGY_HPP

#include "ai_pipe/frame_metadata.hpp"
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace ai_pipe {

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
} // namespace ai_pipe
#endif