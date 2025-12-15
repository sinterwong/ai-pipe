/**
 * @file thread_safe_queue.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Thread-safe queue implementations
 * @version 0.2
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_THREAD_SAFE_QUEUE_HPP
#define AI_PIPE_THREAD_SAFE_QUEUE_HPP

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <vector>

namespace ai_pipe {

/**
 * @brief Thread-safe FIFO queue with blocking and non-blocking operations
 * @tparam T The type of elements stored in the queue
 */
template <typename T> class ThreadSafeQueue {
public:
  ThreadSafeQueue() = default;
  ~ThreadSafeQueue() = default;

  // Non-copyable, non-movable for thread safety
  ThreadSafeQueue(const ThreadSafeQueue &) = delete;
  ThreadSafeQueue &operator=(const ThreadSafeQueue &) = delete;
  ThreadSafeQueue(ThreadSafeQueue &&) = delete;
  ThreadSafeQueue &operator=(ThreadSafeQueue &&) = delete;

  /**
   * @brief Push an element to the back of the queue
   * @param value The value to push
   */
  void push(T value) {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_queue.push(std::move(value));
    }
    m_condition.notify_one();
  }

  /**
   * @brief Try to pop an element without blocking
   * @return The popped element, or std::nullopt if queue is empty
   */
  [[nodiscard]] std::optional<T> tryPop() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_queue.empty()) {
      return std::nullopt;
    }

    T value = std::move(m_queue.front());
    m_queue.pop();
    return value;
  }

  /**
   * @brief Wait for an element with timeout
   * @param timeout Maximum time to wait
   * @return The popped element, or std::nullopt on timeout
   */
  [[nodiscard]] std::optional<T>
  waitPopFor(const std::chrono::milliseconds &timeout) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (!m_condition.wait_for(lock, timeout,
                              [this] { return !m_queue.empty(); })) {
      return std::nullopt;
    }

    T value = std::move(m_queue.front());
    m_queue.pop();
    return value;
  }

  /**
   * @brief Block until an element is available, then pop it
   * @return The popped element
   */
  [[nodiscard]] T waitPop() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_condition.wait(lock, [this] { return !m_queue.empty(); });

    T value = std::move(m_queue.front());
    m_queue.pop();
    return value;
  }

  /**
   * @brief Check if the queue is empty
   * @return true if empty
   */
  [[nodiscard]] bool empty() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.empty();
  }

  /**
   * @brief Get the current size of the queue
   * @return Number of elements in the queue
   */
  [[nodiscard]] std::size_t size() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.size();
  }

  /**
   * @brief Remove all elements from the queue
   */
  void clear() {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::queue<T> empty_queue;
    m_queue.swap(empty_queue);
  }

private:
  mutable std::mutex m_mutex;
  std::queue<T> m_queue;
  std::condition_variable m_condition;
};

/**
 * @brief Thread-safe priority queue with blocking and non-blocking operations
 * @tparam T The type of elements stored in the queue
 * @tparam Compare Comparison function for priority ordering
 */
template <typename T, typename Compare = std::less<T>>
class ThreadSafePriorityQueue {
public:
  ThreadSafePriorityQueue() = default;
  ~ThreadSafePriorityQueue() = default;

  // Non-copyable, non-movable for thread safety
  ThreadSafePriorityQueue(const ThreadSafePriorityQueue &) = delete;
  ThreadSafePriorityQueue &operator=(const ThreadSafePriorityQueue &) = delete;
  ThreadSafePriorityQueue(ThreadSafePriorityQueue &&) = delete;
  ThreadSafePriorityQueue &operator=(ThreadSafePriorityQueue &&) = delete;

  /**
   * @brief Push an element into the priority queue
   * @param value The value to push
   */
  void push(T value) {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_queue.push(std::move(value));
    }
    m_condition.notify_one();
  }

  /**
   * @brief Try to pop the highest-priority element without blocking
   * @return The popped element, or std::nullopt if queue is empty
   */
  [[nodiscard]] std::optional<T> tryPop() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_queue.empty()) {
      return std::nullopt;
    }

    // Note: std::priority_queue::top() returns const ref, need to copy
    T value = std::move(const_cast<T &>(m_queue.top()));
    m_queue.pop();
    return value;
  }

  /**
   * @brief Wait for an element with timeout
   * @param timeout Maximum time to wait
   * @return The popped element, or std::nullopt on timeout
   */
  [[nodiscard]] std::optional<T>
  waitPopFor(const std::chrono::milliseconds &timeout) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (!m_condition.wait_for(lock, timeout,
                              [this] { return !m_queue.empty(); })) {
      return std::nullopt;
    }

    T value = std::move(const_cast<T &>(m_queue.top()));
    m_queue.pop();
    return value;
  }

  /**
   * @brief Block until an element is available, then pop it
   * @return The popped element
   */
  [[nodiscard]] T waitPop() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_condition.wait(lock, [this] { return !m_queue.empty(); });

    T value = std::move(const_cast<T &>(m_queue.top()));
    m_queue.pop();
    return value;
  }

  /**
   * @brief Check if the queue is empty
   * @return true if empty
   */
  [[nodiscard]] bool empty() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.empty();
  }

  /**
   * @brief Get the current size of the queue
   * @return Number of elements in the queue
   */
  [[nodiscard]] std::size_t size() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_queue.size();
  }

  /**
   * @brief Remove all elements from the queue
   */
  void clear() {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::priority_queue<T, std::vector<T>, Compare> empty_queue;
    m_queue.swap(empty_queue);
  }

private:
  mutable std::mutex m_mutex;
  std::priority_queue<T, std::vector<T>, Compare> m_queue;
  std::condition_variable m_condition;
};

} // namespace ai_pipe

#endif // AI_PIPE_THREAD_SAFE_QUEUE_HPP