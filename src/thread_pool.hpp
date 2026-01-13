/**
 * @file thread_pool.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Modern thread pool implementation with graceful shutdown support
 * @version 0.3
 * @date 2025-09-01
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_THREAD_POOL_HPP
#define AI_PIPE_THREAD_POOL_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace ai_pipe {

/**
 * @brief Thread pool configuration options
 */
struct ThreadPoolConfig {
  std::size_t num_threads = std::thread::hardware_concurrency();
  std::size_t max_queue_size = 1024;
  std::chrono::milliseconds submit_timeout{5000};
  bool wait_for_tasks_on_stop = true; // Graceful shutdown by default
};

/**
 * @brief Exception handler callback type
 */
using TaskExceptionHandler =
    std::function<void(const std::exception_ptr &, const std::string &)>;

/**
 * @brief Modern thread pool with bounded queue and graceful shutdown
 *
 * Features:
 * - Bounded task queue with configurable size
 * - Graceful and immediate shutdown modes
 * - Task submission timeout
 * - Custom exception handling
 * - Thread-safe statistics
 */
class ThreadPool {
public:
  /**
   * @brief Construct with number of threads and optional max queue size
   * @param num_threads Number of worker threads
   * @param max_queue_size Maximum pending tasks (0 = unlimited)
   */
  explicit ThreadPool(std::size_t num_threads,
                      std::size_t max_queue_size = 1024)
      : m_config{num_threads, max_queue_size, std::chrono::milliseconds{5000},
                 true},
        m_state{State::KRunning}, m_pendingTasks{0}, m_completedTasks{0} {
    startWorkers(num_threads);
  }

  /**
   * @brief Construct with full configuration
   * @param config Thread pool configuration
   */
  explicit ThreadPool(const ThreadPoolConfig &config)
      : m_config{config}, m_state{State::KRunning}, m_pendingTasks{0},
        m_completedTasks{0} {
    startWorkers(config.num_threads);
  }

  ~ThreadPool() { shutdown(m_config.wait_for_tasks_on_stop); }

  // Non-copyable, non-movable
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  /**
   * @brief Submit a task for execution
   * @tparam F Callable type
   * @tparam Args Argument types
   * @param func Callable object
   * @param args Arguments to pass to the callable
   * @return Future for the task result
   * @throws std::runtime_error if pool is stopped or queue is full
   */
  template <typename F, typename... Args>
  [[nodiscard]] auto submit(F &&func, Args &&...args)
      -> std::future<std::invoke_result_t<F, Args...>> {
    using ReturnType = std::invoke_result_t<F, Args...>;

    if (!isRunning()) {
      throw std::runtime_error("ThreadPool: Cannot submit to stopped pool");
    }

    // Create bound callable with perfect forwarding
    auto bound_task = [f = std::forward<F>(func),
                       ... captured_args = std::forward<Args>(args)]() mutable {
      return std::invoke(
          std::forward<decltype(f)>(f),
          std::forward<decltype(captured_args)>(captured_args)...);
    };

    auto packaged = std::make_shared<std::packaged_task<ReturnType()>>(
        std::move(bound_task));
    std::future<ReturnType> result = packaged->get_future();

    enqueueTask([packaged = std::move(packaged)]() { (*packaged)(); });

    return result;
  }

  /**
   * @brief Try to submit a task without blocking
   * @return Future if successful, nullopt if queue is full or pool stopped
   */
  template <typename F, typename... Args>
  [[nodiscard]] auto trySubmit(F &&func, Args &&...args)
      -> std::optional<std::future<std::invoke_result_t<F, Args...>>> {
    using ReturnType = std::invoke_result_t<F, Args...>;

    if (!isRunning()) {
      return std::nullopt;
    }

    auto bound_task = [f = std::forward<F>(func),
                       ... captured_args = std::forward<Args>(args)]() mutable {
      return std::invoke(
          std::forward<decltype(f)>(f),
          std::forward<decltype(captured_args)>(captured_args)...);
    };

    auto packaged = std::make_shared<std::packaged_task<ReturnType()>>(
        std::move(bound_task));
    std::future<ReturnType> result = packaged->get_future();

    if (!tryEnqueueTask(
            [packaged = std::move(packaged)]() { (*packaged)(); })) {
      return std::nullopt;
    }

    return result;
  }

  /**
   * @brief Gracefully shutdown: wait for pending tasks to complete
   */
  void shutdown() { shutdown(true); }

  /**
   * @brief Shutdown with option to wait or discard pending tasks
   * @param wait_for_tasks If true, wait for pending tasks; if false, discard
   * them
   */
  void shutdown(bool wait_for_tasks) {
    State expected = State::KRunning;
    if (!m_state.compare_exchange_strong(expected, State::KStopping,
                                         std::memory_order_acq_rel)) {
      // Already stopping or stopped
      waitForWorkers();
      return;
    }

    if (!wait_for_tasks) {
      // Immediate shutdown: clear queue
      std::lock_guard<std::mutex> lock(m_queueMutex);
      std::queue<Task> empty;
      m_taskQueue.swap(empty);
      m_pendingTasks.store(0, std::memory_order_relaxed);
    }

    // Wake up all workers
    m_notEmpty.notify_all();
    m_notFull.notify_all();

    waitForWorkers();

    m_state.store(State::KStopped, std::memory_order_release);
  }

  /**
   * @brief Wait for all pending tasks to complete (without stopping)
   */
  void waitForTasks() {
    std::unique_lock<std::mutex> lock(m_queueMutex);
    m_allTasksDone.wait(lock, [this] {
      return m_pendingTasks.load(std::memory_order_acquire) == 0 ||
             !isRunning();
    });
  }

  /**
   * @brief Set custom exception handler for task failures
   * @param handler Callback invoked when a task throws
   */
  void setExceptionHandler(TaskExceptionHandler handler) {
    std::lock_guard<std::mutex> lock(m_handlerMutex);
    m_exceptionHandler = std::move(handler);
  }

  // -------------------------------------------------------------------------
  // Status and Statistics
  // -------------------------------------------------------------------------

  [[nodiscard]] bool isRunning() const {
    return m_state.load(std::memory_order_acquire) == State::KRunning;
  }

  [[nodiscard]] bool isStopped() const {
    return m_state.load(std::memory_order_acquire) == State::KStopped;
  }

  [[nodiscard]] std::size_t threadCount() const { return m_workers.size(); }

  [[nodiscard]] std::size_t pendingTasks() const {
    return m_pendingTasks.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::size_t completedTasks() const {
    return m_completedTasks.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::size_t queueCapacity() const {
    return m_config.max_queue_size;
  }

private:
  enum class State : std::uint8_t { KRunning, KStopping, KStopped };

  using Task = std::function<void()>;

  void startWorkers(std::size_t count) {
    m_workers.reserve(count);
    for (std::size_t i = 0; i < count; ++i) {
      m_workers.emplace_back(&ThreadPool::workerLoop, this);
    }
  }

  void waitForWorkers() {
    for (auto &worker : m_workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

  void workerLoop() {
    while (true) {
      Task task;

      {
        std::unique_lock<std::mutex> lock(m_queueMutex);
        m_notEmpty.wait(lock, [this] {
          return m_state.load(std::memory_order_acquire) != State::KRunning ||
                 !m_taskQueue.empty();
        });

        // Exit condition: stopping and no more tasks
        if (m_state.load(std::memory_order_acquire) != State::KRunning &&
            m_taskQueue.empty()) {
          return;
        }

        if (m_taskQueue.empty()) {
          continue; // Spurious wakeup
        }

        task = std::move(m_taskQueue.front());
        m_taskQueue.pop();
      }

      m_notFull.notify_one();

      executeTask(std::move(task));
    }
  }

  void executeTask(Task task) {
    try {
      if (task) {
        task();
      }
    } catch (...) {
      handleException(std::current_exception());
    }

    // Update statistics
    m_completedTasks.fetch_add(1, std::memory_order_relaxed);
    auto remaining = m_pendingTasks.fetch_sub(1, std::memory_order_acq_rel) - 1;

    if (remaining == 0) {
      m_allTasksDone.notify_all();
    }
  }

  void handleException(std::exception_ptr eptr) {
    TaskExceptionHandler handler;
    {
      std::lock_guard<std::mutex> lock(m_handlerMutex);
      handler = m_exceptionHandler;
    }

    if (handler) {
      try {
        std::rethrow_exception(eptr);
      } catch (const std::exception &e) {
        handler(eptr, e.what());
      } catch (...) {
        handler(eptr, "Unknown exception");
      }
    }
    // If no handler, silently ignore (task's future will hold the exception)
  }

  void enqueueTask(Task task) {
    {
      std::unique_lock<std::mutex> lock(m_queueMutex);

      const bool has_capacity = m_config.max_queue_size == 0 ||
                                m_taskQueue.size() < m_config.max_queue_size;

      if (!has_capacity) {
        // Wait with timeout
        if (!m_notFull.wait_for(lock, m_config.submit_timeout, [this] {
              return m_state.load(std::memory_order_acquire) !=
                         State::KRunning ||
                     m_config.max_queue_size == 0 ||
                     m_taskQueue.size() < m_config.max_queue_size;
            })) {
          throw std::runtime_error("ThreadPool: Task submission timed out");
        }
      }

      if (m_state.load(std::memory_order_acquire) != State::KRunning) {
        throw std::runtime_error("ThreadPool: Cannot submit to stopped pool");
      }

      m_taskQueue.push(std::move(task));
      m_pendingTasks.fetch_add(1, std::memory_order_relaxed);
    }

    m_notEmpty.notify_one();
  }

  [[nodiscard]] bool tryEnqueueTask(Task task) {
    {
      std::lock_guard<std::mutex> lock(m_queueMutex);

      if (m_state.load(std::memory_order_acquire) != State::KRunning) {
        return false;
      }

      if (m_config.max_queue_size > 0 &&
          m_taskQueue.size() >= m_config.max_queue_size) {
        return false;
      }

      m_taskQueue.push(std::move(task));
      m_pendingTasks.fetch_add(1, std::memory_order_relaxed);
    }

    m_notEmpty.notify_one();
    return true;
  }

private:
  // Configuration (immutable after construction)
  const ThreadPoolConfig m_config;

  // State
  std::atomic<State> m_state;
  std::vector<std::thread> m_workers;

  // Task queue
  std::queue<Task> m_taskQueue;
  mutable std::mutex m_queueMutex;
  std::condition_variable m_notEmpty;
  std::condition_variable m_notFull;
  std::condition_variable m_allTasksDone;

  // Statistics
  std::atomic<std::size_t> m_pendingTasks;
  std::atomic<std::size_t> m_completedTasks;

  // Exception handling
  TaskExceptionHandler m_exceptionHandler;
  std::mutex m_handlerMutex;
};

/**
 * @brief RAII wrapper for automatic thread pool shutdown
 */
class ScopedThreadPool {
public:
  explicit ScopedThreadPool(std::size_t num_threads,
                            std::size_t max_queue_size = 1024)
      : m_pool(num_threads, max_queue_size) {}

  explicit ScopedThreadPool(const ThreadPoolConfig &config) : m_pool(config) {}

  ~ScopedThreadPool() = default;

  ThreadPool &get() { return m_pool; }
  const ThreadPool &get() const { return m_pool; }

  ThreadPool *operator->() { return &m_pool; }
  const ThreadPool *operator->() const { return &m_pool; }

private:
  ThreadPool m_pool;
};

} // namespace ai_pipe

#endif // AI_PIPE_THREAD_POOL_HPP