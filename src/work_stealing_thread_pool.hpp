/**
 * @file work_stealing_thread_pool.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Work-stealing thread pool implementation for high-performance
 * scenarios
 * @version 1.0
 * @date 2025-02-05
 *
 * @copyright Copyright (c) 2025
 *
 * This implementation addresses lock contention issues in the standard
 * ThreadPool by giving each worker thread its own local task queue. When a
 * worker's queue is empty, it can steal tasks from other workers' queues.
 *
 * Key features:
 * - Per-worker local queues (LIFO for locality)
 * - Work-stealing from other workers (FIFO steal for fairness)
 * - Fine-grained locking per queue
 * - Thread-local worker identification for efficient task submission
 */

#ifndef AI_PIPE_WORK_STEALING_THREAD_POOL_HPP
#define AI_PIPE_WORK_STEALING_THREAD_POOL_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace ai_pipe {

/**
 * @brief Work-stealing deque for task management
 *
 * This deque supports:
 * - Local push/pop at the front (LIFO, used by owning worker)
 * - Steal from the back (FIFO, used by other workers)
 *
 * Uses fine-grained mutex locking. While not fully lock-free, this design
 * significantly reduces contention compared to a single global queue.
 */
template <typename T> class WorkStealingDeque {
public:
  WorkStealingDeque() = default;
  ~WorkStealingDeque() = default;

  // Non-copyable, non-movable
  WorkStealingDeque(const WorkStealingDeque &) = delete;
  WorkStealingDeque &operator=(const WorkStealingDeque &) = delete;
  WorkStealingDeque(WorkStealingDeque &&) = delete;
  WorkStealingDeque &operator=(WorkStealingDeque &&) = delete;

  /**
   * @brief Push a task to the front (local operation, LIFO)
   * @param item Task to push
   */
  void pushFront(T item) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_deque.push_front(std::move(item));
  }

  /**
   * @brief Pop a task from the front (local operation, LIFO)
   * @return Task if available, nullopt otherwise
   */
  [[nodiscard]] std::optional<T> popFront() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_deque.empty()) {
      return std::nullopt;
    }
    T item = std::move(m_deque.front());
    m_deque.pop_front();
    return item;
  }

  /**
   * @brief Steal a task from the back (used by other workers, FIFO)
   * @return Task if available, nullopt otherwise
   */
  [[nodiscard]] std::optional<T> stealBack() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_deque.empty()) {
      return std::nullopt;
    }
    T item = std::move(m_deque.back());
    m_deque.pop_back();
    return item;
  }

  /**
   * @brief Try to steal multiple tasks at once (batch stealing)
   * @param max_count Maximum number of tasks to steal
   * @return Vector of stolen tasks
   */
  [[nodiscard]] std::vector<T> stealBatch(std::size_t max_count) {
    std::vector<T> stolen;
    std::lock_guard<std::mutex> lock(m_mutex);

    std::size_t count = std::min(max_count, m_deque.size() / 2 + 1);
    stolen.reserve(count);

    for (std::size_t i = 0; i < count && !m_deque.empty(); ++i) {
      stolen.push_back(std::move(m_deque.back()));
      m_deque.pop_back();
    }
    return stolen;
  }

  /**
   * @brief Check if the deque is empty
   */
  [[nodiscard]] bool empty() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_deque.empty();
  }

  /**
   * @brief Get current size
   */
  [[nodiscard]] std::size_t size() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_deque.size();
  }

  /**
   * @brief Clear all tasks
   */
  void clear() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_deque.clear();
  }

private:
  mutable std::mutex m_mutex;
  std::deque<T> m_deque;
};

/**
 * @brief Configuration for WorkStealingThreadPool
 */
struct WorkStealingConfig {
  std::size_t num_threads = std::thread::hardware_concurrency();
  std::size_t max_queue_size_per_worker = 256; // 0 = unlimited
  std::chrono::milliseconds submit_timeout{5000};
  bool wait_for_tasks_on_stop = true;
  std::size_t steal_batch_size = 4; // Number of tasks to steal at once
};

/**
 * @brief Exception handler callback type
 */
using WorkStealingExceptionHandler =
    std::function<void(const std::exception_ptr &, const std::string &)>;

/**
 * @brief High-performance work-stealing thread pool
 *
 * Addresses lock contention in traditional thread pools by:
 * 1. Giving each worker its own local task queue
 * 2. Workers steal from others when their queue is empty
 * 3. Using thread-local storage for efficient worker identification
 */
class WorkStealingThreadPool {
public:
  /**
   * @brief Construct with number of threads
   * @param num_threads Number of worker threads
   * @param max_queue_size Maximum tasks per worker queue (0 = unlimited)
   */
  explicit WorkStealingThreadPool(std::size_t num_threads,
                                  std::size_t max_queue_size = 256)
      : m_config{num_threads, max_queue_size, std::chrono::milliseconds{5000},
                 true, 4},
        m_state{State::KRunning}, m_nextWorkerIndex{0}, m_pendingTasks{0},
        m_completedTasks{0} {
    initialize(num_threads);
  }

  /**
   * @brief Construct with full configuration
   */
  explicit WorkStealingThreadPool(const WorkStealingConfig &config)
      : m_config{config}, m_state{State::KRunning}, m_nextWorkerIndex{0},
        m_pendingTasks{0}, m_completedTasks{0} {
    initialize(config.num_threads);
  }

  ~WorkStealingThreadPool() { shutdown(m_config.wait_for_tasks_on_stop); }

  // Non-copyable, non-movable
  WorkStealingThreadPool(const WorkStealingThreadPool &) = delete;
  WorkStealingThreadPool &operator=(const WorkStealingThreadPool &) = delete;
  WorkStealingThreadPool(WorkStealingThreadPool &&) = delete;
  WorkStealingThreadPool &operator=(WorkStealingThreadPool &&) = delete;

  /**
   * @brief Submit a task for execution
   * @tparam F Callable type
   * @tparam Args Argument types
   * @param func Callable object
   * @param args Arguments to pass to the callable
   * @return Future for the task result
   */
  template <typename F, typename... Args>
  [[nodiscard]] auto submit(F &&func, Args &&...args)
      -> std::future<std::invoke_result_t<F, Args...>> {
    using ReturnType = std::invoke_result_t<F, Args...>;

    if (!isRunning()) {
      throw std::runtime_error(
          "WorkStealingThreadPool: Cannot submit to stopped pool");
    }

    // Create bound callable
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
   * @brief Fire-and-forget submission for void tasks
   *
   * Skips the packaged_task/shared_ptr/future machinery of submit() -
   * the right choice when the caller never consumes a result (the
   * execution engine's per-node tasks). Never throws.
   *
   * @return true if the task was enqueued, false if the pool is not
   *         running or enqueueing failed (e.g. submission timeout)
   */
  bool post(std::function<void()> task) noexcept {
    if (!task || !isRunning()) {
      return false;
    }
    try {
      enqueueTask(std::move(task));
      return true;
    } catch (...) {
      return false;
    }
  }

  /**
   * @brief Try to submit a task without blocking
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
   * @brief Gracefully shutdown
   */
  void shutdown() { shutdown(true); }

  /**
   * @brief Shutdown with option to wait or discard
   */
  void shutdown(bool wait_for_tasks) {
    State expected = State::KRunning;
    if (!m_state.compare_exchange_strong(expected, State::KStopping,
                                         std::memory_order_acq_rel)) {
      waitForWorkers();
      return;
    }

    if (!wait_for_tasks) {
      // Clear all queues
      for (auto &queue : m_queues) {
        queue->clear();
      }
      m_pendingTasks.store(0, std::memory_order_relaxed);
      m_unclaimedTasks.store(0, std::memory_order_relaxed);
    }

    // Wake all workers
    {
      std::lock_guard<std::mutex> lock(m_globalMutex);
    }
    m_globalCondition.notify_all();

    waitForWorkers();

    m_state.store(State::KStopped, std::memory_order_release);
  }

  /**
   * @brief Wait for all pending tasks
   */
  void waitForTasks() {
    std::unique_lock<std::mutex> lock(m_globalMutex);
    m_allTasksDone.wait(lock, [this] {
      return m_pendingTasks.load(std::memory_order_acquire) == 0 ||
             !isRunning();
    });
  }

  /**
   * @brief Set exception handler
   */
  void setExceptionHandler(WorkStealingExceptionHandler handler) {
    std::lock_guard<std::mutex> lock(m_handlerMutex);
    m_exceptionHandler = std::move(handler);
  }

  // Status and Statistics
  [[nodiscard]] bool isRunning() const {
    return m_state.load(std::memory_order_acquire) == State::KRunning;
  }

  [[nodiscard]] bool isStopped() const {
    return m_state.load(std::memory_order_acquire) == State::KStopped;
  }

  [[nodiscard]] std::size_t threadCount() const { return m_workers.size(); }

  // Alias for compatibility with original ThreadPool
  [[nodiscard]] std::size_t workerCount() const { return threadCount(); }

  [[nodiscard]] std::size_t pendingTasks() const {
    return m_pendingTasks.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::size_t completedTasks() const {
    return m_completedTasks.load(std::memory_order_acquire);
  }

  [[nodiscard]] std::size_t queueCapacity() const {
    return m_config.max_queue_size_per_worker * m_config.num_threads;
  }

  /**
   * @brief Get statistics per worker queue
   * @return Vector of queue sizes for each worker
   */
  [[nodiscard]] std::vector<std::size_t> queueSizes() const {
    std::vector<std::size_t> sizes;
    sizes.reserve(m_queues.size());
    for (const auto &queue : m_queues) {
      sizes.push_back(queue->size());
    }
    return sizes;
  }

  /**
   * @brief Get the steal count (for statistics/debugging)
   */
  [[nodiscard]] std::size_t stealCount() const {
    return m_stealCount.load(std::memory_order_relaxed);
  }

private:
  enum class State : std::uint8_t { KRunning, KStopping, KStopped };

  using Task = std::function<void()>;
  using TaskQueue = WorkStealingDeque<Task>;

  // Thread-local storage for current worker index
  // Using inline static with thread_local for C++17+ compatibility
  static inline thread_local std::optional<std::size_t> tl_worker_index =
      std::nullopt;

  void initialize(std::size_t num_threads) {
    // Create task queues
    m_queues.reserve(num_threads);
    for (std::size_t i = 0; i < num_threads; ++i) {
      m_queues.push_back(std::make_unique<TaskQueue>());
    }

    // Start workers
    m_workers.reserve(num_threads);
    for (std::size_t i = 0; i < num_threads; ++i) {
      m_workers.emplace_back(&WorkStealingThreadPool::workerLoop, this, i);
    }
  }

  void waitForWorkers() {
    for (auto &worker : m_workers) {
      if (worker.joinable()) {
        worker.join();
      }
    }
  }

  /**
   * @brief Main worker loop (event-driven)
   *
   * Fast path never touches the global mutex: pop locally (LIFO), then
   * steal. Only a worker that found nothing blocks on the condition
   * variable, keyed off m_unclaimedTasks, so an idle pool consumes zero
   * CPU (previously each worker woke every 1ms and scanned all queues).
   *
   * NOTE: sizing uses m_queues.size(), which is fully populated before
   * any worker starts. m_workers must not be read here - it is still
   * being appended by the constructor while early workers run.
   *
   * @param worker_index Index of this worker
   */
  void workerLoop(std::size_t worker_index) {
    // Set thread-local worker index
    tl_worker_index = worker_index;

    // Create random number generator for victim selection
    std::mt19937 rng(std::random_device{}() +
                     static_cast<unsigned>(worker_index));
    std::uniform_int_distribution<std::size_t> dist(0, m_queues.size() - 1);

    while (true) {
      Task task;

      // Fast path: local queue first (LIFO), then steal
      if (auto local_task = m_queues[worker_index]->popFront()) {
        task = std::move(*local_task);
      } else {
        task = trySteal(worker_index, rng, dist);
      }

      if (task) {
        m_unclaimedTasks.fetch_sub(1, std::memory_order_acq_rel);
        executeTask(std::move(task));
        continue;
      }

      // Slow path: nothing found anywhere
      if (m_state.load(std::memory_order_acquire) != State::KRunning) {
        // Draining shutdown: exit once no work remains to claim
        if (m_unclaimedTasks.load(std::memory_order_acquire) == 0) {
          return;
        }
        // Another worker may still publish; retry the fast path
        std::this_thread::yield();
        continue;
      }

      std::unique_lock<std::mutex> lock(m_globalMutex);
      m_globalCondition.wait(lock, [this] {
        return m_unclaimedTasks.load(std::memory_order_acquire) > 0 ||
               m_state.load(std::memory_order_acquire) != State::KRunning;
      });
    }
  }

  /**
   * @brief Try to steal a task from another worker
   */
  Task trySteal(std::size_t worker_index, std::mt19937 &rng,
                std::uniform_int_distribution<std::size_t> &dist) {
    const std::size_t num_workers = m_queues.size();
    if (num_workers <= 1) {
      return nullptr;
    }

    // Start from a random victim to avoid thundering herd
    std::size_t start = dist(rng);

    for (std::size_t i = 0; i < num_workers; ++i) {
      std::size_t victim = (start + i) % num_workers;
      if (victim == worker_index) {
        continue;
      }

      if (auto stolen = m_queues[victim]->stealBack()) {
        m_stealCount.fetch_add(1, std::memory_order_relaxed);
        return std::move(*stolen);
      }
    }

    return nullptr;
  }

  void executeTask(Task task) {
    try {
      if (task) {
        task();
      }
    } catch (...) {
      handleException(std::current_exception());
    }

    m_completedTasks.fetch_add(1, std::memory_order_relaxed);
    auto remaining = m_pendingTasks.fetch_sub(1, std::memory_order_acq_rel) - 1;

    if (remaining == 0) {
      m_allTasksDone.notify_all();
    }
  }

  void handleException(std::exception_ptr eptr) {
    WorkStealingExceptionHandler handler;
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
  }

  /**
   * @brief Enqueue a task to appropriate queue
   */
  void enqueueTask(Task task) {
    std::size_t target_queue = selectTargetQueue();

    // Check queue capacity if limited
    if (m_config.max_queue_size_per_worker > 0) {
      // Wait if queue is full (with timeout)
      auto start = std::chrono::steady_clock::now();
      while (m_queues[target_queue]->size() >=
             m_config.max_queue_size_per_worker) {
        if (!isRunning()) {
          throw std::runtime_error(
              "WorkStealingThreadPool: Cannot submit to stopped pool");
        }

        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed >= m_config.submit_timeout) {
          throw std::runtime_error(
              "WorkStealingThreadPool: Task submission timed out");
        }

        // Try another queue
        target_queue = (target_queue + 1) % m_queues.size();
        if (m_queues[target_queue]->size() <
            m_config.max_queue_size_per_worker) {
          break;
        }

        std::this_thread::yield();
      }
    }

    if (!isRunning()) {
      throw std::runtime_error(
          "WorkStealingThreadPool: Cannot submit to stopped pool");
    }

    m_queues[target_queue]->pushFront(std::move(task));
    m_pendingTasks.fetch_add(1, std::memory_order_relaxed);
    publishTask();
  }

  /**
   * @brief Try to enqueue without blocking
   */
  [[nodiscard]] bool tryEnqueueTask(Task task) {
    if (!isRunning()) {
      return false;
    }

    std::size_t target_queue = selectTargetQueue();

    // Check capacity
    if (m_config.max_queue_size_per_worker > 0) {
      // Try all queues
      for (std::size_t i = 0; i < m_queues.size(); ++i) {
        std::size_t idx = (target_queue + i) % m_queues.size();
        if (m_queues[idx]->size() < m_config.max_queue_size_per_worker) {
          target_queue = idx;
          break;
        }
        if (i == m_queues.size() - 1) {
          return false; // All queues full
        }
      }
    }

    m_queues[target_queue]->pushFront(std::move(task));
    m_pendingTasks.fetch_add(1, std::memory_order_relaxed);
    publishTask();

    return true;
  }

  /**
   * @brief Publish a newly enqueued task to sleeping workers
   *
   * The empty lock/unlock of m_globalMutex orders the m_unclaimedTasks
   * increment against a worker's predicate check, closing the classic
   * check-then-sleep lost-wakeup window.
   */
  void publishTask() {
    m_unclaimedTasks.fetch_add(1, std::memory_order_release);
    { std::lock_guard<std::mutex> lock(m_globalMutex); }
    m_globalCondition.notify_one();
  }

  /**
   * @brief Select target queue for task submission
   *
   * If called from a worker thread, use that worker's queue.
   * Otherwise, use round-robin distribution.
   */
  [[nodiscard]] std::size_t selectTargetQueue() {
    // If this is a worker thread, use its own queue
    if (tl_worker_index.has_value()) {
      return *tl_worker_index;
    }

    // Round-robin for external threads
    return m_nextWorkerIndex.fetch_add(1, std::memory_order_relaxed) %
           m_queues.size();
  }

private:
  // Configuration
  const WorkStealingConfig m_config;

  // State
  std::atomic<State> m_state;
  std::vector<std::thread> m_workers;

  // Per-worker task queues
  std::vector<std::unique_ptr<TaskQueue>> m_queues;

  // Global synchronization (for sleep/wake)
  std::mutex m_globalMutex;
  std::condition_variable m_globalCondition;
  std::condition_variable m_allTasksDone;

  // Round-robin counter for external submissions
  std::atomic<std::size_t> m_nextWorkerIndex;

  // Statistics
  std::atomic<std::size_t> m_pendingTasks;
  std::atomic<std::size_t> m_completedTasks;

  // Tasks pushed but not yet claimed by any worker; drives the sleep/wake
  // protocol (workers block until this is nonzero or the pool stops).
  std::atomic<std::size_t> m_unclaimedTasks{0};
  std::atomic<std::size_t> m_stealCount{0};

  // Exception handling
  WorkStealingExceptionHandler m_exceptionHandler;
  std::mutex m_handlerMutex;
};

/**
 * @brief RAII wrapper for WorkStealingThreadPool
 */
class ScopedWorkStealingThreadPool {
public:
  explicit ScopedWorkStealingThreadPool(std::size_t num_threads,
                                        std::size_t max_queue_size = 256)
      : m_pool(num_threads, max_queue_size) {}

  explicit ScopedWorkStealingThreadPool(const WorkStealingConfig &config)
      : m_pool(config) {}

  ~ScopedWorkStealingThreadPool() = default;

  WorkStealingThreadPool &get() { return m_pool; }
  const WorkStealingThreadPool &get() const { return m_pool; }

  WorkStealingThreadPool *operator->() { return &m_pool; }
  const WorkStealingThreadPool *operator->() const { return &m_pool; }

private:
  WorkStealingThreadPool m_pool;
};

} // namespace ai_pipe

#endif // AI_PIPE_WORK_STEALING_THREAD_POOL_HPP
