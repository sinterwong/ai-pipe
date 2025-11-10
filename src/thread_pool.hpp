/**
 * @file thread_pool.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief A simple thread pool implementation.
 * @version 0.2
 * @date 2025-09-01
 * @copyright Copyright (c) 2022
 *
 */

#ifndef AI_PIPE_SIMPLE_THREAD_POOL_HPP
#define AI_PIPE_SIMPLE_THREAD_POOL_HPP

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace ai_pipe {

class ThreadPool {
public:
  explicit ThreadPool(size_t numThreads, size_t maxQueueSize = 1024)
      : m_maxQueueSize(maxQueueSize), m_state(State::RUNNING) {
    m_threads.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i) {
      m_threads.emplace_back(&ThreadPool::worker, this);
    }
  }

  ~ThreadPool() { stop(); }

  ThreadPool(const ThreadPool &) = delete;
  ThreadPool &operator=(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  ThreadPool &operator=(ThreadPool &&) = delete;

  template <typename F, typename... Args>
  auto submit(F &&f,
              Args &&...args) -> std::future<std::invoke_result_t<F, Args...>> {
    using ReturnType = std::invoke_result_t<F, Args...>;

    if (m_state != State::RUNNING) {
      throw std::runtime_error(
          "ThreadPool is not running, cannot submit task.");
    }

    auto bind_func = [f = std::forward<F>(f),
                      ... args = std::forward<Args>(args)]() mutable {
      return std::invoke(std::forward<decltype(f)>(f),
                         std::forward<decltype(args)>(args)...);
    };

    auto packagedTask =
        std::make_shared<std::packaged_task<ReturnType()>>(std::move(bind_func));
    std::future<ReturnType> result = packagedTask->get_future();

    enqueueTask([packagedTask]() { (*packagedTask)(); });

    return result;
  }

private:
  enum class State { RUNNING, STOPPING, STOPPED };

  void worker() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_notEmpty.wait(lock, [this] {
          return m_state != State::RUNNING || !m_taskQueue.empty();
        });

        if (m_state != State::RUNNING && m_taskQueue.empty()) {
          return;
        }

        task = std::move(m_taskQueue.front());
        m_taskQueue.pop();
      }

      m_notFull.notify_one();

      try {
        if (task) {
          task();
        }
      } catch (const std::exception &e) {
        std::cerr << "Task execution failed: " << e.what() << std::endl;
      } catch (...) {
        std::cerr << "Task execution failed with unknown error" << std::endl;
      }
    }
  }

  void stop() {
    State expected = State::RUNNING;
    if (!m_state.compare_exchange_strong(expected, State::STOPPING)) {
      return;
    }

    m_notEmpty.notify_all();
    m_notFull.notify_all();

    for (auto &thread : m_threads) {
      if (thread.joinable()) {
        thread.join();
      }
    }

    m_threads.clear();
    std::queue<std::function<void()>> empty_queue;
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_taskQueue.swap(empty_queue);
      m_state = State::STOPPED;
    }
  }

  void enqueueTask(std::function<void()> task) {
    {
      std::unique_lock<std::mutex> lock(m_mutex);
      if (!m_notFull.wait_for(lock, std::chrono::seconds(5), [this] {
            return m_state != State::RUNNING ||
                   m_taskQueue.size() < m_maxQueueSize;
          })) {
        throw std::runtime_error("Task queue is full, submission timed out.");
      }

      if (m_state != State::RUNNING) {
        throw std::runtime_error("ThreadPool is stopping, cannot submit task.");
      }

      m_taskQueue.emplace(std::move(task));
    }
    m_notEmpty.notify_one();
  }

  const size_t m_maxQueueSize;
  std::atomic<State> m_state;
  std::vector<std::thread> m_threads;
  std::queue<std::function<void()>> m_taskQueue;
  std::mutex m_mutex;
  std::condition_variable m_notFull;
  std::condition_variable m_notEmpty;
};

} // namespace ai_pipe
#endif // AI_PIPE_SIMPLE_THREAD_POOL_HPP
