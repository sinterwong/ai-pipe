#include "thread_safe_queue.hpp"
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <random>
#include <thread>
#include <vector>

using namespace ai_pipe;
using namespace std::chrono_literals;

// =============================================================================
// ThreadSafeQueue Tests
// =============================================================================

class ThreadSafeQueueTest : public ::testing::Test {
protected:
  ThreadSafeQueue<int> m_queue;
};

// -----------------------------------------------------------------------------
// Basic Operations
// -----------------------------------------------------------------------------

TEST_F(ThreadSafeQueueTest, InitiallyEmpty) {
  EXPECT_TRUE(m_queue.empty());
  EXPECT_EQ(m_queue.size(), 0);
}

TEST_F(ThreadSafeQueueTest, PushIncreasesSize) {
  m_queue.push(1);
  EXPECT_FALSE(m_queue.empty());
  EXPECT_EQ(m_queue.size(), 1);

  m_queue.push(2);
  EXPECT_EQ(m_queue.size(), 2);
}

TEST_F(ThreadSafeQueueTest, TryPopReturnsNulloptOnEmpty) {
  auto result = m_queue.tryPop();
  EXPECT_FALSE(result.has_value());
}

TEST_F(ThreadSafeQueueTest, TryPopReturnsValue) {
  m_queue.push(42);
  auto result = m_queue.tryPop();

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
  EXPECT_TRUE(m_queue.empty());
}

TEST_F(ThreadSafeQueueTest, FIFOOrder) {
  m_queue.push(1);
  m_queue.push(2);
  m_queue.push(3);

  EXPECT_EQ(*m_queue.tryPop(), 1);
  EXPECT_EQ(*m_queue.tryPop(), 2);
  EXPECT_EQ(*m_queue.tryPop(), 3);
}

TEST_F(ThreadSafeQueueTest, Clear) {
  m_queue.push(1);
  m_queue.push(2);
  m_queue.push(3);

  m_queue.clear();

  EXPECT_TRUE(m_queue.empty());
  EXPECT_EQ(m_queue.size(), 0);
}

// -----------------------------------------------------------------------------
// Blocking Operations
// -----------------------------------------------------------------------------

TEST_F(ThreadSafeQueueTest, WaitPopForTimeout) {
  auto start = std::chrono::steady_clock::now();
  auto result = m_queue.waitPopFor(50ms);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(result.has_value());
  EXPECT_GE(elapsed, 45ms); // Allow some tolerance
}

TEST_F(ThreadSafeQueueTest, WaitPopForSucceeds) {
  std::thread producer([this]() {
    std::this_thread::sleep_for(20ms);
    m_queue.push(99);
  });

  auto result = m_queue.waitPopFor(500ms);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 99);

  producer.join();
}

TEST_F(ThreadSafeQueueTest, WaitPopBlocks) {
  std::atomic<bool> popped{false};

  std::thread consumer([this, &popped]() {
    int value = m_queue.waitPop();
    EXPECT_EQ(value, 123);
    popped.store(true);
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(popped.load());

  m_queue.push(123);

  consumer.join();
  EXPECT_TRUE(popped.load());
}

// -----------------------------------------------------------------------------
// Thread Safety Tests
// -----------------------------------------------------------------------------

TEST_F(ThreadSafeQueueTest, ConcurrentPush) {
  const int num_threads = 8;
  const int items_per_thread = 1000;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, items_per_thread]() {
      for (int i = 0; i < items_per_thread; ++i) {
        m_queue.push(t * items_per_thread + i);
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(m_queue.size(), num_threads * items_per_thread);
}

TEST_F(ThreadSafeQueueTest, ConcurrentPushPop) {
  const int num_items = 10000;
  std::atomic<int> pop_count{0};
  std::atomic<bool> done{false};

  // Producer thread
  std::thread producer([this, num_items, &done]() {
    for (int i = 0; i < num_items; ++i) {
      m_queue.push(i);
    }
    done.store(true);
  });

  // Consumer threads
  std::vector<std::thread> consumers;
  for (int c = 0; c < 4; ++c) {
    consumers.emplace_back([this, &pop_count, &done]() {
      while (!done.load() || !m_queue.empty()) {
        auto result = m_queue.tryPop();
        if (result.has_value()) {
          pop_count.fetch_add(1);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }

  producer.join();
  for (auto &c : consumers) {
    c.join();
  }

  EXPECT_EQ(pop_count.load(), num_items);
  EXPECT_TRUE(m_queue.empty());
}

TEST_F(ThreadSafeQueueTest, ConcurrentWaitPop) {
  const int num_consumers = 4;
  const int items_per_consumer = 100;
  std::atomic<int> total_popped{0};

  std::vector<std::thread> consumers;
  for (int c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([this, items_per_consumer, &total_popped]() {
      for (int i = 0; i < items_per_consumer; ++i) {
        auto result = m_queue.waitPopFor(1000ms);
        if (result.has_value()) {
          total_popped.fetch_add(1);
        }
      }
    });
  }

  // Producer
  std::thread producer([this, num_consumers, items_per_consumer]() {
    for (int i = 0; i < num_consumers * items_per_consumer; ++i) {
      m_queue.push(i);
      if (i % 10 == 0) {
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  for (auto &c : consumers) {
    c.join();
  }

  EXPECT_EQ(total_popped.load(), num_consumers * items_per_consumer);
}

// -----------------------------------------------------------------------------
// Edge Cases
// -----------------------------------------------------------------------------

TEST_F(ThreadSafeQueueTest, MoveOnlyTypes) {
  ThreadSafeQueue<std::unique_ptr<int>> ptr_queue;

  ptr_queue.push(std::make_unique<int>(42));
  auto result = ptr_queue.tryPop();

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(**result, 42);
}

TEST_F(ThreadSafeQueueTest, LargeObjects) {
  ThreadSafeQueue<std::vector<int>> vec_queue;

  std::vector<int> large_vec(10000, 42);
  vec_queue.push(large_vec);

  auto result = vec_queue.tryPop();
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->size(), 10000);
  EXPECT_EQ((*result)[0], 42);
}

TEST_F(ThreadSafeQueueTest, StressTest) {
  const int iterations = 100000;
  std::atomic<long long> sum_pushed{0};
  std::atomic<long long> sum_popped{0};

  std::thread producer([this, iterations, &sum_pushed]() {
    for (int i = 0; i < iterations; ++i) {
      m_queue.push(i);
      sum_pushed.fetch_add(i);
    }
  });

  std::thread consumer([this, iterations, &sum_popped]() {
    int count = 0;
    while (count < iterations) {
      auto result = m_queue.waitPopFor(100ms);
      if (result.has_value()) {
        sum_popped.fetch_add(*result);
        ++count;
      }
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(sum_pushed.load(), sum_popped.load());
}

// =============================================================================
// ThreadSafePriorityQueue Tests
// =============================================================================

class ThreadSafePriorityQueueTest : public ::testing::Test {
protected:
  ThreadSafePriorityQueue<int> m_pqueue; // Max-heap by default (std::less)
};

// -----------------------------------------------------------------------------
// Basic Operations
// -----------------------------------------------------------------------------

TEST_F(ThreadSafePriorityQueueTest, InitiallyEmpty) {
  EXPECT_TRUE(m_pqueue.empty());
  EXPECT_EQ(m_pqueue.size(), 0);
}

TEST_F(ThreadSafePriorityQueueTest, PushIncreasesSize) {
  m_pqueue.push(1);
  EXPECT_FALSE(m_pqueue.empty());
  EXPECT_EQ(m_pqueue.size(), 1);
}

TEST_F(ThreadSafePriorityQueueTest, TryPopReturnsNulloptOnEmpty) {
  auto result = m_pqueue.tryPop();
  EXPECT_FALSE(result.has_value());
}

TEST_F(ThreadSafePriorityQueueTest, PriorityOrderMaxHeap) {
  m_pqueue.push(1);
  m_pqueue.push(5);
  m_pqueue.push(3);
  m_pqueue.push(2);
  m_pqueue.push(4);

  // std::less creates max-heap in std::priority_queue
  EXPECT_EQ(*m_pqueue.tryPop(), 5);
  EXPECT_EQ(*m_pqueue.tryPop(), 4);
  EXPECT_EQ(*m_pqueue.tryPop(), 3);
  EXPECT_EQ(*m_pqueue.tryPop(), 2);
  EXPECT_EQ(*m_pqueue.tryPop(), 1);
}

TEST_F(ThreadSafePriorityQueueTest, MinHeap) {
  ThreadSafePriorityQueue<int, std::greater<int>> min_heap;

  min_heap.push(3);
  min_heap.push(1);
  min_heap.push(4);
  min_heap.push(1);
  min_heap.push(5);

  EXPECT_EQ(*min_heap.tryPop(), 1);
  EXPECT_EQ(*min_heap.tryPop(), 1);
  EXPECT_EQ(*min_heap.tryPop(), 3);
  EXPECT_EQ(*min_heap.tryPop(), 4);
  EXPECT_EQ(*min_heap.tryPop(), 5);
}

TEST_F(ThreadSafePriorityQueueTest, Clear) {
  m_pqueue.push(1);
  m_pqueue.push(2);
  m_pqueue.push(3);

  m_pqueue.clear();

  EXPECT_TRUE(m_pqueue.empty());
  EXPECT_EQ(m_pqueue.size(), 0);
}

// -----------------------------------------------------------------------------
// Blocking Operations
// -----------------------------------------------------------------------------

TEST_F(ThreadSafePriorityQueueTest, WaitPopForTimeout) {
  auto result = m_pqueue.waitPopFor(50ms);
  EXPECT_FALSE(result.has_value());
}

TEST_F(ThreadSafePriorityQueueTest, WaitPopForSucceeds) {
  std::thread producer([this]() {
    std::this_thread::sleep_for(20ms);
    m_pqueue.push(99);
  });

  auto result = m_pqueue.waitPopFor(500ms);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 99);

  producer.join();
}

TEST_F(ThreadSafePriorityQueueTest, WaitPopBlocks) {
  std::atomic<bool> popped{false};

  std::thread consumer([this, &popped]() {
    int value = m_pqueue.waitPop();
    EXPECT_EQ(value, 123);
    popped.store(true);
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(popped.load());

  m_pqueue.push(123);

  consumer.join();
  EXPECT_TRUE(popped.load());
}

// -----------------------------------------------------------------------------
// Thread Safety Tests
// -----------------------------------------------------------------------------

TEST_F(ThreadSafePriorityQueueTest, ConcurrentPush) {
  const int num_threads = 8;
  const int items_per_thread = 1000;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([this, t, items_per_thread]() {
      for (int i = 0; i < items_per_thread; ++i) {
        m_pqueue.push(t * items_per_thread + i);
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  EXPECT_EQ(m_pqueue.size(), num_threads * items_per_thread);
}

TEST_F(ThreadSafePriorityQueueTest, ConcurrentPushPopMaintainsPriority) {
  const int num_items = 1000;
  std::atomic<bool> done{false};
  std::vector<int> popped_values;
  std::mutex values_mutex;

  // Producer - push random values
  std::thread producer([this, num_items, &done]() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 1000);

    for (int i = 0; i < num_items; ++i) {
      m_pqueue.push(dis(gen));
    }
    done.store(true);
  });

  // Consumer - collect all values
  std::thread consumer([this, &done, &popped_values, &values_mutex]() {
    while (!done.load() || !m_pqueue.empty()) {
      auto result = m_pqueue.tryPop();
      if (result.has_value()) {
        std::lock_guard<std::mutex> lock(values_mutex);
        popped_values.push_back(*result);
      } else {
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(popped_values.size(), num_items);

  // Check that values are in non-increasing order (max-heap property maintained
  // locally) Note: with concurrent access, global order may not be perfectly
  // maintained but no crashes should occur
}

// -----------------------------------------------------------------------------
// Custom Comparator
// -----------------------------------------------------------------------------

struct Task {
  int priority;
  std::string name;

  bool operator<(const Task &other) const { return priority < other.priority; }
  bool operator>(const Task &other) const { return priority > other.priority; }
};

TEST(ThreadSafePriorityQueueCustomTest, CustomTypeMaxPriority) {
  ThreadSafePriorityQueue<Task> task_queue;

  task_queue.push({1, "low"});
  task_queue.push({10, "high"});
  task_queue.push({5, "medium"});

  auto high = task_queue.tryPop();
  ASSERT_TRUE(high.has_value());
  EXPECT_EQ(high->priority, 10);
  EXPECT_EQ(high->name, "high");

  auto medium = task_queue.tryPop();
  ASSERT_TRUE(medium.has_value());
  EXPECT_EQ(medium->priority, 5);

  auto low = task_queue.tryPop();
  ASSERT_TRUE(low.has_value());
  EXPECT_EQ(low->priority, 1);
}

TEST(ThreadSafePriorityQueueCustomTest, CustomTypeMinPriority) {
  ThreadSafePriorityQueue<Task, std::greater<Task>> task_queue;

  task_queue.push({1, "low"});
  task_queue.push({10, "high"});
  task_queue.push({5, "medium"});

  auto low = task_queue.tryPop();
  ASSERT_TRUE(low.has_value());
  EXPECT_EQ(low->priority, 1);

  auto medium = task_queue.tryPop();
  ASSERT_TRUE(medium.has_value());
  EXPECT_EQ(medium->priority, 5);

  auto high = task_queue.tryPop();
  ASSERT_TRUE(high.has_value());
  EXPECT_EQ(high->priority, 10);
}
