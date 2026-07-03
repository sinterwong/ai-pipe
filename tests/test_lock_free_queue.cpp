/**
 * @file test_lock_free_queue.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Comprehensive GTest unit tests for LockFreeMPMCQueue and
 * LockFreeNodeQueue
 * @version 1.1
 * @date 2026-02-06
 *
 * Test coverage:
 * 1. Core MPMC queue: push/pop, capacity rounding, boundary conditions
 * 2. SPSC (single producer/single consumer) correctness
 * 3. MPMC concurrent stress test with data integrity verification
 * 4. ABA problem verification via sequence-tag validation
 * 5. Drop policy: DropHead, DropTail, KeepLatest
 * 6. Statistics tracking
 * 7. Drop callback notification
 * 8. Clear and query operations
 * 9. Edge cases
 * 10. Performance benchmarks (latency < 100ns target)
 *
 * @copyright Copyright (c) 2026
 */

#include "lock_free_queue.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <iostream>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace ai_pipe;

namespace ai_pipe_unit_test::lock_free_queue {

class LockFreeMPMCQueueTest : public ::testing::Test {
protected:
  template <typename Q> static void fillQueue(Q &q, int start = 0) {
    for (std::size_t i = 0; i < q.capacity(); ++i) {
      ASSERT_TRUE(q.tryPush(static_cast<int>(start + i)));
    }
  }
};

class LockFreeNodeQueueTest : public ::testing::Test {
protected:
  static LockFreeNodeQueue<int>::Config
  makeConfig(std::size_t capacity, LockFreeDropPolicy policy, bool stats = true,
             std::size_t keep_latest_n = 1, const std::string &node = "",
             const std::string &port = "") {
    return LockFreeNodeQueue<int>::Config{
        .capacity = capacity,
        .drop_policy = policy,
        .keep_latest_n = keep_latest_n,
        .track_statistics = stats,
        .node_name = node,
        .port_name = port,
    };
  }
};

class LockFreeConcurrencyTest : public ::testing::Test {};

class LockFreePerformanceTest : public ::testing::Test {
protected:
  void SetUp() override {
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
    GTEST_SKIP() << "Performance thresholds are meaningless under sanitizers";
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
    GTEST_SKIP() << "Performance thresholds are meaningless under sanitizers";
#endif
#endif
  }
};

TEST_F(LockFreeMPMCQueueTest, CapacityRoundsUpToPowerOf2) {
  LockFreeMPMCQueue<int> q3(3);
  EXPECT_EQ(q3.capacity(), 4u);

  LockFreeMPMCQueue<int> q5(5);
  EXPECT_EQ(q5.capacity(), 8u);

  LockFreeMPMCQueue<int> q16(16);
  EXPECT_EQ(q16.capacity(), 16u);

  LockFreeMPMCQueue<int> q17(17);
  EXPECT_EQ(q17.capacity(), 32u);

  LockFreeMPMCQueue<int> q1(1);
  EXPECT_EQ(q1.capacity(), 2u); // Minimum is 2
}

TEST_F(LockFreeMPMCQueueTest, EmptyOnConstruction) {
  LockFreeMPMCQueue<int> q(8);
  EXPECT_TRUE(q.empty());
  EXPECT_EQ(q.size(), 0u);
  EXPECT_FALSE(q.isFull());
}

TEST_F(LockFreeMPMCQueueTest, SinglePushPop) {
  LockFreeMPMCQueue<int> q(4);
  ASSERT_TRUE(q.tryPush(42));
  EXPECT_EQ(q.size(), 1u);
  EXPECT_FALSE(q.empty());

  int val = 0;
  ASSERT_TRUE(q.tryPop(val));
  EXPECT_EQ(val, 42);
  EXPECT_TRUE(q.empty());
}

TEST_F(LockFreeMPMCQueueTest, FIFOOrder) {
  LockFreeMPMCQueue<int> q(8);
  for (int i = 0; i < 8; ++i) {
    ASSERT_TRUE(q.tryPush(std::move(i)));
  }
  EXPECT_TRUE(q.isFull());

  for (int i = 0; i < 8; ++i) {
    int val = -1;
    ASSERT_TRUE(q.tryPop(val));
    EXPECT_EQ(val, i);
  }
  EXPECT_TRUE(q.empty());
}

TEST_F(LockFreeMPMCQueueTest, FullQueueRejectsPush) {
  LockFreeMPMCQueue<int> q(4);
  fillQueue(q);
  ASSERT_TRUE(q.isFull());
  EXPECT_FALSE(q.tryPush(999));
}

TEST_F(LockFreeMPMCQueueTest, EmptyQueueRejectsPop) {
  LockFreeMPMCQueue<int> q(4);
  int val = 0;
  EXPECT_FALSE(q.tryPop(val));
}

TEST_F(LockFreeMPMCQueueTest, WrapAround) {
  LockFreeMPMCQueue<int> q(4);
  for (int round = 0; round < 3; ++round) {
    for (int i = 0; i < 4; ++i) {
      ASSERT_TRUE(q.tryPush(round * 100 + i));
    }
    for (int i = 0; i < 4; ++i) {
      int val = -1;
      ASSERT_TRUE(q.tryPop(val));
      EXPECT_EQ(val, round * 100 + i);
    }
  }
}

TEST_F(LockFreeMPMCQueueTest, ForcePushEvictsOldest) {
  LockFreeMPMCQueue<int> q(4);
  fillQueue(q);
  ASSERT_TRUE(q.isFull());

  int evicted = -1;
  bool did_evict = q.forcePush(99, evicted);
  EXPECT_TRUE(did_evict);
  EXPECT_EQ(evicted, 0); // Oldest item was 0

  // Queue should now contain [1, 2, 3, 99]
  int val;
  ASSERT_TRUE(q.tryPop(val));
  EXPECT_EQ(val, 1);
  ASSERT_TRUE(q.tryPop(val));
  EXPECT_EQ(val, 2);
  ASSERT_TRUE(q.tryPop(val));
  EXPECT_EQ(val, 3);
  ASSERT_TRUE(q.tryPop(val));
  EXPECT_EQ(val, 99);
  EXPECT_TRUE(q.empty());
}

TEST_F(LockFreeMPMCQueueTest, ForcePushNoEvictionWhenNotFull) {
  LockFreeMPMCQueue<int> q(4);
  q.tryPush(1);
  q.tryPush(2);

  int evicted = -1;
  bool did_evict = q.forcePush(3, evicted);
  EXPECT_FALSE(did_evict);
  EXPECT_EQ(q.size(), 3u);
}

TEST_F(LockFreeConcurrencyTest, SPSC_SequentialIntegrity) {
  constexpr int n = 100000;
  LockFreeMPMCQueue<int> q(1024);

  std::vector<int> received;
  received.reserve(n);

  std::thread consumer([&]() {
    int val;
    int count = 0;
    while (count < n) {
      if (q.tryPop(val)) {
        received.push_back(val);
        count++;
      }
    }
  });

  for (int i = 0; i < n; ++i) {
    while (!q.tryPush(std::move(i))) {
    }
  }

  consumer.join();

  ASSERT_EQ(received.size(), static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    EXPECT_EQ(received[i], i) << "FIFO violation at index " << i;
  }
}

TEST_F(LockFreeConcurrencyTest, MPMC_DataIntegrity_4P4C) {
  constexpr int k_producers = 4;
  constexpr int k_consumers = 4;
  constexpr int k_items_per_producer = 50000;
  constexpr int k_total_items = k_producers * k_items_per_producer;

  LockFreeMPMCQueue<std::uint64_t> q(4096);

  std::vector<std::vector<std::uint64_t>> consumer_results(k_consumers);

  auto encode = [](int producer_id, int seq) -> std::uint64_t {
    return (static_cast<std::uint64_t>(producer_id) << 32) |
           static_cast<std::uint64_t>(seq);
  };

  std::vector<std::thread> producers;
  std::vector<std::thread> consumers;

  std::atomic<bool> all_done{false};
  for (int c = 0; c < k_consumers; ++c) {
    consumers.emplace_back([&, c]() {
      std::uint64_t val;
      while (!all_done.load(std::memory_order_relaxed) || !q.empty()) {
        if (q.tryPop(val)) {
          consumer_results[c].push_back(val);
        }
      }
      // Final drain
      while (q.tryPop(val)) {
        consumer_results[c].push_back(val);
      }
    });
  }

  for (int p = 0; p < k_producers; ++p) {
    producers.emplace_back([&, p]() {
      for (int i = 0; i < k_items_per_producer; ++i) {
        auto val = encode(p, i);
        while (!q.tryPush(std::move(val))) {
        }
      }
    });
  }

  for (auto &t : producers)
    t.join();
  all_done.store(true, std::memory_order_release);
  for (auto &t : consumers)
    t.join();

  // Verify: all items consumed exactly once (no duplicates, no losses)
  std::set<std::uint64_t> all_items;
  for (auto &result : consumer_results) {
    for (auto v : result) {
      EXPECT_TRUE(all_items.insert(v).second) << "Duplicate item: " << v;
    }
  }
  EXPECT_EQ(all_items.size(), static_cast<size_t>(k_total_items));

  for (int p = 0; p < k_producers; ++p) {
    for (int i = 0; i < k_items_per_producer; ++i) {
      auto expected = encode(p, i);
      EXPECT_TRUE(all_items.count(expected) > 0)
          << "Missing item from producer " << p << " seq " << i;
    }
  }
}

TEST_F(LockFreeConcurrencyTest, ABA_SequenceTagPreventsCorruption) {
  constexpr int n = 200000;
  LockFreeMPMCQueue<int> q(4); // Small capacity forces frequent wrap

  std::atomic<bool> stop{false};
  std::atomic<int> errors{0};
  std::atomic<int> total_consumed{0};

  std::vector<std::thread> threads;

  // 2 producers
  for (int p = 0; p < 2; ++p) {
    threads.emplace_back([&, p]() {
      int base = p * n;
      for (int i = 0; i < n; ++i) {
        while (!q.tryPush(base + i) && !stop.load(std::memory_order_relaxed)) {
        }
      }
    });
  }

  // 2 consumers
  for (int c = 0; c < 2; ++c) {
    threads.emplace_back([&]() {
      int val;
      while (!stop.load(std::memory_order_relaxed) || !q.empty()) {
        if (q.tryPop(val)) {
          if (val < 0 || val >= 2 * n) {
            errors.fetch_add(1, std::memory_order_relaxed);
          }
          total_consumed.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  // Wait for producers
  threads[0].join();
  threads[1].join();
  stop.store(true, std::memory_order_release);

  // Wait for consumers
  threads[2].join();
  threads[3].join();

  EXPECT_EQ(errors.load(), 0) << "Data corruption detected (ABA problem)";
  EXPECT_EQ(total_consumed.load(), 2 * n);
}

TEST_F(LockFreeNodeQueueTest, DropTail_RejectsOnFull) {
  auto cfg = makeConfig(4, LockFreeDropPolicy::DropTail);
  LockFreeNodeQueue<int> q(cfg);

  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(q.push(i));
  }

  EXPECT_FALSE(q.push(99)); // Should be rejected

  auto &stats = q.statistics();
  EXPECT_EQ(stats.total_rejected.load(), 1u);

  // Original items preserved in FIFO order
  for (int i = 0; i < 4; ++i) {
    auto val = q.tryPop();
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, i);
  }
}

TEST_F(LockFreeNodeQueueTest, DropHead_EvictsOldest) {
  auto cfg = makeConfig(4, LockFreeDropPolicy::DropHead);
  LockFreeNodeQueue<int> q(cfg);

  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(q.push(i));
  }

  ASSERT_TRUE(q.push(99)); // Should succeed (evicting oldest)

  auto &stats = q.statistics();
  EXPECT_EQ(stats.total_dropped.load(), 1u);

  // First item should now be 1 (0 was evicted)
  auto val = q.tryPop();
  ASSERT_TRUE(val.has_value());
  EXPECT_EQ(*val, 1);
}

TEST_F(LockFreeNodeQueueTest, DropHead_MultipleEvictions) {
  auto cfg = makeConfig(4, LockFreeDropPolicy::DropHead);
  LockFreeNodeQueue<int> q(cfg);

  // Overfill with 8 items (only 4 can be stored)
  for (int i = 0; i < 8; ++i) {
    q.push(i);
  }

  auto &stats = q.statistics();
  EXPECT_EQ(stats.total_dropped.load(), 4u);

  // Should have the latest 4 items: [4, 5, 6, 7]
  std::vector<int> items;
  while (auto val = q.tryPop()) {
    items.push_back(*val);
  }
  ASSERT_EQ(items.size(), 4u);
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(items[i], i + 4);
  }
}

TEST_F(LockFreeNodeQueueTest, KeepLatest_MaintainsWindow) {
  auto cfg = makeConfig(8, LockFreeDropPolicy::KeepLatest, true, 2);
  LockFreeNodeQueue<int> q(cfg);

  for (int i = 0; i < 5; ++i) {
    q.push(i);
  }

  // Queue should have at most keep_latest_n items
  EXPECT_LE(q.size(), 2u);

  auto val1 = q.tryPop();
  ASSERT_TRUE(val1.has_value());
  auto val2 = q.tryPop();
  if (val2.has_value()) {
    EXPECT_GE(*val2, 3); // Either 3 or 4
  }
}

TEST_F(LockFreeNodeQueueTest, Statistics_TracksPushPop) {
  auto cfg = makeConfig(8, LockFreeDropPolicy::DropTail);
  LockFreeNodeQueue<int> q(cfg);

  for (int i = 0; i < 5; ++i) {
    q.push(i);
  }
  for (int i = 0; i < 3; ++i) {
    (void)q.tryPop();
  }

  auto &stats = q.statistics();
  EXPECT_EQ(stats.total_pushed.load(), 5u);
  EXPECT_EQ(stats.total_popped.load(), 3u);
  EXPECT_GE(stats.peak_size.load(), 5u);
}

TEST_F(LockFreeNodeQueueTest, Statistics_DropTailTracksRejections) {
  auto cfg = makeConfig(2, LockFreeDropPolicy::DropTail);
  LockFreeNodeQueue<int> q(cfg);

  q.push(1);
  q.push(2);
  q.push(3); // Should be rejected

  auto &stats = q.statistics();
  EXPECT_EQ(stats.total_rejected.load(), 1u);
  EXPECT_EQ(stats.total_dropped.load(), 0u);
}

TEST_F(LockFreeNodeQueueTest, Statistics_DropHeadTracksDrops) {
  auto cfg = makeConfig(2, LockFreeDropPolicy::DropHead);
  LockFreeNodeQueue<int> q(cfg);

  q.push(1);
  q.push(2);
  q.push(3); // Should evict oldest

  auto &stats = q.statistics();
  EXPECT_EQ(stats.total_dropped.load(), 1u);
  EXPECT_EQ(stats.total_rejected.load(), 0u);
}

TEST_F(LockFreeNodeQueueTest, Statistics_Reset) {
  auto cfg = makeConfig(8, LockFreeDropPolicy::DropHead);
  LockFreeNodeQueue<int> q(cfg);

  q.push(1);
  q.push(2);
  (void)q.tryPop();

  q.resetStatistics();

  auto &stats = q.statistics();
  EXPECT_EQ(stats.total_pushed.load(), 0u);
  EXPECT_EQ(stats.total_popped.load(), 0u);
}

TEST_F(LockFreeNodeQueueTest, DropCallback_FiresOnEviction) {
  std::atomic<int> callback_count{0};
  std::string last_reason;

  auto cfg =
      makeConfig(2, LockFreeDropPolicy::DropHead, true, 1, "TestNode", "input");
  LockFreeNodeQueue<int> q(cfg);

  q.setDropCallback([&](const DropEvent &event) {
    callback_count.fetch_add(1, std::memory_order_relaxed);
    last_reason = event.reason;
  });

  q.push(1);
  q.push(2);
  q.push(3); // Triggers eviction

  EXPECT_GE(callback_count.load(), 1);
}

TEST_F(LockFreeNodeQueueTest, Clear) {
  LockFreeNodeQueue<int> q(8);

  for (int i = 0; i < 5; ++i) {
    q.push(i);
  }
  ASSERT_EQ(q.size(), 5u);

  q.clear();
  EXPECT_TRUE(q.empty());
  EXPECT_EQ(q.size(), 0u);
}

TEST_F(LockFreeNodeQueueTest, FillRatio) {
  LockFreeNodeQueue<int> q(8);

  EXPECT_LT(q.fillRatio(), 0.01);

  for (int i = 0; i < 4; ++i) {
    q.push(i);
  }

  // Capacity is 8 (already power of 2), so 4/8 = 0.5
  double ratio = q.fillRatio();
  EXPECT_GE(ratio, 0.4);
  EXPECT_LE(ratio, 0.6);
}

TEST_F(LockFreeMPMCQueueTest, MinimumCapacity) {
  LockFreeMPMCQueue<int> q(1); // Rounded up to 2
  EXPECT_EQ(q.capacity(), 2u);

  ASSERT_TRUE(q.tryPush(1));
  ASSERT_TRUE(q.tryPush(2));
  EXPECT_FALSE(q.tryPush(3));

  int val;
  ASSERT_TRUE(q.tryPop(val));
  EXPECT_EQ(val, 1);
}

TEST_F(LockFreeMPMCQueueTest, LargeCapacity) {
  LockFreeMPMCQueue<int> q(10000);
  EXPECT_EQ(q.capacity(), 16384u);

  for (int i = 0; i < 10000; ++i) {
    ASSERT_TRUE(q.tryPush(std::move(i)));
  }
  EXPECT_EQ(q.size(), 10000u);
}

TEST_F(LockFreeMPMCQueueTest, RapidPushPopCycle) {
  LockFreeMPMCQueue<int> q(4);

  for (int i = 0; i < 100000; ++i) {
    ASSERT_TRUE(q.tryPush(std::move(i)));
    int val;
    ASSERT_TRUE(q.tryPop(val));
    EXPECT_EQ(val, i);
  }
}

TEST_F(LockFreePerformanceTest, SingleThreadLatency) {
  constexpr int n = 1000000;
  LockFreeMPMCQueue<int> q(1024);

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < n; ++i) {
    q.tryPush(std::move(i));
    int val;
    q.tryPop(val);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto total_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();

  double avg_ns = static_cast<double>(total_ns) / (2.0 * n);
  std::cout << "  [PERF] Single-thread avg latency: " << avg_ns
            << " ns/op (target: <100ns)" << std::endl;

  EXPECT_LT(avg_ns, 100.0) << "Performance target missed: " << avg_ns
                           << " ns/op exceeds 100ns threshold";
}

TEST_F(LockFreePerformanceTest, MPMCThroughput) {
  constexpr int k_producers = 4;
  constexpr int k_consumers = 4;
  constexpr int k_items_per_producer = 500000;

  LockFreeMPMCQueue<std::uint64_t> q(4096);

  std::atomic<bool> all_done{false};
  std::atomic<std::uint64_t> total_consumed{0};

  auto start = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> consumers;
  for (int c = 0; c < k_consumers; ++c) {
    consumers.emplace_back([&]() {
      std::uint64_t val;
      while (!all_done.load(std::memory_order_relaxed) || !q.empty()) {
        if (q.tryPop(val)) {
          total_consumed.fetch_add(1, std::memory_order_relaxed);
        }
      }
      while (q.tryPop(val)) {
        total_consumed.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::vector<std::thread> producers;
  for (int p = 0; p < k_producers; ++p) {
    producers.emplace_back([&, p]() {
      for (int i = 0; i < k_items_per_producer; ++i) {
        auto val = static_cast<std::uint64_t>(p * k_items_per_producer + i);
        while (!q.tryPush(std::move(val))) {
        }
      }
    });
  }

  for (auto &t : producers)
    t.join();
  all_done.store(true, std::memory_order_release);
  for (auto &t : consumers)
    t.join();

  auto end = std::chrono::high_resolution_clock::now();
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
          .count();

  auto total_items = k_producers * k_items_per_producer;
  double throughput_mops = static_cast<double>(total_items) /
                           (static_cast<double>(elapsed_ms) / 1000.0) / 1e6;

  std::cout << "  [PERF] MPMC throughput: " << throughput_mops << " Mops/sec ("
            << total_items << " items in " << elapsed_ms << " ms)" << std::endl;

  EXPECT_EQ(total_consumed.load(), static_cast<std::uint64_t>(total_items));
}

TEST_F(LockFreeConcurrencyTest, ForcePush_ExtremeContention_SmallCapacity) {
  // Target: Verify DropHead consistency when capacity is tiny but many threads are pushing
  constexpr int k_threads = 8;
  constexpr int k_items_per_thread = 1000;
  auto cfg = LockFreeNodeQueue<int>::Config{
      .capacity = 2,
      .drop_policy = LockFreeDropPolicy::DropHead,
      .track_statistics = true
  };
  LockFreeNodeQueue<int> q(cfg);

  std::vector<std::thread> producers;
  for (int i = 0; i < k_threads; ++i) {
    producers.emplace_back([&]() {
      for (int j = 0; j < k_items_per_thread; ++j) {
        q.push(j);
      }
    });
  }

  for (auto &t : producers) t.join();

  auto& stats = q.statistics();
  int drain_count = 0;
  while (q.tryPop()) drain_count++;

  EXPECT_EQ(q.size(), 0u);
  // total_pushed = total_dropped + total_popped + current_size
  // Since we just drained, popped should increase.
  // Actually NodeQueue stats track total_popped separately.

  auto pushed = stats.total_pushed.load();
  auto dropped = stats.total_dropped.load();
  auto popped = stats.total_popped.load();

  EXPECT_EQ(pushed, dropped + popped);
  EXPECT_LE(popped, pushed);
}

} // namespace ai_pipe_unit_test::lock_free_queue

// =============================================================================
// tryPeek (P4.2) - single-consumer non-destructive front access
// =============================================================================

TEST(LockFreePeekTest, PeekDoesNotConsume) {
  LockFreeMPMCQueue<int> q(8);
  ASSERT_TRUE(q.tryPush(11));
  ASSERT_TRUE(q.tryPush(22));

  int val = 0;
  EXPECT_TRUE(q.tryPeek(val));
  EXPECT_EQ(val, 11);
  EXPECT_TRUE(q.tryPeek(val));
  EXPECT_EQ(val, 11) << "peek must not advance";
  EXPECT_EQ(q.size(), 2u);

  ASSERT_TRUE(q.tryPop(val));
  EXPECT_EQ(val, 11);
  EXPECT_TRUE(q.tryPeek(val));
  EXPECT_EQ(val, 22);
}

TEST(LockFreePeekTest, PeekEmptyReturnsFalse) {
  LockFreeMPMCQueue<int> q(4);
  int val = 0;
  EXPECT_FALSE(q.tryPeek(val));
}

TEST(LockFreePeekTest, NodeQueuePeekOptional) {
  LockFreeNodeQueue<int> q(4);
  EXPECT_FALSE(q.tryPeek().has_value());
  ASSERT_TRUE(q.push(7));
  EXPECT_EQ(q.tryPeek().value_or(-1), 7);
  EXPECT_EQ(q.size(), 1u);
}

// =============================================================================
// Drop events carry real frame ids once an accessor is set (P4.2)
// =============================================================================

TEST(LockFreeDropFrameIdTest, DropEventUsesAccessorFrameId) {
  struct Packet {
    FrameId frame{0};
  };
  using PacketPtr = std::shared_ptr<Packet>;

  LockFreeNodeQueue<PacketPtr>::Config config{
      .capacity = 2,
      .drop_policy = LockFreeDropPolicy::DropHead,
      .keep_latest_n = 1,
      .track_statistics = true,
      .node_name = "n",
      .port_name = "p",
  };
  LockFreeNodeQueue<PacketPtr> q(config);
  q.setFrameIdAccessor([](const PacketPtr &p) -> std::optional<FrameId> {
    return p ? std::optional<FrameId>(p->frame) : std::nullopt;
  });

  std::vector<FrameId> dropped;
  q.setDropCallback(
      [&](const DropEvent &event) { dropped.push_back(event.frame_id); });

  for (FrameId f = 1; f <= 5; ++f) {
    auto packet = std::make_shared<Packet>();
    packet->frame = f;
    ASSERT_TRUE(q.push(packet));
  }

  ASSERT_FALSE(dropped.empty());
  for (auto f : dropped) {
    EXPECT_NE(f, frame_constants::k_invalid_frame_id);
  }
  EXPECT_EQ(dropped.front(), 1u) << "DropHead evicts oldest first";
}
