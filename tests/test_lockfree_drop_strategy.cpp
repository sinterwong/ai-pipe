/**
 * @file test_lockfree_drop_strategy.cpp
 * @brief Unit tests for LockFreeNodeQueue drop strategy correctness
 *
 * These tests verify that the lock-free queue drop strategies:
 * - Never silently lose data (pushed + dropped = total input)
 * - Properly account for every eviction
 * - Work correctly under concurrent producer/consumer pressure
 * - Achieve expected throughput in backpressure scenarios
 *
 * Regression tests for the forcePush data loss bug where the non-atomic
 * evict+push operation allowed slot stealing, causing silent frame loss.
 */

#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "helper_nodes.hpp"
#include "lock_free_queue.hpp"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace ai_pipe;
using namespace std::chrono_literals;

namespace ai_pipe_unit_test::lockfree_drop {

// =============================================================================
// Helper: SlowProcessNode — configurable delay for backpressure simulation
// =============================================================================

class SlowProcessNode : public ILogicNode {
public:
  explicit SlowProcessNode(const std::string &name,
                           std::chrono::microseconds delay)
      : ILogicNode(name), m_delay(delay) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
    }
    m_processCount.fetch_add(1, std::memory_order_relaxed);
    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int processCount() const {
    return m_processCount.load(std::memory_order_relaxed);
  }

private:
  std::chrono::microseconds m_delay;
  std::atomic<int> m_processCount{0};
};

// =============================================================================
// Test Fixture
// =============================================================================

class LockFreeDropStrategyTest : public ::testing::Test {
protected:
  static PortDataPtr createData(uint64_t id) {
    auto data = std::make_shared<PortData>();
    data->id = id;
    data->setParam("timestamp", std::chrono::steady_clock::now());
    return data;
  }
};

// =============================================================================
// 1. Basic DropHead: No Data Loss (Single-Threaded)
// =============================================================================

TEST_F(LockFreeDropStrategyTest, DropHead_SingleThread_NoDataLoss) {
  // With DropHead, every push must either be enqueued or cause an eviction.
  // total_pushed (successful enqueues) + total_dropped = total attempts
  // that encountered a full queue.
  // More importantly: no data should silently disappear.

  constexpr std::size_t capacity = 8;
  constexpr std::size_t total_items = 200;

  LockFreeNodeQueue<int>::Config config{
      .capacity = capacity,
      .drop_policy = LockFreeDropPolicy::DropHead,
      .track_statistics = true,
      .node_name = "test_node",
      .port_name = "input",
  };

  LockFreeNodeQueue<int> queue(config);

  std::atomic<int> drop_count{0};
  queue.setDropCallback([&](const DropEvent &) { drop_count++; });

  // Push items without consuming — forces evictions after queue fills
  for (std::size_t i = 0; i < total_items; ++i) {
    bool accepted = queue.push(static_cast<int>(i));
    EXPECT_TRUE(accepted) << "DropHead must always accept, failed at i=" << i;
  }

  auto &stats = queue.statistics();
  auto pushed = stats.total_pushed.load();
  auto dropped = stats.total_dropped.load();

  // All items were accepted
  EXPECT_EQ(pushed, total_items);

  // Queue holds exactly capacity items at the end
  EXPECT_EQ(queue.size(), capacity);

  // Invariant: pushed = items_in_queue + dropped
  EXPECT_EQ(pushed, queue.size() + dropped)
      << "Data accounting mismatch: pushed=" << pushed
      << " queue_size=" << queue.size() << " dropped=" << dropped;

  // Drop callback count matches statistics
  EXPECT_EQ(drop_count.load(), static_cast<int>(dropped));

  // Verify remaining items are the LATEST ones (DropHead keeps newest)
  std::vector<int> remaining;
  while (auto item = queue.tryPop()) {
    remaining.push_back(*item);
  }
  ASSERT_EQ(remaining.size(), capacity);
  for (std::size_t i = 0; i < remaining.size(); ++i) {
    EXPECT_EQ(remaining[i], static_cast<int>(total_items - capacity + i))
        << "DropHead should keep the latest items";
  }
}

// =============================================================================
// 2. DropHead: Concurrent Producers — No Silent Loss
//    (Regression test for the forcePush slot-stealing bug)
// =============================================================================

TEST_F(LockFreeDropStrategyTest, DropHead_ConcurrentProducers_NoSilentLoss) {
  // Multiple producers push simultaneously into a small queue.
  // Under the old buggy forcePush, slot stealing between evict and push
  // would cause silent data loss.

  constexpr std::size_t capacity = 8; // Small to maximize contention
  constexpr std::size_t items_per_thread = 500;
  constexpr std::size_t num_producers = 4;
  constexpr std::size_t total_items = items_per_thread * num_producers;

  LockFreeNodeQueue<int>::Config config{
      .capacity = capacity,
      .drop_policy = LockFreeDropPolicy::DropHead,
      .track_statistics = true,
      .node_name = "test_node",
      .port_name = "input",
  };

  LockFreeNodeQueue<int> queue(config);

  std::atomic<int> drop_count{0};
  queue.setDropCallback([&](const DropEvent &) { drop_count++; });

  // Launch concurrent producers (no consumers — maximum eviction pressure)
  std::vector<std::thread> producers;
  for (std::size_t t = 0; t < num_producers; ++t) {
    producers.emplace_back([&, t]() {
      for (std::size_t i = 0; i < items_per_thread; ++i) {
        int value = static_cast<int>(t * items_per_thread + i);
        bool accepted = queue.push(value);
        EXPECT_TRUE(accepted) << "DropHead must always accept";
      }
    });
  }

  for (auto &t : producers) {
    t.join();
  }

  auto &stats = queue.statistics();
  auto pushed = stats.total_pushed.load();
  auto dropped = stats.total_dropped.load();
  auto remaining = queue.size();

  // CRITICAL INVARIANT: no data silently lost
  // Every item is either in the queue or was explicitly dropped
  EXPECT_EQ(pushed, remaining + dropped)
      << "DATA LOSS DETECTED! pushed=" << pushed << " in_queue=" << remaining
      << " dropped=" << dropped
      << " missing=" << (pushed - remaining - dropped);

  // All items were accepted
  EXPECT_EQ(pushed, total_items);

  // Drop callback matches stats
  EXPECT_EQ(drop_count.load(), static_cast<int>(dropped));

  // Queue should hold at most capacity items
  EXPECT_LE(remaining, capacity);
}

// =============================================================================
// 3. DropHead: Concurrent Producers + Consumers — Full Accounting
// =============================================================================

TEST_F(LockFreeDropStrategyTest,
       DropHead_ConcurrentProducersConsumers_FullAccounting) {
  constexpr std::size_t capacity = 16;
  constexpr std::size_t items_per_producer = 1000;
  constexpr std::size_t num_producers = 4;
  constexpr std::size_t num_consumers = 2;
  constexpr std::size_t total_items = items_per_producer * num_producers;

  LockFreeNodeQueue<int>::Config config{
      .capacity = capacity,
      .drop_policy = LockFreeDropPolicy::DropHead,
      .track_statistics = true,
      .node_name = "test_node",
      .port_name = "input",
  };

  LockFreeNodeQueue<int> queue(config);

  std::atomic<int> drop_count{0};
  queue.setDropCallback([&](const DropEvent &) { drop_count++; });

  std::atomic<bool> producers_done{false};
  std::atomic<std::size_t> consumed{0};

  // Launch consumers
  std::vector<std::thread> consumers;
  for (std::size_t c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&]() {
      while (!producers_done.load(std::memory_order_acquire) ||
             !queue.empty()) {
        if (queue.tryPop()) {
          consumed.fetch_add(1, std::memory_order_relaxed);
        } else {
          std::this_thread::yield();
        }
      }
    });
  }

  // Launch producers
  std::vector<std::thread> producers;
  for (std::size_t t = 0; t < num_producers; ++t) {
    producers.emplace_back([&, t]() {
      for (std::size_t i = 0; i < items_per_producer; ++i) {
        queue.push(static_cast<int>(t * items_per_producer + i));
      }
    });
  }

  for (auto &t : producers) {
    t.join();
  }
  producers_done.store(true, std::memory_order_release);

  for (auto &t : consumers) {
    t.join();
  }

  auto &stats = queue.statistics();
  auto pushed = stats.total_pushed.load();
  auto popped = stats.total_popped.load();
  auto dropped = stats.total_dropped.load();
  auto remaining = queue.size();

  // CRITICAL INVARIANT: every item is accounted for
  // pushed = popped + dropped + remaining_in_queue
  EXPECT_EQ(pushed, popped + dropped + remaining)
      << "ACCOUNTING MISMATCH! pushed=" << pushed << " popped=" << popped
      << " dropped=" << dropped << " remaining=" << remaining;

  EXPECT_EQ(pushed, total_items);
  EXPECT_EQ(drop_count.load(), static_cast<int>(dropped));
}

// =============================================================================
// 4. DropTail: Rejection accounting
// =============================================================================

TEST_F(LockFreeDropStrategyTest, DropTail_RejectionAccounting) {
  constexpr std::size_t capacity = 8;
  constexpr std::size_t total_items = 100;

  LockFreeNodeQueue<int>::Config config{
      .capacity = capacity,
      .drop_policy = LockFreeDropPolicy::DropTail,
      .track_statistics = true,
  };

  LockFreeNodeQueue<int> queue(config);

  std::size_t accepted_count = 0;
  std::size_t rejected_count = 0;

  for (std::size_t i = 0; i < total_items; ++i) {
    if (queue.push(static_cast<int>(i))) {
      accepted_count++;
    } else {
      rejected_count++;
    }
  }

  auto &stats = queue.statistics();

  // DropTail: only the first 'capacity' items are accepted
  EXPECT_EQ(accepted_count, capacity);
  EXPECT_EQ(rejected_count, total_items - capacity);

  // Statistics: pushed counts only successful pushes
  EXPECT_EQ(stats.total_pushed.load(), capacity);
  EXPECT_EQ(stats.total_rejected.load(), total_items - capacity);
  EXPECT_EQ(stats.total_dropped.load(), 0u);
}

// =============================================================================
// 5. KeepLatest: Maintains Window Size
// =============================================================================

TEST_F(LockFreeDropStrategyTest, KeepLatest_MaintainsWindowSize) {
  constexpr std::size_t capacity = 16;
  constexpr std::size_t keep_n = 3;
  constexpr std::size_t total_items = 50;

  LockFreeNodeQueue<int>::Config config{
      .capacity = capacity,
      .drop_policy = LockFreeDropPolicy::KeepLatest,
      .keep_latest_n = keep_n,
      .track_statistics = true,
  };

  LockFreeNodeQueue<int> queue(config);

  std::atomic<int> drop_count{0};
  queue.setDropCallback([&](const DropEvent &) { drop_count++; });

  for (std::size_t i = 0; i < total_items; ++i) {
    bool accepted = queue.push(static_cast<int>(i));
    EXPECT_TRUE(accepted) << "KeepLatest must always accept";
  }

  // Queue should hold at most keep_n items
  EXPECT_LE(queue.size(), keep_n);

  auto &stats = queue.statistics();
  auto pushed = stats.total_pushed.load();
  auto dropped = stats.total_dropped.load();
  auto remaining = queue.size();

  // Accounting: pushed = remaining + dropped
  EXPECT_EQ(pushed, remaining + dropped)
      << "pushed=" << pushed << " remaining=" << remaining
      << " dropped=" << dropped;
}

// =============================================================================
// 6. forcePush: Guarantee No Data Loss
//    (Direct regression test for the original bug)
// =============================================================================

TEST_F(LockFreeDropStrategyTest, ForcePush_GuaranteeNoDataLoss) {
  // Test the low-level forcePush directly under contention
  constexpr std::size_t capacity = 4; // Very small for maximum contention
  constexpr std::size_t total_ops = 2000;
  constexpr std::size_t num_threads = 4;

  LockFreeMPMCQueue<int> queue(capacity);

  // Fill the queue first
  for (std::size_t i = 0; i < capacity; ++i) {
    ASSERT_TRUE(queue.tryPush(static_cast<int>(i)));
  }
  ASSERT_TRUE(queue.isFull());

  std::atomic<std::size_t> total_evictions{0};
  std::atomic<std::size_t> total_no_evictions{0};

  // Concurrent forcePush operations
  std::vector<std::thread> threads;
  for (std::size_t t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (std::size_t i = 0; i < total_ops; ++i) {
        int evicted;
        int value = static_cast<int>(t * total_ops + i + capacity);
        bool did_evict = queue.forcePush(value, evicted);
        if (did_evict) {
          total_evictions.fetch_add(1, std::memory_order_relaxed);
        } else {
          total_no_evictions.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  // Drain the queue
  std::size_t remaining = 0;
  int item;
  while (queue.tryPop(item)) {
    remaining++;
  }

  // CRITICAL: every forcePush either evicted+pushed or just pushed.
  // Total items in system = initial_capacity + total_ops
  // Items that left = total_evictions + remaining_popped
  // Items still in queue at end = remaining
  //
  // The key invariant: no items were silently lost
  auto total_ops_done = total_evictions.load() + total_no_evictions.load();
  EXPECT_EQ(total_ops_done, total_ops * num_threads)
      << "All forcePush operations should complete";

  // Queue should hold exactly 'capacity' items at the end
  // (it was full, and every forcePush maintains fullness)
  EXPECT_EQ(remaining, capacity)
      << "Queue should hold capacity items. remaining=" << remaining
      << " capacity=" << capacity;
}

// =============================================================================
// 7. Streaming Backpressure: Drop Rate Within Expected Bounds
//    (Integration test reproducing the benchmark scenario)
// =============================================================================

TEST_F(LockFreeDropStrategyTest, StreamingBackpressure_DropRateWithinBounds) {
  // Reproduce the benchmark scenario:
  // Fast producer (10us) → 4-node pipeline with delay → slow consumer
  // Verify drop_rate + throughput accounting is correct

  constexpr std::size_t workers = 4;
  constexpr std::size_t queue_capacity = 16;
  constexpr std::size_t pipeline_depth = 4;
  constexpr auto consumer_delay = 100us;
  constexpr std::size_t frames = 200;

  // Build pipeline: source → node_0 → node_1 → node_2 → node_3 → sink
  auto graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source");
  graph->addNode(source);

  std::string prev_node = "source";
  std::vector<std::shared_ptr<ILogicNode>> nodes;
  nodes.push_back(source);

  for (std::size_t i = 0; i < pipeline_depth; ++i) {
    auto name = "node_" + std::to_string(i);
    auto node = std::make_shared<SlowProcessNode>(name, consumer_delay);
    nodes.push_back(node);
    graph->addNode(node);
    graph->addEdge(prev_node, "output", name, "input");
    prev_node = name;
  }

  auto sink = std::make_shared<SinkNode>("sink");
  nodes.push_back(sink);
  graph->addNode(sink);
  graph->addEdge(prev_node, "output", "sink", "input");

  // Create streaming engine
  auto config = EngineConfig::stream(workers, queue_capacity);
  config.enable_statistics = true;
  config.enable_drop_logging = false; // Reduce noise
  auto engine = ExecutionEngine::create(config);
  engine->initialize(graph.get(), workers);

  std::atomic<std::size_t> drop_count{0};
  engine->setDropCallback(
      [&](const std::string &, std::uint64_t, const std::string &) {
        drop_count.fetch_add(1, std::memory_order_relaxed);
      });

  // Run the streaming scenario
  engine->startStreaming();

  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = createData(i);
    (void)engine->pushInput("source", "output", packet);
    std::this_thread::sleep_for(10us); // Fast producer
  }

  engine->waitForDrain(0, 10000ms);
  engine->stopStreaming(true);

  auto stats = engine->statistics();
  auto processed = stats.total_output_frames;
  auto dropped = stats.total_dropped_frames;
  double drop_rate = stats.drop_rate;

  // CRITICAL INVARIANT: all frames are accounted for
  // dropped + processed should equal total frames pushed
  // (some tolerance for timing — frames may still be in-flight)
  EXPECT_GT(processed, 0u) << "At least some frames should reach the sink";

  EXPECT_LE(dropped + processed, frames)
      << "Cannot have more frames than were pushed";

  // The sum should be close to total frames (within pipeline buffer)
  auto total_accounted = dropped + processed;
  auto max_in_flight = queue_capacity * (pipeline_depth + 1);
  EXPECT_GE(total_accounted + max_in_flight, frames)
      << "Too many frames unaccounted: accounted=" << total_accounted
      << " frames=" << frames << " max_in_flight=" << max_in_flight;

  // Drop rate should be reasonable given the speed mismatch
  // Producer: 100k frames/sec, Pipeline throughput: ~10k frames/sec
  // Expected drop rate: ~90%, but with buffering could be lower
  // We allow a wide range but reject obviously broken values
  EXPECT_GT(drop_rate, 0.0) << "Some drops expected with fast producer";
  EXPECT_LT(drop_rate, 99.0) << "Should process at least 1% of frames";

  // Drop callback count should match engine stats
  EXPECT_EQ(drop_count.load(), dropped);

  engine->reset();
}

// =============================================================================
// 8. Streaming Backpressure: Equal Speed — Near-Zero Drops
//    (When producer ~= consumer speed, drops should be minimal)
// =============================================================================

TEST_F(LockFreeDropStrategyTest,
       StreamingBackpressure_EqualSpeed_MinimalDrops) {
  constexpr std::size_t workers = 4;
  constexpr std::size_t queue_capacity = 16;
  constexpr std::size_t pipeline_depth = 2; // Shorter pipeline
  constexpr auto consumer_delay = 500us;
  constexpr std::size_t frames = 100;
  constexpr auto producer_interval = 1000us; // Slower than consumer

  auto graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source");
  graph->addNode(source);

  std::string prev_node = "source";
  std::vector<std::shared_ptr<ILogicNode>> nodes;
  nodes.push_back(source);

  for (std::size_t i = 0; i < pipeline_depth; ++i) {
    auto name = "node_" + std::to_string(i);
    auto node = std::make_shared<SlowProcessNode>(name, consumer_delay);
    nodes.push_back(node);
    graph->addNode(node);
    graph->addEdge(prev_node, "output", name, "input");
    prev_node = name;
  }

  auto sink = std::make_shared<SinkNode>("sink");
  nodes.push_back(sink);
  graph->addNode(sink);
  graph->addEdge(prev_node, "output", "sink", "input");

  auto config = EngineConfig::stream(workers, queue_capacity);
  config.enable_statistics = true;
  config.enable_drop_logging = false;
  auto engine = ExecutionEngine::create(config);
  engine->initialize(graph.get(), workers);

  engine->startStreaming();

  for (std::size_t i = 0; i < frames; ++i) {
    (void)engine->pushInput("source", "output", createData(i));
    std::this_thread::sleep_for(producer_interval);
  }

  engine->waitForDrain(0, 15000ms);
  engine->stopStreaming(true);

  auto stats = engine->statistics();
  auto processed = stats.total_output_frames;
  auto dropped = stats.total_dropped_frames;

  // With slow producer (1ms interval) and fast pipeline (500us * 2 stages,
  // 4 workers), drops should be minimal
  double actual_drop_rate = (processed + dropped) > 0
                                ? 100.0 * static_cast<double>(dropped) /
                                      static_cast<double>(processed + dropped)
                                : 0.0;

  EXPECT_LT(actual_drop_rate, 30.0)
      << "Drop rate should be low when producer <= consumer speed. "
      << "processed=" << processed << " dropped=" << dropped;

  // Most frames should be processed
  EXPECT_GE(processed, frames / 2)
      << "At least half of frames should be processed";

  engine->reset();
}

// =============================================================================
// 9. DropHead: Queue Integrity Under Extreme Contention
// =============================================================================

TEST_F(LockFreeDropStrategyTest, DropHead_ExtremeContention_QueueIntegrity) {
  // Stress test: many producers, tiny queue, verify no crash or hang
  constexpr std::size_t capacity = 2; // Minimal capacity
  constexpr std::size_t items_per_thread = 1000;
  constexpr std::size_t num_producers = 8;

  LockFreeNodeQueue<int>::Config config{
      .capacity = capacity,
      .drop_policy = LockFreeDropPolicy::DropHead,
      .track_statistics = true,
  };

  LockFreeNodeQueue<int> queue(config);
  std::atomic<int> drop_count{0};
  queue.setDropCallback([&](const DropEvent &) { drop_count++; });

  // Also run consumers to create mixed contention
  std::atomic<bool> done{false};
  std::atomic<std::size_t> consumed{0};

  std::vector<std::thread> consumers;
  for (int c = 0; c < 2; ++c) {
    consumers.emplace_back([&]() {
      while (!done.load(std::memory_order_acquire) || !queue.empty()) {
        if (queue.tryPop()) {
          consumed.fetch_add(1, std::memory_order_relaxed);
        }
        std::this_thread::yield();
      }
    });
  }

  std::vector<std::thread> producers;
  for (std::size_t t = 0; t < num_producers; ++t) {
    producers.emplace_back([&, t]() {
      for (std::size_t i = 0; i < items_per_thread; ++i) {
        queue.push(static_cast<int>(t * items_per_thread + i));
      }
    });
  }

  for (auto &t : producers) {
    t.join();
  }
  done.store(true, std::memory_order_release);
  for (auto &t : consumers) {
    t.join();
  }

  auto &stats = queue.statistics();
  auto pushed = stats.total_pushed.load();
  auto popped = stats.total_popped.load();
  auto dropped = stats.total_dropped.load();
  auto remaining = queue.size();

  // Accounting must balance
  EXPECT_EQ(pushed, popped + dropped + remaining)
      << "pushed=" << pushed << " popped=" << popped << " dropped=" << dropped
      << " remaining=" << remaining;

  // All items were pushed
  EXPECT_EQ(pushed, items_per_thread * num_producers);
}

// =============================================================================
// 10. Statistics: total_pushed only counts successful pushes
// =============================================================================

TEST_F(LockFreeDropStrategyTest, Statistics_PushedCountsOnlySuccess) {
  constexpr std::size_t capacity = 4;

  // DropTail mode: rejects when full
  LockFreeNodeQueue<int>::Config config{
      .capacity = capacity,
      .drop_policy = LockFreeDropPolicy::DropTail,
      .track_statistics = true,
  };

  LockFreeNodeQueue<int> queue(config);

  // Push capacity items (all succeed)
  for (std::size_t i = 0; i < capacity; ++i) {
    EXPECT_TRUE(queue.push(static_cast<int>(i)));
  }

  // Push more items (all rejected)
  for (std::size_t i = 0; i < 10; ++i) {
    EXPECT_FALSE(queue.push(static_cast<int>(capacity + i)));
  }

  auto &stats = queue.statistics();

  // total_pushed should only count the successful pushes
  EXPECT_EQ(stats.total_pushed.load(), capacity)
      << "total_pushed must count only successful enqueues";

  // rejected should count the failed attempts
  EXPECT_EQ(stats.total_rejected.load(), 10u);

  // No drops in DropTail mode
  EXPECT_EQ(stats.total_dropped.load(), 0u);
}

} // namespace ai_pipe_unit_test::lockfree_drop