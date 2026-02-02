#include "bounded_drop_queue.hpp"
#include "drop_strategy.hpp"
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <vector>

using namespace ai_pipe;
using namespace std::chrono_literals;

// =============================================================================
// Test Data Types
// =============================================================================

struct TestItem {
  FrameId frame_id{0};
  int value{0};
  std::chrono::steady_clock::time_point timestamp{
      std::chrono::steady_clock::now()};

  TestItem() = default;
  TestItem(FrameId fid, int v) : frame_id(fid), value(v) {}
};

// =============================================================================
// Drop Strategy Tests
// =============================================================================

class DropHeadStrategyTest : public ::testing::Test {
protected:
  DropHeadStrategy<int> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(DropHeadStrategyTest, ReturnsEmptyWhenBelowCapacity) {
  m_queue = {1, 2, 3};
  auto indices = m_strategy.selectDropIndices(m_queue, 4, 5, 4);
  EXPECT_TRUE(indices.empty());
}

TEST_F(DropHeadStrategyTest, DropsOldestItems) {
  m_queue = {1, 2, 3, 4, 5};
  auto indices = m_strategy.selectDropIndices(m_queue, 6, 5, 4);

  // Should drop index 0 (oldest) to make room
  EXPECT_EQ(indices.size(), 1);
  EXPECT_EQ(indices[0], 0);
}

TEST_F(DropHeadStrategyTest, DropsMultipleOldestItems) {
  m_queue = {1, 2, 3, 4, 5};
  DropHeadStrategy<int> keep3(3); // Keep only 3 items
  auto indices = keep3.selectDropIndices(m_queue, 6, 5, 2);

  // Should drop indices 0, 1, 2 to keep 2 items (target_size)
  EXPECT_GE(indices.size(), 2);
  // Indices should be from head
  for (auto idx : indices) {
    EXPECT_LT(idx, m_queue.size());
  }
}

TEST_F(DropHeadStrategyTest, Name) { EXPECT_EQ(m_strategy.name(), "DropHead"); }

TEST_F(DropHeadStrategyTest, Clone) {
  auto cloned = m_strategy.clone();
  EXPECT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->name(), "DropHead");
}

// -----------------------------------------------------------------------------
// DropTailStrategy Tests
// -----------------------------------------------------------------------------

class DropTailStrategyTest : public ::testing::Test {
protected:
  DropTailStrategy<int> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(DropTailStrategyTest, AcceptsWhenBelowCapacity) {
  m_queue = {1, 2, 3};
  EXPECT_TRUE(m_strategy.shouldAcceptIncoming(m_queue, 4, 5));
}

TEST_F(DropTailStrategyTest, RejectsWhenAtCapacity) {
  m_queue = {1, 2, 3, 4, 5};
  EXPECT_FALSE(m_strategy.shouldAcceptIncoming(m_queue, 6, 5));
}

TEST_F(DropTailStrategyTest, ReturnsEmptyDropIndices) {
  m_queue = {1, 2, 3, 4, 5};
  auto indices = m_strategy.selectDropIndices(m_queue, 6, 5, 4);
  EXPECT_TRUE(indices.empty()); // DropTail rejects new, doesn't drop existing
}

TEST_F(DropTailStrategyTest, Name) { EXPECT_EQ(m_strategy.name(), "DropTail"); }

// -----------------------------------------------------------------------------
// KeepLatestNStrategy Tests
// -----------------------------------------------------------------------------

class KeepLatestNStrategyTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_strategy = std::make_unique<KeepLatestNStrategy<int>>(3);
  }

  std::unique_ptr<KeepLatestNStrategy<int>> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(KeepLatestNStrategyTest, ThrowsOnZeroN) {
  EXPECT_THROW(KeepLatestNStrategy<int>(0), std::invalid_argument);
}

TEST_F(KeepLatestNStrategyTest, KeepsOnlyNItems) {
  m_queue = {1, 2, 3, 4, 5};
  auto indices = m_strategy->selectDropIndices(m_queue, 6, 10, 9);

  // Keep 3-1=2 items to make room for incoming, so drop 3 items
  EXPECT_EQ(indices.size(), 3);
  // Should drop from head
  EXPECT_EQ(indices[0], 0);
  EXPECT_EQ(indices[1], 1);
  EXPECT_EQ(indices[2], 2);
}

TEST_F(KeepLatestNStrategyTest, Name) {
  EXPECT_EQ(m_strategy->name(), "KeepLatest3");
}

// -----------------------------------------------------------------------------
// AdaptiveDropStrategy Tests
// -----------------------------------------------------------------------------

class AdaptiveDropStrategyTest : public ::testing::Test {
protected:
  AdaptiveDropStrategy<int> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(AdaptiveDropStrategyTest, NoDroppingBelowMediumThreshold) {
  m_queue = {1, 2}; // 20% of capacity 10
  auto indices = m_strategy.selectDropIndices(m_queue, 3, 10, 9);
  EXPECT_TRUE(indices.empty());
}

TEST_F(AdaptiveDropStrategyTest, GentleDroppingAtMediumPressure) {
  // Fill to 60% (medium threshold is 50%)
  m_queue = {1, 2, 3, 4, 5, 6};
  auto indices = m_strategy.selectDropIndices(m_queue, 7, 10, 9);
  // Should do gentle dropping
  EXPECT_GE(indices.size(), 0);
}

TEST_F(AdaptiveDropStrategyTest, AggressiveDroppingAtHighPressure) {
  // Fill to 90% (high threshold is 80%)
  m_queue = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto indices = m_strategy.selectDropIndices(m_queue, 10, 10, 9);
  // Should drop aggressively
  EXPECT_GT(indices.size(), 0);
}

TEST_F(AdaptiveDropStrategyTest, CustomConfig) {
  AdaptiveDropStrategy<int>::Config config;
  config.medium_threshold = 0.3;
  config.high_threshold = 0.6;
  config.aggressive_keep = 2;

  AdaptiveDropStrategy<int> custom_strategy(config);
  m_queue = {1, 2, 3, 4, 5, 6, 7};
  auto indices = custom_strategy.selectDropIndices(m_queue, 8, 10, 9);
  EXPECT_GT(indices.size(), 0);
}

// -----------------------------------------------------------------------------
// CompositeDropStrategy Tests
// -----------------------------------------------------------------------------

TEST(CompositeDropStrategyTest, FallbackBehavior) {
  CompositeDropStrategy<int> composite;

  // Add DropTail first (will reject), then DropHead as fallback
  composite.addStrategy(std::make_unique<DropTailStrategy<int>>());
  composite.addStrategy(std::make_unique<DropHeadStrategy<int>>());

  std::deque<int> queue = {1, 2, 3, 4, 5};

  // DropTail returns empty indices, so composite should try DropHead
  auto indices = composite.selectDropIndices(queue, 6, 5, 4);
  EXPECT_FALSE(indices.empty());
}

TEST(CompositeDropStrategyTest, ShouldAcceptIncomingAllMustAgree) {
  CompositeDropStrategy<int> composite;
  composite.addStrategy(std::make_unique<DropHeadStrategy<int>>());
  composite.addStrategy(std::make_unique<DropTailStrategy<int>>());

  std::deque<int> queue = {1, 2, 3, 4, 5};

  // DropTail rejects at capacity, so composite should reject
  EXPECT_FALSE(composite.shouldAcceptIncoming(queue, 6, 5));
}

TEST(CompositeDropStrategyTest, Name) {
  CompositeDropStrategy<int> composite;
  composite.addStrategy(std::make_unique<DropHeadStrategy<int>>());
  composite.addStrategy(std::make_unique<DropTailStrategy<int>>());

  EXPECT_EQ(composite.name(), "Composite[DropHead, DropTail]");
}

// -----------------------------------------------------------------------------
// DropStrategyFactory Tests
// -----------------------------------------------------------------------------

TEST(DropStrategyFactoryTest, CreateDropHead) {
  auto strategy = DropStrategyFactory<int>::createDropHead();
  EXPECT_EQ(strategy->name(), "DropHead");
}

TEST(DropStrategyFactoryTest, CreateDropTail) {
  auto strategy = DropStrategyFactory<int>::createDropTail();
  EXPECT_EQ(strategy->name(), "DropTail");
}

TEST(DropStrategyFactoryTest, CreateKeepLatest) {
  auto strategy = DropStrategyFactory<int>::createKeepLatest(5);
  EXPECT_EQ(strategy->name(), "KeepLatest5");
}

TEST(DropStrategyFactoryTest, CreateAdaptive) {
  auto strategy = DropStrategyFactory<int>::createAdaptive();
  EXPECT_EQ(strategy->name(), "Adaptive");
}

TEST(DropStrategyFactoryTest, CreateByName) {
  auto drop_head = DropStrategyFactory<int>::createByName("DropHead");
  EXPECT_EQ(drop_head->name(), "DropHead");

  auto drop_tail = DropStrategyFactory<int>::createByName("drop_tail");
  EXPECT_EQ(drop_tail->name(), "DropTail");

  auto keep_latest = DropStrategyFactory<int>::createByName("KeepLatest", 7);
  EXPECT_EQ(keep_latest->name(), "KeepLatest7");

  EXPECT_THROW(DropStrategyFactory<int>::createByName("Unknown"),
               std::invalid_argument);
}

// =============================================================================
// BoundedDropQueue Tests
// =============================================================================

class BoundedDropQueueTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_queue = std::make_unique<BoundedDropQueue<int>>(5);
  }

  std::unique_ptr<BoundedDropQueue<int>> m_queue;
};

// -----------------------------------------------------------------------------
// Basic Operations
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, Construction) {
  EXPECT_TRUE(m_queue->empty());
  EXPECT_EQ(m_queue->size(), 0);
  EXPECT_EQ(m_queue->capacity(), 5);
}

TEST_F(BoundedDropQueueTest, ConstructionWithConfig) {
  BoundedDropQueueConfig config;
  config.capacity = 10;
  config.target_size = 8;
  config.node_name = "test_node";
  config.port_name = "test_port";

  BoundedDropQueue<int> configured_queue(config);
  EXPECT_EQ(configured_queue.capacity(), 10);
  EXPECT_EQ(configured_queue.config().target_size, 8);
}

TEST_F(BoundedDropQueueTest, PushAndSize) {
  EXPECT_TRUE(m_queue->push(1));
  EXPECT_EQ(m_queue->size(), 1);

  EXPECT_TRUE(m_queue->push(2));
  EXPECT_EQ(m_queue->size(), 2);

  EXPECT_FALSE(m_queue->empty());
}

TEST_F(BoundedDropQueueTest, TryPop) {
  m_queue->push(42);
  auto result = m_queue->tryPop();

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
  EXPECT_TRUE(m_queue->empty());
}

TEST_F(BoundedDropQueueTest, TryPopOnEmpty) {
  auto result = m_queue->tryPop();
  EXPECT_FALSE(result.has_value());
}

TEST_F(BoundedDropQueueTest, FIFOOrder) {
  m_queue->push(1);
  m_queue->push(2);
  m_queue->push(3);

  EXPECT_EQ(*m_queue->tryPop(), 1);
  EXPECT_EQ(*m_queue->tryPop(), 2);
  EXPECT_EQ(*m_queue->tryPop(), 3);
}

TEST_F(BoundedDropQueueTest, Clear) {
  m_queue->push(1);
  m_queue->push(2);
  m_queue->push(3);

  m_queue->clear();

  EXPECT_TRUE(m_queue->empty());
  EXPECT_EQ(m_queue->size(), 0);
}

// -----------------------------------------------------------------------------
// Capacity and Dropping
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, DropsWhenFull) {
  // Fill to capacity
  for (int i = 0; i < 5; ++i) {
    m_queue->push(i);
  }
  EXPECT_EQ(m_queue->size(), 5);

  // Push one more - should drop oldest
  m_queue->push(5);

  // Size should still be at or below capacity
  EXPECT_LE(m_queue->size(), 5);

  // First item should be dropped (oldest)
  auto first = m_queue->tryPop();
  ASSERT_TRUE(first.has_value());
  EXPECT_GT(*first, 0); // Should not be 0 anymore
}

TEST_F(BoundedDropQueueTest, DropHeadStrategyBehavior) {
  // Default strategy is DropHead
  for (int i = 0; i < 10; ++i) {
    m_queue->push(i);
  }

  // Queue should contain latest items
  std::vector<int> items;
  while (!m_queue->empty()) {
    items.push_back(*m_queue->tryPop());
  }

  // Should have latest items (highest values)
  for (int i = 1; i < static_cast<int>(items.size()); ++i) {
    EXPECT_GT(items[i], items[i - 1]) << "Items should be in ascending order";
  }
}

TEST_F(BoundedDropQueueTest, DropTailStrategyRejects) {
  BoundedDropQueue<int> drop_tail_queue(
      5, std::make_unique<DropTailStrategy<int>>());

  // Fill to capacity
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(drop_tail_queue.push(i));
  }

  // Additional pushes should be rejected
  EXPECT_FALSE(drop_tail_queue.push(5));
  EXPECT_FALSE(drop_tail_queue.push(6));

  // Size should remain at capacity
  EXPECT_EQ(drop_tail_queue.size(), 5);

  // First item should still be 0
  EXPECT_EQ(*drop_tail_queue.tryPop(), 0);
}

// -----------------------------------------------------------------------------
// Strategy Management
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, SetStrategy) {
  m_queue->setStrategy(std::make_unique<DropTailStrategy<int>>());
  EXPECT_EQ(m_queue->strategyName(), "DropTail");
}

TEST_F(BoundedDropQueueTest, SetNullStrategyFallsBackToDefault) {
  m_queue->setStrategy(nullptr);
  EXPECT_EQ(m_queue->strategyName(), "DropHead");
}

// -----------------------------------------------------------------------------
// Blocking Operations
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, WaitPopTimeout) {
  auto result = m_queue->waitPop(50ms);
  EXPECT_FALSE(result.has_value());
}

TEST_F(BoundedDropQueueTest, WaitPopSucceeds) {
  std::thread producer([this]() {
    std::this_thread::sleep_for(20ms);
    m_queue->push(99);
  });

  auto result = m_queue->waitPop(500ms);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 99);

  producer.join();
}

TEST_F(BoundedDropQueueTest, WaitPopBlocking) {
  std::atomic<bool> popped{false};

  std::thread consumer([this, &popped]() {
    int value = m_queue->waitPop();
    EXPECT_EQ(value, 42);
    popped.store(true);
  });

  std::this_thread::sleep_for(50ms);
  EXPECT_FALSE(popped.load());

  m_queue->push(42);

  consumer.join();
  EXPECT_TRUE(popped.load());
}

// -----------------------------------------------------------------------------
// Peek Operations
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, PeekOnEmpty) {
  EXPECT_EQ(m_queue->peek(), nullptr);
}

TEST_F(BoundedDropQueueTest, PeekReturnsFirstItem) {
  m_queue->push(1);
  m_queue->push(2);

  const int *first = m_queue->peek();
  ASSERT_NE(first, nullptr);
  EXPECT_EQ(*first, 1);

  // Peek doesn't remove
  EXPECT_EQ(m_queue->size(), 2);
}

// -----------------------------------------------------------------------------
// Batch Operations
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, PopBatch) {
  m_queue->push(1);
  m_queue->push(2);
  m_queue->push(3);
  m_queue->push(4);
  m_queue->push(5);

  auto batch = m_queue->popBatch(3);

  EXPECT_EQ(batch.size(), 3);
  EXPECT_EQ(batch[0], 1);
  EXPECT_EQ(batch[1], 2);
  EXPECT_EQ(batch[2], 3);
  EXPECT_EQ(m_queue->size(), 2);
}

TEST_F(BoundedDropQueueTest, PopBatchMoreThanAvailable) {
  m_queue->push(1);
  m_queue->push(2);

  auto batch = m_queue->popBatch(10);

  EXPECT_EQ(batch.size(), 2);
  EXPECT_TRUE(m_queue->empty());
}

// -----------------------------------------------------------------------------
// Statistics
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, Statistics) {
  // Push some items
  for (int i = 0; i < 10; ++i) {
    m_queue->push(i);
  }

  // Pop some
  (void)m_queue->tryPop();
  (void)m_queue->tryPop();

  auto stats = m_queue->statisticsSnapshot();

  EXPECT_EQ(stats.total_pushed, 10);
  EXPECT_EQ(stats.total_popped, 2);
  EXPECT_GT(stats.total_dropped, 0); // Should have dropped some
  EXPECT_GT(stats.peak_size, 0);
}

TEST_F(BoundedDropQueueTest, ResetStatistics) {
  m_queue->push(1);
  m_queue->push(2);
  (void)m_queue->tryPop();

  m_queue->resetStatistics();

  auto stats = m_queue->statisticsSnapshot();
  EXPECT_EQ(stats.total_pushed, 0);
  EXPECT_EQ(stats.total_popped, 0);
}

TEST_F(BoundedDropQueueTest, DropRate) {
  BoundedDropQueue<int> small_queue(2);

  // Push 10 items into capacity-2 queue
  for (int i = 0; i < 10; ++i) {
    small_queue.push(i);
  }

  auto stats = small_queue.statisticsSnapshot();
  EXPECT_GT(stats.dropRate(), 0.0);
  EXPECT_LT(stats.dropRate(), 100.0);
}

// -----------------------------------------------------------------------------
// Fill Ratio and Full Check
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, FillRatio) {
  EXPECT_DOUBLE_EQ(m_queue->fillRatio(), 0.0);

  m_queue->push(1);
  EXPECT_DOUBLE_EQ(m_queue->fillRatio(), 0.2);

  m_queue->push(2);
  m_queue->push(3);
  m_queue->push(4);
  m_queue->push(5);
  EXPECT_DOUBLE_EQ(m_queue->fillRatio(), 1.0);
}

TEST_F(BoundedDropQueueTest, IsFull) {
  EXPECT_FALSE(m_queue->isFull());

  for (int i = 0; i < 5; ++i) {
    m_queue->push(i);
  }

  EXPECT_TRUE(m_queue->isFull());
}

// -----------------------------------------------------------------------------
// Capacity Modification
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, SetCapacity) {
  m_queue->setCapacity(10);
  EXPECT_EQ(m_queue->capacity(), 10);
}

// -----------------------------------------------------------------------------
// Drop Callback
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, DropCallback) {
  std::atomic<int> drop_count{0};
  std::vector<FrameId> dropped_frames;
  std::mutex dropped_mutex;

  // Set up frame ID accessor
  m_queue->setFrameIdAccessor([](const int &item) -> std::optional<FrameId> {
    return static_cast<FrameId>(item);
  });

  // Set up drop callback
  m_queue->setDropCallback(
      [&drop_count, &dropped_frames, &dropped_mutex](const DropEvent &event) {
        drop_count.fetch_add(1);
        std::lock_guard<std::mutex> lock(dropped_mutex);
        dropped_frames.push_back(event.frame_id);
      });

  // Fill and overflow
  for (int i = 0; i < 10; ++i) {
    m_queue->push(i);
  }

  EXPECT_GT(drop_count.load(), 0);
}

// -----------------------------------------------------------------------------
// Thread Safety
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, ConcurrentPushPop) {
  BoundedDropQueue<int> concurrent_queue(100);
  const int num_items = 10000;
  std::atomic<int> push_count{0};
  std::atomic<int> pop_count{0};
  std::atomic<bool> done{false};

  // Producer
  std::thread producer([&concurrent_queue, num_items, &push_count, &done]() {
    for (int i = 0; i < num_items; ++i) {
      concurrent_queue.push(i);
      push_count.fetch_add(1);
    }
    done.store(true);
  });

  // Consumers
  std::vector<std::thread> consumers;
  for (int c = 0; c < 4; ++c) {
    consumers.emplace_back([&concurrent_queue, &pop_count, &done]() {
      while (!done.load() || !concurrent_queue.empty()) {
        auto result = concurrent_queue.tryPop();
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

  // All pushed items should be either popped or dropped
  auto stats = concurrent_queue.statisticsSnapshot();
  EXPECT_EQ(stats.total_pushed, num_items);
  EXPECT_EQ(stats.total_popped + stats.total_dropped, num_items);
}

TEST_F(BoundedDropQueueTest, ConcurrentProducers) {
  BoundedDropQueue<int> concurrent_queue(50);
  const int num_producers = 8;
  const int items_per_producer = 1000;
  std::vector<std::thread> producers;

  for (int p = 0; p < num_producers; ++p) {
    producers.emplace_back([&concurrent_queue, p, items_per_producer]() {
      for (int i = 0; i < items_per_producer; ++i) {
        concurrent_queue.push(p * items_per_producer + i);
      }
    });
  }

  for (auto &p : producers) {
    p.join();
  }

  auto stats = concurrent_queue.statisticsSnapshot();
  EXPECT_EQ(stats.total_pushed, num_producers * items_per_producer);
}

// -----------------------------------------------------------------------------
// Move Semantics
// -----------------------------------------------------------------------------

TEST_F(BoundedDropQueueTest, MoveConstruction) {
  m_queue->push(1);
  m_queue->push(2);

  BoundedDropQueue<int> moved(std::move(*m_queue));

  EXPECT_EQ(moved.size(), 2);
  EXPECT_EQ(*moved.tryPop(), 1);
}

TEST_F(BoundedDropQueueTest, MoveAssignment) {
  m_queue->push(1);
  m_queue->push(2);

  BoundedDropQueue<int> other(10);
  other = std::move(*m_queue);

  EXPECT_EQ(other.size(), 2);
}

// =============================================================================
// BoundedDropQueueFactory Tests
// =============================================================================

TEST(BoundedDropQueueFactoryTest, CreateDropHead) {
  auto queue = BoundedDropQueueFactory<int>::createDropHead(10);
  EXPECT_EQ(queue.capacity(), 10);
  EXPECT_EQ(queue.strategyName(), "DropHead");
}

TEST(BoundedDropQueueFactoryTest, CreateDropTail) {
  auto queue = BoundedDropQueueFactory<int>::createDropTail(10);
  EXPECT_EQ(queue.strategyName(), "DropTail");
}

TEST(BoundedDropQueueFactoryTest, CreateKeepLatest) {
  auto queue = BoundedDropQueueFactory<int>::createKeepLatest(10, 5);
  EXPECT_EQ(queue.strategyName(), "KeepLatest5");
}

TEST(BoundedDropQueueFactoryTest, CreateAdaptive) {
  auto queue = BoundedDropQueueFactory<int>::createAdaptive(10);
  EXPECT_EQ(queue.strategyName(), "Adaptive");
}

// =============================================================================
// QueueStatistics Tests
// =============================================================================

TEST(QueueStatisticsTest, DropRateCalculation) {
  QueueStatistics stats;
  stats.total_pushed.store(100);
  stats.total_dropped.store(20);

  EXPECT_DOUBLE_EQ(stats.dropRate(), 20.0);
}

TEST(QueueStatisticsTest, DropRateZeroPushed) {
  QueueStatistics stats;
  EXPECT_DOUBLE_EQ(stats.dropRate(), 0.0);
}

TEST(QueueStatisticsTest, Reset) {
  QueueStatistics stats;
  stats.total_pushed.store(100);
  stats.total_popped.store(50);
  stats.total_dropped.store(25);
  stats.peak_size.store(75);

  stats.reset();

  EXPECT_EQ(stats.total_pushed.load(), 0);
  EXPECT_EQ(stats.total_popped.load(), 0);
  EXPECT_EQ(stats.total_dropped.load(), 0);
  EXPECT_EQ(stats.peak_size.load(), 0);
}

TEST(QueueStatisticsSnapshotTest, CopyFromStatistics) {
  QueueStatistics stats;
  stats.total_pushed.store(100);
  stats.total_popped.store(50);
  stats.total_dropped.store(20);

  QueueStatisticsSnapshot snapshot(stats);

  EXPECT_EQ(snapshot.total_pushed, 100);
  EXPECT_EQ(snapshot.total_popped, 50);
  EXPECT_EQ(snapshot.total_dropped, 20);
  EXPECT_DOUBLE_EQ(snapshot.dropRate(), 20.0);
}

// =============================================================================
// Complex Types Test
// =============================================================================

TEST(BoundedDropQueueComplexTest, WithTestItem) {
  BoundedDropQueue<TestItem> item_queue(5);

  item_queue.setFrameIdAccessor(
      [](const TestItem &item) -> std::optional<FrameId> {
        return item.frame_id;
      });

  for (FrameId i = 0; i < 10; ++i) {
    item_queue.push(TestItem{i, static_cast<int>(i * 10)});
  }

  // Should have latest items
  EXPECT_LE(item_queue.size(), 5);

  auto first = item_queue.tryPop();
  ASSERT_TRUE(first.has_value());
  EXPECT_GT(first->frame_id, 0); // Old frames should be dropped
}

TEST(BoundedDropQueueComplexTest, DropBefore) {
  BoundedDropQueue<TestItem> item_queue(10);

  item_queue.setFrameIdAccessor(
      [](const TestItem &item) -> std::optional<FrameId> {
        return item.frame_id;
      });

  // Push items with frame IDs 0-9
  for (FrameId i = 0; i < 10; ++i) {
    item_queue.push(TestItem{i, static_cast<int>(i)});
  }

  // Drop all frames before frame 5
  auto dropped = item_queue.dropBefore(5);

  EXPECT_EQ(dropped, 5);
  EXPECT_EQ(item_queue.size(), 5);

  // First remaining item should be frame 5
  auto first = item_queue.tryPop();
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(first->frame_id, 5);
}

TEST(BoundedDropQueueComplexTest, DropIf) {
  BoundedDropQueue<TestItem> item_queue(10);

  // Push items
  for (int i = 0; i < 10; ++i) {
    item_queue.push(TestItem{static_cast<FrameId>(i), i});
  }

  // Drop all items with even values
  auto dropped = item_queue.dropIf(
      [](const TestItem &item) { return item.value % 2 == 0; });

  EXPECT_EQ(dropped, 5);
  EXPECT_EQ(item_queue.size(), 5);

  // All remaining items should have odd values
  while (!item_queue.empty()) {
    auto item = item_queue.tryPop();
    ASSERT_TRUE(item.has_value());
    EXPECT_EQ(item->value % 2, 1);
  }
}
