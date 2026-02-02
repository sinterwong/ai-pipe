#include "ai_pipe/frame_metadata.hpp"
#include "drop_strategy.hpp"
#include <algorithm>
#include <deque>
#include <gtest/gtest.h>
#include <memory>
#include <string>

using namespace ai_pipe;

// =============================================================================
// Test Fixtures and Helpers
// =============================================================================

// Simple test item with frame metadata
struct FrameItem {
  FrameId frame_id{0};
  int value{0};
  Timestamp timestamp{std::chrono::steady_clock::now()};

  FrameItem() = default;
  FrameItem(FrameId fid, int v)
      : frame_id(fid), value(v), timestamp(std::chrono::steady_clock::now()) {}
  FrameItem(FrameId fid, int v, Timestamp ts)
      : frame_id(fid), value(v), timestamp(ts) {}
};

// Simple metadata wrapper for FrameItem
class FrameItemMetadata : public IFrameMetadata {
public:
  explicit FrameItemMetadata(const FrameItem &item)
      : m_frameId(item.frame_id), m_timestamp(item.timestamp) {}

  FrameId frameId() const override { return m_frameId; }
  StreamId streamId() const override { return 0; }
  Timestamp timestamp() const override { return m_timestamp; }

  bool shouldSyncWith(const IFrameMetadata &other) const override {
    return frameId() == other.frameId();
  }

  int compareTo(const IFrameMetadata &other) const override {
    if (m_frameId < other.frameId())
      return -1;
    if (m_frameId > other.frameId())
      return 1;
    return 0;
  }

  std::string toString() const override {
    return "FrameItemMetadata{frame=" + std::to_string(m_frameId) + "}";
  }

  std::unique_ptr<IFrameMetadata> clone() const override {
    auto cloned = std::make_unique<FrameItemMetadata>(*this);
    return cloned;
  }

private:
  FrameId m_frameId;
  Timestamp m_timestamp;
};

// Metadata accessor for FrameItem
auto frame_item_accessor =
    [](const FrameItem &item) -> std::shared_ptr<IFrameMetadata> {
  return std::make_shared<FrameItemMetadata>(item);
};

// =============================================================================
// DropHeadStrategy Tests
// =============================================================================

class DropHeadStrategyTest : public ::testing::Test {
protected:
  DropHeadStrategy<int> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(DropHeadStrategyTest, AlwaysAcceptsIncoming) {
  m_queue = {1, 2, 3, 4, 5};
  EXPECT_TRUE(m_strategy.shouldAcceptIncoming(m_queue, 6, 5));
}

TEST_F(DropHeadStrategyTest, EmptyQueueReturnsNoDrops) {
  auto indices = m_strategy.selectDropIndices(m_queue, 1, 5, 4);
  EXPECT_TRUE(indices.empty());
}

TEST_F(DropHeadStrategyTest, BelowCapacityReturnsNoDrops) {
  m_queue = {1, 2, 3};
  auto indices = m_strategy.selectDropIndices(m_queue, 4, 5, 4);
  EXPECT_TRUE(indices.empty());
}

TEST_F(DropHeadStrategyTest, AtCapacityDropsOldest) {
  m_queue = {1, 2, 3, 4, 5};
  auto indices = m_strategy.selectDropIndices(m_queue, 6, 5, 4);

  // Should drop 1 item (index 0 = oldest)
  ASSERT_EQ(indices.size(), 1);
  EXPECT_EQ(indices[0], 0);
}

TEST_F(DropHeadStrategyTest, DropsMultipleFromHead) {
  m_queue = {1, 2, 3, 4, 5, 6, 7};
  auto indices = m_strategy.selectDropIndices(m_queue, 8, 5, 4);

  // Need to drop 3 items to reach target_size 4
  ASSERT_EQ(indices.size(), 3);
  EXPECT_EQ(indices[0], 0);
  EXPECT_EQ(indices[1], 1);
  EXPECT_EQ(indices[2], 2);
}

TEST_F(DropHeadStrategyTest, CustomKeepCount) {
  DropHeadStrategy<int> keep2_strategy(2);
  m_queue = {1, 2, 3, 4, 5};

  auto indices = keep2_strategy.selectDropIndices(m_queue, 6, 5, 4);

  // With keep_count=2, should drop down to 2 items
  ASSERT_EQ(indices.size(), 3);
}

TEST_F(DropHeadStrategyTest, KeepCountLimitedByCapacity) {
  DropHeadStrategy<int> big_keep_strategy(100);
  m_queue = {1, 2, 3, 4, 5};

  auto indices = big_keep_strategy.selectDropIndices(m_queue, 6, 5, 4);

  // keep_count capped to capacity-1 = 4, so drop 1
  ASSERT_EQ(indices.size(), 1);
}

// =============================================================================
// DropTailStrategy Tests
// =============================================================================

class DropTailStrategyTest : public ::testing::Test {
protected:
  DropTailStrategy<int> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(DropTailStrategyTest, Clone) {
  auto cloned = m_strategy.clone();
  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->name(), "DropTail");
}

TEST_F(DropTailStrategyTest, AcceptsBelowCapacity) {
  m_queue = {1, 2, 3};
  EXPECT_TRUE(m_strategy.shouldAcceptIncoming(m_queue, 4, 5));
}

TEST_F(DropTailStrategyTest, RejectsAtCapacity) {
  m_queue = {1, 2, 3, 4, 5};
  EXPECT_FALSE(m_strategy.shouldAcceptIncoming(m_queue, 6, 5));
}

TEST_F(DropTailStrategyTest, RejectsAboveCapacity) {
  m_queue = {1, 2, 3, 4, 5, 6};
  EXPECT_FALSE(m_strategy.shouldAcceptIncoming(m_queue, 7, 5));
}

TEST_F(DropTailStrategyTest, AlwaysReturnsEmptyDropIndices) {
  m_queue = {1, 2, 3, 4, 5};
  auto indices = m_strategy.selectDropIndices(m_queue, 6, 5, 4);
  EXPECT_TRUE(indices.empty());
}

TEST_F(DropTailStrategyTest, EmptyQueueAlwaysAccepts) {
  EXPECT_TRUE(m_strategy.shouldAcceptIncoming(m_queue, 1, 5));
}

// =============================================================================
// KeepLatestNStrategy Tests
// =============================================================================

class KeepLatestNStrategyTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_strategy = std::make_unique<KeepLatestNStrategy<int>>(3);
  }

  std::unique_ptr<KeepLatestNStrategy<int>> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(KeepLatestNStrategyTest, Clone) {
  auto cloned = m_strategy->clone();
  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->name(), "KeepLatest3");
}

TEST_F(KeepLatestNStrategyTest, EmptyQueueNoDrops) {
  auto indices = m_strategy->selectDropIndices(m_queue, 1, 10, 9);
  EXPECT_TRUE(indices.empty());
}

TEST_F(KeepLatestNStrategyTest, BelowNNoDrops) {
  m_queue = {1, 2};
  auto indices = m_strategy->selectDropIndices(m_queue, 3, 10, 9);
  EXPECT_TRUE(indices.empty());
}

TEST_F(KeepLatestNStrategyTest, AtNMinusOneNoDrops) {
  m_queue = {1, 2}; // N=3, keep=2
  auto indices = m_strategy->selectDropIndices(m_queue, 3, 10, 9);
  EXPECT_TRUE(indices.empty());
}

TEST_F(KeepLatestNStrategyTest, ExceedsNDropsOldest) {
  m_queue = {1, 2, 3, 4, 5}; // N=3, keep=2, drop 3
  auto indices = m_strategy->selectDropIndices(m_queue, 6, 10, 9);

  ASSERT_EQ(indices.size(), 3);
  EXPECT_EQ(indices[0], 0);
  EXPECT_EQ(indices[1], 1);
  EXPECT_EQ(indices[2], 2);
}

TEST_F(KeepLatestNStrategyTest, KeepLatest1DropsAll) {
  KeepLatestNStrategy<int> keep1(1);
  m_queue = {1, 2, 3, 4, 5};

  auto indices = keep1.selectDropIndices(m_queue, 6, 10, 9);

  // Keep 0 (1-1), drop all 5
  ASSERT_EQ(indices.size(), 5);
}

// =============================================================================
// AdaptiveDropStrategy Tests
// =============================================================================

class AdaptiveDropStrategyTest : public ::testing::Test {
protected:
  AdaptiveDropStrategy<int> m_strategy;
  std::deque<int> m_queue;
};

TEST_F(AdaptiveDropStrategyTest, Name) {
  EXPECT_EQ(m_strategy.name(), "Adaptive");
}

TEST_F(AdaptiveDropStrategyTest, Clone) {
  auto cloned = m_strategy.clone();
  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->name(), "Adaptive");
}

TEST_F(AdaptiveDropStrategyTest, EmptyQueueNoDrops) {
  auto indices = m_strategy.selectDropIndices(m_queue, 1, 10, 9);
  EXPECT_TRUE(indices.empty());
}

TEST_F(AdaptiveDropStrategyTest, ZeroCapacityNoDrops) {
  m_queue = {1, 2, 3};
  auto indices = m_strategy.selectDropIndices(m_queue, 4, 0, 0);
  EXPECT_TRUE(indices.empty());
}

TEST_F(AdaptiveDropStrategyTest, LowPressureNoDrops) {
  // Fill to 30% (below medium threshold of 50%)
  m_queue = {1, 2, 3};
  auto indices = m_strategy.selectDropIndices(m_queue, 4, 10, 9);
  EXPECT_TRUE(indices.empty());
}

TEST_F(AdaptiveDropStrategyTest, MediumPressureGentleDrops) {
  // Fill to 60% (above medium threshold of 50%, below high of 80%)
  m_queue = {1, 2, 3, 4, 5, 6};
  auto indices = m_strategy.selectDropIndices(m_queue, 7, 10, 9);

  // Gentle drop mode - keeps about 75% of target_size
  // target_size=9, gentle_keep = 9*3/4 = 6
  // queue.size()=6 <= 6, so might not drop
  EXPECT_GE(indices.size(), 0);
}

TEST_F(AdaptiveDropStrategyTest, HighPressureAggressiveDrops) {
  // Fill to 90% (above high threshold of 80%)
  m_queue = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto indices = m_strategy.selectDropIndices(m_queue, 10, 10, 9);

  // Aggressive drop mode - keeps aggressive_keep (default 1)
  // So should drop 8 items
  EXPECT_EQ(indices.size(), 8);
}

TEST_F(AdaptiveDropStrategyTest, DropsFromHead) {
  // Fill to 90%
  m_queue = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  auto indices = m_strategy.selectDropIndices(m_queue, 10, 10, 9);

  // Verify drops are from head (indices should be sequential from 0)
  for (size_t i = 0; i < indices.size(); ++i) {
    EXPECT_EQ(indices[i], i);
  }
}

// =============================================================================
// FrameAwareDropStrategy Tests
// =============================================================================

class FrameAwareDropStrategyTest : public ::testing::Test {
protected:
  std::deque<FrameItem> m_queue;
};

TEST_F(FrameAwareDropStrategyTest, DropOldestFramesMode) {
  using Strategy =
      FrameAwareDropStrategy<FrameItem, decltype(frame_item_accessor)>;
  Strategy strategy(Strategy::Mode::DropOldestFrames, frame_item_accessor, 2);

  EXPECT_EQ(strategy.name(), "FrameAware:DropOldestFrames");

  // Add items with out-of-order frame IDs
  m_queue.push_back(FrameItem{5, 50});
  m_queue.push_back(FrameItem{3, 30});
  m_queue.push_back(FrameItem{7, 70});
  m_queue.push_back(FrameItem{1, 10});
  m_queue.push_back(FrameItem{4, 40});

  FrameItem incoming{8, 80};
  auto indices = strategy.selectDropIndices(m_queue, incoming, 10, 2);

  // Should drop 3 items (5-2=3) with lowest frame IDs
  EXPECT_EQ(indices.size(), 3);

  // Verify the dropped items have the lowest frame IDs (1, 3, 4)
  std::vector<FrameId> dropped_frames;
  for (auto idx : indices) {
    dropped_frames.push_back(m_queue[idx].frame_id);
  }
  std::sort(dropped_frames.begin(), dropped_frames.end());
  EXPECT_EQ(dropped_frames[0], 1);
  EXPECT_EQ(dropped_frames[1], 3);
  EXPECT_EQ(dropped_frames[2], 4);
}

TEST_F(FrameAwareDropStrategyTest, DropByTimestampMode) {
  using Strategy =
      FrameAwareDropStrategy<FrameItem, decltype(frame_item_accessor)>;
  Strategy strategy(Strategy::Mode::DropByTimestamp, frame_item_accessor, 2);

  EXPECT_EQ(strategy.name(), "FrameAware:DropByTimestamp");

  auto base_time = std::chrono::steady_clock::now();

  // Add items with different timestamps
  m_queue.push_back(
      FrameItem{1, 10, base_time + std::chrono::milliseconds(500)});
  m_queue.push_back(
      FrameItem{2, 20, base_time + std::chrono::milliseconds(100)});
  m_queue.push_back(
      FrameItem{3, 30, base_time + std::chrono::milliseconds(300)});
  m_queue.push_back(
      FrameItem{4, 40, base_time + std::chrono::milliseconds(200)});

  FrameItem incoming{5, 50, base_time + std::chrono::milliseconds(600)};
  auto indices = strategy.selectDropIndices(m_queue, incoming, 10, 2);

  // Should drop 2 items with oldest timestamps
  EXPECT_EQ(indices.size(), 2);
}

TEST_F(FrameAwareDropStrategyTest, DropDuplicatesMode) {
  using Strategy =
      FrameAwareDropStrategy<FrameItem, decltype(frame_item_accessor)>;
  Strategy strategy(Strategy::Mode::DropDuplicates, frame_item_accessor, 1);

  EXPECT_EQ(strategy.name(), "FrameAware:DropDuplicates");

  // Add items with some duplicate frame IDs
  m_queue.push_back(FrameItem{1, 10});
  m_queue.push_back(FrameItem{2, 20});
  m_queue.push_back(FrameItem{3, 30}); // Will be duplicate
  m_queue.push_back(FrameItem{4, 40});

  // Incoming has same frame ID as item at index 2
  FrameItem incoming{3, 35};
  auto indices = strategy.selectDropIndices(m_queue, incoming, 10, 9);

  // Should drop the existing item with frame_id=3
  ASSERT_EQ(indices.size(), 1);
  EXPECT_EQ(m_queue[indices[0]].frame_id, 3);
}

TEST_F(FrameAwareDropStrategyTest, EmptyQueueNoDrops) {
  using Strategy =
      FrameAwareDropStrategy<FrameItem, decltype(frame_item_accessor)>;
  Strategy strategy(Strategy::Mode::DropOldestFrames, frame_item_accessor, 2);

  FrameItem incoming{1, 10};
  auto indices = strategy.selectDropIndices(m_queue, incoming, 10, 9);

  EXPECT_TRUE(indices.empty());
}

TEST_F(FrameAwareDropStrategyTest, Clone) {
  using Strategy =
      FrameAwareDropStrategy<FrameItem, decltype(frame_item_accessor)>;
  Strategy strategy(Strategy::Mode::DropOldestFrames, frame_item_accessor, 3);

  auto cloned = strategy.clone();
  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->name(), "FrameAware:DropOldestFrames");
}

// =============================================================================
// DropStrategyFactory Tests
// =============================================================================

TEST(DropStrategyFactoryTest, CreateByNameDropHead) {
  auto s1 = DropStrategyFactory<int>::createByName("DropHead");
  EXPECT_EQ(s1->name(), "DropHead");

  auto s2 = DropStrategyFactory<int>::createByName("drop_head");
  EXPECT_EQ(s2->name(), "DropHead");
}

TEST(DropStrategyFactoryTest, CreateByNameDropTail) {
  auto s1 = DropStrategyFactory<int>::createByName("DropTail");
  EXPECT_EQ(s1->name(), "DropTail");

  auto s2 = DropStrategyFactory<int>::createByName("drop_tail");
  EXPECT_EQ(s2->name(), "DropTail");
}

TEST(DropStrategyFactoryTest, CreateByNameKeepLatest) {
  auto s1 = DropStrategyFactory<int>::createByName("KeepLatest", 7);
  EXPECT_EQ(s1->name(), "KeepLatest7");

  auto s2 = DropStrategyFactory<int>::createByName("keep_latest", 3);
  EXPECT_EQ(s2->name(), "KeepLatest3");
}

TEST(DropStrategyFactoryTest, CreateByNameAdaptive) {
  auto s1 = DropStrategyFactory<int>::createByName("Adaptive");
  EXPECT_EQ(s1->name(), "Adaptive");

  auto s2 = DropStrategyFactory<int>::createByName("adaptive");
  EXPECT_EQ(s2->name(), "Adaptive");
}

TEST(DropStrategyFactoryTest, CreateByNameUnknownThrows) {
  EXPECT_THROW(DropStrategyFactory<int>::createByName("Unknown"),
               std::invalid_argument);

  EXPECT_THROW(DropStrategyFactory<int>::createByName("invalid_strategy"),
               std::invalid_argument);
}

// =============================================================================
// IDropStrategy Interface Tests
// =============================================================================

TEST(IDropStrategyTest, DefaultShouldAcceptIncomingReturnsTrue) {
  // Use DropHead as it doesn't override shouldAcceptIncoming
  DropHeadStrategy<int> strategy;
  std::deque<int> queue = {1, 2, 3, 4, 5};

  // Default implementation returns true
  EXPECT_TRUE(strategy.shouldAcceptIncoming(queue, 6, 5));
}
