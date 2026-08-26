#include "ai_pipe/frame_metadata.hpp"
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <thread>

using namespace ai_pipe;
using namespace std::chrono_literals;

// Frame Constants Tests

TEST(FrameConstantsTest, InvalidFrameId) {
  EXPECT_EQ(frame_constants::k_invalid_frame_id, 0);
}

TEST(FrameConstantsTest, EndOfStreamFrameId) {
  EXPECT_EQ(frame_constants::k_end_of_stream_frame_id,
            std::numeric_limits<FrameId>::max());
}

TEST(FrameConstantsTest, DefaultStreamId) {
  EXPECT_EQ(frame_constants::k_default_stream_id, 0);
}

TEST(FrameConstantsTest, MaxFrameDrift) {
  EXPECT_EQ(frame_constants::k_max_frame_drift, 100);
}

// BasicFrameMetadata Tests

class BasicFrameMetadataTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_metadata = std::make_unique<BasicFrameMetadata>(100, 1);
  }

  std::unique_ptr<BasicFrameMetadata> m_metadata;
};

TEST_F(BasicFrameMetadataTest, DefaultConstruction) {
  BasicFrameMetadata meta;

  EXPECT_EQ(meta.frameId(), frame_constants::k_invalid_frame_id);
  EXPECT_EQ(meta.streamId(), frame_constants::k_default_stream_id);
  EXPECT_FALSE(meta.isValid());
  EXPECT_FALSE(meta.isEndOfStream());
}

TEST_F(BasicFrameMetadataTest, ConstructionWithFrameId) {
  BasicFrameMetadata meta(42);

  EXPECT_EQ(meta.frameId(), 42);
  EXPECT_EQ(meta.streamId(), frame_constants::k_default_stream_id);
  EXPECT_TRUE(meta.isValid());
}

TEST_F(BasicFrameMetadataTest, ConstructionWithAllParams) {
  auto ts = std::chrono::steady_clock::now();
  BasicFrameMetadata meta(100, 5, ts);

  EXPECT_EQ(meta.frameId(), 100);
  EXPECT_EQ(meta.streamId(), 5);
  EXPECT_EQ(meta.timestamp(), ts);
}

TEST_F(BasicFrameMetadataTest, FrameId) {
  EXPECT_EQ(m_metadata->frameId(), 100);
}

TEST_F(BasicFrameMetadataTest, StreamId) {
  EXPECT_EQ(m_metadata->streamId(), 1);
}

TEST_F(BasicFrameMetadataTest, IsValid) {
  EXPECT_TRUE(m_metadata->isValid());

  BasicFrameMetadata invalid;
  EXPECT_FALSE(invalid.isValid());
}

TEST_F(BasicFrameMetadataTest, IsEndOfStream) {
  EXPECT_FALSE(m_metadata->isEndOfStream());

  BasicFrameMetadata eos(frame_constants::k_end_of_stream_frame_id);
  EXPECT_TRUE(eos.isEndOfStream());
}

TEST_F(BasicFrameMetadataTest, ShouldSyncWith) {
  BasicFrameMetadata same_frame(100, 2);
  BasicFrameMetadata different_frame(200, 1);

  EXPECT_TRUE(m_metadata->shouldSyncWith(same_frame));
  EXPECT_FALSE(m_metadata->shouldSyncWith(different_frame));
}

TEST_F(BasicFrameMetadataTest, CompareTo) {
  BasicFrameMetadata smaller(50, 1);
  BasicFrameMetadata larger(150, 1);

  EXPECT_LT(m_metadata->compareTo(larger), 0);
  EXPECT_GT(m_metadata->compareTo(smaller), 0);

  // For same frame ID, comparison depends on timestamp
  // Creating with same frame ID but potentially different timestamp
  auto ts = m_metadata->timestamp();
  BasicFrameMetadata equal(100, 2, ts);
  EXPECT_EQ(m_metadata->compareTo(equal), 0);
}

TEST_F(BasicFrameMetadataTest, CompareToWithSameFrameIdDifferentTimestamp) {
  auto earlier = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(10ms);
  auto later = std::chrono::steady_clock::now();

  BasicFrameMetadata early_meta(100, 1, earlier);
  BasicFrameMetadata late_meta(100, 1, later);

  EXPECT_LT(early_meta.compareTo(late_meta), 0);
  EXPECT_GT(late_meta.compareTo(early_meta), 0);
}

TEST_F(BasicFrameMetadataTest, Clone) {
  auto cloned = m_metadata->clone();

  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->frameId(), m_metadata->frameId());
  EXPECT_EQ(cloned->streamId(), m_metadata->streamId());
}

TEST_F(BasicFrameMetadataTest, ToString) {
  std::string str = m_metadata->toString();

  EXPECT_NE(str.find("100"), std::string::npos); // frame_id
  EXPECT_NE(str.find("1"), std::string::npos);   // stream_id
}

TEST_F(BasicFrameMetadataTest, Setters) {
  m_metadata->setFrameId(200);
  EXPECT_EQ(m_metadata->frameId(), 200);

  m_metadata->setStreamId(10);
  EXPECT_EQ(m_metadata->streamId(), 10);

  auto new_ts = std::chrono::steady_clock::now();
  m_metadata->setTimestamp(new_ts);
  EXPECT_EQ(m_metadata->timestamp(), new_ts);
}

TEST_F(BasicFrameMetadataTest, CopyConstruction) {
  BasicFrameMetadata copy(*m_metadata);

  EXPECT_EQ(copy.frameId(), m_metadata->frameId());
  EXPECT_EQ(copy.streamId(), m_metadata->streamId());
}

TEST_F(BasicFrameMetadataTest, CopyAssignment) {
  BasicFrameMetadata copy;
  copy = *m_metadata;

  EXPECT_EQ(copy.frameId(), m_metadata->frameId());
  EXPECT_EQ(copy.streamId(), m_metadata->streamId());
}

TEST_F(BasicFrameMetadataTest, MoveConstruction) {
  BasicFrameMetadata original(100, 5);
  BasicFrameMetadata moved(std::move(original));

  EXPECT_EQ(moved.frameId(), 100);
  EXPECT_EQ(moved.streamId(), 5);
}

TEST_F(BasicFrameMetadataTest, MoveAssignment) {
  BasicFrameMetadata original(100, 5);
  BasicFrameMetadata moved;
  moved = std::move(original);

  EXPECT_EQ(moved.frameId(), 100);
  EXPECT_EQ(moved.streamId(), 5);
}

// TimestampFrameMetadata Tests

class TimestampFrameMetadataTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_baseTime = std::chrono::steady_clock::now();
    m_metadata = std::make_unique<TimestampFrameMetadata>(
        100, 1, m_baseTime, std::chrono::milliseconds{50});
  }

  std::chrono::steady_clock::time_point m_baseTime;
  std::unique_ptr<TimestampFrameMetadata> m_metadata;
};

TEST_F(TimestampFrameMetadataTest, DefaultConstruction) {
  TimestampFrameMetadata meta;

  EXPECT_EQ(meta.frameId(), frame_constants::k_invalid_frame_id);
  EXPECT_EQ(meta.streamId(), frame_constants::k_default_stream_id);
}

TEST_F(TimestampFrameMetadataTest, ConstructionWithParams) {
  EXPECT_EQ(m_metadata->frameId(), 100);
  EXPECT_EQ(m_metadata->streamId(), 1);
  EXPECT_EQ(m_metadata->timestamp(), m_baseTime);
}

TEST_F(TimestampFrameMetadataTest, ShouldSyncWithinTolerance) {
  // Within 50ms tolerance
  TimestampFrameMetadata close_meta(200, 2,
                                    m_baseTime + std::chrono::milliseconds{30});

  EXPECT_TRUE(m_metadata->shouldSyncWith(close_meta));
}

TEST_F(TimestampFrameMetadataTest, ShouldNotSyncOutsideTolerance) {
  // Outside 50ms tolerance
  TimestampFrameMetadata far_meta(200, 2,
                                  m_baseTime + std::chrono::milliseconds{100});

  EXPECT_FALSE(m_metadata->shouldSyncWith(far_meta));
}

TEST_F(TimestampFrameMetadataTest, ShouldSyncAtExactBoundary) {
  // Exactly at 50ms boundary
  TimestampFrameMetadata boundary_meta(
      200, 2, m_baseTime + std::chrono::milliseconds{50});

  EXPECT_TRUE(m_metadata->shouldSyncWith(boundary_meta));
}

TEST_F(TimestampFrameMetadataTest, CompareToByTimestamp) {
  TimestampFrameMetadata earlier(50, 1,
                                 m_baseTime - std::chrono::milliseconds{100});
  TimestampFrameMetadata later(150, 1,
                               m_baseTime + std::chrono::milliseconds{100});

  EXPECT_GT(m_metadata->compareTo(earlier), 0);
  EXPECT_LT(m_metadata->compareTo(later), 0);
}

TEST_F(TimestampFrameMetadataTest, CompareToEqual) {
  TimestampFrameMetadata same_time(200, 2, m_baseTime);

  EXPECT_EQ(m_metadata->compareTo(same_time), 0);
}

TEST_F(TimestampFrameMetadataTest, Clone) {
  auto cloned = m_metadata->clone();

  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->frameId(), m_metadata->frameId());
  EXPECT_EQ(cloned->streamId(), m_metadata->streamId());
}

TEST_F(TimestampFrameMetadataTest, ToString) {
  std::string str = m_metadata->toString();

  EXPECT_NE(str.find("TimestampFrame"), std::string::npos);
  EXPECT_NE(str.find("100"), std::string::npos);
}

TEST_F(TimestampFrameMetadataTest, SetSyncTolerance) {
  TimestampFrameMetadata other(200, 2,
                               m_baseTime + std::chrono::milliseconds{80});

  // With default 50ms tolerance, should not sync
  EXPECT_FALSE(m_metadata->shouldSyncWith(other));

  // Increase tolerance to 100ms
  m_metadata->setSyncTolerance(std::chrono::milliseconds{100});
  EXPECT_TRUE(m_metadata->shouldSyncWith(other));
}

TEST_F(TimestampFrameMetadataTest, DefaultSyncTolerance) {
  EXPECT_EQ(TimestampFrameMetadata::k_default_sync_tolerance,
            std::chrono::milliseconds{33});
}

// FrameMetadataFactory Tests

TEST(FrameMetadataFactoryTest, CreateBasic) {
  auto meta1 = FrameMetadataFactory::createBasic();
  auto meta2 = FrameMetadataFactory::createBasic();

  EXPECT_NE(meta1.frameId(), meta2.frameId());
  EXPECT_TRUE(meta1.isValid());
  EXPECT_TRUE(meta2.isValid());
}

TEST(FrameMetadataFactoryTest, CreateBasicWithStreamId) {
  auto meta = FrameMetadataFactory::createBasic(5);

  EXPECT_EQ(meta.streamId(), 5);
  EXPECT_TRUE(meta.isValid());
}

TEST(FrameMetadataFactoryTest, CreateBasicAutoIncrement) {
  auto meta1 = FrameMetadataFactory::createBasic();
  auto meta2 = FrameMetadataFactory::createBasic();
  auto meta3 = FrameMetadataFactory::createBasic();

  // Frame IDs should be increasing
  EXPECT_LT(meta1.frameId(), meta2.frameId());
  EXPECT_LT(meta2.frameId(), meta3.frameId());
}

TEST(FrameMetadataFactoryTest, CreateEndOfStream) {
  auto eos = FrameMetadataFactory::createEndOfStream();

  EXPECT_TRUE(eos.isEndOfStream());
  EXPECT_EQ(eos.frameId(), frame_constants::k_end_of_stream_frame_id);
}

TEST(FrameMetadataFactoryTest, CreateEndOfStreamWithStreamId) {
  auto eos = FrameMetadataFactory::createEndOfStream(10);

  EXPECT_TRUE(eos.isEndOfStream());
  EXPECT_EQ(eos.streamId(), 10);
}

// Comparison Operators Tests

TEST(FrameMetadataComparisonTest, LessThan) {
  BasicFrameMetadata a(10);
  BasicFrameMetadata b(20);

  EXPECT_TRUE(a < b);
  EXPECT_FALSE(b < a);
}

TEST(FrameMetadataComparisonTest, GreaterThan) {
  BasicFrameMetadata a(10);
  BasicFrameMetadata b(20);

  EXPECT_TRUE(b > a);
  EXPECT_FALSE(a > b);
}

TEST(FrameMetadataComparisonTest, LessThanOrEqual) {
  BasicFrameMetadata a(10);
  BasicFrameMetadata b(20);
  BasicFrameMetadata c(10);

  EXPECT_TRUE(a <= b);
  EXPECT_TRUE(a <= c);
  EXPECT_FALSE(b <= a);
}

TEST(FrameMetadataComparisonTest, GreaterThanOrEqual) {
  BasicFrameMetadata a(10);
  BasicFrameMetadata b(20);

  EXPECT_TRUE(b >= a);
  EXPECT_FALSE(a >= b);

  // Same frame ID with same timestamp
  auto ts = std::chrono::steady_clock::now();
  BasicFrameMetadata c(10, 0, ts);
  BasicFrameMetadata d(10, 0, ts);
  EXPECT_TRUE(c >= d);
}

TEST(FrameMetadataComparisonTest, Equal) {
  // Same frame ID with same timestamp should be equal
  auto ts = std::chrono::steady_clock::now();
  BasicFrameMetadata a(10, 1, ts);
  BasicFrameMetadata b(10, 2, ts); // Different stream, same frame and timestamp

  EXPECT_TRUE(a == b);
}

TEST(FrameMetadataComparisonTest, NotEqual) {
  BasicFrameMetadata a(10);
  BasicFrameMetadata b(20);

  EXPECT_TRUE(a != b);
  EXPECT_FALSE(a != a);
}

// IFrameMetadata Interface Tests

TEST(IFrameMetadataInterfaceTest, PolymorphicUsage) {
  std::unique_ptr<IFrameMetadata> basic =
      std::make_unique<BasicFrameMetadata>(100, 1);
  std::unique_ptr<IFrameMetadata> timestamp =
      std::make_unique<TimestampFrameMetadata>(
          100, 1, std::chrono::steady_clock::now());

  EXPECT_EQ(basic->frameId(), 100);
  EXPECT_EQ(timestamp->frameId(), 100);

  EXPECT_TRUE(basic->isValid());
  EXPECT_TRUE(timestamp->isValid());
}

TEST(IFrameMetadataInterfaceTest, ClonePreservesType) {
  BasicFrameMetadata original(100, 5);
  auto cloned = original.clone();

  // Clone should work correctly
  EXPECT_EQ(cloned->frameId(), 100);
  EXPECT_EQ(cloned->streamId(), 5);
}

// Edge Cases

TEST(FrameMetadataEdgeCasesTest, MaxFrameId) {
  // Max valid frame ID (one less than end-of-stream)
  FrameId max_valid = frame_constants::k_end_of_stream_frame_id - 1;
  BasicFrameMetadata meta(max_valid);

  EXPECT_TRUE(meta.isValid());
  EXPECT_FALSE(meta.isEndOfStream());
}

TEST(FrameMetadataEdgeCasesTest, FrameIdOne) {
  BasicFrameMetadata meta(1);

  EXPECT_TRUE(meta.isValid());
  EXPECT_EQ(meta.frameId(), 1);
}

TEST(FrameMetadataEdgeCasesTest, MultipleStreamIds) {
  BasicFrameMetadata stream0(100, 0);
  BasicFrameMetadata stream1(100, 1);
  BasicFrameMetadata stream_max(100, std::numeric_limits<StreamId>::max());

  EXPECT_EQ(stream0.streamId(), 0);
  EXPECT_EQ(stream1.streamId(), 1);
  EXPECT_EQ(stream_max.streamId(), std::numeric_limits<StreamId>::max());

  // Same frame ID, should sync regardless of stream
  EXPECT_TRUE(stream0.shouldSyncWith(stream1));
}

TEST(FrameMetadataEdgeCasesTest, TimestampPrecision) {
  auto t1 = std::chrono::steady_clock::now();
  auto t2 = t1 + std::chrono::nanoseconds{1};

  BasicFrameMetadata m1(100, 1, t1);
  BasicFrameMetadata m2(100, 1, t2);

  // Same frame ID, slightly different timestamp
  // compareTo should detect the timestamp difference
  EXPECT_LE(m1.compareTo(m2), 0);
}

// Thread Safety Tests (Factory)

TEST(FrameMetadataThreadSafetyTest, ConcurrentFactoryCreation) {
  std::vector<FrameId> created_ids;
  std::mutex ids_mutex;

  std::vector<std::thread> threads;
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&created_ids, &ids_mutex]() {
      for (int j = 0; j < 100; ++j) {
        auto meta = FrameMetadataFactory::createBasic();
        std::lock_guard<std::mutex> lock(ids_mutex);
        created_ids.push_back(meta.frameId());
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_EQ(created_ids.size(), 1000);

  // All IDs should be unique
  std::sort(created_ids.begin(), created_ids.end());
  auto unique_end = std::unique(created_ids.begin(), created_ids.end());
  EXPECT_EQ(std::distance(created_ids.begin(), unique_end), 1000);
}
