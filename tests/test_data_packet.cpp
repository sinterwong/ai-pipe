#include "ai_pipe/data_packet.hpp"
#include "ai_pipe/data_types.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <type_traits>

using namespace ai_pipe;

namespace ai_pipe_unit_test::data_packet {

TEST(DataPacketTest, SetAndGetRoundTrip) {
  DataPacket packet;
  packet.setParam("count", 42);
  packet.setParam("name", std::string("frame"));
  packet.setParam("ratio", 0.5);

  EXPECT_EQ(packet.param<int>("count").value(), 42);
  EXPECT_EQ(packet.param<std::string>("name").value(), "frame");
  EXPECT_DOUBLE_EQ(packet.param<double>("ratio").value(), 0.5);
  EXPECT_EQ(packet.paramCount(), 3u);
}

TEST(DataPacketTest, SetOverwritesExistingKey) {
  DataPacket packet;
  packet.setParam("value", 1);
  packet.setParam("value", 2);

  EXPECT_EQ(packet.param<int>("value").value(), 2);
  EXPECT_EQ(packet.paramCount(), 1u);
}

TEST(DataPacketTest, OverwriteMayChangeType) {
  DataPacket packet;
  packet.setParam("value", 1);
  packet.setParam("value", std::string("now a string"));

  EXPECT_EQ(packet.param<std::string>("value").value(), "now a string");
  EXPECT_FALSE(packet.has<int>("value"));
}

TEST(DataPacketTest, MissingKeyIsError) {
  DataPacket packet;
  auto missing = packet.param<int>("absent");
  ASSERT_FALSE(missing.isOk());
  EXPECT_EQ(missing.errorCode(), ErrorCode::InvalidArgument);
}

TEST(DataPacketTest, TypeMismatchIsError) {
  DataPacket packet;
  packet.setParam("count", 42);
  auto mismatch = packet.param<std::string>("count");
  ASSERT_FALSE(mismatch.isOk());
  EXPECT_EQ(mismatch.errorCode(), ErrorCode::InvalidArgument);
}

TEST(DataPacketTest, ValueOrProvidesFallback) {
  DataPacket packet;
  packet.setParam("present", 7);

  EXPECT_EQ(packet.param<int>("present").valueOr(-1), 7);
  EXPECT_EQ(packet.param<int>("absent").valueOr(-1), -1);
  // Present but wrong type is an error, so the fallback applies
  EXPECT_EQ(packet.param<double>("present").valueOr(-2.0), -2.0);
}

TEST(DataPacketTest, ResultStyleParamAccess) {
  DataPacket packet;
  packet.setParam("count", 42);

  auto ok = packet.param<int>("count");
  ASSERT_TRUE(ok.isOk());
  EXPECT_EQ(ok.value(), 42);

  auto missing = packet.param<int>("absent");
  ASSERT_FALSE(missing.isOk());
  EXPECT_EQ(missing.errorCode(), ai_pipe::ErrorCode::InvalidArgument);

  auto mismatch = packet.param<std::string>("count");
  ASSERT_FALSE(mismatch.isOk());
  EXPECT_EQ(mismatch.errorCode(), ai_pipe::ErrorCode::InvalidArgument);
}

TEST(TypedParamTest, ResultStyleRead) {
  const TypedParam<int> k_count{"count"};
  DataPacket packet;

  EXPECT_FALSE(k_count.read(packet).isOk());
  k_count.set(packet, 5);
  auto result = k_count.read(packet);
  ASSERT_TRUE(result.isOk());
  EXPECT_EQ(result.value(), 5);
}

TEST(DataPacketTest, HasVariants) {
  DataPacket packet;
  packet.setParam("count", 42);

  EXPECT_TRUE(packet.has("count"));
  EXPECT_FALSE(packet.has("absent"));
  EXPECT_TRUE(packet.has<int>("count"));
  EXPECT_FALSE(packet.has<double>("count"));
  EXPECT_TRUE(packet.has<int>());
  EXPECT_FALSE(packet.has<float>());
}

TEST(DataPacketTest, SharedPtrPayload) {
  DataPacket packet;
  auto payload = std::make_shared<int>(99);
  packet.setParam("buffer", payload);

  auto out = packet.param<std::shared_ptr<int>>("buffer").value();
  EXPECT_EQ(*out, 99);
  EXPECT_EQ(payload.use_count(), 3); // local + packet copy + out
}

TEST(DataPacketTest, ParamKeysListsInsertionOrder) {
  DataPacket packet;
  packet.setParam("a", 1);
  packet.setParam("b", 2);
  packet.setParam("c", 3);

  auto keys = packet.paramKeys();
  ASSERT_EQ(keys.size(), 3u);
  EXPECT_EQ(keys[0], "a");
  EXPECT_EQ(keys[1], "b");
  EXPECT_EQ(keys[2], "c");
}

TEST(TypedParamTest, TypedAccess) {
  const TypedParam<int> k_count{"count"};
  const TypedParam<std::string> k_label{"label"};

  DataPacket packet;
  k_count.set(packet, 5);
  k_label.set(packet, "hello");

  EXPECT_EQ(k_count.read(packet).value(), 5);
  EXPECT_EQ(k_label.read(packet).value(), "hello");
  EXPECT_TRUE(k_count.in(packet));
  EXPECT_EQ(k_count.key(), "count");
}

TEST(TypedParamTest, ReadOnMissingIsError) {
  const TypedParam<int> k_missing{"missing"};
  DataPacket packet;

  EXPECT_FALSE(k_missing.read(packet).isOk());
  EXPECT_FALSE(k_missing.in(packet));
}

} // namespace ai_pipe_unit_test::data_packet

// Ownership model

// Packets flowing through the graph are immutable by type
static_assert(std::is_same_v<ai_pipe::PortDataPtr,
                             std::shared_ptr<const ai_pipe::PortData>>,
              "PortDataPtr must be a shared_ptr to const PortData");
static_assert(std::is_same_v<ai_pipe::MutablePortDataPtr,
                             std::shared_ptr<ai_pipe::PortData>>,
              "MutablePortDataPtr must be the mutable creation-side handle");

TEST(OwnershipModelTest, MutableCopyIsIndependent) {
  auto original = std::make_shared<ai_pipe::PortData>();
  original->id = 42;
  original->stream_id = 3;
  original->setParam("value", 10);

  ai_pipe::PortDataPtr received = original; // hand-off: now immutable

  auto copy = ai_pipe::mutableCopy(received);
  ASSERT_NE(copy, nullptr);
  EXPECT_EQ(copy->id, 42u);
  EXPECT_EQ(copy->stream_id, 3u);
  EXPECT_EQ(copy->param<int>("value").value(), 10);

  copy->setParam("value", 99);
  EXPECT_EQ(copy->param<int>("value").value(), 99);
  EXPECT_EQ(received->param<int>("value").value(), 10)
      << "original must be intact";
}

TEST(OwnershipModelTest, MutableCopyOfNullIsNull) {
  EXPECT_EQ(ai_pipe::mutableCopy(nullptr), nullptr);
}
