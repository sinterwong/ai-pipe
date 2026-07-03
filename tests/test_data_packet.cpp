/**
 * @file test_data_packet.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Tests for DataPacket flat storage and TypedParam
 * @version 0.1
 * @date 2026-07-04
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/data_packet.hpp"
#include "ai_pipe/data_types.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <type_traits>

using namespace ai_pipe::common_utils;

namespace ai_pipe_unit_test::data_packet {

TEST(DataPacketTest, SetAndGetRoundTrip) {
  DataPacket packet;
  packet.setParam("count", 42);
  packet.setParam("name", std::string("frame"));
  packet.setParam("ratio", 0.5);

  EXPECT_EQ(packet.getParam<int>("count"), 42);
  EXPECT_EQ(packet.getParam<std::string>("name"), "frame");
  EXPECT_DOUBLE_EQ(packet.getParam<double>("ratio"), 0.5);
  EXPECT_EQ(packet.paramCount(), 3u);
}

TEST(DataPacketTest, SetOverwritesExistingKey) {
  DataPacket packet;
  packet.setParam("value", 1);
  packet.setParam("value", 2);

  EXPECT_EQ(packet.getParam<int>("value"), 2);
  EXPECT_EQ(packet.paramCount(), 1u);
}

TEST(DataPacketTest, OverwriteMayChangeType) {
  DataPacket packet;
  packet.setParam("value", 1);
  packet.setParam("value", std::string("now a string"));

  EXPECT_EQ(packet.getParam<std::string>("value"), "now a string");
  EXPECT_FALSE(packet.has<int>("value"));
}

TEST(DataPacketTest, MissingKeyThrows) {
  DataPacket packet;
  EXPECT_THROW((void)packet.getParam<int>("absent"), std::runtime_error);
}

TEST(DataPacketTest, TypeMismatchThrows) {
  DataPacket packet;
  packet.setParam("count", 42);
  EXPECT_THROW((void)packet.getParam<std::string>("count"),
               std::runtime_error);
}

TEST(DataPacketTest, OptionalParamSemantics) {
  DataPacket packet;
  packet.setParam("present", 7);

  EXPECT_EQ(packet.getOptionalParam<int>("present").value_or(-1), 7);
  EXPECT_FALSE(packet.getOptionalParam<int>("absent").has_value());
  // Present but wrong type still throws (explicit contract)
  EXPECT_THROW((void)packet.getOptionalParam<std::string>("present"),
               std::runtime_error);
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

  auto out = packet.getParam<std::shared_ptr<int>>("buffer");
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

  EXPECT_EQ(k_count.get(packet), 5);
  EXPECT_EQ(k_label.get(packet), "hello");
  EXPECT_TRUE(k_count.in(packet));
  EXPECT_EQ(k_count.key(), "count");
}

TEST(TypedParamTest, TryGetOnMissing) {
  const TypedParam<int> k_missing{"missing"};
  DataPacket packet;

  EXPECT_FALSE(k_missing.tryGet(packet).has_value());
  EXPECT_FALSE(k_missing.in(packet));
}

} // namespace ai_pipe_unit_test::data_packet

// =============================================================================
// Ownership model (P3.3)
// =============================================================================

// Packets flowing through the graph are immutable by type
static_assert(
    std::is_same_v<ai_pipe::PortDataPtr,
                   std::shared_ptr<const ai_pipe::PortData>>,
    "PortDataPtr must be a shared_ptr to const PortData");
static_assert(
    std::is_same_v<ai_pipe::MutablePortDataPtr,
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
  EXPECT_EQ(copy->getParam<int>("value"), 10);

  copy->setParam("value", 99);
  EXPECT_EQ(copy->getParam<int>("value"), 99);
  EXPECT_EQ(received->getParam<int>("value"), 10) << "original must be intact";
}

TEST(OwnershipModelTest, MutableCopyOfNullIsNull) {
  EXPECT_EQ(ai_pipe::mutableCopy(nullptr), nullptr);
}
