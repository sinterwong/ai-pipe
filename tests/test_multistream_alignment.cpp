/**
 * @file test_multistream_alignment.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Multi-stream join alignment tests (F5)
 *
 * Covers the two alignment policies added for multi-stream pipelines:
 *   - StreamFrameId: (stream_id, frame_id) composite pairing with
 *     per-stream engine stamping
 *   - Timestamp: header-timestamp pairing within a configurable
 *     tolerance (wires the TimestampFrameMetadata semantics into the
 *     engine's aligned-gather path)
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "helper_nodes.hpp"
#include <gtest/gtest.h>
#include <mutex>

using namespace ai_pipe;
using namespace std::chrono_literals;

namespace ai_pipe_unit_test::multistream_alignment {

struct SeenFrame {
  StreamId stream;
  FrameId frame;
  Timestamp timestamp;
};

/// Join node recording the (stream, frame, ts) of both inputs per execution
class StreamRecordingJoin : public ILogicNode {
public:
  explicit StreamRecordingJoin(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    SeenFrame left{};
    SeenFrame right{};
    if (auto it = inputs.find("input1"); it != inputs.end() && it->second) {
      left = {it->second->stream_id, it->second->id, it->second->timestamp};
    }
    if (auto it = inputs.find("input2"); it != inputs.end() && it->second) {
      right = {it->second->stream_id, it->second->id, it->second->timestamp};
    }
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_pairs.emplace_back(left, right);
    }
    outputs["output"] = std::make_shared<PortData>();
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input1", "input2"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  [[nodiscard]] std::vector<std::pair<SeenFrame, SeenFrame>> pairs() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_pairs;
  }

private:
  mutable std::mutex m_mutex;
  std::vector<std::pair<SeenFrame, SeenFrame>> m_pairs;
};

class MultiStreamAlignmentTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_graph = std::make_unique<Graph>();
    m_srcA = std::make_shared<PassThroughNode>("srcA");
    m_srcB = std::make_shared<PassThroughNode>("srcB");
    m_join = std::make_shared<StreamRecordingJoin>("join");

    m_graph->addNode(m_srcA);
    m_graph->addNode(m_srcB);
    m_graph->addNode(m_join);
    m_graph->addEdge("srcA", "output", "join", "input1");
    m_graph->addEdge("srcB", "output", "join", "input2");
  }

  static PortDataPtr makeFrame(StreamId stream, FrameId frame, Timestamp ts) {
    auto packet = std::make_shared<PortData>();
    packet->stream_id = stream;
    packet->id = frame;
    packet->timestamp = ts;
    return packet;
  }

  std::unique_ptr<ExecutionEngine> makeEngine(AlignmentPolicy policy,
                                              std::chrono::microseconds tol =
                                                  33000us) {
    auto config = EngineConfig::stream(4, 16);
    config.alignment_policy = policy;
    config.alignment_tolerance = tol;
    auto engine = ExecutionEngine::create(config);
    EXPECT_TRUE(engine->initialize(m_graph.get()).isOk());
    engine->setDropCallback(
        [this](const std::string &, std::uint64_t, const std::string &) {
          m_dropCount.fetch_add(1);
        });
    EXPECT_TRUE(engine->startStreaming().isOk());
    return engine;
  }

  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<PassThroughNode> m_srcA;
  std::shared_ptr<PassThroughNode> m_srcB;
  std::shared_ptr<StreamRecordingJoin> m_join;
  std::atomic<int> m_dropCount{0};
};

TEST_F(MultiStreamAlignmentTest, StreamFrameIdPairsInterleavedStreams) {
  auto engine = makeEngine(AlignmentPolicy::StreamFrameId);

  // Two streams with colliding per-stream frame numbers, interleaved
  // identically on both branches.
  const auto base = std::chrono::steady_clock::now();
  const std::vector<std::pair<StreamId, FrameId>> sequence = {
      {0, 1}, {1, 1}, {0, 2}, {1, 2}};
  for (std::size_t i = 0; i < sequence.size(); ++i) {
    const auto ts = base + std::chrono::milliseconds(10 * i);
    (void)engine->pushInput("srcA",
                            makeFrame(sequence[i].first, sequence[i].second,
                                      ts));
    (void)engine->pushInput("srcB",
                            makeFrame(sequence[i].first, sequence[i].second,
                                      ts));
  }

  ASSERT_TRUE(engine->waitForDrain(0, 10000ms).isOk());
  engine->stopStreaming(false);

  const auto pairs = m_join->pairs();
  ASSERT_EQ(pairs.size(), sequence.size());
  for (std::size_t i = 0; i < pairs.size(); ++i) {
    EXPECT_EQ(pairs[i].first.stream, sequence[i].first);
    EXPECT_EQ(pairs[i].first.frame, sequence[i].second);
    EXPECT_EQ(pairs[i].second.stream, sequence[i].first);
    EXPECT_EQ(pairs[i].second.frame, sequence[i].second);
  }
  EXPECT_EQ(m_dropCount.load(), 0);
}

TEST_F(MultiStreamAlignmentTest, StreamFrameIdRejectsCrossStreamIdCollision) {
  auto engine = makeEngine(AlignmentPolicy::StreamFrameId);

  // Branch A saw stream0 frame1; branch B never will (lost upstream).
  // Both streams use frame number 1, so FrameId-only alignment would
  // pair stream0/frame1 with stream1/frame1. StreamFrameId must
  // instead discard the unpairable stream0 head and pair stream1.
  const auto base = std::chrono::steady_clock::now();
  (void)engine->pushInput("srcA", makeFrame(0, 1, base));
  (void)engine->pushInput("srcA", makeFrame(1, 1, base + 20ms));
  (void)engine->pushInput("srcB", makeFrame(1, 1, base + 20ms));

  ASSERT_TRUE(engine->waitForDrain(0, 10000ms).isOk());
  engine->stopStreaming(false);

  const auto pairs = m_join->pairs();
  ASSERT_EQ(pairs.size(), 1u);
  EXPECT_EQ(pairs[0].first.stream, 1u);
  EXPECT_EQ(pairs[0].first.frame, 1u);
  EXPECT_EQ(pairs[0].second.stream, 1u);
  EXPECT_EQ(pairs[0].second.frame, 1u);
  EXPECT_GE(m_dropCount.load(), 1);
}

TEST_F(MultiStreamAlignmentTest, StreamFrameIdStampsPerStreamMonotonicIds) {
  auto engine = makeEngine(AlignmentPolicy::StreamFrameId);

  // Unassigned ids on two interleaved streams: stamping must be
  // per-stream monotonic (1,2,3 within each stream), not global.
  for (int i = 0; i < 3; ++i) {
    for (StreamId stream : {StreamId{0}, StreamId{1}}) {
      auto packet = std::make_shared<PortData>();
      packet->stream_id = stream;
      // Identical identity on both branches so the join drains fully.
      (void)engine->pushInput("srcA", PortDataPtr(packet));
      auto copy = std::make_shared<PortData>(*packet);
      (void)engine->pushInput("srcB", PortDataPtr(copy));
    }
  }

  ASSERT_TRUE(engine->waitForDrain(0, 10000ms).isOk());
  engine->stopStreaming(false);

  const auto pairs = m_join->pairs();
  ASSERT_EQ(pairs.size(), 6u);
  FrameId next_expected[2] = {1, 1};
  for (const auto &[left, right] : pairs) {
    ASSERT_LT(left.stream, 2u);
    EXPECT_EQ(left.frame, next_expected[left.stream]);
    EXPECT_EQ(right.stream, left.stream);
    EXPECT_EQ(right.frame, left.frame);
    ++next_expected[left.stream];
  }
}

TEST_F(MultiStreamAlignmentTest, TimestampPairsWithinTolerance) {
  auto engine = makeEngine(AlignmentPolicy::Timestamp, 10000us);

  // Ids are deliberately unrelated across branches: pairing must come
  // from timestamps alone.
  const auto base = std::chrono::steady_clock::now();
  (void)engine->pushInput("srcA", makeFrame(0, 101, base));
  (void)engine->pushInput("srcB", makeFrame(1, 201, base + 5ms));
  (void)engine->pushInput("srcA", makeFrame(0, 102, base + 100ms));
  (void)engine->pushInput("srcB", makeFrame(1, 202, base + 103ms));

  ASSERT_TRUE(engine->waitForDrain(0, 10000ms).isOk());
  engine->stopStreaming(false);

  const auto pairs = m_join->pairs();
  ASSERT_EQ(pairs.size(), 2u);
  EXPECT_EQ(pairs[0].first.frame, 101u);
  EXPECT_EQ(pairs[0].second.frame, 201u);
  EXPECT_EQ(pairs[1].first.frame, 102u);
  EXPECT_EQ(pairs[1].second.frame, 202u);
  EXPECT_EQ(m_dropCount.load(), 0);
}

TEST_F(MultiStreamAlignmentTest, TimestampDropsHeadBeyondTolerance) {
  auto engine = makeEngine(AlignmentPolicy::Timestamp, 10000us);

  const auto base = std::chrono::steady_clock::now();
  // srcA's first frame lags srcB's head by 50ms > 10ms tolerance: it
  // can never pair and must be discarded, letting the follow-up frame
  // (2ms apart) pair instead.
  (void)engine->pushInput("srcA", makeFrame(0, 101, base));
  (void)engine->pushInput("srcA", makeFrame(0, 102, base + 48ms));
  (void)engine->pushInput("srcB", makeFrame(1, 201, base + 50ms));

  ASSERT_TRUE(engine->waitForDrain(0, 10000ms).isOk());
  engine->stopStreaming(false);

  const auto pairs = m_join->pairs();
  ASSERT_EQ(pairs.size(), 1u);
  EXPECT_EQ(pairs[0].first.frame, 102u);
  EXPECT_EQ(pairs[0].second.frame, 201u);
  EXPECT_GE(m_dropCount.load(), 1);
}

} // namespace ai_pipe_unit_test::multistream_alignment
