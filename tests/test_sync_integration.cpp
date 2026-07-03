/**
 * @file test_sync_integration.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief End-to-end frame synchronization tests (P4.3)
 *
 * Validates the Phase 4 wiring through the real engine: a fork-join
 * pipeline with an asymmetric slow branch and tiny DropHead queues is
 * flooded until frames get evicted, and the join node must still only
 * ever observe frame-aligned input pairs.
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "helper_nodes.hpp"
#include <gtest/gtest.h>
#include <mutex>
#include <thread>

using namespace ai_pipe;
using namespace std::chrono_literals;

namespace ai_pipe_unit_test::sync_integration {

/// Join node that records the frame-id pair of every processed input set
class RecordingJoinNode : public ILogicNode {
public:
  explicit RecordingJoinNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    FrameId left = 0;
    FrameId right = 0;
    if (auto it = inputs.find("input1"); it != inputs.end() && it->second) {
      left = it->second->frameId();
    }
    if (auto it = inputs.find("input2"); it != inputs.end() && it->second) {
      right = it->second->frameId();
    }
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_pairs.emplace_back(left, right);
    }

    auto out = std::make_shared<PortData>();
    outputs["output"] = out;
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input1", "input2"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  [[nodiscard]] std::vector<std::pair<FrameId, FrameId>> pairs() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_pairs;
  }

private:
  mutable std::mutex m_mutex;
  std::vector<std::pair<FrameId, FrameId>> m_pairs;
};

class SyncIntegrationTest : public ::testing::Test {
protected:
  void SetUp() override { m_graph = std::make_unique<Graph>(); }

  static PortDataPtr makeFrame() {
    // Unassigned id: the engine stamps a monotonic FrameId on push
    return std::make_shared<PortData>();
  }

  std::unique_ptr<Graph> m_graph;
};

TEST_F(SyncIntegrationTest, JoinReceivesOnlyAlignedPairsUnderDrops) {
  // fork -> branch1 (fast) ---> join -> (sink is the join itself)
  //      -> branch2 (slow) --/
  auto fork = std::make_shared<PassThroughNode>("fork");
  auto branch1 = std::make_shared<PassThroughNode>("branch1");
  auto branch2 = std::make_shared<PassThroughNode>("branch2", 15ms); // slow
  auto join = std::make_shared<RecordingJoinNode>("join");

  m_graph->addNode(fork);
  m_graph->addNode(branch1);
  m_graph->addNode(branch2);
  m_graph->addNode(join);
  m_graph->addEdge("fork", "output", "branch1", "input");
  m_graph->addEdge("fork", "output", "branch2", "input");
  m_graph->addEdge("branch1", "output", "join", "input1");
  m_graph->addEdge("branch2", "output", "join", "input2");

  auto engine = createStreamEngine(4, 4);

  // Tiny DropHead queue on the slow branch guarantees evictions
  QueueConfig tiny;
  tiny.capacity = 2;
  tiny.drop_strategy = "DropHead";
  engine->setNodeQueueConfig("branch2", tiny);

  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());

  std::atomic<int> drop_count{0};
  engine->setDropCallback(
      [&](const std::string &, std::uint64_t, const std::string &) {
        drop_count.fetch_add(1);
      });

  ASSERT_TRUE(engine->startStreaming().isOk());

  // Flood: 40 frames at 2ms while branch2 needs 15ms per frame
  for (int i = 0; i < 40; ++i) {
    (void)engine->pushInput("fork", makeFrame());
    std::this_thread::sleep_for(2ms);
  }

  (void)engine->waitForDrain(0, 10000ms);
  engine->stopStreaming(false);

  const auto pairs = join->pairs();

  ASSERT_FALSE(pairs.empty()) << "join should have processed some frames";
  EXPECT_GT(drop_count.load(), 0) << "test setup must actually induce drops";

  for (const auto &[left, right] : pairs) {
    EXPECT_EQ(left, right) << "join processed a misaligned pair (" << left
                           << ", " << right << ")";
  }

  // Frame ids seen at the join must be strictly increasing (no stale
  // re-pairing after alignment discards)
  for (std::size_t i = 1; i < pairs.size(); ++i) {
    EXPECT_LT(pairs[i - 1].first, pairs[i].first);
  }
}

TEST_F(SyncIntegrationTest, SymmetricBranchesLoseNothing) {
  // Control case: no backpressure, alignment must be a pure pass-through.
  auto fork = std::make_shared<PassThroughNode>("fork");
  auto branch1 = std::make_shared<PassThroughNode>("branch1");
  auto branch2 = std::make_shared<PassThroughNode>("branch2");
  auto join = std::make_shared<RecordingJoinNode>("join");

  m_graph->addNode(fork);
  m_graph->addNode(branch1);
  m_graph->addNode(branch2);
  m_graph->addNode(join);
  m_graph->addEdge("fork", "output", "branch1", "input");
  m_graph->addEdge("fork", "output", "branch2", "input");
  m_graph->addEdge("branch1", "output", "join", "input1");
  m_graph->addEdge("branch2", "output", "join", "input2");

  auto engine = createStreamEngine(4, 64);
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());
  ASSERT_TRUE(engine->startStreaming().isOk());

  constexpr int k_frames = 20;
  for (int i = 0; i < k_frames; ++i) {
    (void)engine->pushInput("fork", makeFrame());
    std::this_thread::sleep_for(1ms);
  }

  ASSERT_TRUE(engine->waitForDrain(0, 10000ms).isOk());
  engine->stopStreaming(false);

  const auto pairs = join->pairs();
  EXPECT_EQ(pairs.size(), static_cast<std::size_t>(k_frames));
  for (const auto &[left, right] : pairs) {
    EXPECT_EQ(left, right);
  }
}

} // namespace ai_pipe_unit_test::sync_integration
