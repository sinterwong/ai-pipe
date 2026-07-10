/**
 * @file test_join_timeout.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Join alignment timeout degradation tests (F6)
 *
 * Validates EngineConfig::join_wait_timeout with both degradation
 * policies: PartialInputs (execute with the ports that have data) and
 * SkipFrame (discard the stuck head so later frames pair normally),
 * plus the default wait-forever behavior when the timeout is 0.
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

namespace ai_pipe_unit_test::join_timeout {

/// Join node recording which ports were present in each execution
class PortRecordingJoin : public ILogicNode {
public:
  explicit PortRecordingJoin(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    std::vector<std::pair<std::string, FrameId>> seen;
    for (const auto &[port, data] : inputs) {
      seen.emplace_back(port, data ? data->frameId() : FrameId{0});
    }
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_executions.push_back(std::move(seen));
    }
    outputs["output"] = std::make_shared<PortData>();
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input1", "input2"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  [[nodiscard]] std::vector<std::vector<std::pair<std::string, FrameId>>>
  executions() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_executions;
  }

private:
  mutable std::mutex m_mutex;
  std::vector<std::vector<std::pair<std::string, FrameId>>> m_executions;
};

class JoinTimeoutTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_graph = std::make_unique<Graph>();
    m_srcA = std::make_shared<PassThroughNode>("srcA");
    m_srcB = std::make_shared<PassThroughNode>("srcB");
    m_join = std::make_shared<PortRecordingJoin>("join");

    m_graph->addNode(m_srcA);
    m_graph->addNode(m_srcB);
    m_graph->addNode(m_join);
    m_graph->addEdge("srcA", "output", "join", "input1");
    m_graph->addEdge("srcB", "output", "join", "input2");
  }

  static PortDataPtr makeFrame(FrameId frame) {
    auto packet = std::make_shared<PortData>();
    packet->id = frame;
    return packet;
  }

  std::unique_ptr<ExecutionEngine>
  makeEngine(std::chrono::milliseconds timeout, JoinTimeoutPolicy policy) {
    auto config = EngineConfig::stream(4, 16);
    config.join_wait_timeout = timeout;
    config.join_timeout_policy = policy;
    auto engine = ExecutionEngine::create(config);
    EXPECT_TRUE(engine->initialize(m_graph.get()).isOk());
    engine->setDropCallback(
        [this](const std::string &, std::uint64_t, const std::string &) {
          m_dropCount.fetch_add(1);
        });
    EXPECT_TRUE(engine->startStreaming().isOk());
    return engine;
  }

  static bool waitUntil(const std::function<bool()> &predicate,
                        std::chrono::milliseconds limit) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
      if (predicate()) {
        return true;
      }
      std::this_thread::sleep_for(2ms);
    }
    return predicate();
  }

  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<PassThroughNode> m_srcA;
  std::shared_ptr<PassThroughNode> m_srcB;
  std::shared_ptr<PortRecordingJoin> m_join;
  std::atomic<int> m_dropCount{0};
};

TEST_F(JoinTimeoutTest, PartialInputsExecutesWithAvailablePorts) {
  auto engine = makeEngine(30ms, JoinTimeoutPolicy::PartialInputs);

  // Only branch A ever gets data: the join must eventually run with
  // input1 alone instead of waiting forever.
  (void)engine->pushInput("srcA", makeFrame(1));

  ASSERT_TRUE(waitUntil([&] { return !m_join->executions().empty(); }, 2000ms))
      << "join never degraded to partial inputs";
  engine->stopStreaming(false);

  const auto executions = m_join->executions();
  ASSERT_EQ(executions.size(), 1u);
  ASSERT_EQ(executions[0].size(), 1u);
  EXPECT_EQ(executions[0][0].first, "input1");
  EXPECT_EQ(executions[0][0].second, 1u);
  EXPECT_GE(engine->statistics().total_join_timeouts, 1u);
}

TEST_F(JoinTimeoutTest, SkipFrameDiscardsStuckHeadThenPairsNormally) {
  auto engine = makeEngine(30ms, JoinTimeoutPolicy::SkipFrame);

  // Frame 1 only reaches branch A; it must be skipped (dropped), not
  // delivered, and the next complete pair must flow normally.
  (void)engine->pushInput("srcA", makeFrame(1));

  ASSERT_TRUE(waitUntil([&] { return m_dropCount.load() >= 1; }, 2000ms))
      << "stuck head was never skipped";

  (void)engine->pushInput("srcA", makeFrame(2));
  (void)engine->pushInput("srcB", makeFrame(2));

  ASSERT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  const auto executions = m_join->executions();
  ASSERT_EQ(executions.size(), 1u);
  ASSERT_EQ(executions[0].size(), 2u);
  for (const auto &[port, frame] : executions[0]) {
    EXPECT_EQ(frame, 2u) << "join saw the skipped frame on " << port;
  }
  EXPECT_GE(engine->statistics().total_join_timeouts, 1u);
}

TEST_F(JoinTimeoutTest, ZeroTimeoutWaitsIndefinitely) {
  auto engine = makeEngine(0ms, JoinTimeoutPolicy::PartialInputs);

  (void)engine->pushInput("srcA", makeFrame(1));
  std::this_thread::sleep_for(100ms);

  EXPECT_TRUE(m_join->executions().empty())
      << "join executed despite disabled timeout";
  EXPECT_EQ(m_dropCount.load(), 0);

  // The waiting head must still pair once the partner arrives.
  (void)engine->pushInput("srcB", makeFrame(1));
  ASSERT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  const auto executions = m_join->executions();
  ASSERT_EQ(executions.size(), 1u);
  EXPECT_EQ(executions[0].size(), 2u);
}

TEST_F(JoinTimeoutTest, TimelyPairsAreNeverDegraded) {
  auto engine = makeEngine(50ms, JoinTimeoutPolicy::SkipFrame);

  for (FrameId frame = 1; frame <= 5; ++frame) {
    (void)engine->pushInput("srcA", makeFrame(frame));
    (void)engine->pushInput("srcB", makeFrame(frame));
  }

  ASSERT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  const auto executions = m_join->executions();
  ASSERT_EQ(executions.size(), 5u);
  for (const auto &execution : executions) {
    EXPECT_EQ(execution.size(), 2u);
  }
  EXPECT_EQ(m_dropCount.load(), 0);
  EXPECT_EQ(engine->statistics().total_join_timeouts, 0u);
}

} // namespace ai_pipe_unit_test::join_timeout
