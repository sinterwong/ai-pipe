#include "ai_pipe/data_types.hpp"
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <thread>
#include <vector>

namespace ai_pipe::test {

class TestSourceNode : public ILogicNode {
public:
  explicit TestSourceNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto packet = std::make_shared<PortData>();
    packet->id = m_counter.fetch_add(1, std::memory_order_relaxed);
    packet->setParam("timestamp", std::chrono::steady_clock::now());
    outputs["output"] = packet;
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {}; // Source has no input ports
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  void reset() { m_counter.store(0, std::memory_order_relaxed); }
  std::uint64_t counter() const { return m_counter.load(); }

private:
  std::atomic<std::uint64_t> m_counter{0};
};

class TestPassthroughNode : public ILogicNode {
public:
  explicit TestPassthroughNode(
      const std::string &name,
      std::chrono::microseconds delay = std::chrono::microseconds{0})
      : ILogicNode(name), m_delay(delay) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
    }

    auto it = inputs.find("input");
    if (it != inputs.end()) {
      outputs["output"] = it->second;
    }
    m_executionCount.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  std::uint64_t executionCount() const { return m_executionCount.load(); }
  void resetCount() { m_executionCount.store(0); }

private:
  std::chrono::microseconds m_delay;
  std::atomic<std::uint64_t> m_executionCount{0};
};

class TestSinkNode : public ILogicNode {
public:
  explicit TestSinkNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap & /*outputs*/,
               std::shared_ptr<PipelineContext> /*context*/) override {
    auto it = inputs.find("input");
    if (it != inputs.end() && it->second) {
      m_processed.fetch_add(1, std::memory_order_relaxed);
      m_lastId.store(it->second->id, std::memory_order_relaxed);
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  std::uint64_t processed() const { return m_processed.load(); }
  std::uint64_t lastId() const { return m_lastId.load(); }
  void reset() {
    m_processed.store(0);
    m_lastId.store(0);
  }

private:
  std::atomic<std::uint64_t> m_processed{0};
  std::atomic<std::uint64_t> m_lastId{0};
};

class SlowConsumerNode : public ILogicNode {
public:
  explicit SlowConsumerNode(const std::string &name,
                            std::chrono::milliseconds process_time)
      : ILogicNode(name), m_processTime(process_time) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    std::this_thread::sleep_for(m_processTime);

    auto it = inputs.find("input");
    if (it != inputs.end()) {
      outputs["output"] = it->second;
    }
    m_processed.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  std::uint64_t processed() const { return m_processed.load(); }
  void reset() { m_processed.store(0); }

private:
  std::chrono::milliseconds m_processTime;
  std::atomic<std::uint64_t> m_processed{0};
};

class StreamBackpressureTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a simple linear pipeline: source -> pass -> sink
    m_source = std::make_shared<TestSourceNode>("source");
    m_pass = std::make_shared<TestPassthroughNode>("pass");
    m_sink = std::make_shared<TestSinkNode>("sink");

    m_graph = std::make_unique<Graph>();
    m_graph->addNode(m_source);
    m_graph->addNode(m_pass);
    m_graph->addNode(m_sink);
    m_graph->addEdge("source", "output", "pass", "input");
    m_graph->addEdge("pass", "output", "sink", "input");
  }

  void TearDown() override {
    if (m_engine) {
      m_engine->stopStreaming(false);
      m_engine.reset();
    }
  }

  std::unique_ptr<ExecutionEngine>
  createStreamEngine(std::uint8_t workers = 4,
                     std::size_t queue_capacity = 32) {
    EngineConfig config;
    config.mode = ExecutionMode::STREAM;
    config.num_workers = workers;
    config.default_queue_capacity = queue_capacity;
    config.enable_statistics = true;
    return ExecutionEngine::create(config);
  }

  std::shared_ptr<TestSourceNode> m_source;
  std::shared_ptr<TestPassthroughNode> m_pass;
  std::shared_ptr<TestSinkNode> m_sink;
  std::unique_ptr<Graph> m_graph;
  std::unique_ptr<ExecutionEngine> m_engine;
};

TEST_F(StreamBackpressureTest, StatisticsResetBehavior) {
  m_engine = createStreamEngine(2, 16);
  m_engine->initialize(m_graph.get(), 2);
  m_engine->startStreaming();

  // Push some frames
  const std::size_t frames = 50;
  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    packet->setParam("timestamp", std::chrono::steady_clock::now());
    (void)m_engine->pushInput("source", "output", packet);
  }

  // Wait for processing
  m_engine->waitForDrain(0, std::chrono::milliseconds{5000});
  m_engine->stopStreaming(true);

  // Get statistics BEFORE reset - should have non-zero values
  auto stats_before = m_engine->statistics();
  EXPECT_GT(stats_before.total_frames_processed, 0u)
      << "Statistics should show processed frames before reset";

  // Reset the engine
  m_engine->reset();

  // Get statistics AFTER reset - this is the BUG
  auto stats_after = m_engine->statistics();

  // This assertion documents the bug - it will PASS because of the bug
  EXPECT_EQ(stats_after.total_frames_processed, 0u)
      << "BUG CONFIRMED: Statistics are zeroed after reset()";
}

TEST_F(StreamBackpressureTest, StatisticsBeforeReset_Workaround) {
  m_engine = createStreamEngine(2, 16);
  m_engine->initialize(m_graph.get(), 2);
  m_engine->startStreaming();

  const std::size_t frames = 100;
  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    packet->setParam("timestamp", std::chrono::steady_clock::now());
    (void)m_engine->pushInput("source", "output", packet);
  }

  m_engine->waitForDrain(0, std::chrono::milliseconds{5000});
  m_engine->stopStreaming(true);

  // CORRECT: Get statistics BEFORE reset
  auto stats = m_engine->statistics();

  EXPECT_GT(stats.total_frames_processed, 0u)
      << "Should have processed frames when statistics retrieved before reset";

  // Verify sink node also counted correctly
  EXPECT_EQ(m_sink->processed(), stats.total_frames_processed)
      << "Sink count should match engine statistics";

  // Now safe to reset
  m_engine->reset();
}

TEST_F(StreamBackpressureTest, StatisticsAccumulateDuringStreaming) {
  m_engine = createStreamEngine(4, 32);
  m_engine->initialize(m_graph.get(), 4);
  m_engine->startStreaming();

  std::vector<std::uint64_t> checkpoints;
  const std::size_t batch_size = 20;
  const std::size_t num_batches = 5;

  for (std::size_t batch = 0; batch < num_batches; ++batch) {
    for (std::size_t i = 0; i < batch_size; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = batch * batch_size + i;
      (void)m_engine->pushInput("source", "output", packet);
    }

    // Small delay to allow processing
    std::this_thread::sleep_for(std::chrono::milliseconds{50});

    auto stats = m_engine->statistics();
    checkpoints.push_back(stats.total_frames_processed);
  }

  m_engine->waitForDrain(0, std::chrono::milliseconds{5000});
  m_engine->stopStreaming(true);

  // Verify monotonic increase
  for (std::size_t i = 1; i < checkpoints.size(); ++i) {
    EXPECT_GE(checkpoints[i], checkpoints[i - 1])
        << "Statistics should monotonically increase at checkpoint " << i;
  }

  auto final_stats = m_engine->statistics();
  EXPECT_EQ(final_stats.total_frames_processed, batch_size * num_batches)
      << "Final count should equal total frames pushed";
}

TEST_F(StreamBackpressureTest, QueueDepthOnSourceNode_ReturnsZero) {
  m_engine = createStreamEngine(2, 32);
  m_engine->initialize(m_graph.get(), 2);
  m_engine->startStreaming();

  // Push data
  for (int i = 0; i < 10; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    (void)m_engine->pushInput("source", "output", packet);
  }

  // Query queue depth on source node
  auto depth = m_engine->queueDepth("source");

  // This will be 0 because source has no input ports
  // The data is pushed to the DOWNSTREAM node's input queue
  EXPECT_EQ(depth, 0u) << "API LIMITATION: queueDepth('source') returns 0 "
                          "because source has no input ports";

  m_engine->stopStreaming(false);
}

TEST_F(StreamBackpressureTest, QueueDepthOnDownstreamNode_ShowsActualDepth) {
  // Create a slow consumer to build up queue
  auto slow_node =
      std::make_shared<SlowConsumerNode>("slow", std::chrono::milliseconds{50});
  auto sink = std::make_shared<TestSinkNode>("sink");

  auto graph = std::make_unique<Graph>();
  graph->addNode(slow_node);
  graph->addNode(sink);
  graph->addEdge("slow", "output", "sink", "input");

  m_engine = createStreamEngine(1, 100); // Small worker count to create backlog
  m_engine->initialize(graph.get(), 1);
  m_engine->startStreaming();

  // Push many frames quickly
  const std::size_t frames = 20;
  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    packet->setParam("timestamp", std::chrono::steady_clock::now());
    (void)m_engine->pushInput("slow", "input", packet);
  }

  // Immediately check queue depth on the slow node
  auto depth = m_engine->queueDepth("slow", "input");

  // Should have some items queued
  EXPECT_GT(depth, 0u)
      << "Queue depth on downstream node should show queued items";

  m_engine->stopStreaming(false);
}

TEST_F(StreamBackpressureTest, QueueDepthWithExplicitPort) {
  m_engine = createStreamEngine(1, 32);
  m_engine->initialize(m_graph.get(), 1);
  m_engine->startStreaming();

  // Push data to pass node's input
  const std::size_t frames = 15;
  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    (void)m_engine->pushInput("source", "output", packet);
  }

  // Small delay to allow some propagation but not full processing
  std::this_thread::sleep_for(std::chrono::microseconds{100});

  // Query with explicit port name
  auto depth_explicit = m_engine->queueDepth("pass", "input");
  auto depth_default =
      m_engine->queueDepth("pass"); // Should use first input port

  EXPECT_EQ(depth_explicit, depth_default)
      << "Explicit and default port queries should return same value";

  m_engine->stopStreaming(false);
}

TEST_F(StreamBackpressureTest, QueueDepthOnNonExistentNode_ReturnsZero) {
  m_engine = createStreamEngine(2, 16);
  m_engine->initialize(m_graph.get(), 2);

  auto depth = m_engine->queueDepth("non_existent_node");
  EXPECT_EQ(depth, 0u) << "queueDepth on non-existent node should return 0";
}

TEST_F(StreamBackpressureTest, QueueDepthOnNonExistentPort_ReturnsZero) {
  m_engine = createStreamEngine(2, 16);
  m_engine->initialize(m_graph.get(), 2);

  auto depth = m_engine->queueDepth("pass", "non_existent_port");
  EXPECT_EQ(depth, 0u) << "queueDepth on non-existent port should return 0";
}

TEST_F(StreamBackpressureTest, HasQueueCapacityBehavior) {
  const std::size_t queue_capacity = 8;
  m_engine = createStreamEngine(1, queue_capacity);
  m_engine->initialize(m_graph.get(), 1);
  m_engine->startStreaming();

  // Initially should have capacity
  EXPECT_TRUE(m_engine->hasQueueCapacity("pass", "input"))
      << "Empty queue should have capacity";

  // Note: Without a slow consumer, we can't easily test full queue
  // because items get processed too quickly

  m_engine->stopStreaming(false);
}

TEST_F(StreamBackpressureTest, BackpressureWithCorrectStatistics) {
  // Create a pipeline with slow consumer
  auto slow_node =
      std::make_shared<SlowConsumerNode>("slow", std::chrono::milliseconds{10});
  auto sink = std::make_shared<TestSinkNode>("sink");

  auto graph = std::make_unique<Graph>();
  graph->addNode(slow_node);
  graph->addNode(sink);
  graph->addEdge("slow", "output", "sink", "input");

  const std::size_t queue_capacity = 16;
  m_engine = createStreamEngine(2, queue_capacity);
  m_engine->initialize(graph.get(), 2);
  m_engine->startStreaming();

  // Fast producer
  const std::size_t frames = 50;
  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    packet->setParam("timestamp", std::chrono::steady_clock::now());
    (void)m_engine->pushInput("slow", "input", packet);
    std::this_thread::sleep_for(
        std::chrono::microseconds{100}); // Fast but not instant
  }

  // Wait for drain
  bool drained = m_engine->waitForDrain(0, std::chrono::milliseconds{10000});
  EXPECT_TRUE(drained) << "Should drain within timeout";

  // IMPORTANT: Get statistics BEFORE stopStreaming/reset
  auto stats = m_engine->statistics();

  m_engine->stopStreaming(true);

  // Verify processing happened
  EXPECT_GT(stats.total_frames_processed, 0u)
      << "CRITICAL: processed should be > 0 when stats retrieved before reset";

  EXPECT_EQ(sink->processed(), stats.total_frames_processed)
      << "Sink count should match statistics";

  // Now safe to reset
  m_engine->reset();

  // After reset, stats are zeroed (this is the bug in the benchmark)
  auto stats_after_reset = m_engine->statistics();
  EXPECT_EQ(stats_after_reset.total_frames_processed, 0u)
      << "Stats are zeroed after reset (known behavior)";
}

TEST_F(StreamBackpressureTest, QueueDepthDuringBackpressure) {
  // Create slow pipeline
  auto slow_node =
      std::make_shared<SlowConsumerNode>("slow", std::chrono::milliseconds{20});
  auto sink = std::make_shared<TestSinkNode>("sink");

  auto graph = std::make_unique<Graph>();
  graph->addNode(slow_node);
  graph->addNode(sink);
  graph->addEdge("slow", "output", "sink", "input");

  const std::size_t queue_capacity = 32;
  m_engine = createStreamEngine(1, queue_capacity);
  m_engine->initialize(graph.get(), 1);
  m_engine->startStreaming();

  std::size_t max_observed_depth = 0;
  std::vector<std::size_t> depth_samples;

  // Push frames and monitor queue depth
  const std::size_t frames = 30;
  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    (void)m_engine->pushInput("slow", "input", packet);

    // Sample queue depth on the slow node (not source!)
    auto depth = m_engine->queueDepth("slow", "input");
    depth_samples.push_back(depth);
    max_observed_depth = std::max(max_observed_depth, depth);

    std::this_thread::sleep_for(std::chrono::milliseconds{1});
  }

  m_engine->stopStreaming(false);

  // Should have observed non-zero queue depths
  EXPECT_GT(max_observed_depth, 0u)
      << "Should observe queue buildup during backpressure";

  // Print for debugging
  std::cout << "Max observed queue depth: " << max_observed_depth << std::endl;
  std::cout << "Queue capacity: " << queue_capacity << std::endl;
}

// =============================================================================
// Regression Tests for Potential Fixes
// =============================================================================

/**
 * @test [Proposed Fix] Statistics should survive reset() if explicitly
 * requested
 *
 * This test will FAIL with current implementation, documenting the desired
 * behavior.
 */
TEST_F(StreamBackpressureTest, DISABLED_ProposedFix_StatisticsPreserveOption) {
  // This test documents desired behavior for a proposed fix:
  // engine->reset(/* preserve_statistics = */ true);

  m_engine = createStreamEngine(2, 16);
  m_engine->initialize(m_graph.get(), 2);
  m_engine->startStreaming();

  const std::size_t frames = 50;
  for (std::size_t i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    (void)m_engine->pushInput("source", "output", packet);
  }

  m_engine->waitForDrain(0, std::chrono::milliseconds{5000});
  m_engine->stopStreaming(true);

  // Proposed API: reset(preserve_statistics)
  // m_engine->reset(true);  // Preserve statistics

  // auto stats = m_engine->statistics();
  // EXPECT_GT(stats.total_frames_processed, 0u);
}

/**
 * @test [Proposed Fix] queueDepth should work with node output ports
 *
 * This test will FAIL with current implementation, documenting the desired
 * behavior.
 */
TEST_F(StreamBackpressureTest, DISABLED_ProposedFix_QueueDepthForOutputPorts) {
  // This test documents desired behavior for a proposed fix:
  // queueDepth should be able to query output port queues

  m_engine = createStreamEngine(1, 32);
  m_engine->initialize(m_graph.get(), 1);
  m_engine->startStreaming();

  for (int i = 0; i < 10; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    (void)m_engine->pushInput("source", "output", packet);
  }

  // Proposed API: queueDepth(node, port, direction)
  // auto depth = m_engine->queueDepth("source", "output",
  // PortDirection::Output); EXPECT_GT(depth, 0u);

  m_engine->stopStreaming(false);
}

} // namespace ai_pipe::test
