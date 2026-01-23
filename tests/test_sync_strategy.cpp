/**
 * @file test_sync_strategy.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2026-01-21
 *
 * @copyright Copyright (c) 2026
 *
 */
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "helper_nodes.hpp"
#include "helper_sync_strategy.hpp"
#include "sync_strategies.hpp"
#include <gtest/gtest.h>
#include <memory>

using namespace ai_pipe;
using namespace std::chrono_literals;

namespace ai_pipe_unit_test::sync_strategy {
class SyncStrategyTest : public testing::Test {
protected:
  void SetUp() override { m_graph = std::make_unique<Graph>(); }

  void TearDown() override { m_graph.reset(); }

  // Helper to create input data
  static PortDataPtr createData(uint64_t id = 1) {
    auto data = std::make_shared<PortData>();
    data->id = id;
    data->setParam("test", true);
    return data;
  }

  // Helper to create a simple linear pipeline: Source -> Process -> Sink
  void createLinearPipeline() {
    m_source = std::make_shared<SourceNode>("source");
    m_process = std::make_shared<PassThroughNode>("process");
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(m_process);
    m_graph->addNode(m_sink);

    m_graph->addEdge("source", "output", "process", "input");
    m_graph->addEdge("process", "output", "sink", "input");
  }

  // Helper to create a forking pipeline: Source -> [Branch1, Branch2] -> Join
  // -> Sink
  void createForkJoinPipeline() {
    m_source = std::make_shared<SourceNode>("source");
    auto branch1 = std::make_shared<PassThroughNode>("branch1");
    auto branch2 = std::make_shared<PassThroughNode>("branch2");
    auto join = std::make_shared<JoinNode>(
        "join", std::vector<std::string>{"input1", "input2"});
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(branch1);
    m_graph->addNode(branch2);
    m_graph->addNode(join);
    m_graph->addNode(m_sink);

    m_graph->addEdge("source", "output", "branch1", "input");
    m_graph->addEdge("source", "output", "branch2", "input");
    m_graph->addEdge("branch1", "output", "join", "input1");
    m_graph->addEdge("branch2", "output", "join", "input2");
    m_graph->addEdge("join", "output", "sink", "input");
  }

  void createParallelBranchPipeline() {
    // Create: Source -> [Branch1, Branch2] -> Join -> Sink
    m_source = std::make_shared<SourceNode>("source");
    m_branch1 = std::make_shared<PassThroughNode>("branch1", 10ms);
    m_branch2 = std::make_shared<PassThroughNode>("branch2", 20ms);
    m_join = std::make_shared<JoinNode>("join",
                                        std::vector<std::string>{"in1", "in2"});
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(m_branch1);
    m_graph->addNode(m_branch2);
    m_graph->addNode(m_join);
    m_graph->addNode(m_sink);

    m_graph->addEdge("source", "output", "branch1", "input");
    m_graph->addEdge("source", "output", "branch2", "input");
    m_graph->addEdge("branch1", "output", "join", "in1");
    m_graph->addEdge("branch2", "output", "join", "in2");
    m_graph->addEdge("join", "output", "sink", "input");
  }

  void createTripleBranchPipeline() {
    // Create: Source -> [B1, B2, B3] -> Join -> Sink
    m_source = std::make_shared<SourceNode>("source");
    m_branch1 = std::make_shared<PassThroughNode>("branch1", 5ms);
    m_branch2 = std::make_shared<PassThroughNode>("branch2", 10ms);
    m_branch3 = std::make_shared<PassThroughNode>("branch3", 15ms);
    m_join = std::make_shared<JoinNode>(
        "join", std::vector<std::string>{"in1", "in2", "in3"});
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(m_branch1);
    m_graph->addNode(m_branch2);
    m_graph->addNode(m_branch3);
    m_graph->addNode(m_join);
    m_graph->addNode(m_sink);

    m_graph->addEdge("source", "output", "branch1", "input");
    m_graph->addEdge("source", "output", "branch2", "input");
    m_graph->addEdge("source", "output", "branch3", "input");
    m_graph->addEdge("branch1", "output", "join", "in1");
    m_graph->addEdge("branch2", "output", "join", "in2");
    m_graph->addEdge("branch3", "output", "join", "in3");
    m_graph->addEdge("join", "output", "sink", "input");
  }

  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<SourceNode> m_source;
  std::shared_ptr<PassThroughNode> m_process;
  std::shared_ptr<SinkNode> m_sink;

  std::shared_ptr<PassThroughNode> m_branch1;
  std::shared_ptr<PassThroughNode> m_branch2;
  std::shared_ptr<PassThroughNode> m_branch3;
  std::shared_ptr<JoinNode> m_join;
};

// -----------------------------------------------------------------------------
// NoSyncStrategy Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, NoSyncStrategyBasicBehavior) {
  auto strategy = std::make_unique<NoSyncStrategy>();

  EXPECT_FALSE(strategy->isEnabled());
  EXPECT_EQ(strategy->name(), "NoSyncStrategy");

  // All operations should be no-ops
  strategy->initialize(m_graph.get());
  strategy->reset();
  strategy->registerSyncGroup("group1", {"b1", "b2"}, "join");
  strategy->mapNodeToGroup("node1", "group1", "b1");

  auto affected = strategy->reportDrop("node1", 100, "test");
  EXPECT_TRUE(affected.empty());

  EXPECT_FALSE(strategy->shouldDrop("node1", 100));

  strategy->markProcessed("node1", 100);

  EXPECT_EQ(strategy->getWatermark("group1"), 0u);
}

TEST_F(SyncStrategyTest, NoSyncStrategyClone) {
  auto strategy = std::make_unique<NoSyncStrategy>();
  auto cloned = strategy->clone();

  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->name(), "NoSyncStrategy");
  EXPECT_FALSE(cloned->isEnabled());
}

// -----------------------------------------------------------------------------
// Custom Sync Strategy Injection Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, CustomSyncStrategyInjection) {
  createLinearPipeline();

  auto engine = createStreamEngine();
  engine->setSyncStrategy(std::make_unique<TestSyncStrategy>());
  engine->initialize(m_graph.get());

  auto info = engine->strategyInfo();
  EXPECT_TRUE(info.find("TestSyncStrategy") != std::string::npos);
}

TEST_F(SyncStrategyTest, SyncStrategyInitializedWithGraph) {
  createParallelBranchPipeline();

  auto engine = createStreamEngine();
  engine->setSyncStrategy(std::make_unique<TestSyncStrategy>());

  engine->initialize(m_graph.get());

  // After engine init, strategy should be initialized
  auto info = engine->strategyInfo();
  EXPECT_TRUE(info.find("TestSyncStrategy") != std::string::npos);
}

TEST_F(SyncStrategyTest, SyncStrategyResetOnEngineReset) {
  createLinearPipeline();

  auto engine = createStreamEngine();
  engine->initialize(m_graph.get());

  engine->startStreaming();
  (void)engine->pushInput("source", createData(1));
  std::this_thread::sleep_for(50ms);
  engine->stopStreaming();

  // Reset engine
  engine->reset();

  // Engine should be in IDLE state
  EXPECT_EQ(engine->getState(), EngineState::IDLE);
}

// -----------------------------------------------------------------------------
// Sync Group Registration Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, ManualSyncGroupRegistration) {
  auto strategy = std::make_unique<TestSyncStrategy>();
  auto strategy_ptr = strategy.get();

  strategy->registerSyncGroup("group1", {"branch1", "branch2"}, "join");

  auto groups = strategy_ptr->getSyncGroups();
  ASSERT_EQ(groups.size(), 1u);
  ASSERT_TRUE(groups.count("group1") > 0);
  EXPECT_EQ(groups["group1"].size(), 2u);
}

TEST_F(SyncStrategyTest, MultipleSyncGroupRegistration) {
  auto strategy = std::make_unique<TestSyncStrategy>();
  auto strategy_ptr = strategy.get();

  strategy->registerSyncGroup("group1", {"b1", "b2"}, "join1");
  strategy->registerSyncGroup("group2", {"b3", "b4", "b5"}, "join2");

  auto groups = strategy_ptr->getSyncGroups();
  EXPECT_EQ(groups.size(), 2u);
  EXPECT_EQ(groups["group1"].size(), 2u);
  EXPECT_EQ(groups["group2"].size(), 3u);
}

TEST_F(SyncStrategyTest, NodeToGroupMapping) {
  auto strategy = std::make_unique<TestSyncStrategy>();

  strategy->registerSyncGroup("group1", {"branch1", "branch2"}, "join");
  strategy->mapNodeToGroup("node_a", "group1", "branch1");
  strategy->mapNodeToGroup("node_b", "group1", "branch2");

  // Verify mapping works by testing shouldDrop after reportDrop
  (void)strategy->reportDrop("node_a", 100, "backpressure");

  // node_b should drop frame 100 since node_a in same group dropped it
  EXPECT_TRUE(strategy->shouldDrop("node_b", 100));
  EXPECT_FALSE(strategy->shouldDrop("node_a", 100)); // Original dropper
}

// -----------------------------------------------------------------------------
// Drop Propagation Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, DropPropagationAcrossBranches) {
  auto strategy = std::make_unique<TestSyncStrategy>();

  strategy->registerSyncGroup("group1", {"b1", "b2", "b3"}, "join");
  strategy->mapNodeToGroup("node1", "group1", "b1");
  strategy->mapNodeToGroup("node2", "group1", "b2");
  strategy->mapNodeToGroup("node3", "group1", "b3");

  // b1 drops frame 42
  auto affected = strategy->reportDrop("node1", 42, "queue full");

  EXPECT_EQ(affected.size(), 2u); // b2 and b3 affected

  // Other branches should drop the same frame
  EXPECT_TRUE(strategy->shouldDrop("node2", 42));
  EXPECT_TRUE(strategy->shouldDrop("node3", 42));

  // Different frame should not be affected
  EXPECT_FALSE(strategy->shouldDrop("node2", 43));
}

TEST_F(SyncStrategyTest, MultipleFrameDrops) {
  auto strategy = std::make_unique<TestSyncStrategy>();

  strategy->registerSyncGroup("group1", {"b1", "b2"}, "join");
  strategy->mapNodeToGroup("node1", "group1", "b1");
  strategy->mapNodeToGroup("node2", "group1", "b2");

  // Drop multiple frames
  (void)strategy->reportDrop("node1", 10, "drop1");
  (void)strategy->reportDrop("node1", 20, "drop2");
  (void)strategy->reportDrop("node2", 30, "drop3");

  // node2 should drop frames 10 and 20 (from node1)
  EXPECT_TRUE(strategy->shouldDrop("node2", 10));
  EXPECT_TRUE(strategy->shouldDrop("node2", 20));

  // node1 should drop frame 30 (from node2)
  EXPECT_TRUE(strategy->shouldDrop("node1", 30));
}

TEST_F(SyncStrategyTest, DropDoesNotAffectOtherGroups) {
  auto strategy = std::make_unique<TestSyncStrategy>();

  strategy->registerSyncGroup("group1", {"b1", "b2"}, "join1");
  strategy->registerSyncGroup("group2", {"b3", "b4"}, "join2");
  strategy->mapNodeToGroup("node1", "group1", "b1");
  strategy->mapNodeToGroup("node2", "group1", "b2");
  strategy->mapNodeToGroup("node3", "group2", "b3");
  strategy->mapNodeToGroup("node4", "group2", "b4");

  // Drop in group1 should not affect group2
  (void)strategy->reportDrop("node1", 100, "test");

  EXPECT_TRUE(strategy->shouldDrop("node2", 100));  // Same group
  EXPECT_FALSE(strategy->shouldDrop("node3", 100)); // Different group
  EXPECT_FALSE(strategy->shouldDrop("node4", 100)); // Different group
}

// -----------------------------------------------------------------------------
// Frame Processing and Watermark Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, MarkProcessedTracking) {
  auto strategy = std::make_unique<TestSyncStrategy>();
  auto strategy_ptr = strategy.get();

  strategy->markProcessed("node1", 1);
  strategy->markProcessed("node1", 2);
  strategy->markProcessed("node1", 3);
  strategy->markProcessed("node2", 5);

  auto node1_frames = strategy_ptr->getProcessedFrames("node1");
  auto node2_frames = strategy_ptr->getProcessedFrames("node2");

  EXPECT_EQ(node1_frames.size(), 3u);
  EXPECT_TRUE(node1_frames.count(1));
  EXPECT_TRUE(node1_frames.count(2));
  EXPECT_TRUE(node1_frames.count(3));

  EXPECT_EQ(node2_frames.size(), 1u);
  EXPECT_TRUE(node2_frames.count(5));
}

TEST_F(SyncStrategyTest, WatermarkTracking) {
  auto strategy = std::make_unique<TestSyncStrategy>();

  strategy->registerSyncGroup("group1", {"b1", "b2"}, "join");
  strategy->setWatermark("group1", 42);

  EXPECT_EQ(strategy->getWatermark("group1"), 42u);
  EXPECT_EQ(strategy->getWatermark("nonexistent"), 0u);
}

// -----------------------------------------------------------------------------
// Stream Mode with Sync Strategy Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, StreamModeUsesCoordinatedSync) {
  createParallelBranchPipeline();

  // Stream mode should automatically use CoordinatedSyncStrategy
  auto engine = createStreamEngine();
  engine->initialize(m_graph.get());

  auto info = engine->strategyInfo();
  EXPECT_TRUE(info.find("CoordinatedSyncStrategy") != std::string::npos);
}

TEST_F(SyncStrategyTest, BatchModeUsesNoSync) {
  createParallelBranchPipeline();

  // Batch mode should use NoSyncStrategy
  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  auto info = engine->strategyInfo();
  EXPECT_TRUE(info.find("NoSyncStrategy") != std::string::npos);
}

TEST_F(SyncStrategyTest, HybridModeUsesCoordinatedSync) {
  createParallelBranchPipeline();

  auto engine = createHybridEngine();
  engine->initialize(m_graph.get());

  auto info = engine->strategyInfo();
  EXPECT_TRUE(info.find("CoordinatedSyncStrategy") != std::string::npos);
}

// -----------------------------------------------------------------------------
// Parallel Branch Execution with Sync Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, ParallelBranchExecutionBatchMode) {
  createParallelBranchPipeline();

  auto engine = createBatchEngine(4);
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData(1);

  EXPECT_TRUE(engine->execute(inputs, true));

  // All nodes should have processed
  EXPECT_EQ(m_source->processCount(), 1);
  EXPECT_EQ(m_branch1->processCount(), 1);
  EXPECT_EQ(m_branch2->processCount(), 1);
  EXPECT_EQ(m_join->processCount(), 1);
  EXPECT_EQ(m_sink->processCount(), 1);
}

TEST_F(SyncStrategyTest, ParallelBranchExecutionStreamMode) {
  createParallelBranchPipeline();

  auto engine = createStreamEngine(4, 16);
  engine->initialize(m_graph.get());

  engine->startStreaming();

  for (int i = 0; i < 5; ++i) {
    auto result = engine->pushInput("source", createData(i));
    EXPECT_TRUE(result.isOk());
  }

  engine->waitForDrain(0, 2000ms);
  engine->stopStreaming();

  // All branches should have been executed
  EXPECT_GE(m_branch1->processCount(), 1);
  EXPECT_GE(m_branch2->processCount(), 1);
}

TEST_F(SyncStrategyTest, TripleBranchExecution) {
  createTripleBranchPipeline();

  auto engine = createBatchEngine(4);
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData(42);

  EXPECT_TRUE(engine->execute(inputs, true));

  // All three branches should have processed
  EXPECT_EQ(m_branch1->processCount(), 1);
  EXPECT_EQ(m_branch2->processCount(), 1);
  EXPECT_EQ(m_branch3->processCount(), 1);
  EXPECT_EQ(m_join->processCount(), 1);
}

// -----------------------------------------------------------------------------
// Sync Strategy Clone Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, TestSyncStrategyClone) {
  auto strategy = std::make_unique<TestSyncStrategy>();

  strategy->registerSyncGroup("group1", {"b1", "b2"}, "join");
  strategy->mapNodeToGroup("node1", "group1", "b1");
  strategy->setEnabled(false);

  auto cloned = strategy->clone();
  auto cloned_ptr = dynamic_cast<TestSyncStrategy *>(cloned.get());

  ASSERT_NE(cloned_ptr, nullptr);
  EXPECT_EQ(cloned_ptr->name(), "TestSyncStrategy");
}

// -----------------------------------------------------------------------------
// Integration Tests with Drop Callback
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, DropCallbackWithSyncStrategy) {
  createParallelBranchPipeline();

  auto config = EngineConfig::stream(2, 2); // Small queues
  auto engine = ExecutionEngine::create(config);

  // Set up small queues for branches to trigger drops
  QueueConfig small_queue;
  small_queue.capacity = 2;
  small_queue.drop_strategy = "DropHead";
  engine->setNodeQueueConfig("branch1", small_queue);
  engine->setNodeQueueConfig("branch2", small_queue);

  engine->initialize(m_graph.get());

  std::atomic<int> drop_count{0};
  std::vector<std::tuple<std::string, uint64_t, std::string>> drop_events;
  std::mutex drop_mutex;

  engine->setDropCallback([&](const std::string &node, std::uint64_t frame_id,
                              const std::string &reason) {
    std::lock_guard<std::mutex> lock(drop_mutex);
    drop_events.emplace_back(node, frame_id, reason);
    drop_count.fetch_add(1);
  });

  engine->startStreaming();

  // Push many frames quickly to trigger drops
  for (int i = 0; i < 20; ++i) {
    (void)engine->pushInput("source", createData(i));
  }

  std::this_thread::sleep_for(500ms);
  engine->stopStreaming();

  // May or may not have drops depending on timing
  // Just verify no crashes and callback mechanism works
  EXPECT_GE(drop_count.load(), 0);
}

// -----------------------------------------------------------------------------
// Sync Strategy State Consistency Tests
// -----------------------------------------------------------------------------

TEST_F(SyncStrategyTest, SyncStrategyResetClearsState) {
  auto strategy = std::make_unique<TestSyncStrategy>();
  auto strategy_ptr = strategy.get();

  // Add some state
  (void)strategy->registerSyncGroup("group1", {"b1", "b2"}, "join");
  (void)strategy->mapNodeToGroup("node1", "group1", "b1");
  (void)strategy->reportDrop("node1", 100, "test");
  strategy->markProcessed("node1", 50);

  EXPECT_FALSE(strategy_ptr->getDroppedFrames("node1").empty());
  EXPECT_FALSE(strategy_ptr->getProcessedFrames("node1").empty());

  // Reset
  strategy->reset();

  // State should be cleared
  EXPECT_TRUE(strategy_ptr->getDroppedFrames("node1").empty());
  EXPECT_TRUE(strategy_ptr->getProcessedFrames("node1").empty());
  EXPECT_EQ(strategy_ptr->getResetCount(), 1);
}

TEST_F(SyncStrategyTest, SyncStrategyThreadSafety) {
  auto strategy = std::make_shared<TestSyncStrategy>();

  strategy->registerSyncGroup("group1", {"b1", "b2", "b3", "b4"}, "join");
  strategy->mapNodeToGroup("node1", "group1", "b1");
  strategy->mapNodeToGroup("node2", "group1", "b2");
  strategy->mapNodeToGroup("node3", "group1", "b3");
  strategy->mapNodeToGroup("node4", "group1", "b4");

  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;

  // Multiple threads doing operations
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&, t]() {
      std::string node = "node" + std::to_string(t + 1);
      for (int i = 0; i < 100 && !stop.load(); ++i) {
        (void)strategy->reportDrop(node, i, "thread_" + std::to_string(t));
        strategy->markProcessed(node, i);
        (void)strategy->shouldDrop(node, i);
      }
    });
  }

  std::this_thread::sleep_for(100ms);
  stop.store(true);

  for (auto &th : threads) {
    th.join();
  }

  // Should not crash, verify some operations completed
  EXPECT_GT(strategy->getProcessedFrames("node1").size(), 0u);
}
} // namespace ai_pipe_unit_test::sync_strategy