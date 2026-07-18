#include "ai_pipe/compiled_graph.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include "sync_strategies.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

using namespace ai_pipe;

namespace {

class DummyNode : public ILogicNode {
public:
  explicit DummyNode(const std::string &name) : ILogicNode(name) {}
  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {}
};

// R4.2: strategies initialize from a CompiledGraph snapshot
CompiledGraph makeTrivialCompiledGraph() {
  Graph graph;
  graph.addNode(std::make_shared<DummyNode>("solo"));
  auto compiled = CompiledGraph::compile(graph);
  EXPECT_TRUE(compiled.isOk()) << compiled.errorMessage();
  return std::move(compiled).value();
}

} // namespace

// =============================================================================
// NoSyncStrategy Tests
// =============================================================================

class NoSyncStrategyTest : public ::testing::Test {
protected:
  NoSyncStrategy m_strategy;
};

TEST_F(NoSyncStrategyTest, Name) {
  EXPECT_EQ(m_strategy.name(), "NoSyncStrategy");
}

TEST_F(NoSyncStrategyTest, Clone) {
  auto cloned = m_strategy.clone();
  ASSERT_NE(cloned, nullptr);
  EXPECT_EQ(cloned->name(), "NoSyncStrategy");
}

TEST_F(NoSyncStrategyTest, IsEnabledReturnsFalse) {
  EXPECT_FALSE(m_strategy.isEnabled());
}

TEST_F(NoSyncStrategyTest, InitializeIsNoop) {
  // Should not crash
  auto compiled = makeTrivialCompiledGraph();
  m_strategy.initialize(compiled);
}

TEST_F(NoSyncStrategyTest, ResetIsNoop) {
  // Should not crash
  m_strategy.reset();
}

TEST_F(NoSyncStrategyTest, RegisterSyncGroupIsNoop) {
  // Should not crash
  m_strategy.registerSyncGroup("group1", {"branch_A", "branch_B"}, "join_node");
}

TEST_F(NoSyncStrategyTest, MapNodeToGroupIsNoop) {
  // Should not crash
  m_strategy.mapNodeToGroup("node1", "group1", "branch_A");
}

TEST_F(NoSyncStrategyTest, ReportDropReturnsEmpty) {
  auto affected = m_strategy.reportDrop("node1", 100, "backpressure");
  EXPECT_TRUE(affected.empty());
}

TEST_F(NoSyncStrategyTest, ShouldDropAlwaysReturnsFalse) {
  // No synchronization, so nothing should be dropped
  EXPECT_FALSE(m_strategy.shouldDrop("node1", 100));
  EXPECT_FALSE(m_strategy.shouldDrop("node2", 200));
  EXPECT_FALSE(m_strategy.shouldDrop("any_node", 0));
  EXPECT_FALSE(m_strategy.shouldDrop("", 999999));
}

TEST_F(NoSyncStrategyTest, MarkProcessedIsNoop) {
  // Should not crash
  m_strategy.markProcessed("node1", 100);
}

TEST_F(NoSyncStrategyTest, GetWatermarkAlwaysReturnsZero) {
  EXPECT_EQ(m_strategy.getWatermark("group1"), 0);
  EXPECT_EQ(m_strategy.getWatermark("group2"), 0);
  EXPECT_EQ(m_strategy.getWatermark("nonexistent"), 0);
  EXPECT_EQ(m_strategy.getWatermark(""), 0);
}

// =============================================================================
// NoSyncStrategy Integration Tests
// =============================================================================

TEST(NoSyncStrategyIntegrationTest, SimulatePipelineExecution) {
  NoSyncStrategy strategy;

  // Setup
  auto compiled = makeTrivialCompiledGraph();
  strategy.initialize(compiled);
  strategy.registerSyncGroup("group1", {"branch_A", "branch_B"}, "join");
  strategy.mapNodeToGroup("node_A", "group1", "branch_A");
  strategy.mapNodeToGroup("node_B", "group1", "branch_B");

  // Simulate frame processing
  for (FrameId frame = 1; frame <= 10; ++frame) {
    // Report drops (should have no effect)
    if (frame == 5) {
      auto affected = strategy.reportDrop("node_A", frame, "backpressure");
      EXPECT_TRUE(affected.empty());
    }

    // Check if should drop (always false)
    EXPECT_FALSE(strategy.shouldDrop("node_A", frame));
    EXPECT_FALSE(strategy.shouldDrop("node_B", frame));

    // Mark processed (no-op)
    strategy.markProcessed("node_A", frame);
    strategy.markProcessed("node_B", frame);
  }

  // Watermark always zero
  EXPECT_EQ(strategy.getWatermark("group1"), 0);

  // Reset
  strategy.reset();
}

TEST(NoSyncStrategyIntegrationTest, MultipleGroups) {
  NoSyncStrategy strategy;

  strategy.registerSyncGroup("group1", {"A1", "A2"}, "join1");
  strategy.registerSyncGroup("group2", {"B1", "B2"}, "join2");

  strategy.mapNodeToGroup("node_A1", "group1", "A1");
  strategy.mapNodeToGroup("node_A2", "group1", "A2");
  strategy.mapNodeToGroup("node_B1", "group2", "B1");
  strategy.mapNodeToGroup("node_B2", "group2", "B2");

  // All operations should be no-ops
  EXPECT_FALSE(strategy.shouldDrop("node_A1", 100));
  EXPECT_FALSE(strategy.shouldDrop("node_B1", 100));

  auto affected1 = strategy.reportDrop("node_A1", 100, "test");
  auto affected2 = strategy.reportDrop("node_B1", 200, "test");

  EXPECT_TRUE(affected1.empty());
  EXPECT_TRUE(affected2.empty());
}

// =============================================================================
// ISyncStrategy Interface Compliance Tests
// =============================================================================

TEST(ISyncStrategyTest, NoSyncStrategyImplementsInterface) {
  std::unique_ptr<ISyncStrategy> strategy = std::make_unique<NoSyncStrategy>();

  // All interface methods should be callable
  auto compiled = makeTrivialCompiledGraph();
  strategy->initialize(compiled);
  strategy->reset();
  strategy->registerSyncGroup("group", {}, "");
  strategy->mapNodeToGroup("node", "group", "branch");

  auto affected = strategy->reportDrop("node", 1, "reason");
  EXPECT_TRUE(affected.empty());

  EXPECT_FALSE(strategy->shouldDrop("node", 1));

  strategy->markProcessed("node", 1);

  EXPECT_EQ(strategy->getWatermark("group"), 0);
  EXPECT_FALSE(strategy->isEnabled());
  EXPECT_EQ(strategy->name(), "NoSyncStrategy");

  auto cloned = strategy->clone();
  EXPECT_NE(cloned, nullptr);
}

// =============================================================================
// Factory Function Tests
// =============================================================================

TEST(SyncStrategyFactoryTest, CreateNoSyncStrategy) {
  auto strategy = createNoSyncStrategy();

  ASSERT_NE(strategy, nullptr);
  EXPECT_EQ(strategy->name(), "NoSyncStrategy");
  EXPECT_FALSE(strategy->isEnabled());
}

TEST(SyncStrategyFactoryTest, CreateNoSyncStrategyReturnsUniqueInstances) {
  auto strategy1 = createNoSyncStrategy();
  auto strategy2 = createNoSyncStrategy();

  EXPECT_NE(strategy1.get(), strategy2.get());
}

// =============================================================================
// Polymorphism Tests
// =============================================================================

TEST(SyncStrategyPolymorphismTest, UseAsBasePointer) {
  std::vector<std::unique_ptr<ISyncStrategy>> strategies;

  strategies.push_back(createNoSyncStrategy());
  strategies.push_back(std::make_unique<NoSyncStrategy>());

  for (const auto &strategy : strategies) {
    EXPECT_FALSE(strategy->isEnabled());
    EXPECT_FALSE(strategy->shouldDrop("any_node", 100));
    EXPECT_EQ(strategy->getWatermark("any_group"), 0);
  }
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST(NoSyncStrategyEdgeCasesTest, EmptyStrings) {
  NoSyncStrategy strategy;

  strategy.registerSyncGroup("", {}, "");
  strategy.mapNodeToGroup("", "", "");

  EXPECT_FALSE(strategy.shouldDrop("", 0));
  EXPECT_EQ(strategy.getWatermark(""), 0);

  auto affected = strategy.reportDrop("", 0, "");
  EXPECT_TRUE(affected.empty());
}

TEST(NoSyncStrategyEdgeCasesTest, MaxFrameId) {
  NoSyncStrategy strategy;

  FrameId max_frame = std::numeric_limits<FrameId>::max();

  EXPECT_FALSE(strategy.shouldDrop("node", max_frame));
  EXPECT_EQ(strategy.getWatermark("group"), 0);

  auto affected = strategy.reportDrop("node", max_frame, "test");
  EXPECT_TRUE(affected.empty());
}

TEST(NoSyncStrategyEdgeCasesTest, LargeBranchList) {
  NoSyncStrategy strategy;

  std::vector<BranchId> branches;
  for (int i = 0; i < 1000; ++i) {
    branches.push_back("branch_" + std::to_string(i));
  }

  // Should handle large branch lists without issues
  strategy.registerSyncGroup("large_group", branches, "join");

  EXPECT_FALSE(strategy.isEnabled());
}

TEST(NoSyncStrategyEdgeCasesTest, SpecialCharactersInNames) {
  NoSyncStrategy strategy;

  strategy.registerSyncGroup("group/with/slashes",
                             {"branch:colon", "branch@at"}, "join#hash");
  strategy.mapNodeToGroup("node.dot", "group/with/slashes", "branch:colon");

  EXPECT_FALSE(strategy.shouldDrop("node.dot", 100));
  EXPECT_EQ(strategy.getWatermark("group/with/slashes"), 0);
}
