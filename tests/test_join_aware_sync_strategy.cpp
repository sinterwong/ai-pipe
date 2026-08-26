#include "ai_pipe/context.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "join_aware_sync_strategy.hpp"
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

namespace ai_pipe {
namespace testing {

class MockNode : public ILogicNode {
public:
  explicit MockNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {}
};

class JoinAwareSyncStrategyTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_strategy = std::make_unique<JoinAwareSyncStrategy>();
  }

  std::unique_ptr<JoinAwareSyncStrategy> m_strategy;
  Graph m_graph;

  // strategies initialize from the compiled snapshot
  [[nodiscard]] CompiledGraph compiledGraph() {
    auto compiled = CompiledGraph::compile(m_graph);
    EXPECT_TRUE(compiled.isOk()) << compiled.errorMessage();
    return std::move(compiled).value();
  }

  void addNode(const std::string &name) {
    m_graph.addNode(std::make_shared<MockNode>(name));
  }

  void addEdge(const std::string &from, const std::string &to) {
    m_graph.addEdge(from, "", to, "");
  }
};

// Diamond topology:
//      A
//     / \
//    B   C
//     \ /
//      D
TEST_F(JoinAwareSyncStrategyTest, SimpleDiamondPattern) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");

  addEdge("A", "B");
  addEdge("A", "C");
  addEdge("B", "D");
  addEdge("C", "D");

  auto compiled = compiledGraph();
  m_strategy->initialize(compiled);

  auto b_mappings = m_strategy->getNodeMappings("B");
  auto c_mappings = m_strategy->getNodeMappings("C");

  ASSERT_FALSE(b_mappings.empty()) << "B should be mapped to a sync group";
  ASSERT_FALSE(c_mappings.empty()) << "C should be mapped to a sync group";
  EXPECT_EQ(b_mappings[0].first, c_mappings[0].first) << "Same sync group";
  EXPECT_NE(b_mappings[0].second, c_mappings[0].second) << "Different branches";

  auto affected = m_strategy->reportDrop("B", 100, "test_drop");
  EXPECT_FALSE(affected.empty());
  EXPECT_TRUE(m_strategy->shouldDrop("C", 100));
}

// Divergent topology (no join):
//      A
//     / \
//    B   C
//    |   |
//    D   E
TEST_F(JoinAwareSyncStrategyTest, DivergentBranchesNoSync) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");
  addNode("E");

  addEdge("A", "B");
  addEdge("A", "C");
  addEdge("B", "D");
  addEdge("C", "E");

  auto compiled = compiledGraph();
  m_strategy->initialize(compiled);

  EXPECT_TRUE(m_strategy->getNodeMappings("B").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("C").empty());
}

// Deep branch topology:
//      A
//     / \
//    B   C
//    |   |
//    D   E
//     \ /
//      F
TEST_F(JoinAwareSyncStrategyTest, DeepBranchSync) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");
  addNode("E");
  addNode("F");

  addEdge("A", "B");
  addEdge("A", "C");
  addEdge("B", "D");
  addEdge("C", "E");
  addEdge("D", "F");
  addEdge("E", "F");

  auto compiled = compiledGraph();
  m_strategy->initialize(compiled);

  auto b_map = m_strategy->getNodeMappings("B");
  auto d_map = m_strategy->getNodeMappings("D");
  auto c_map = m_strategy->getNodeMappings("C");
  auto e_map = m_strategy->getNodeMappings("E");

  ASSERT_FALSE(b_map.empty());
  ASSERT_FALSE(d_map.empty());

  // B and D should be on the same logical branch
  EXPECT_EQ(b_map[0].second, d_map[0].second) << "B and D on same branch";

  // KEY TEST: Deep node drop propagation
  (void)m_strategy->reportDrop("D", 200, "backpressure");
  EXPECT_TRUE(m_strategy->shouldDrop("E", 200)) << "E drops when D drops";
  EXPECT_TRUE(m_strategy->shouldDrop("C", 200)) << "C drops when D drops";
}

// Asymmetric branches:
//      A
//     / \
//    B   C
//    |   |
//    D   |
//    |   |
//    E   |
//     \ /
//      F
TEST_F(JoinAwareSyncStrategyTest, AsymmetricBranchLengths) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");
  addNode("E");
  addNode("F");

  addEdge("A", "B");
  addEdge("A", "C");
  addEdge("B", "D");
  addEdge("D", "E");
  addEdge("E", "F");
  addEdge("C", "F");

  auto compiled = compiledGraph();
  m_strategy->initialize(compiled);

  auto b_map = m_strategy->getNodeMappings("B");
  auto d_map = m_strategy->getNodeMappings("D");
  auto e_map = m_strategy->getNodeMappings("E");

  ASSERT_FALSE(b_map.empty());
  EXPECT_EQ(b_map[0].second, d_map[0].second) << "B-D same branch";
  EXPECT_EQ(d_map[0].second, e_map[0].second) << "D-E same branch";

  // KEY TEST: Deepest node drop notification
  (void)m_strategy->reportDrop("E", 600, "backpressure");
  EXPECT_TRUE(m_strategy->shouldDrop("C", 600))
      << "C drops when E (deep) drops";
}

// Linear chain (no fork-join):
//  A -> B -> C -> D
TEST_F(JoinAwareSyncStrategyTest, LinearChainNoSync) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");

  addEdge("A", "B");
  addEdge("B", "C");
  addEdge("C", "D");

  auto compiled = compiledGraph();
  m_strategy->initialize(compiled);

  EXPECT_TRUE(m_strategy->getNodeMappings("A").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("B").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("C").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("D").empty());
}

// Nested Fork-Join:
//      A
//     / \
//    B   \
//   / \   \
//  C   D   E
//   \ /   /
//    F --/
//    |
//    G
TEST_F(JoinAwareSyncStrategyTest, NestedForkJoin) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");
  addNode("E");
  addNode("F");
  addNode("G");

  addEdge("A", "B");
  addEdge("A", "E");
  addEdge("B", "C");
  addEdge("B", "D");
  addEdge("C", "F");
  addEdge("D", "F");
  addEdge("E", "F");
  addEdge("F", "G");

  auto compiled = compiledGraph();
  m_strategy->initialize(compiled);

  // C and D should be in a sync group (inner fork-join: B -> C,D -> F)
  // B and E should be in a sync group (outer fork-join: A -> B,E -> F)

  auto c_maps = m_strategy->getNodeMappings("C");
  auto d_maps = m_strategy->getNodeMappings("D");
  auto e_maps = m_strategy->getNodeMappings("E");
  auto b_maps = m_strategy->getNodeMappings("B");

  ASSERT_FALSE(c_maps.empty());
  ASSERT_FALSE(d_maps.empty());
  ASSERT_FALSE(e_maps.empty());
  // B is an intermediate node in the outer loop, and a fork for the inner loop.
  // Our algorithm maps intermediate nodes of paths to branches.
  ASSERT_FALSE(b_maps.empty());

  bool shared_inner = false;
  for (auto &cm : c_maps) {
    for (auto &dm : d_maps) {
      if (cm.first == dm.first)
        shared_inner = true;
    }
  }
  EXPECT_TRUE(shared_inner);

  bool shared_outer = false;
  for (auto &bm : b_maps) {
    for (auto &em : e_maps) {
      if (bm.first == em.first)
        shared_outer = true;
    }
  }
  EXPECT_TRUE(shared_outer);

  // If C drops, D should drop (inner)
  (void)m_strategy->reportDrop("C", 777, "inner_drop");
  EXPECT_TRUE(m_strategy->shouldDrop("D", 777));

  // Also, if B is on a branch parallel to E, E should drop (outer)
  // Since C is "under" B, it depends on how mapping is implemented.
  // If C is part of the path for branch B, it should also trigger B's group
  // drops.
  EXPECT_TRUE(m_strategy->shouldDrop("E", 777));
}

// Manual Registration Surface (ISyncStrategy contract)
//
// The engine wires the strategy through initialize() topology analysis;
// registerSyncGroup/mapNodeToGroup are the manual path for custom or
// engine-independent use and must behave equivalently.

TEST_F(JoinAwareSyncStrategyTest, ManualRegistrationMirrorsTopologyAnalysis) {
  m_strategy->registerSyncGroup("g", {"left", "right"}, "join");
  m_strategy->mapNodeToGroup("B", "g", "left");
  m_strategy->mapNodeToGroup("C", "g", "right");

  EXPECT_TRUE(m_strategy->tracksNode("B"));
  EXPECT_TRUE(m_strategy->tracksNode("C"));
  EXPECT_FALSE(m_strategy->tracksNode("unmapped"));

  // Drop on one branch propagates to the sibling, exactly as with
  // initialize()-derived groups
  auto affected = m_strategy->reportDrop("B", 42, "manual");
  ASSERT_EQ(affected.size(), 1u);
  EXPECT_EQ(affected[0], "right");
  EXPECT_TRUE(m_strategy->shouldDrop("C", 42));
  EXPECT_FALSE(m_strategy->shouldDrop("C", 43));
}

TEST_F(JoinAwareSyncStrategyTest, WatermarkTracksSlowestBranch) {
  m_strategy->registerSyncGroup("g", {"left", "right"}, "");
  m_strategy->mapNodeToGroup("B", "g", "left");
  m_strategy->mapNodeToGroup("C", "g", "right");

  EXPECT_EQ(m_strategy->getWatermark("g"), 0u);

  // Both branches complete frame 1; the left branch races ahead to 2
  m_strategy->markProcessed("B", 1);
  m_strategy->markProcessed("C", 1);
  m_strategy->markProcessed("B", 2);

  EXPECT_EQ(m_strategy->getWatermark("g"), 1u);

  m_strategy->markProcessed("C", 2);
  EXPECT_EQ(m_strategy->getWatermark("g"), 2u);

  // Unknown groups read as watermark 0 rather than failing
  EXPECT_EQ(m_strategy->getWatermark("no_such_group"), 0u);
}

TEST_F(JoinAwareSyncStrategyTest, CloneThenInitializeMatchesOriginal) {
  // The engine clones a user-supplied strategy and re-runs initialize()
  // on the clone (Pipeline::withSyncStrategy path); the clone must be
  // independently usable
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");
  addEdge("A", "B");
  addEdge("A", "C");
  addEdge("B", "D");
  addEdge("C", "D");

  auto clone = m_strategy->clone();
  ASSERT_NE(clone, nullptr);
  EXPECT_EQ(clone->name(), m_strategy->name());
  EXPECT_TRUE(clone->isEnabled());

  auto compiled = compiledGraph();
  clone->initialize(compiled);
  EXPECT_TRUE(clone->tracksNode("B"));
  EXPECT_TRUE(clone->tracksNode("C"));

  auto affected = clone->reportDrop("B", 5, "clone_drop");
  EXPECT_FALSE(affected.empty());
  EXPECT_TRUE(clone->shouldDrop("C", 5));
  // The original is untouched by activity on the clone
  EXPECT_FALSE(m_strategy->shouldDrop("C", 5));
}

// ISyncStrategy default tracksNode() (interface contract)

namespace {

// Minimal strategy overriding only the pure virtuals, so the default
// tracksNode() implementation (delegate to isEnabled) is exercised
class MinimalSyncStrategy : public ISyncStrategy {
public:
  explicit MinimalSyncStrategy(bool enabled) : m_enabled(enabled) {}

  void initialize(const CompiledGraph &) override {}
  void reset() override {}
  void registerSyncGroup(const SyncGroupId &, const std::vector<BranchId> &,
                         const std::string &) override {}
  void mapNodeToGroup(const std::string &, const SyncGroupId &,
                      const BranchId &) override {}
  [[nodiscard]] std::vector<BranchId> reportDrop(const std::string &, FrameId,
                                                 const std::string &) override {
    return {};
  }
  [[nodiscard]] bool shouldDrop(const std::string &, FrameId) const override {
    return false;
  }
  void markProcessed(const std::string &, FrameId) override {}
  [[nodiscard]] FrameId getWatermark(const SyncGroupId &) const override {
    return 0;
  }
  [[nodiscard]] bool isEnabled() const override { return m_enabled; }
  [[nodiscard]] std::string name() const override { return "Minimal"; }
  [[nodiscard]] std::unique_ptr<ISyncStrategy> clone() const override {
    return std::make_unique<MinimalSyncStrategy>(m_enabled);
  }

private:
  bool m_enabled;
};

} // namespace

TEST(ISyncStrategyDefaultsTest, TracksNodeDelegatesToIsEnabled) {
  MinimalSyncStrategy enabled(true);
  MinimalSyncStrategy disabled(false);

  EXPECT_TRUE(enabled.tracksNode("any"));
  EXPECT_FALSE(disabled.tracksNode("any"));
}

} // namespace testing
} // namespace ai_pipe