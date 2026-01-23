/**
 * @file test_join_aware_sync_strategy.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2026-01-23
 *
 * @copyright Copyright (c) 2026
 *
 */
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

  void addNode(const std::string &name) {
    m_graph.addNode(std::make_shared<MockNode>(name));
  }

  void addEdge(const std::string &from, const std::string &to) {
    m_graph.addEdge(from, "", to, "");
  }
};

/**
 * Diamond topology:
 *       A
 *      / \
 *     B   C
 *      \ /
 *       D
 */
TEST_F(JoinAwareSyncStrategyTest, SimpleDiamondPattern) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");

  addEdge("A", "B");
  addEdge("A", "C");
  addEdge("B", "D");
  addEdge("C", "D");

  m_strategy->initialize(&m_graph);

  auto b_mappings = m_strategy->getNodeMappings("B");
  auto c_mappings = m_strategy->getNodeMappings("C");

  ASSERT_FALSE(b_mappings.empty()) << "B should be mapped to a sync group";
  ASSERT_FALSE(c_mappings.empty()) << "C should be mapped to a sync group";
  EXPECT_EQ(b_mappings[0].first, c_mappings[0].first) << "Same sync group";
  EXPECT_NE(b_mappings[0].second, c_mappings[0].second) << "Different branches";

  // Test drop propagation
  auto affected = m_strategy->reportDrop("B", 100, "test_drop");
  EXPECT_FALSE(affected.empty());
  EXPECT_TRUE(m_strategy->shouldDrop("C", 100));
}

/**
 * Divergent topology (no join):
 *       A
 *      / \
 *     B   C
 *     |   |
 *     D   E
 */
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

  m_strategy->initialize(&m_graph);

  EXPECT_TRUE(m_strategy->getNodeMappings("B").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("C").empty());
}

/**
 * Deep branch topology:
 *       A
 *      / \
 *     B   C
 *     |   |
 *     D   E
 *      \ /
 *       F
 */
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

  m_strategy->initialize(&m_graph);

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

/**
 * Asymmetric branches:
 *       A
 *      / \
 *     B   C
 *     |   |
 *     D   |
 *     |   |
 *     E   |
 *      \ /
 *       F
 */
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

  m_strategy->initialize(&m_graph);

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

/**
 * Linear chain (no fork-join):
 *   A -> B -> C -> D
 */
TEST_F(JoinAwareSyncStrategyTest, LinearChainNoSync) {
  addNode("A");
  addNode("B");
  addNode("C");
  addNode("D");

  addEdge("A", "B");
  addEdge("B", "C");
  addEdge("C", "D");

  m_strategy->initialize(&m_graph);

  EXPECT_TRUE(m_strategy->getNodeMappings("A").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("B").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("C").empty());
  EXPECT_TRUE(m_strategy->getNodeMappings("D").empty());
}

} // namespace testing
} // namespace ai_pipe