/**
 * @file test_compiled_graph.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Tests for CompiledGraph: indexing, topology, routing, cycle detection
 * @version 0.1
 * @date 2026-07-03
 *
 * @copyright Copyright (c) 2026
 */
#include "ai_pipe/compiled_graph.hpp"
#include "helper_nodes.hpp"
#include <gtest/gtest.h>

using namespace ai_pipe;
using NodeIndex = CompiledGraph::NodeIndex;

namespace ai_pipe_unit_test::compiled_graph {

class CompiledGraphTest : public ::testing::Test {
protected:
  Graph m_graph;

  void addPassThrough(const std::string &name) {
    m_graph.addNode(std::make_shared<PassThroughNode>(name));
  }

  void addJoin(const std::string &name, const std::vector<std::string> &ports) {
    m_graph.addNode(std::make_shared<JoinNode>(name, ports));
  }

  static std::size_t positionOf(const std::vector<NodeIndex> &order,
                                NodeIndex idx) {
    return static_cast<std::size_t>(std::find(order.begin(), order.end(), idx) -
                                    order.begin());
  }
};

TEST_F(CompiledGraphTest, EmptyGraphIsRejected) {
  auto result = CompiledGraph::compile(m_graph);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.errorCode(), ErrorCode::GraphEmpty);
}

TEST_F(CompiledGraphTest, LinearChainTopology) {
  addPassThrough("a");
  addPassThrough("b");
  addPassThrough("c");
  ASSERT_TRUE(m_graph.addEdge("a", "output", "b", "input"));
  ASSERT_TRUE(m_graph.addEdge("b", "output", "c", "input"));

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_TRUE(result);
  const auto &cg = result.value();

  EXPECT_EQ(cg.nodeCount(), 3u);

  auto ia = cg.indexOf("a");
  auto ib = cg.indexOf("b");
  auto ic = cg.indexOf("c");
  ASSERT_NE(ia, CompiledGraph::k_invalid_index);
  ASSERT_NE(ib, CompiledGraph::k_invalid_index);
  ASSERT_NE(ic, CompiledGraph::k_invalid_index);

  EXPECT_TRUE(cg.isSource(ia));
  EXPECT_FALSE(cg.isSource(ib));
  EXPECT_TRUE(cg.isSink(ic));
  EXPECT_FALSE(cg.isSink(ib));

  EXPECT_EQ(cg.inDegree(ia), 0);
  EXPECT_EQ(cg.inDegree(ib), 1);
  EXPECT_EQ(cg.inDegree(ic), 1);

  const auto &order = cg.topologicalOrder();
  ASSERT_EQ(order.size(), 3u);
  EXPECT_LT(positionOf(order, ia), positionOf(order, ib));
  EXPECT_LT(positionOf(order, ib), positionOf(order, ic));

  ASSERT_EQ(cg.sourceNodes().size(), 1u);
  EXPECT_EQ(cg.sourceNodes()[0], ia);
  ASSERT_EQ(cg.sinkNodes().size(), 1u);
  EXPECT_EQ(cg.sinkNodes()[0], ic);
}

TEST_F(CompiledGraphTest, RoutingTableMatchesEdges) {
  addPassThrough("src");
  addJoin("join", {"input1", "input2"});
  addPassThrough("mid");
  ASSERT_TRUE(m_graph.addEdge("src", "output", "mid", "input"));
  ASSERT_TRUE(m_graph.addEdge("src", "output", "join", "input1"));
  ASSERT_TRUE(m_graph.addEdge("mid", "output", "join", "input2"));

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_TRUE(result);
  const auto &cg = result.value();

  auto isrc = cg.indexOf("src");
  auto ijoin = cg.indexOf("join");
  auto imid = cg.indexOf("mid");

  const auto &src_edges = cg.outEdges(isrc);
  ASSERT_EQ(src_edges.size(), 2u);
  for (const auto &e : src_edges) {
    EXPECT_EQ(e.source_port, "output");
    EXPECT_TRUE(e.dest_node == imid || e.dest_node == ijoin);
    if (e.dest_node == imid) {
      EXPECT_EQ(e.dest_port, "input");
    } else {
      EXPECT_EQ(e.dest_port, "input1");
    }
  }

  EXPECT_EQ(cg.inDegree(ijoin), 2);
  EXPECT_TRUE(cg.outEdges(ijoin).empty());
  EXPECT_TRUE(cg.isSink(ijoin));
}

TEST_F(CompiledGraphTest, ParallelEdgesCountedButAdjacencyDeduped) {
  // Two distinct edges between the same node pair via different ports
  m_graph.addNode(std::make_shared<PassThroughNode>("a"));
  addJoin("b", {"input1", "input2"});
  ASSERT_TRUE(m_graph.addEdge("a", "output", "b", "input1"));
  ASSERT_TRUE(m_graph.addEdge("a", "output", "b", "input2"));

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_TRUE(result);
  const auto &cg = result.value();

  auto ia = cg.indexOf("a");
  auto ib = cg.indexOf("b");

  EXPECT_EQ(cg.inDegree(ib), 2);           // per-edge
  EXPECT_EQ(cg.outEdges(ia).size(), 2u);   // per-edge
  EXPECT_EQ(cg.successors(ia).size(), 1u); // deduplicated
  EXPECT_EQ(cg.predecessors(ib).size(), 1u);
}

TEST_F(CompiledGraphTest, ForkJoinTopologicalOrder) {
  addPassThrough("source");
  addPassThrough("branch1");
  addPassThrough("branch2");
  addJoin("join", {"input1", "input2"});
  ASSERT_TRUE(m_graph.addEdge("source", "output", "branch1", "input"));
  ASSERT_TRUE(m_graph.addEdge("source", "output", "branch2", "input"));
  ASSERT_TRUE(m_graph.addEdge("branch1", "output", "join", "input1"));
  ASSERT_TRUE(m_graph.addEdge("branch2", "output", "join", "input2"));

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_TRUE(result);
  const auto &cg = result.value();
  const auto &order = cg.topologicalOrder();

  auto isource = cg.indexOf("source");
  auto ijoin = cg.indexOf("join");
  EXPECT_EQ(positionOf(order, isource), 0u);
  EXPECT_EQ(positionOf(order, ijoin), order.size() - 1);
}

TEST_F(CompiledGraphTest, CycleIsDetected) {
  // Build a cycle through the raw Graph API (Graph::addEdge does not
  // detect cycles by design; detection is deferred to compilation).
  auto a = std::make_shared<JoinNode>("a", std::vector<std::string>{"input"});
  auto b = std::make_shared<JoinNode>("b", std::vector<std::string>{"input"});
  m_graph.addNode(a);
  m_graph.addNode(b);
  ASSERT_TRUE(m_graph.addEdge("a", "output", "b", "input"));
  ASSERT_TRUE(m_graph.addEdge("b", "output", "a", "input"));

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.errorCode(), ErrorCode::GraphCycleDetected);
}

TEST_F(CompiledGraphTest, DeepChainDoesNotOverflow) {
  // Iterative Kahn must handle chains far deeper than any recursion limit.
  constexpr int k_depth = 50000;
  for (int i = 0; i < k_depth; ++i) {
    addPassThrough("n" + std::to_string(i));
  }
  for (int i = 0; i + 1 < k_depth; ++i) {
    ASSERT_TRUE(m_graph.addEdge("n" + std::to_string(i), "output",
                                "n" + std::to_string(i + 1), "input"));
  }

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_TRUE(result);
  EXPECT_EQ(result.value().topologicalOrder().size(),
            static_cast<std::size_t>(k_depth));
}

TEST_F(CompiledGraphTest, UnconnectedRequiredInputPortRejected) {
  addPassThrough("src");
  // Join declares two inputs but only input1 gets an edge: with
  // in-degree > 0 it would wait for input2 forever.
  addJoin("join", {"input1", "input2"});
  ASSERT_TRUE(m_graph.addEdge("src", "output", "join", "input1"));

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.errorCode(), ErrorCode::InvalidConfiguration);
  // Error names the starving port and node
  auto err = result.errorMessage();
  EXPECT_NE(err.find("input2"), std::string::npos);
  EXPECT_NE(err.find("join"), std::string::npos);
}

TEST_F(CompiledGraphTest, SourceNodePortsAreExemptFromConnectivity) {
  // A source node's declared input ports are fed externally; a graph
  // consisting of source -> sink must compile even though the source
  // has a declared, unconnected input port.
  addPassThrough("entry"); // declares "input", in-degree 0
  addPassThrough("exit");
  ASSERT_TRUE(m_graph.addEdge("entry", "output", "exit", "input"));

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_TRUE(result) << result.errorMessage();
}

TEST_F(CompiledGraphTest, LookupByNameAndPointer) {
  auto node = std::make_shared<PassThroughNode>("only");
  m_graph.addNode(node);

  auto result = CompiledGraph::compile(m_graph);
  ASSERT_TRUE(result);
  const auto &cg = result.value();

  EXPECT_EQ(cg.indexOf("only"), cg.indexOfPtr(node.get()));
  EXPECT_EQ(cg.indexOf("missing"), CompiledGraph::k_invalid_index);
  EXPECT_EQ(cg.indexOfPtr(nullptr), CompiledGraph::k_invalid_index);
  EXPECT_EQ(cg.node(cg.indexOf("only")).get(), node.get());
}

} // namespace ai_pipe_unit_test::compiled_graph
