#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include <algorithm>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

using namespace ai_pipe;

// Mock Node Implementation

class MockNode : public ILogicNode {
public:
  explicit MockNode(const std::string &name) : ILogicNode(name) {
    // Default ports for testing edge connections
    m_outputPorts = {"out", "out1", "out2", "out3", "output", "output_port"};
    m_inputPorts = {"in", "in1", "in2", "in3", "input", "input_port"};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    (void)inputs;
    (void)outputs;
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return m_inputPorts;
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return m_outputPorts;
  }

  void setInputPorts(const std::vector<std::string> &ports) {
    m_inputPorts = ports;
  }

  void setOutputPorts(const std::vector<std::string> &ports) {
    m_outputPorts = ports;
  }

private:
  std::vector<std::string> m_inputPorts;
  std::vector<std::string> m_outputPorts;
};

// Graph Basic Tests

class GraphTest : public ::testing::Test {
protected:
  void SetUp() override { m_graph = std::make_unique<Graph>(); }

  std::shared_ptr<MockNode> createNode(const std::string &name) {
    return std::make_shared<MockNode>(name);
  }

  std::unique_ptr<Graph> m_graph;
};

TEST_F(GraphTest, DefaultConstruction) {
  EXPECT_TRUE(m_graph->getNodes().empty());
  EXPECT_TRUE(m_graph->getEdges().empty());
  EXPECT_FALSE(m_graph->hasCycle());
}

TEST_F(GraphTest, AddNode) {
  auto node = createNode("node1");

  EXPECT_TRUE(m_graph->addNode(node));
  EXPECT_EQ(m_graph->getNodes().size(), 1);
  EXPECT_EQ(m_graph->getNode("node1"), node);
}

TEST_F(GraphTest, AddMultipleNodes) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");
  auto node3 = createNode("node3");

  EXPECT_TRUE(m_graph->addNode(node1));
  EXPECT_TRUE(m_graph->addNode(node2));
  EXPECT_TRUE(m_graph->addNode(node3));

  EXPECT_EQ(m_graph->getNodes().size(), 3);
}

TEST_F(GraphTest, AddDuplicateNodeFails) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node1"); // Same name

  EXPECT_TRUE(m_graph->addNode(node1));
  EXPECT_FALSE(m_graph->addNode(node2));
  EXPECT_EQ(m_graph->getNodes().size(), 1);
}

TEST_F(GraphTest, GetNodeReturnsNullForNonexistent) {
  auto node = createNode("node1");
  m_graph->addNode(node);

  EXPECT_EQ(m_graph->getNode("nonexistent"), nullptr);
}

TEST_F(GraphTest, AddEdge) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");

  m_graph->addNode(node1);
  m_graph->addNode(node2);

  EXPECT_TRUE(m_graph->addEdge("node1", "output", "node2", "input"));
  EXPECT_EQ(m_graph->getEdges().size(), 1);
}

TEST_F(GraphTest, AddEdgeWithNonexistentSourceFails) {
  auto node2 = createNode("node2");
  m_graph->addNode(node2);

  EXPECT_FALSE(m_graph->addEdge("nonexistent", "output", "node2", "input"));
}

TEST_F(GraphTest, AddEdgeWithNonexistentDestFails) {
  auto node1 = createNode("node1");
  m_graph->addNode(node1);

  EXPECT_FALSE(m_graph->addEdge("node1", "output", "nonexistent", "input"));
}

TEST_F(GraphTest, AddMultipleEdges) {
  auto source = createNode("source");
  auto proc1 = createNode("proc1");
  auto proc2 = createNode("proc2");
  auto sink = createNode("sink");

  m_graph->addNode(source);
  m_graph->addNode(proc1);
  m_graph->addNode(proc2);
  m_graph->addNode(sink);

  EXPECT_TRUE(m_graph->addEdge("source", "out", "proc1", "in"));
  EXPECT_TRUE(m_graph->addEdge("source", "out", "proc2", "in"));
  EXPECT_TRUE(m_graph->addEdge("proc1", "out", "sink", "in1"));
  EXPECT_TRUE(m_graph->addEdge("proc2", "out", "sink", "in2"));

  EXPECT_EQ(m_graph->getEdges().size(), 4);
}

// Degree Tests

TEST_F(GraphTest, InDegreeSourceNode) {
  auto source = createNode("source");
  auto sink = createNode("sink");

  m_graph->addNode(source);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "out", "sink", "in");

  EXPECT_EQ(m_graph->getInDegree(source), 0);
  EXPECT_EQ(m_graph->getInDegree(sink), 1);
}

TEST_F(GraphTest, OutDegreeSourceNode) {
  auto source = createNode("source");
  auto sink = createNode("sink");

  m_graph->addNode(source);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "out", "sink", "in");

  EXPECT_EQ(m_graph->getOutDegree(source), 1);
  EXPECT_EQ(m_graph->getOutDegree(sink), 0);
}

TEST_F(GraphTest, InDegreeMultipleInputs) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");
  auto node3 = createNode("node3");
  auto join = createNode("join");

  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addNode(node3);
  m_graph->addNode(join);

  m_graph->addEdge("node1", "out", "join", "in1");
  m_graph->addEdge("node2", "out", "join", "in2");
  m_graph->addEdge("node3", "out", "join", "in3");

  EXPECT_EQ(m_graph->getInDegree(join), 3);
}

TEST_F(GraphTest, OutDegreeMultipleOutputs) {
  auto source = createNode("source");
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");
  auto node3 = createNode("node3");

  m_graph->addNode(source);
  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addNode(node3);

  m_graph->addEdge("source", "out1", "node1", "in");
  m_graph->addEdge("source", "out2", "node2", "in");
  m_graph->addEdge("source", "out3", "node3", "in");

  EXPECT_EQ(m_graph->getOutDegree(source), 3);
}

// Neighbor Tests

TEST_F(GraphTest, GetOutgoingNeighbors) {
  auto source = createNode("source");
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");

  m_graph->addNode(source);
  m_graph->addNode(node1);
  m_graph->addNode(node2);

  m_graph->addEdge("source", "out1", "node1", "in");
  m_graph->addEdge("source", "out2", "node2", "in");

  auto neighbors = m_graph->getOutgoingNeighbors(source);

  EXPECT_EQ(neighbors.size(), 2);
  EXPECT_NE(std::find(neighbors.begin(), neighbors.end(), node1),
            neighbors.end());
  EXPECT_NE(std::find(neighbors.begin(), neighbors.end(), node2),
            neighbors.end());
}

TEST_F(GraphTest, GetIncomingNeighbors) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");
  auto sink = createNode("sink");

  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addNode(sink);

  m_graph->addEdge("node1", "out", "sink", "in1");
  m_graph->addEdge("node2", "out", "sink", "in2");

  auto neighbors = m_graph->getIncomingNeighbors(sink);

  EXPECT_EQ(neighbors.size(), 2);
  EXPECT_NE(std::find(neighbors.begin(), neighbors.end(), node1),
            neighbors.end());
  EXPECT_NE(std::find(neighbors.begin(), neighbors.end(), node2),
            neighbors.end());
}

TEST_F(GraphTest, GetOutgoingNeighborsEmpty) {
  auto sink = createNode("sink");
  m_graph->addNode(sink);

  auto neighbors = m_graph->getOutgoingNeighbors(sink);
  EXPECT_TRUE(neighbors.empty());
}

TEST_F(GraphTest, GetIncomingNeighborsEmpty) {
  auto source = createNode("source");
  m_graph->addNode(source);

  auto neighbors = m_graph->getIncomingNeighbors(source);
  EXPECT_TRUE(neighbors.empty());
}

// Edge Query Tests

TEST_F(GraphTest, GetIncomingEdges) {
  auto source1 = createNode("source1");
  auto source2 = createNode("source2");
  auto sink = createNode("sink");

  m_graph->addNode(source1);
  m_graph->addNode(source2);
  m_graph->addNode(sink);

  m_graph->addEdge("source1", "out", "sink", "in1");
  m_graph->addEdge("source2", "out", "sink", "in2");

  auto edges = m_graph->getIncomingEdges(sink);

  EXPECT_EQ(edges.size(), 2);
}

TEST_F(GraphTest, GetOutgoingEdges) {
  auto source = createNode("source");
  auto sink1 = createNode("sink1");
  auto sink2 = createNode("sink2");

  m_graph->addNode(source);
  m_graph->addNode(sink1);
  m_graph->addNode(sink2);

  m_graph->addEdge("source", "out1", "sink1", "in");
  m_graph->addEdge("source", "out2", "sink2", "in");

  auto edges = m_graph->getOutgoingEdges(source);

  EXPECT_EQ(edges.size(), 2);
}

TEST_F(GraphTest, EdgeContainsCorrectPorts) {
  auto source = createNode("source");
  auto sink = createNode("sink");

  m_graph->addNode(source);
  m_graph->addNode(sink);

  m_graph->addEdge("source", "output_port", "sink", "input_port");

  auto edges = m_graph->getEdges();
  ASSERT_EQ(edges.size(), 1);

  EXPECT_EQ(edges[0].source_node, source);
  EXPECT_EQ(edges[0].source_port, "output_port");
  EXPECT_EQ(edges[0].dest_node, sink);
  EXPECT_EQ(edges[0].dest_port, "input_port");
}

// Cycle Detection Tests

TEST_F(GraphTest, NoCycleInLinearGraph) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");
  auto node3 = createNode("node3");

  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addNode(node3);

  m_graph->addEdge("node1", "out", "node2", "in");
  m_graph->addEdge("node2", "out", "node3", "in");

  EXPECT_FALSE(m_graph->hasCycle());
}

TEST_F(GraphTest, NoCycleInDiamondGraph) {
  auto source = createNode("source");
  auto left = createNode("left");
  auto right = createNode("right");
  auto sink = createNode("sink");

  m_graph->addNode(source);
  m_graph->addNode(left);
  m_graph->addNode(right);
  m_graph->addNode(sink);

  m_graph->addEdge("source", "out1", "left", "in");
  m_graph->addEdge("source", "out2", "right", "in");
  m_graph->addEdge("left", "out", "sink", "in1");
  m_graph->addEdge("right", "out", "sink", "in2");

  EXPECT_FALSE(m_graph->hasCycle());
}

TEST_F(GraphTest, DetectSimpleCycle) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");

  m_graph->addNode(node1);
  m_graph->addNode(node2);

  m_graph->addEdge("node1", "out", "node2", "in");
  m_graph->addEdge("node2", "out", "node1", "in");

  EXPECT_TRUE(m_graph->hasCycle());
}

TEST_F(GraphTest, DetectThreeNodeCycle) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");
  auto node3 = createNode("node3");

  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addNode(node3);

  m_graph->addEdge("node1", "out", "node2", "in");
  m_graph->addEdge("node2", "out", "node3", "in");
  m_graph->addEdge("node3", "out", "node1", "in");

  EXPECT_TRUE(m_graph->hasCycle());
}

TEST_F(GraphTest, DetectSelfLoop) {
  auto node = createNode("node");
  m_graph->addNode(node);

  m_graph->addEdge("node", "out", "node", "in");

  EXPECT_TRUE(m_graph->hasCycle());
}

TEST_F(GraphTest, NoCycleWithMultipleBranches) {
  // Complex DAG with multiple branches
  auto source = createNode("source");
  auto a = createNode("a");
  auto b = createNode("b");
  auto c = createNode("c");
  auto d = createNode("d");
  auto sink = createNode("sink");

  m_graph->addNode(source);
  m_graph->addNode(a);
  m_graph->addNode(b);
  m_graph->addNode(c);
  m_graph->addNode(d);
  m_graph->addNode(sink);

  m_graph->addEdge("source", "out", "a", "in");
  m_graph->addEdge("source", "out", "b", "in");
  m_graph->addEdge("a", "out", "c", "in");
  m_graph->addEdge("b", "out", "c", "in");
  m_graph->addEdge("b", "out", "d", "in");
  m_graph->addEdge("c", "out", "sink", "in1");
  m_graph->addEdge("d", "out", "sink", "in2");

  EXPECT_FALSE(m_graph->hasCycle());
}

// Clear Tests

TEST_F(GraphTest, Clear) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");

  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addEdge("node1", "out", "node2", "in");

  EXPECT_EQ(m_graph->getNodes().size(), 2);
  EXPECT_EQ(m_graph->getEdges().size(), 1);

  m_graph->clear();

  EXPECT_TRUE(m_graph->getNodes().empty());
  EXPECT_TRUE(m_graph->getEdges().empty());
}

// Move Semantics Tests

TEST_F(GraphTest, MoveConstruction) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");

  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addEdge("node1", "out", "node2", "in");

  Graph moved(std::move(*m_graph));

  EXPECT_EQ(moved.getNodes().size(), 2);
  EXPECT_EQ(moved.getEdges().size(), 1);
  EXPECT_EQ(moved.getNode("node1"), node1);
}

TEST_F(GraphTest, MoveAssignment) {
  auto node1 = createNode("node1");
  auto node2 = createNode("node2");

  m_graph->addNode(node1);
  m_graph->addNode(node2);
  m_graph->addEdge("node1", "out", "node2", "in");

  Graph moved;
  moved = std::move(*m_graph);

  EXPECT_EQ(moved.getNodes().size(), 2);
  EXPECT_EQ(moved.getEdges().size(), 1);
}

// Complex Topology Tests

TEST_F(GraphTest, ComplexPipelineTopology) {
  // Create a complex pipeline:
  //   Source -> Decoder -> [Resize, ColorConvert] -> Merge -> Inference -> Sink
  auto source = createNode("source");
  auto decoder = createNode("decoder");
  auto resize = createNode("resize");
  auto color = createNode("color_convert");
  auto merge = createNode("merge");
  auto inference = createNode("inference");
  auto sink = createNode("sink");

  m_graph->addNode(source);
  m_graph->addNode(decoder);
  m_graph->addNode(resize);
  m_graph->addNode(color);
  m_graph->addNode(merge);
  m_graph->addNode(inference);
  m_graph->addNode(sink);

  m_graph->addEdge("source", "out", "decoder", "in");
  m_graph->addEdge("decoder", "out", "resize", "in");
  m_graph->addEdge("decoder", "out", "color_convert", "in");
  m_graph->addEdge("resize", "out", "merge", "in1");
  m_graph->addEdge("color_convert", "out", "merge", "in2");
  m_graph->addEdge("merge", "out", "inference", "in");
  m_graph->addEdge("inference", "out", "sink", "in");

  EXPECT_EQ(m_graph->getNodes().size(), 7);
  EXPECT_EQ(m_graph->getEdges().size(), 7);
  EXPECT_FALSE(m_graph->hasCycle());

  EXPECT_EQ(m_graph->getInDegree(source), 0);
  EXPECT_EQ(m_graph->getOutDegree(source), 1);
  EXPECT_EQ(m_graph->getInDegree(merge), 2);
  EXPECT_EQ(m_graph->getOutDegree(decoder), 2);
  EXPECT_EQ(m_graph->getInDegree(sink), 1);
  EXPECT_EQ(m_graph->getOutDegree(sink), 0);
}

TEST_F(GraphTest, DisconnectedComponents) {
  // Two separate subgraphs
  auto a1 = createNode("a1");
  auto a2 = createNode("a2");
  auto b1 = createNode("b1");
  auto b2 = createNode("b2");

  m_graph->addNode(a1);
  m_graph->addNode(a2);
  m_graph->addNode(b1);
  m_graph->addNode(b2);

  m_graph->addEdge("a1", "out", "a2", "in");
  m_graph->addEdge("b1", "out", "b2", "in");

  EXPECT_EQ(m_graph->getNodes().size(), 4);
  EXPECT_EQ(m_graph->getEdges().size(), 2);
  EXPECT_FALSE(m_graph->hasCycle());

  // a1 and b1 have no incoming neighbors
  EXPECT_TRUE(m_graph->getIncomingNeighbors(a1).empty());
  EXPECT_TRUE(m_graph->getIncomingNeighbors(b1).empty());

  // a2 and b2 have no outgoing neighbors
  EXPECT_TRUE(m_graph->getOutgoingNeighbors(a2).empty());
  EXPECT_TRUE(m_graph->getOutgoingNeighbors(b2).empty());
}

// Edge Cases

TEST_F(GraphTest, SingleNodeGraph) {
  auto node = createNode("single");
  m_graph->addNode(node);

  EXPECT_EQ(m_graph->getNodes().size(), 1);
  EXPECT_TRUE(m_graph->getEdges().empty());
  EXPECT_EQ(m_graph->getInDegree(node), 0);
  EXPECT_EQ(m_graph->getOutDegree(node), 0);
  EXPECT_FALSE(m_graph->hasCycle());
}

TEST_F(GraphTest, EmptyGraphHasNoCycle) { EXPECT_FALSE(m_graph->hasCycle()); }

TEST_F(GraphTest, NestedDiamondTopology) {
  // Nested Diamond:
  //      /-- B --\
  // A --|         |-- E --\
  //      \-- C --/         |-- G
  //      /-- D --\         |
  // F --|         |-- H --/
  //      \-------/
  auto a = createNode("A");
  auto b = createNode("B");
  auto c = createNode("C");
  auto d = createNode("D");
  auto e = createNode("E");
  auto f = createNode("F");
  auto g = createNode("G");
  auto h = createNode("H");

  m_graph->addNode(a);
  m_graph->addNode(b);
  m_graph->addNode(c);
  m_graph->addNode(d);
  m_graph->addNode(e);
  m_graph->addNode(f);
  m_graph->addNode(g);
  m_graph->addNode(h);

  m_graph->addEdge("A", "out", "B", "in");
  m_graph->addEdge("A", "out", "C", "in");
  m_graph->addEdge("B", "out", "E", "in");
  m_graph->addEdge("C", "out", "E", "in");
  m_graph->addEdge("F", "out", "D", "in");
  m_graph->addEdge("F", "out", "H", "in");
  m_graph->addEdge("D", "out", "H", "in");
  m_graph->addEdge("E", "out", "G", "in");
  m_graph->addEdge("H", "out", "G", "in");

  EXPECT_FALSE(m_graph->hasCycle());
  EXPECT_EQ(m_graph->getInDegree(g), 2);
  EXPECT_EQ(m_graph->getInDegree(e), 2);
  EXPECT_EQ(m_graph->getOutDegree(a), 2);
  EXPECT_EQ(m_graph->getOutDegree(f), 2);
}

TEST_F(GraphTest, LargeChain) {
  const int chain_len = 100;
  std::vector<std::shared_ptr<MockNode>> nodes;
  for (int i = 0; i < chain_len; ++i) {
    auto node = createNode("node" + std::to_string(i));
    nodes.push_back(node);
    m_graph->addNode(node);
  }

  for (int i = 0; i < chain_len - 1; ++i) {
    m_graph->addEdge("node" + std::to_string(i), "out",
                     "node" + std::to_string(i + 1), "in");
  }

  EXPECT_EQ(m_graph->getNodes().size(), chain_len);
  EXPECT_EQ(m_graph->getEdges().size(), chain_len - 1);
  EXPECT_FALSE(m_graph->hasCycle());
}

TEST_F(GraphTest, MultipleSourcesAndSinks) {
  auto s1 = createNode("S1");
  auto s2 = createNode("S2");
  auto m = createNode("M");
  auto k1 = createNode("K1");
  auto k2 = createNode("K2");

  m_graph->addNode(s1);
  m_graph->addNode(s2);
  m_graph->addNode(m);
  m_graph->addNode(k1);
  m_graph->addNode(k2);

  m_graph->addEdge("S1", "out", "M", "in1");
  m_graph->addEdge("S2", "out", "M", "in2");
  m_graph->addEdge("M", "out", "K1", "in");
  m_graph->addEdge("M", "out", "K2", "in");

  EXPECT_EQ(m_graph->getInDegree(s1), 0);
  EXPECT_EQ(m_graph->getInDegree(s2), 0);
  EXPECT_EQ(m_graph->getOutDegree(k1), 0);
  EXPECT_EQ(m_graph->getOutDegree(k2), 0);
  EXPECT_EQ(m_graph->getInDegree(m), 2);
  EXPECT_EQ(m_graph->getOutDegree(m), 2);
}

TEST_F(GraphTest, AddNullNodeFails) { EXPECT_FALSE(m_graph->addNode(nullptr)); }

// Port Payload Type Validation Tests

namespace {

// Node declaring concrete payload types on its ports
template <typename OutT, typename InT> class TypedNode : public ILogicNode {
public:
  explicit TypedNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {}

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  std::type_index portPayloadType(const std::string &port) const override {
    if (port == "output") {
      return typeid(OutT);
    }
    if (port == "input") {
      return typeid(InT);
    }
    return typeid(void);
  }
};

} // namespace

class GraphPortTypeTest : public ::testing::Test {
protected:
  Graph m_graph;
};

TEST_F(GraphPortTypeTest, MatchingTypesConnect) {
  m_graph.addNode(std::make_shared<TypedNode<int, void>>("producer"));
  m_graph.addNode(std::make_shared<TypedNode<void, int>>("consumer"));

  EXPECT_TRUE(m_graph.addEdge("producer", "output", "consumer", "input"));
}

TEST_F(GraphPortTypeTest, MismatchedTypesRejected) {
  m_graph.addNode(std::make_shared<TypedNode<int, void>>("producer"));
  m_graph.addNode(std::make_shared<TypedNode<void, double>>("consumer"));

  EXPECT_FALSE(m_graph.addEdge("producer", "output", "consumer", "input"));
  EXPECT_TRUE(m_graph.getEdges().empty());
}

TEST_F(GraphPortTypeTest, UntypedEndpointAlwaysConnects) {
  // producer declares int output; consumer leaves input untyped (void)
  m_graph.addNode(std::make_shared<TypedNode<int, void>>("producer"));
  m_graph.addNode(std::make_shared<TypedNode<void, void>>("consumer"));

  EXPECT_TRUE(m_graph.addEdge("producer", "output", "consumer", "input"));
}

TEST_F(GraphPortTypeTest, NullNodeDegreeQueriesDoNotThrow) {
  // Graph no longer throws; null queries degrade to 0 with a log.
  EXPECT_NO_THROW({
    EXPECT_EQ(m_graph.getInDegree(nullptr), 0);
    EXPECT_EQ(m_graph.getOutDegree(nullptr), 0);
  });
}

TEST_F(GraphPortTypeTest, LegacyNodesUnaffected) {
  // MockNode does not override portPayloadType -> fully untyped
  m_graph.addNode(std::make_shared<MockNode>("a"));
  m_graph.addNode(std::make_shared<MockNode>("b"));

  EXPECT_TRUE(m_graph.addEdge("a", "out", "b", "in"));
}
