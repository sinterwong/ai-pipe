#include "ai_pipe/compiled_graph.hpp"
#include "benchmark_nodes.hpp"
#include <benchmark/benchmark.h>

using namespace ai_pipe;
using namespace ai_pipe::benchmark;

namespace {

// Build a linear chain: n0 -> n1 -> ... -> n{count-1}
Graph buildChainGraph(int node_count) {
  Graph graph;
  for (int i = 0; i < node_count; ++i) {
    graph.addNode(
        std::make_shared<PassthroughNode>("node" + std::to_string(i)));
  }
  for (int i = 0; i + 1 < node_count; ++i) {
    graph.addEdge("node" + std::to_string(i), "output",
                  "node" + std::to_string(i + 1), "input");
  }
  return graph;
}

} // namespace

// Downstream resolution: O(E) scan baseline versus precompiled routing table.

static void BM_Graph_GetOutgoingEdges_Scan(::benchmark::State &state) {
  const int node_count = static_cast<int>(state.range(0));
  Graph graph = buildChainGraph(node_count);
  // Query a node in the middle so the scan cannot shortcut
  auto probe = graph.getNode("node" + std::to_string(node_count / 2));

  for (auto _ : state) {
    auto edges = graph.getOutgoingEdges(probe);
    ::benchmark::DoNotOptimize(edges);
  }
  state.SetLabel(std::to_string(node_count - 1) + " edges scanned");
}
BENCHMARK(BM_Graph_GetOutgoingEdges_Scan)->Arg(16)->Arg(128)->Arg(1024);

static void BM_CompiledGraph_OutEdges(::benchmark::State &state) {
  const int node_count = static_cast<int>(state.range(0));
  Graph graph = buildChainGraph(node_count);
  auto compiled = CompiledGraph::compile(graph);
  const auto &cg = compiled.value();
  auto probe = graph.getNode("node" + std::to_string(node_count / 2));

  for (auto _ : state) {
    const auto index = cg.indexOfPtr(probe.get());
    const auto &edges = cg.outEdges(index);
    ::benchmark::DoNotOptimize(edges.data());
  }
  state.SetLabel("O(1) table lookup");
}
BENCHMARK(BM_CompiledGraph_OutEdges)->Arg(16)->Arg(128)->Arg(1024);

// Construction and compilation cost

static void BM_Graph_Construction_Chain(::benchmark::State &state) {
  const int node_count = static_cast<int>(state.range(0));

  for (auto _ : state) {
    Graph graph = buildChainGraph(node_count);
    ::benchmark::DoNotOptimize(graph.getEdges().size());
  }
  state.SetItemsProcessed(state.iterations() * node_count);
}
BENCHMARK(BM_Graph_Construction_Chain)->Arg(128)->Arg(1024)->Arg(8192);

static void BM_CompiledGraph_Compile(::benchmark::State &state) {
  const int node_count = static_cast<int>(state.range(0));
  Graph graph = buildChainGraph(node_count);

  for (auto _ : state) {
    auto compiled = CompiledGraph::compile(graph);
    ::benchmark::DoNotOptimize(compiled.isOk());
  }
  state.SetItemsProcessed(state.iterations() * node_count);
}
BENCHMARK(BM_CompiledGraph_Compile)->Arg(128)->Arg(1024)->Arg(8192);

// Cycle detection with iterative Kahn traversal.

static void BM_Graph_HasCycle(::benchmark::State &state) {
  const int node_count = static_cast<int>(state.range(0));
  Graph graph = buildChainGraph(node_count);

  for (auto _ : state) {
    ::benchmark::DoNotOptimize(graph.hasCycle());
  }
}
BENCHMARK(BM_Graph_HasCycle)->Arg(128)->Arg(1024)->Arg(8192);
