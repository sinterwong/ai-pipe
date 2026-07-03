/**
 * @file graph_benchmark.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Micro-benchmarks for graph topology queries and routing (P1.5)
 *
 * Quantifies the Phase 1 refactor: the engine previously resolved a node's
 * downstream connections with Graph::getOutgoingEdges (a full O(E) edge-list
 * scan per node completion); it now uses CompiledGraph's precompiled
 * per-node OutEdge table. Both mechanisms are benchmarked side by side on
 * the same topology, along with graph construction and compilation cost.
 *
 * @copyright Copyright (c) 2026
 */

#include "benchmark_nodes.hpp"
#include "compiled_graph.hpp"
#include <benchmark/benchmark.h>

using namespace ai_pipe;
using namespace ai_pipe::benchmark;

namespace {

/**
 * @brief Build a linear chain: n0 -> n1 -> ... -> n{count-1}
 */
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

// =============================================================================
// Downstream resolution: legacy O(E) scan vs precompiled routing table
// =============================================================================

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

// =============================================================================
// Construction and compilation cost
// =============================================================================

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

// =============================================================================
// Cycle detection (iterative Kahn since P1.2)
// =============================================================================

static void BM_Graph_HasCycle(::benchmark::State &state) {
  const int node_count = static_cast<int>(state.range(0));
  Graph graph = buildChainGraph(node_count);

  for (auto _ : state) {
    ::benchmark::DoNotOptimize(graph.hasCycle());
  }
}
BENCHMARK(BM_Graph_HasCycle)->Arg(128)->Arg(1024)->Arg(8192);
