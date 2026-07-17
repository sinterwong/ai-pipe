/**
 * @file portdatamap_benchmark.cpp
 * @brief F9 profiling: PortDataMap cost on the per-execution hot path
 *
 * TODO item F9 proposes replacing the PortDataMap
 * (std::map<std::string, PortDataPtr>, built once per node execution)
 * with indexed port arrays - a breaking process() API change that the
 * roadmap gates on profiling evidence. These benchmarks produce that
 * evidence:
 *
 * 1. BM_PortDataMap_PerExecution: the exact container work one node
 *    execution performs today (construct inputs map, construct outputs
 *    map, per-edge find() during propagation, destroy both).
 * 2. BM_IndexedPorts_PerExecution: the same data flow through the
 *    proposed replacement (pre-sized vector indexed by port position).
 *    The delta to (1) is the maximum possible saving per execution.
 * 3. BM_Stream_PerNodeExecutionOverhead: total framework cost per node
 *    execution in a streaming passthrough pipeline (scheduling, queues,
 *    alignment, statistics, and the maps) - the denominator that puts
 *    the delta in context.
 *
 * Verdict recorded in docs/Performance_Report.md section 6 and TODO F9.
 */

#include "benchmark_utils.hpp"
#include <benchmark/benchmark.h>
#include <map>
#include <vector>

namespace ai_pipe::benchmark {

// =============================================================================
// 1. Current container: std::map keyed by port name
// =============================================================================

static void BM_PortDataMap_PerExecution(::benchmark::State &state) {
  const auto port_count = static_cast<std::size_t>(state.range(0));

  // Port names live in NodeState (input_ports) / CompiledGraph (edge
  // source ports) - already materialized, so key construction is not
  // part of the per-execution cost, but the map's internal key copy is.
  std::vector<std::string> input_ports;
  for (std::size_t i = 0; i < port_count; ++i) {
    input_ports.push_back(port_count == 1 ? "input"
                                          : "input" + std::to_string(i + 1));
  }
  const std::string output_port = "output";
  auto packet = std::make_shared<PortData>();

  for (auto _ : state) {
    // executeNodeTask: fresh maps per execution
    PortDataMap inputs;
    PortDataMap outputs;
    // gather: one insertion per input port
    for (const auto &port : input_ports) {
      inputs[port] = packet;
    }
    // node process(): reads inputs by key, writes one output
    for (const auto &port : input_ports) {
      ::benchmark::DoNotOptimize(inputs.find(port));
    }
    outputs[output_port] = packet;
    // propagateOutputs: find() per out-edge
    ::benchmark::DoNotOptimize(outputs.find(output_port));
    ::benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["ports"] = static_cast<double>(port_count);
}

BENCHMARK(BM_PortDataMap_PerExecution)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Unit(::benchmark::kNanosecond);

// =============================================================================
// 2. Proposed replacement: index-addressed port arrays
// =============================================================================

static void BM_IndexedPorts_PerExecution(::benchmark::State &state) {
  const auto port_count = static_cast<std::size_t>(state.range(0));
  auto packet = std::make_shared<PortData>();

  for (auto _ : state) {
    // Port indices are compile-graph facts; the per-execution work is
    // just the array lifecycle and indexed stores/loads.
    std::vector<PortDataPtr> inputs(port_count);
    std::vector<PortDataPtr> outputs(1);
    for (std::size_t i = 0; i < port_count; ++i) {
      inputs[i] = packet;
    }
    for (std::size_t i = 0; i < port_count; ++i) {
      ::benchmark::DoNotOptimize(inputs[i].get());
    }
    outputs[0] = packet;
    ::benchmark::DoNotOptimize(outputs[0].get());
    ::benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["ports"] = static_cast<double>(port_count);
}

BENCHMARK(BM_IndexedPorts_PerExecution)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Unit(::benchmark::kNanosecond);

// =============================================================================
// 3. Context: total framework overhead per node execution
// =============================================================================

static void BM_Stream_PerNodeExecutionOverhead(::benchmark::State &state) {
  const std::uint8_t workers = 4;
  const auto depth = static_cast<std::size_t>(state.range(0));
  const std::size_t frames_per_iter = 1000;
  // source + depth passthroughs + sink all execute once per frame
  const std::size_t nodes_per_frame = depth + 2;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, 32, true);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    engine->startStreaming();
    for (std::size_t i = 0; i < frames_per_iter; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i + 1;
      (void)engine->pushInput(topology.source_node, "output", packet);
    }
    engine->waitForDrain(0, std::chrono::milliseconds{10000});
    engine->stopStreaming(true);
    engine->reset();
  }

  // items = node executions, so per-item time is the framework cost
  // one PortDataMap pair currently rides on.
  state.SetItemsProcessed(
      state.iterations() *
      static_cast<std::int64_t>(frames_per_iter * nodes_per_frame));
  state.counters["depth"] = static_cast<double>(depth);
  state.counters["nodes_per_frame"] = static_cast<double>(nodes_per_frame);
}

BENCHMARK(BM_Stream_PerNodeExecutionOverhead)
    ->Args({4})
    ->Args({8})
    ->Unit(::benchmark::kMillisecond)
    ->MinTime(3.0);

} // namespace ai_pipe::benchmark
