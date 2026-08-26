#include "benchmark_utils.hpp"
#include <benchmark/benchmark.h>

namespace ai_pipe::benchmark {

// Framework Overhead Benchmarks

// Measure pure framework overhead with passthrough nodes
static void BM_Batch_FrameworkOverhead(::benchmark::State &state) {
  const auto depth = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 64);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations());
  state.counters["depth"] = static_cast<double>(depth);
  state.counters["workers"] = static_cast<double>(workers);
}

BENCHMARK(BM_Batch_FrameworkOverhead)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Args({32})
    ->Unit(::benchmark::kMicrosecond);

// Worker Scaling Benchmarks

// Measure worker scaling efficiency with compute workload
static void BM_Batch_WorkerScaling_Compute(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t depth = 8;
  const std::size_t iterations = 50000;

  auto topology = buildComputePipeline(depth, iterations);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 256);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() * depth);
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["depth"] = static_cast<double>(depth);
  state.counters["iterations_per_node"] = static_cast<double>(iterations);
}

BENCHMARK(BM_Batch_WorkerScaling_Compute)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond)
    ->MinTime(2.0);

// Measure worker scaling with IO-bound workload
static void BM_Batch_WorkerScaling_IO(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t depth = 8;
  const auto delay = std::chrono::microseconds{100};

  auto topology = buildLinearPipeline(depth, delay);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 256);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() * depth);
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["delay_us"] = static_cast<double>(delay.count());
}

BENCHMARK(BM_Batch_WorkerScaling_IO)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond);

// Pipeline Depth Benchmarks

// Measure impact of pipeline depth
static void BM_Batch_DepthScaling(::benchmark::State &state) {
  const auto depth = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 1024);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() * depth);
  state.counters["depth"] = static_cast<double>(depth);
}

BENCHMARK(BM_Batch_DepthScaling)
    ->Apply(PipelineDepthArgs)
    ->Unit(::benchmark::kMicrosecond);

// Payload Size Benchmarks

// Measure impact of payload size on throughput
static void BM_Batch_PayloadSize(::benchmark::State &state) {
  const auto payload_size = static_cast<std::size_t>(state.range(0));
  const std::size_t depth = 4;
  const std::size_t workers = 4;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, payload_size);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations());
  state.SetBytesProcessed(state.iterations() *
                          static_cast<std::int64_t>(payload_size));
  state.counters["payload_bytes"] = static_cast<double>(payload_size);
}

BENCHMARK(BM_Batch_PayloadSize)
    ->Apply(PayloadSizeArgs)
    ->Unit(::benchmark::kMicrosecond);

// Fork-Join Benchmarks

// Measure fork-join performance with varying branch count
static void BM_Batch_Diamond_BranchCount(::benchmark::State &state) {
  const auto branches = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 8;
  const auto branch_delay = std::chrono::microseconds{50};

  auto topology = buildDiamondPipeline(branches, branch_delay);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 1024);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() * branches);
  state.counters["branches"] = static_cast<double>(branches);
  state.counters["workers"] = static_cast<double>(workers);

  // Calculate parallelization efficiency
  // Ideal time = branch_delay (all parallel) vs actual
  auto theoretical_parallel_time = branch_delay.count();
  state.counters["theoretical_parallel_us"] =
      static_cast<double>(theoretical_parallel_time);
}

BENCHMARK(BM_Batch_Diamond_BranchCount)
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Unit(::benchmark::kMicrosecond);

// Measure multi-stage fork-join performance
static void BM_Batch_MultiStage(::benchmark::State &state) {
  const auto stages = static_cast<std::size_t>(state.range(0));
  const std::size_t branches = 4;
  const std::size_t workers = 8;
  const auto branch_delay = std::chrono::microseconds{30};

  auto topology = buildMultiStagePipeline(stages, branches, branch_delay);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 1024);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() * stages * branches);
  state.counters["stages"] = static_cast<double>(stages);
  state.counters["branches_per_stage"] = static_cast<double>(branches);
}

BENCHMARK(BM_Batch_MultiStage)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Unit(::benchmark::kMicrosecond);

// Combined Parameter Benchmarks

// Measure combined effect of workers and depth
static void BM_Batch_WorkerDepth(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const auto depth = static_cast<std::size_t>(state.range(1));
  const auto delay = std::chrono::microseconds{20};

  auto topology = buildLinearPipeline(depth, delay);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 512);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() * depth);
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["depth"] = static_cast<double>(depth);
}

BENCHMARK(BM_Batch_WorkerDepth)
    ->Apply(WorkerDepthArgs)
    ->Unit(::benchmark::kMicrosecond);

// Memory Bandwidth Benchmarks

// Measure memory-intensive workload performance
static void BM_Batch_MemoryBandwidth(::benchmark::State &state) {
  const auto buffer_size = static_cast<std::size_t>(state.range(0));
  const std::size_t copy_count = 10;
  const std::size_t workers = 4;
  const std::size_t depth = 4;

  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source", 1024);
  result.nodes.push_back(source);
  result.graph->addNode(source);

  std::string prev = "source";
  for (std::size_t i = 0; i < depth; ++i) {
    std::string name = "mem_" + std::to_string(i);
    auto node = std::make_shared<MemoryNode>(name, buffer_size, copy_count);
    result.nodes.push_back(node);
    result.graph->addNode(node);
    result.graph->addEdge(prev, "output", name, "input");
    prev = name;
  }

  auto sink = std::make_shared<SinkNode>("sink");
  result.nodes.push_back(sink);
  result.graph->addNode(sink);
  result.graph->addEdge(prev, "output", "sink", "input");

  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(result.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 1024);
    engine->execute(input, true);
    engine->reset();
  }

  // Bytes processed = buffer_size * copy_count * 2 (read+write) * depth *
  // iterations
  auto bytes = buffer_size * copy_count * 2 * depth;
  state.SetBytesProcessed(state.iterations() *
                          static_cast<std::int64_t>(bytes));
  state.counters["buffer_size_kb"] = static_cast<double>(buffer_size) / 1024.0;
}

BENCHMARK(BM_Batch_MemoryBandwidth)
    ->Args({64 * 1024})       // 64KB
    ->Args({256 * 1024})      // 256KB
    ->Args({1024 * 1024})     // 1MB
    ->Args({4 * 1024 * 1024}) // 4MB
    ->Unit(::benchmark::kMillisecond);

// Repeated Execution Benchmarks

// Measure amortized overhead over many executions
static void BM_Batch_RepeatedExecution(::benchmark::State &state) {
  const auto executions = static_cast<std::size_t>(state.range(0));
  const std::size_t depth = 4;
  const std::size_t workers = 4;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    for (std::size_t i = 0; i < executions; ++i) {
      auto input = createSourceInput(i, 256);
      engine->execute(input, true);
      engine->reset();
    }
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(executions));
  state.counters["executions_per_iter"] = static_cast<double>(executions);
}

BENCHMARK(BM_Batch_RepeatedExecution)
    ->Args({10})
    ->Args({50})
    ->Args({100})
    ->Args({500})
    ->Unit(::benchmark::kMillisecond);

} // namespace ai_pipe::benchmark
