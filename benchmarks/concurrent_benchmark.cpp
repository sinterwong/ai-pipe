#include "benchmark_utils.hpp"
#include <atomic>
#include <benchmark/benchmark.h>
#include <latch>
#include <thread>

namespace ai_pipe::benchmark {

// Thread Pool Overhead

// Measure thread pool task dispatch overhead
static void BM_Concurrent_ThreadPoolOverhead(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t tasks = 1000;

  auto topology = buildLinearPipeline(1);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    for (std::size_t i = 0; i < tasks; ++i) {
      auto input = createSourceInput(i, 64);
      engine->execute(input, true);
      engine->reset();
    }
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(tasks));
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["tasks"] = static_cast<double>(tasks);
}

BENCHMARK(BM_Concurrent_ThreadPoolOverhead)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond);

// Parallel Branch Execution

// Measure parallel branch execution efficiency
static void BM_Concurrent_ParallelBranches(::benchmark::State &state) {
  const auto branches = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 16;
  const auto branch_delay = std::chrono::milliseconds{10};

  auto topology = buildDiamondPipeline(
      branches,
      std::chrono::duration_cast<std::chrono::microseconds>(branch_delay));
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto start = std::chrono::steady_clock::now();
    auto input = createSourceInput(0, 256);
    engine->execute(input, true);
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Calculate speedup
    auto serial_time = branches * branch_delay;
    auto actual_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
    auto speedup = static_cast<double>(serial_time.count()) /
                   static_cast<double>(actual_time.count());

    state.counters["speedup"] = speedup;
    state.counters["efficiency"] =
        speedup / static_cast<double>(std::min(branches, workers));

    engine->reset();
  }

  state.counters["branches"] = static_cast<double>(branches);
  state.counters["workers"] = static_cast<double>(workers);
}

BENCHMARK(BM_Concurrent_ParallelBranches)
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Args({32})
    ->Unit(::benchmark::kMillisecond);

// Worker Utilization

// Measure worker utilization with compute-bound workload
static void BM_Concurrent_WorkerUtilization_Compute(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t parallel_tasks = workers * 2;
  const std::size_t iterations = 100000;

  auto topology = buildComputePipeline(parallel_tasks, iterations);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto start = std::chrono::steady_clock::now();
    auto input = createSourceInput(0, 256);
    engine->execute(input, true);
    auto elapsed = std::chrono::steady_clock::now() - start;

    // Estimate utilization
    auto stats = engine->statistics();
    auto processing_time =
        std::chrono::microseconds{stats.total_processing_time_us};
    auto wall_time =
        std::chrono::duration_cast<std::chrono::microseconds>(elapsed);

    // Utilization = (total_processing_time) / (wall_time * workers)
    auto utilization = static_cast<double>(processing_time.count()) /
                       (static_cast<double>(wall_time.count()) * workers);

    state.counters["utilization"] = std::min(utilization, 1.0);

    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(parallel_tasks));
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["parallel_tasks"] = static_cast<double>(parallel_tasks);
}

BENCHMARK(BM_Concurrent_WorkerUtilization_Compute)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond);

// Measure worker utilization with IO-bound workload
static void BM_Concurrent_WorkerUtilization_IO(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t parallel_tasks = workers * 4;
  const auto delay = std::chrono::microseconds{500};

  auto topology = buildLinearPipeline(parallel_tasks, delay);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 256);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(parallel_tasks));
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["parallel_tasks"] = static_cast<double>(parallel_tasks);
  state.counters["delay_us"] = static_cast<double>(delay.count());
}

BENCHMARK(BM_Concurrent_WorkerUtilization_IO)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond);

// Queue Contention

// Measure queue contention with multiple producers
static void BM_Concurrent_QueueContention(::benchmark::State &state) {
  const auto producer_count = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 8;
  const std::size_t queue_capacity = 32;
  const std::size_t pushes_per_producer = 1000;

  auto topology = buildLinearPipeline(4);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    engine->startStreaming();

    std::vector<std::thread> producers;
    std::atomic<std::size_t> successful_pushes{0};
    std::atomic<std::size_t> failed_pushes{0};

    auto start = std::chrono::steady_clock::now();

    for (std::size_t p = 0; p < producer_count; ++p) {
      producers.emplace_back([&, p]() {
        for (std::size_t i = 0; i < pushes_per_producer; ++i) {
          auto packet = std::make_shared<PortData>();
          packet->id = p * pushes_per_producer + i;

          auto result =
              engine->pushInput(topology.source_node, "output", packet);
          if (result.isOk()) {
            successful_pushes.fetch_add(1, std::memory_order_relaxed);
          } else {
            failed_pushes.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
    }

    for (auto &t : producers) {
      t.join();
    }

    auto push_time = std::chrono::steady_clock::now() - start;

    engine->waitForDrain(0, std::chrono::milliseconds{10000});
    engine->stopStreaming(true);

    auto push_rate = static_cast<double>(successful_pushes.load()) /
                     std::chrono::duration<double>(push_time).count();

    state.counters["push_rate"] = push_rate;
    state.counters["success_rate"] =
        100.0 * static_cast<double>(successful_pushes.load()) /
        static_cast<double>(producer_count * pushes_per_producer);

    engine->reset();
  }

  state.counters["producers"] = static_cast<double>(producer_count);
}

BENCHMARK(BM_Concurrent_QueueContention)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Unit(::benchmark::kMillisecond);

// Scaling Analysis

// Strong scaling: fixed problem size, varying workers
static void BM_Concurrent_StrongScaling(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t total_work = 32; // Fixed problem size
  const std::size_t iterations = 30000;

  auto topology = buildComputePipeline(total_work, iterations);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  std::chrono::microseconds baseline_time{0};

  for (auto _ : state) {
    auto start = std::chrono::steady_clock::now();
    auto input = createSourceInput(0, 256);
    engine->execute(input, true);
    auto elapsed = std::chrono::steady_clock::now() - start;

    if (workers == 1) {
      baseline_time =
          std::chrono::duration_cast<std::chrono::microseconds>(elapsed);
    }

    engine->reset();
  }

  state.counters["workers"] = static_cast<double>(workers);
  state.counters["total_work"] = static_cast<double>(total_work);
}

BENCHMARK(BM_Concurrent_StrongScaling)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond);

// Weak scaling: problem size scales with workers
static void BM_Concurrent_WeakScaling(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t work_per_worker = 4;
  const std::size_t total_work = workers * work_per_worker;
  const std::size_t iterations = 30000;

  auto topology = buildComputePipeline(total_work, iterations);
  auto engine = createBenchmarkBatchEngine(workers);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 256);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(total_work));
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["total_work"] = static_cast<double>(total_work);
  state.counters["work_per_worker"] = static_cast<double>(work_per_worker);
}

BENCHMARK(BM_Concurrent_WeakScaling)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond);

// Mixed Workload

// Measure performance with mixed compute/IO workload
static void BM_Concurrent_MixedWorkload(::benchmark::State &state) {
  const auto compute_ratio = state.range(0) / 100.0; // 0-100%
  const std::size_t workers = 8;
  const std::size_t depth = 8;

  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source", 1024);
  result.nodes.push_back(source);
  result.graph->addNode(source);
  result.source_node = "source";

  std::string prev = "source";
  std::size_t compute_nodes = static_cast<std::size_t>(depth * compute_ratio);
  std::size_t io_nodes = depth - compute_nodes;

  for (std::size_t i = 0; i < compute_nodes; ++i) {
    std::string name = "compute_" + std::to_string(i);
    auto node = std::make_shared<ComputeNode>(name, 20000);
    result.nodes.push_back(node);
    result.graph->addNode(node);
    result.graph->addEdge(prev, "output", name, "input");
    prev = name;
  }

  for (std::size_t i = 0; i < io_nodes; ++i) {
    std::string name = "delay_" + std::to_string(i);
    auto node =
        std::make_shared<DelayNode>(name, std::chrono::microseconds{200});
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
    auto input = createSourceInput(0, 512);
    engine->execute(input, true);
    engine->reset();
  }

  state.counters["compute_ratio"] = compute_ratio * 100.0;
  state.counters["compute_nodes"] = static_cast<double>(compute_nodes);
  state.counters["io_nodes"] = static_cast<double>(io_nodes);
}

BENCHMARK(BM_Concurrent_MixedWorkload)
    ->Args({0})   // 0% compute (all IO)
    ->Args({25})  // 25% compute
    ->Args({50})  // 50% compute
    ->Args({75})  // 75% compute
    ->Args({100}) // 100% compute (no IO)
    ->Unit(::benchmark::kMillisecond);

// Context Switch Overhead

// Measure context switch overhead with many small tasks
static void BM_Concurrent_ContextSwitchOverhead(::benchmark::State &state) {
  const auto task_count = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;

  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source", 64);
  result.nodes.push_back(source);
  result.graph->addNode(source);

  std::string prev = "source";
  for (std::size_t i = 0; i < task_count; ++i) {
    std::string name = "pass_" + std::to_string(i);
    auto node = std::make_shared<PassthroughNode>(name);
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
    auto input = createSourceInput(0, 64);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(task_count));
  state.counters["tasks"] = static_cast<double>(task_count);
  state.counters["workers"] = static_cast<double>(workers);
}

BENCHMARK(BM_Concurrent_ContextSwitchOverhead)
    ->Args({10})
    ->Args({50})
    ->Args({100})
    ->Args({500})
    ->Args({1000})
    ->Unit(::benchmark::kMicrosecond);

// Maximum Throughput (Streaming)

// Find maximum sustainable throughput in streaming mode
static void BM_Concurrent_StreamMaxThroughput(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t depth = 4;
  const std::size_t queue_capacity = 64;
  const std::size_t test_frames = 5000;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    engine->startStreaming();

    auto start = std::chrono::steady_clock::now();

    for (std::size_t i = 0; i < test_frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      (void)engine->pushInput(topology.source_node, "output", packet);
    }

    auto push_time = std::chrono::steady_clock::now() - start;

    engine->waitForDrain(0, std::chrono::milliseconds{10000});
    auto total_time = std::chrono::steady_clock::now() - start;

    engine->stopStreaming(true);

    auto stats = engine->statistics();

    auto push_rate = static_cast<double>(test_frames) /
                     std::chrono::duration<double>(push_time).count();
    auto process_rate = static_cast<double>(stats.total_output_frames) /
                        std::chrono::duration<double>(total_time).count();

    state.counters["push_rate_fps"] = push_rate;
    state.counters["process_rate_fps"] = process_rate;
    state.counters["processed"] =
        static_cast<double>(stats.total_output_frames);

    engine->reset();
  }

  state.counters["workers"] = static_cast<double>(workers);
}

BENCHMARK(BM_Concurrent_StreamMaxThroughput)
    ->Apply(WorkerCountArgs)
    ->Unit(::benchmark::kMillisecond)
    ->MinTime(2.0);

} // namespace ai_pipe::benchmark
