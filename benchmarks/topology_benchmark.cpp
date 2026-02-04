/**
 * @file topology_benchmark.cpp
 * @brief Pipeline topology benchmarks
 *
 * Tests various pipeline topologies: linear, diamond, multi-stage fork-join,
 * funnel, parallel pipelines, mesh, and tree structures.
 */

#include "benchmark_nodes.hpp"
#include "benchmark_utils.hpp"
#include "scheduler_strategies.hpp"
#include <benchmark/benchmark.h>

using namespace ai_pipe;
using namespace ai_pipe::benchmark;

// =============================================================================
// Linear Topology Benchmarks
// =============================================================================

/**
 * @brief Benchmark linear pipeline scaling with depth
 */
static void BM_Topology_Linear_Depth(::benchmark::State &state) {
  const int depth = static_cast<int>(state.range(0));
  const std::uint8_t num_workers = 4;

  auto topology = buildLinearPipeline(depth);

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->initialize(topology.graph.get(), num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));
  input_packet->setParam("timestamp", std::chrono::steady_clock::now());

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["depth"] = depth;
  state.counters["workers"] = static_cast<double>(num_workers);
}

BENCHMARK(BM_Topology_Linear_Depth)
    ->RangeMultiplier(2)
    ->Range(1, 128)
    ->Unit(::benchmark::kMicrosecond);

/**
 * @brief Benchmark linear pipeline with compute-intensive nodes
 */
static void BM_Topology_Linear_Compute(::benchmark::State &state) {
  const int depth = static_cast<int>(state.range(0));
  const std::size_t iterations = 1000;
  const std::uint8_t num_workers = 4;

  auto topology = buildComputePipeline(depth, iterations);

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->initialize(topology.graph.get(), num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["depth"] = depth;
  state.counters["iterations_per_node"] = static_cast<double>(iterations);
}

BENCHMARK(BM_Topology_Linear_Compute)
    ->RangeMultiplier(2)
    ->Range(1, 32)
    ->Unit(::benchmark::kMicrosecond);

// =============================================================================
// Diamond (Fork-Join) Topology Benchmarks
// =============================================================================

/**
 * @brief Benchmark diamond topology scaling with branch count
 */
static void BM_Topology_Diamond_Branches(::benchmark::State &state) {
  const int num_branches = static_cast<int>(state.range(0));
  const std::uint8_t num_workers = 8;

  auto topology =
      buildDiamondPipeline(num_branches, std::chrono::microseconds{0});

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(topology.graph.get(), num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["branches"] = num_branches;
  state.counters["workers"] = static_cast<double>(num_workers);
  state.counters["parallelism"] =
      std::min(num_branches, static_cast<int>(num_workers));
}

BENCHMARK(BM_Topology_Diamond_Branches)
    ->RangeMultiplier(2)
    ->Range(2, 64)
    ->Unit(::benchmark::kMicrosecond);

/**
 * @brief Benchmark diamond topology with varying branch delays
 */
static void BM_Topology_Diamond_Delay(::benchmark::State &state) {
  const int delay_us = static_cast<int>(state.range(0));
  const int num_branches = 4;
  const std::uint8_t num_workers = 8;

  auto topology =
      buildDiamondPipeline(num_branches, std::chrono::microseconds{delay_us});

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(topology.graph.get(), num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["branch_delay_us"] = delay_us;
  state.counters["branches"] = num_branches;
}

BENCHMARK(BM_Topology_Diamond_Delay)
    ->RangeMultiplier(10)
    ->Range(10, 10000)
    ->Unit(::benchmark::kMicrosecond);

/**
 * @brief Benchmark diamond with unbalanced branch delays
 */
static void BM_Topology_Diamond_Unbalanced(::benchmark::State &state) {
  const int slow_delay_us = static_cast<int>(state.range(0));
  const int fast_delay_us = 10;
  const std::uint8_t num_workers = 4;

  Graph graph;

  auto source = std::make_shared<SourceNode>("source", 1024);
  auto fast1 = std::make_shared<DelayNode>(
      "fast1", std::chrono::microseconds(fast_delay_us));
  auto fast2 = std::make_shared<DelayNode>(
      "fast2", std::chrono::microseconds(fast_delay_us));
  auto slow1 = std::make_shared<DelayNode>(
      "slow1", std::chrono::microseconds(slow_delay_us));
  auto slow2 = std::make_shared<DelayNode>(
      "slow2", std::chrono::microseconds(slow_delay_us));
  auto aggregator = std::make_shared<AggregatorNode>("aggregator", 4);
  auto sink = std::make_shared<SinkNode>("sink");

  graph.addNode(source);
  graph.addNode(fast1);
  graph.addNode(fast2);
  graph.addNode(slow1);
  graph.addNode(slow2);
  graph.addNode(aggregator);
  graph.addNode(sink);

  graph.addEdge("source", "output", "fast1", "input");
  graph.addEdge("source", "output", "fast2", "input");
  graph.addEdge("source", "output", "slow1", "input");
  graph.addEdge("source", "output", "slow2", "input");
  graph.addEdge("fast1", "output", "aggregator", "input_0");
  graph.addEdge("fast2", "output", "aggregator", "input_1");
  graph.addEdge("slow1", "output", "aggregator", "input_2");
  graph.addEdge("slow2", "output", "aggregator", "input_3");
  graph.addEdge("aggregator", "output", "sink", "input");

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(&graph, num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["slow_delay_us"] = slow_delay_us;
  state.counters["fast_delay_us"] = fast_delay_us;
  state.counters["imbalance_ratio"] =
      static_cast<double>(slow_delay_us) / fast_delay_us;
}

BENCHMARK(BM_Topology_Diamond_Unbalanced)
    ->Args({100})
    ->Args({500})
    ->Args({1000})
    ->Unit(::benchmark::kMicrosecond);

// =============================================================================
// Multi-Stage Fork-Join Benchmarks
// =============================================================================

/**
 * @brief Benchmark multi-stage cascaded fork-join topology
 */
static void BM_Topology_MultiStage(::benchmark::State &state) {
  const int stages = static_cast<int>(state.range(0));
  const int branches_per_stage = static_cast<int>(state.range(1));
  const std::uint8_t num_workers = 8;

  auto topology = buildMultiStagePipeline(stages, branches_per_stage,
                                          std::chrono::microseconds{10});

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(topology.graph.get(), num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["stages"] = stages;
  state.counters["branches_per_stage"] = branches_per_stage;
  state.counters["total_branches"] = stages * branches_per_stage;
}

BENCHMARK(BM_Topology_MultiStage)
    ->Args({1, 4})
    ->Args({2, 4})
    ->Args({4, 4})
    ->Args({8, 4})
    ->Args({4, 2})
    ->Args({4, 8})
    ->Unit(::benchmark::kMicrosecond);

// =============================================================================
// Funnel (Converging) Topology Benchmarks
// =============================================================================

/**
 * @brief Benchmark converging funnel topology
 */
static void BM_Topology_Funnel(::benchmark::State &state) {
  const int initial_branches = static_cast<int>(state.range(0));
  const std::uint8_t num_workers = 8;

  Graph graph;

  auto source = std::make_shared<SourceNode>("source", 1024);
  graph.addNode(source);

  // Stage 1
  std::vector<std::string> stage1_names;
  for (int i = 0; i < initial_branches; ++i) {
    std::string name = "s1_" + std::to_string(i);
    auto node =
        std::make_shared<DelayNode>(name, std::chrono::microseconds(50));
    graph.addNode(node);
    graph.addEdge("source", "output", name, "input");
    stage1_names.push_back(name);
  }

  auto agg1 = std::make_shared<AggregatorNode>("agg1", initial_branches);
  graph.addNode(agg1);
  for (int i = 0; i < initial_branches; ++i) {
    graph.addEdge(stage1_names[i], "output", "agg1",
                  "input_" + std::to_string(i));
  }

  // Stage 2
  int stage2_count = initial_branches / 2;
  std::vector<std::string> stage2_names;
  for (int i = 0; i < stage2_count; ++i) {
    std::string name = "s2_" + std::to_string(i);
    auto node =
        std::make_shared<DelayNode>(name, std::chrono::microseconds(50));
    graph.addNode(node);
    graph.addEdge("agg1", "output", name, "input");
    stage2_names.push_back(name);
  }

  auto agg2 = std::make_shared<AggregatorNode>("agg2", stage2_count);
  graph.addNode(agg2);
  for (int i = 0; i < stage2_count; ++i) {
    graph.addEdge(stage2_names[i], "output", "agg2",
                  "input_" + std::to_string(i));
  }

  auto final_node =
      std::make_shared<DelayNode>("final", std::chrono::microseconds(50));
  graph.addNode(final_node);
  graph.addEdge("agg2", "output", "final", "input");

  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(sink);
  graph.addEdge("final", "output", "sink", "input");

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(&graph, num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["initial_branches"] = initial_branches;
}

BENCHMARK(BM_Topology_Funnel)
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Unit(::benchmark::kMicrosecond);

// =============================================================================
// Parallel Pipelines Benchmark
// =============================================================================

/**
 * @brief Benchmark independent parallel pipelines
 */
static void BM_Topology_Parallel_Width(::benchmark::State &state) {
  const int num_pipelines = static_cast<int>(state.range(0));
  const int depth_per_pipeline = 4;
  const std::uint8_t num_workers = 8;

  auto topology = buildParallelPipelines(num_pipelines, depth_per_pipeline,
                                         std::chrono::microseconds{10});

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->initialize(topology.graph.get(), num_workers);

  PortDataMap inputs;
  for (int p = 0; p < num_pipelines; ++p) {
    auto input_packet = std::make_shared<PortData>();
    input_packet->id = p;
    input_packet->setParam("payload", BenchmarkPayload(p, 1024));
    inputs["p" + std::to_string(p) + "_source_output"] = input_packet;
  }

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["pipelines"] = num_pipelines;
  state.counters["depth_per_pipeline"] = depth_per_pipeline;
}

BENCHMARK(BM_Topology_Parallel_Width)
    ->RangeMultiplier(2)
    ->Range(1, 128)
    ->Unit(::benchmark::kMicrosecond);

// =============================================================================
// Binary Tree Topology Benchmarks
// =============================================================================

/**
 * @brief Benchmark expanding binary tree topology
 */
static void BM_Topology_Tree_Expand(::benchmark::State &state) {
  const int tree_depth = static_cast<int>(state.range(0));
  const std::uint8_t num_workers = 8;

  Graph graph;

  auto source = std::make_shared<SourceNode>("source", 1024);
  graph.addNode(source);

  auto root = std::make_shared<FanOutNode>("root", 2);
  graph.addNode(root);
  graph.addEdge("source", "output", "root", "input");

  std::vector<std::string> current_level = {"root"};

  for (int level = 1; level < tree_depth; ++level) {
    std::vector<std::string> next_level;
    int node_idx = 0;

    for (const auto &parent : current_level) {
      for (int child = 0; child < 2; ++child) {
        std::string name =
            "l" + std::to_string(level) + "_n" + std::to_string(node_idx++);

        if (level < tree_depth - 1) {
          auto node = std::make_shared<FanOutNode>(name, 2);
          graph.addNode(node);
        } else {
          auto node = std::make_shared<PassthroughNode>(name);
          graph.addNode(node);
        }

        graph.addEdge(parent, "output_" + std::to_string(child), name, "input");
        next_level.push_back(name);
      }
    }
    current_level = next_level;
  }

  int leaf_count = static_cast<int>(current_level.size());
  auto sink_agg = std::make_shared<AggregatorNode>("sink_agg", leaf_count);
  graph.addNode(sink_agg);

  for (int i = 0; i < leaf_count; ++i) {
    graph.addEdge(current_level[i], "output", "sink_agg",
                  "input_" + std::to_string(i));
  }

  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(sink);
  graph.addEdge("sink_agg", "output", "sink", "input");

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(&graph, num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["depth"] = tree_depth;
  state.counters["leaves"] = leaf_count;
}

BENCHMARK(BM_Topology_Tree_Expand)
    ->DenseRange(1, 8)
    ->Unit(::benchmark::kMicrosecond);

/**
 * @brief Benchmark reducing binary tree topology
 */
static void BM_Topology_Tree_Reduce(::benchmark::State &state) {
  const int leaf_count = static_cast<int>(state.range(0));
  const std::uint8_t num_workers = 8;

  Graph graph;

  std::vector<std::string> current_level;
  for (int i = 0; i < leaf_count; ++i) {
    std::string name = "leaf_" + std::to_string(i);
    auto node = std::make_shared<SourceNode>(name, 1024);
    graph.addNode(node);
    current_level.push_back(name);
  }

  int level = 0;
  while (current_level.size() > 1) {
    std::vector<std::string> next_level;

    for (std::size_t i = 0; i < current_level.size(); i += 2) {
      std::string name =
          "agg_l" + std::to_string(level) + "_" + std::to_string(i / 2);

      if (i + 1 < current_level.size()) {
        auto node = std::make_shared<AggregatorNode>(name, 2);
        graph.addNode(node);
        graph.addEdge(current_level[i], "output", name, "input_0");
        graph.addEdge(current_level[i + 1], "output", name, "input_1");
      } else {
        auto node = std::make_shared<PassthroughNode>(name);
        graph.addNode(node);
        graph.addEdge(current_level[i], "output", name, "input");
      }
      next_level.push_back(name);
    }

    current_level = next_level;
    level++;
  }

  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(sink);
  graph.addEdge(current_level[0], "output", "sink", "input");

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(&graph, num_workers);

  PortDataMap inputs;
  for (int i = 0; i < leaf_count; ++i) {
    auto input_packet = std::make_shared<PortData>();
    input_packet->id = i;
    input_packet->setParam("payload", BenchmarkPayload(i, 1024));
    inputs["leaf_" + std::to_string(i) + "_output"] = input_packet;
  }

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["leaves"] = leaf_count;
  state.counters["reduction_depth"] = level;
}

BENCHMARK(BM_Topology_Tree_Reduce)
    ->RangeMultiplier(2)
    ->Range(4, 64)
    ->Unit(::benchmark::kMicrosecond);

// =============================================================================
// Streaming Topology Benchmarks
// =============================================================================

/**
 * @brief Benchmark streaming linear pipeline
 */
static void BM_Topology_Stream_Linear(::benchmark::State &state) {
  const int depth = static_cast<int>(state.range(0));
  const std::size_t frames = 100;
  const std::uint8_t num_workers = 4;

  auto topology = buildLinearPipeline(depth, std::chrono::microseconds{10});

  auto engine = createBenchmarkStreamEngine(num_workers, 16);
  engine->initialize(topology.graph.get(), num_workers);

  for (auto _ : state) {
    engine->startStreaming();

    for (std::size_t f = 0; f < frames; ++f) {
      auto packet = std::make_shared<PortData>();
      packet->id = f;
      packet->setParam("payload", BenchmarkPayload(f, 1024));
      packet->setParam("timestamp", std::chrono::steady_clock::now());

      (void)engine->pushInput(topology.source_node, "output", packet);
    }

    engine->stopStreaming(true);
  }

  recordThroughput(state, frames);
  state.counters["depth"] = depth;
}

BENCHMARK(BM_Topology_Stream_Linear)
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Unit(::benchmark::kMillisecond);

/**
 * @brief Benchmark streaming diamond pipeline
 */
static void BM_Topology_Stream_Diamond(::benchmark::State &state) {
  const int branches = static_cast<int>(state.range(0));
  const std::size_t frames = 100;
  const std::uint8_t num_workers = 8;

  auto topology = buildDiamondPipeline(branches, std::chrono::microseconds{10});

  auto engine = createBenchmarkStreamEngine(num_workers, 16, true);
  engine->initialize(topology.graph.get(), num_workers);

  for (auto _ : state) {
    engine->startStreaming();

    for (std::size_t f = 0; f < frames; ++f) {
      auto packet = std::make_shared<PortData>();
      packet->id = f;
      packet->setParam("payload", BenchmarkPayload(f, 1024));
      packet->setParam("timestamp", std::chrono::steady_clock::now());

      (void)engine->pushInput(topology.source_node, "output", packet);
    }

    engine->stopStreaming(true);
  }

  recordThroughput(state, frames);
  state.counters["branches"] = branches;
}

BENCHMARK(BM_Topology_Stream_Diamond)
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// Complexity Level Benchmarks
// =============================================================================

/**
 * @brief Benchmark different complexity levels
 */
static void BM_Topology_Complexity(::benchmark::State &state) {
  const int complexity_level = static_cast<int>(state.range(0));
  const std::uint8_t num_workers = 4;

  Graph graph;

  auto source = std::make_shared<SourceNode>("source", 1024);
  auto sink = std::make_shared<SinkNode>("sink");

  graph.addNode(source);

  switch (complexity_level) {
  case 1: {
    std::string prev = "source";
    for (int i = 0; i < 4; ++i) {
      std::string name = "node_" + std::to_string(i);
      auto node = std::make_shared<PassthroughNode>(name);
      graph.addNode(node);
      graph.addEdge(prev, "output", name, "input");
      prev = name;
    }
    graph.addNode(sink);
    graph.addEdge(prev, "output", "sink", "input");
    break;
  }
  case 2: {
    std::vector<std::string> branches;
    for (int i = 0; i < 4; ++i) {
      std::string name = "branch_" + std::to_string(i);
      auto node = std::make_shared<PassthroughNode>(name);
      graph.addNode(node);
      graph.addEdge("source", "output", name, "input");
      branches.push_back(name);
    }
    auto agg = std::make_shared<AggregatorNode>("agg", 4);
    graph.addNode(agg);
    for (int i = 0; i < 4; ++i) {
      graph.addEdge(branches[i], "output", "agg", "input_" + std::to_string(i));
    }
    graph.addNode(sink);
    graph.addEdge("agg", "output", "sink", "input");
    break;
  }
  case 3: {
    auto fork1 = std::make_shared<FanOutNode>("fork1", 4);
    graph.addNode(fork1);
    graph.addEdge("source", "output", "fork1", "input");

    std::vector<std::string> stage1;
    for (int i = 0; i < 4; ++i) {
      std::string name = "s1_" + std::to_string(i);
      auto node = std::make_shared<PassthroughNode>(name);
      graph.addNode(node);
      graph.addEdge("fork1", "output_" + std::to_string(i), name, "input");
      stage1.push_back(name);
    }

    auto agg1 = std::make_shared<AggregatorNode>("agg1", 4);
    graph.addNode(agg1);
    for (int i = 0; i < 4; ++i) {
      graph.addEdge(stage1[i], "output", "agg1", "input_" + std::to_string(i));
    }

    auto fork2 = std::make_shared<FanOutNode>("fork2", 4);
    graph.addNode(fork2);
    graph.addEdge("agg1", "output", "fork2", "input");

    std::vector<std::string> stage2;
    for (int i = 0; i < 4; ++i) {
      std::string name = "s2_" + std::to_string(i);
      auto node = std::make_shared<PassthroughNode>(name);
      graph.addNode(node);
      graph.addEdge("fork2", "output_" + std::to_string(i), name, "input");
      stage2.push_back(name);
    }

    auto agg2 = std::make_shared<AggregatorNode>("agg2", 4);
    graph.addNode(agg2);
    for (int i = 0; i < 4; ++i) {
      graph.addEdge(stage2[i], "output", "agg2", "input_" + std::to_string(i));
    }

    graph.addNode(sink);
    graph.addEdge("agg2", "output", "sink", "input");
    break;
  }
  default:
    graph.addNode(sink);
    graph.addEdge("source", "output", "sink", "input");
  }

  auto engine = createBenchmarkBatchEngine(num_workers);
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
  engine->initialize(&graph, num_workers);

  auto input_packet = std::make_shared<PortData>();
  input_packet->id = 0;
  input_packet->setParam("payload", BenchmarkPayload(0, 1024));

  PortDataMap inputs;
  inputs["output"] = input_packet;

  for (auto _ : state) {
    engine->execute(inputs);
  }

  state.counters["complexity"] = complexity_level;
}

BENCHMARK(BM_Topology_Complexity)
    ->DenseRange(1, 3)
    ->Unit(::benchmark::kMicrosecond);

// =============================================================================
// Initialization Overhead Benchmarks
// =============================================================================

/**
 * @brief Benchmark graph initialization overhead
 */
static void BM_Topology_InitializationOverhead(::benchmark::State &state) {
  const int num_nodes = static_cast<int>(state.range(0));

  for (auto _ : state) {
    Graph graph;

    auto source = std::make_shared<SourceNode>("source", 1024);
    graph.addNode(source);

    std::string prev = "source";
    for (int i = 0; i < num_nodes; ++i) {
      std::string name = "node_" + std::to_string(i);
      auto node = std::make_shared<PassthroughNode>(name);
      graph.addNode(node);
      graph.addEdge(prev, "output", name, "input");
      prev = name;
    }

    auto sink = std::make_shared<SinkNode>("sink");
    graph.addNode(sink);
    graph.addEdge(prev, "output", "sink", "input");

    ::benchmark::DoNotOptimize(graph);
  }

  state.counters["num_nodes"] = num_nodes + 2;
}

BENCHMARK(BM_Topology_InitializationOverhead)
    ->RangeMultiplier(2)
    ->Range(4, 1024)
    ->Unit(::benchmark::kMicrosecond);

/**
 * @brief Benchmark engine initialization with JoinAwareSyncStrategy
 */
static void BM_Topology_SyncInitOverhead(::benchmark::State &state) {
  const int num_nodes = static_cast<int>(state.range(0));
  const std::uint8_t num_workers = static_cast<std::uint8_t>(state.range(1));

  Graph graph;

  auto source = std::make_shared<SourceNode>("source", 1024);
  graph.addNode(source);

  std::string prev = "source";
  for (int i = 0; i < num_nodes; ++i) {
    std::string name = "node_" + std::to_string(i);
    auto node = std::make_shared<PassthroughNode>(name);
    graph.addNode(node);
    graph.addEdge(prev, "output", name, "input");
    prev = name;
  }

  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(sink);
  graph.addEdge(prev, "output", "sink", "input");

  for (auto _ : state) {
    auto config = EngineConfig::batch(num_workers);
    ExecutionEngine engine(config);
    engine.setSchedulerStrategy(std::make_unique<BatchSchedulerStrategy>());
    engine.setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());
    engine.initialize(&graph, num_workers);

    ::benchmark::DoNotOptimize(engine);
  }

  state.counters["num_nodes"] = num_nodes + 2;
  state.counters["num_workers"] = static_cast<double>(num_workers);
}

BENCHMARK(BM_Topology_SyncInitOverhead)
    ->Args({16, 2})
    ->Args({16, 4})
    ->Args({16, 8})
    ->Args({64, 2})
    ->Args({64, 4})
    ->Args({64, 8})
    ->Args({256, 4})
    ->Unit(::benchmark::kMicrosecond);

// Note: BENCHMARK_MAIN() is provided by benchmark_main.cpp when built as
// bench_all For individual build (bench_topology), link with
// benchmark::benchmark_main
