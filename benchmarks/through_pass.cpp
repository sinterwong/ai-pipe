/**
 * @file through_pass.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Performance benchmarks for DefaultExecutionEngine
 * @version 0.1
 * @date 2025-11-15
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "ai_pipe/graph.hpp"
#include "ai_pipe/logger.hpp"
#include "benchmark_node.hpp"
#include "default_execution_engine.hpp"
#include <benchmark/benchmark.h>
#include <memory>

using namespace ai_pipe;
using namespace ai_pipe::bench;

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * @brief Create a linear pipeline graph
 *
 * Creates a graph with nodes connected in a linear sequence:
 * Node0 -> Node1 -> Node2 -> ... -> NodeN
 *
 * @param numNodes Number of nodes in the pipeline
 * @param delayMicros Processing delay per node in microseconds
 * @return Graph The constructed graph
 */
Graph createLinearGraph(int num_nodes, int64_t delay_micros = 0) {
  Graph graph;

  std::vector<std::shared_ptr<BenchmarkNode>> nodes;
  for (int i = 0; i < num_nodes; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Node" + std::to_string(i),
                                                delay_micros);
    nodes.push_back(node);
    graph.addNode(node);
  }

  // Connect nodes linearly
  for (int i = 0; i < num_nodes - 1; ++i) {
    graph.addEdge("Node" + std::to_string(i), "output",
                  "Node" + std::to_string(i + 1), "input");
  }

  return graph;
}

/**
 * @brief Create a parallel fan-out graph
 *
 * Creates a graph with one source node fanning out to multiple parallel nodes,
 * then converging to a single sink node:
 *
 *        /-> Node1 -\
 * Node0 -+-> Node2 -+-> NodeN
 *        \-> Node3 -/
 *
 * @param numParallel Number of parallel branches
 * @param delayMicros Processing delay per node in microseconds
 * @return Graph The constructed graph
 */
Graph createParallelGraph(int num_parallel, int64_t delay_micros = 0) {
  Graph graph;

  // Source node
  auto source_node = std::make_shared<BenchmarkNode>("Source", delay_micros);
  graph.addNode(source_node);

  // Parallel nodes
  std::vector<std::shared_ptr<BenchmarkNode>> parallel_nodes;
  for (int i = 0; i < num_parallel; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Parallel" + std::to_string(i),
                                                delay_micros);
    parallel_nodes.push_back(node);
    graph.addNode(node);
    graph.addEdge("Source", "output", "Parallel" + std::to_string(i), "input");
  }

  // Sink node (only if we have multiple parallel branches)
  if (num_parallel > 1) {
    auto sink_node = std::make_shared<BenchmarkNode>("Sink", delay_micros);
    graph.addNode(sink_node);

    for (int i = 0; i < num_parallel; ++i) {
      graph.addEdge("Parallel" + std::to_string(i), "output", "Sink", "input");
    }
  }

  return graph;
}

/**
 * @brief Create a diamond-shaped graph
 *
 * Creates a graph with a diamond pattern:
 *
 *        /-> NodeL -\
 * Source +          +-> Sink
 *        \-> NodeR -/
 *
 * @param depth Depth of each branch
 * @param delayMicros Processing delay per node in microseconds
 * @return Graph The constructed graph
 */
Graph createDiamondGraph(int depth = 2, int64_t delay_micros = 0) {
  Graph graph;

  // Source node
  auto source_node = std::make_shared<BenchmarkNode>("Source", delay_micros);
  graph.addNode(source_node);

  // Left branch
  std::vector<std::shared_ptr<BenchmarkNode>> left_branch;
  for (int i = 0; i < depth; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Left" + std::to_string(i),
                                                delay_micros);
    left_branch.push_back(node);
    graph.addNode(node);
  }

  // Right branch
  std::vector<std::shared_ptr<BenchmarkNode>> right_branch;
  for (int i = 0; i < depth; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Right" + std::to_string(i),
                                                delay_micros);
    right_branch.push_back(node);
    graph.addNode(node);
  }

  // Sink node
  auto sink_node = std::make_shared<BenchmarkNode>("Sink", delay_micros);
  graph.addNode(sink_node);

  // Connect source to both branches
  graph.addEdge("Source", "output", "Left0", "input");
  graph.addEdge("Source", "output", "Right0", "input");

  // Connect nodes within left branch
  for (int i = 0; i < depth - 1; ++i) {
    graph.addEdge("Left" + std::to_string(i), "output",
                  "Left" + std::to_string(i + 1), "input");
  }

  // Connect nodes within right branch
  for (int i = 0; i < depth - 1; ++i) {
    graph.addEdge("Right" + std::to_string(i), "output",
                  "Right" + std::to_string(i + 1), "input");
  }

  // Connect both branches to sink
  graph.addEdge("Left" + std::to_string(depth - 1), "output", "Sink", "input");
  graph.addEdge("Right" + std::to_string(depth - 1), "output", "Sink", "input");

  return graph;
}

// ============================================================================
// Benchmark: Linear Pipeline
// ============================================================================

/**
 * @brief Benchmark linear pipeline execution
 *
 * Measures the performance of executing a linear sequence of nodes.
 * This tests the basic scheduling overhead and sequential execution.
 */
static void bmLinearPipeline(benchmark::State &state) {
  const int num_nodes = state.range(0);
  const int64_t delay_micros = state.range(1);

  // Setup logger (only once)
  static bool logger_initialized = false;
  if (!logger_initialized) {
    ai_pipe::logging::LoggerConfig cfg;
    cfg.async_enabled = true;
    cfg.json_output = false;
    cfg.color_enabled = true;
    cfg.file_enabled = false;
    cfg.min_level = ai_pipe::logging::LogLevel::Error;
    ai_pipe::logging::Logger::instance().configure(cfg);
  }

  // Create graph
  auto graph = createLinearGraph(num_nodes, delay_micros);

  // Create engine
  DefaultExecutionEngine engine;
  const uint8_t num_workers = 4;
  if (!engine.initialize(&graph, num_workers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  // Prepare input
  PortDataMap inputs;
  auto input_data = std::make_shared<PortData>();
  input_data->setParam<int>("data", 42);
  inputs["Node0"] = input_data;

  // Benchmark loop
  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  // Report custom metrics
  state.SetItemsProcessed(state.iterations() * num_nodes);
  state.SetLabel("nodes=" + std::to_string(num_nodes) +
                 ",delay=" + std::to_string(delay_micros) + "us");
}

// Register linear pipeline benchmarks
BENCHMARK(bmLinearPipeline)
    ->Args({3, 0})    // 3 nodes, no delay
    ->Args({5, 0})    // 5 nodes, no delay
    ->Args({10, 0})   // 10 nodes, no delay
    ->Args({3, 100})  // 3 nodes, 100us delay
    ->Args({5, 100})  // 5 nodes, 100us delay
    ->Args({10, 100}) // 10 nodes, 100us delay
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Parallel Execution
// ============================================================================

/**
 * @brief Benchmark parallel fan-out execution
 *
 * Measures the performance of executing multiple parallel branches.
 * This tests the engine's ability to utilize multiple threads effectively.
 */
static void bmParallelExecution(benchmark::State &state) {
  const int num_parallel = state.range(0);
  const int64_t delay_micros = state.range(1);

  auto graph = createParallelGraph(num_parallel, delay_micros);

  DefaultExecutionEngine engine;
  const uint8_t num_workers = 8; // More workers for parallel execution
  if (!engine.initialize(&graph, num_workers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto input_data = std::make_shared<PortData>();
  input_data->setParam<int>("data", 42);
  inputs["Source"] = input_data;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          (num_parallel + 2)); // +source +sink
  state.SetLabel("parallel=" + std::to_string(num_parallel) +
                 ",delay=" + std::to_string(delay_micros) + "us");
}

BENCHMARK(bmParallelExecution)
    ->Args({2, 0})   // 2 parallel branches, no delay
    ->Args({4, 0})   // 4 parallel branches, no delay
    ->Args({8, 0})   // 8 parallel branches, no delay
    ->Args({2, 100}) // 2 parallel branches, 100us delay
    ->Args({4, 100}) // 4 parallel branches, 100us delay
    ->Args({8, 100}) // 8 parallel branches, 100us delay
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Worker Thread Scaling
// ============================================================================

/**
 * @brief Benchmark engine performance with different thread counts
 *
 * Measures how the engine scales with different numbers of worker threads.
 * This helps identify the optimal thread pool size.
 */
static void bmWorkerThreadScaling(benchmark::State &state) {
  const uint8_t num_workers = state.range(0);
  const int num_parallel = 8; // Fixed parallel workload
  const int64_t delay_micros = 100;

  auto graph = createParallelGraph(num_parallel, delay_micros);

  DefaultExecutionEngine engine;
  if (!engine.initialize(&graph, num_workers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto input_data = std::make_shared<PortData>();
  input_data->setParam<int>("data", 42);
  inputs["Source"] = input_data;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetLabel("workers=" + std::to_string(num_workers));
}

BENCHMARK(bmWorkerThreadScaling)
    ->Arg(1)  // 1 worker
    ->Arg(2)  // 2 workers
    ->Arg(4)  // 4 workers
    ->Arg(8)  // 8 workers
    ->Arg(16) // 16 workers
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Diamond Graph Pattern
// ============================================================================

/**
 * @brief Benchmark diamond-shaped graph execution
 *
 * Tests a common pattern where work splits and then merges.
 * This is typical in many real-world pipelines.
 */
static void bmDiamondPattern(benchmark::State &state) {
  const int depth = state.range(0);
  const int64_t delay_micros = state.range(1);

  auto graph = createDiamondGraph(depth, delay_micros);

  DefaultExecutionEngine engine;
  const uint8_t num_workers = 4;
  if (!engine.initialize(&graph, num_workers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto input_data = std::make_shared<PortData>();
  input_data->setParam<int>("data", 42);
  inputs["Source"] = input_data;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  int total_nodes = 1 + 2 * depth + 1; // source + 2 branches + sink
  state.SetItemsProcessed(state.iterations() * total_nodes);
  state.SetLabel("depth=" + std::to_string(depth) +
                 ",delay=" + std::to_string(delay_micros) + "us");
}

BENCHMARK(bmDiamondPattern)
    ->Args({2, 0})  // depth=2, no delay
    ->Args({4, 0})  // depth=4, no delay
    ->Args({2, 50}) // depth=2, 50us delay
    ->Args({4, 50}) // depth=4, 50us delay
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Throughput Test
// ============================================================================

/**
 * @brief Benchmark throughput with multiple consecutive executions
 *
 * Measures the overhead of reset() and re-execution.
 * This simulates a scenario where the same pipeline processes
 * multiple inputs sequentially.
 */
static void bmThroughput(benchmark::State &state) {
  const int num_nodes = 5;
  const int64_t delay_micros = 10; // Small delay to simulate real work

  auto graph = createLinearGraph(num_nodes, delay_micros);

  DefaultExecutionEngine engine;
  const uint8_t num_workers = 4;
  if (!engine.initialize(&graph, num_workers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto input_data = std::make_shared<PortData>();
  input_data->setParam<int>("data", 42);
  inputs["Node0"] = input_data;

  int64_t total_executions = 0;

  for (auto _ : state) {
    // Execute multiple times per iteration to measure throughput
    for (int i = 0; i < 10; ++i) {
      engine.execute(inputs, true);
      engine.reset();
      total_executions++;
    }
  }

  state.SetItemsProcessed(total_executions);
  state.SetLabel("executions_per_iter=10");
}

BENCHMARK(bmThroughput)->Unit(benchmark::kMillisecond);

// ============================================================================
// Benchmark: Scheduling Overhead
// ============================================================================

/**
 * @brief Benchmark scheduling overhead with minimal work
 *
 * Measures the pure overhead of the execution engine by using
 * nodes with zero processing delay. This isolates the scheduling,
 * synchronization, and task management costs.
 */
static void bmSchedulingOverhead(benchmark::State &state) {
  const int num_nodes = state.range(0);

  auto graph = createLinearGraph(num_nodes, 0); // Zero delay

  DefaultExecutionEngine engine;
  const uint8_t num_workers = 4;
  if (!engine.initialize(&graph, num_workers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto input_data = std::make_shared<PortData>();
  input_data->setParam<int>("data", 42);
  inputs["Node0"] = input_data;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetItemsProcessed(state.iterations() * num_nodes);
  state.SetLabel("nodes=" + std::to_string(num_nodes) + ",overhead_only");
}

BENCHMARK(bmSchedulingOverhead)
    ->Arg(1)
    ->Arg(5)
    ->Arg(10)
    ->Arg(20)
    ->Arg(50)
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Engine State Transitions
// ============================================================================

/**
 * @brief Benchmark engine initialization and reset
 *
 * Measures the cost of initialize() and reset() operations.
 * This is important for scenarios where the engine is frequently
 * reconfigured.
 */
static void bmStateTransitions(benchmark::State &state) {
  const int num_nodes = 10;

  for (auto _ : state) {
    state.PauseTiming();
    auto graph = createLinearGraph(num_nodes, 0);
    DefaultExecutionEngine engine;
    state.ResumeTiming();

    engine.initialize(&graph, 4);

    state.PauseTiming();
    PortDataMap inputs;
    auto input_data = std::make_shared<PortData>();
    input_data->setParam<int>("data", 42);
    inputs["Node0"] = input_data;
    state.ResumeTiming();

    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetLabel("init+execute+reset");
}

BENCHMARK(bmStateTransitions)->Unit(benchmark::kMicrosecond);

// ============================================================================
// Benchmark: Complex Graph
// ============================================================================

/**
 * @brief Benchmark a complex multi-stage graph
 *
 * Creates a more realistic graph with multiple stages of fan-out and fan-in:
 *
 * Stage1 -> Stage2 (3 parallel) -> Stage3 (2 parallel) -> Stage4
 */
static void bmComplexGraph(benchmark::State &state) {
  const int64_t delay_micros = state.range(0);

  Graph graph;

  // Stage 1: Single node
  auto stage1 = std::make_shared<BenchmarkNode>("Stage1", delay_micros);
  graph.addNode(stage1);

  // Stage 2: Fan-out to 3 nodes
  std::vector<std::shared_ptr<BenchmarkNode>> stage2_nodes;
  for (int i = 0; i < 3; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Stage2_" + std::to_string(i),
                                                delay_micros);
    stage2_nodes.push_back(node);
    graph.addNode(node);
    graph.addEdge("Stage1", "output", "Stage2_" + std::to_string(i), "input");
  }

  // Stage 3: Each stage2 node fans out to 2 nodes
  std::vector<std::shared_ptr<BenchmarkNode>> stage3_nodes;
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 2; ++j) {
      auto node = std::make_shared<BenchmarkNode>(
          "Stage3_" + std::to_string(i) + "_" + std::to_string(j),
          delay_micros);
      stage3_nodes.push_back(node);
      graph.addNode(node);
      graph.addEdge("Stage2_" + std::to_string(i), "output",
                    "Stage3_" + std::to_string(i) + "_" + std::to_string(j),
                    "input");
    }
  }

  // Stage 4: Converge to single node
  auto stage4 = std::make_shared<BenchmarkNode>("Stage4", delay_micros);
  graph.addNode(stage4);
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 2; ++j) {
      graph.addEdge("Stage3_" + std::to_string(i) + "_" + std::to_string(j),
                    "output", "Stage4", "input");
    }
  }

  DefaultExecutionEngine engine;
  const uint8_t num_workers = 8;
  if (!engine.initialize(&graph, num_workers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto input_data = std::make_shared<PortData>();
  input_data->setParam<int>("data", 42);
  inputs["Stage1"] = input_data;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  int total_nodes = 1 + 3 + 6 + 1; // stage1 + stage2 + stage3 + stage4
  state.SetItemsProcessed(state.iterations() * total_nodes);
  state.SetLabel("delay=" + std::to_string(delay_micros) +
                 "us,nodes=" + std::to_string(total_nodes));
}

BENCHMARK(bmComplexGraph)
    ->Arg(0)   // No delay
    ->Arg(50)  // 50us delay
    ->Arg(100) // 100us delay
    ->Unit(benchmark::kMicrosecond);
