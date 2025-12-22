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

#include "ai_pipe/logger.hpp"
#include "benchmark_node.hpp"
#include "default_execution_engine.hpp"
#include "graph.hpp"
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
Graph createLinearGraph(int numNodes, int64_t delayMicros = 0) {
  Graph graph;

  std::vector<std::shared_ptr<BenchmarkNode>> nodes;
  for (int i = 0; i < numNodes; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Node" + std::to_string(i),
                                                delayMicros);
    nodes.push_back(node);
    graph.addNode(node);
  }

  // Connect nodes linearly
  for (int i = 0; i < numNodes - 1; ++i) {
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
Graph createParallelGraph(int numParallel, int64_t delayMicros = 0) {
  Graph graph;

  // Source node
  auto sourceNode = std::make_shared<BenchmarkNode>("Source", delayMicros);
  graph.addNode(sourceNode);

  // Parallel nodes
  std::vector<std::shared_ptr<BenchmarkNode>> parallelNodes;
  for (int i = 0; i < numParallel; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Parallel" + std::to_string(i),
                                                delayMicros);
    parallelNodes.push_back(node);
    graph.addNode(node);
    graph.addEdge("Source", "output", "Parallel" + std::to_string(i), "input");
  }

  // Sink node (only if we have multiple parallel branches)
  if (numParallel > 1) {
    auto sinkNode = std::make_shared<BenchmarkNode>("Sink", delayMicros);
    graph.addNode(sinkNode);

    for (int i = 0; i < numParallel; ++i) {
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
Graph createDiamondGraph(int depth = 2, int64_t delayMicros = 0) {
  Graph graph;

  // Source node
  auto sourceNode = std::make_shared<BenchmarkNode>("Source", delayMicros);
  graph.addNode(sourceNode);

  // Left branch
  std::vector<std::shared_ptr<BenchmarkNode>> leftBranch;
  for (int i = 0; i < depth; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Left" + std::to_string(i),
                                                delayMicros);
    leftBranch.push_back(node);
    graph.addNode(node);
  }

  // Right branch
  std::vector<std::shared_ptr<BenchmarkNode>> rightBranch;
  for (int i = 0; i < depth; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Right" + std::to_string(i),
                                                delayMicros);
    rightBranch.push_back(node);
    graph.addNode(node);
  }

  // Sink node
  auto sinkNode = std::make_shared<BenchmarkNode>("Sink", delayMicros);
  graph.addNode(sinkNode);

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
static void BM_LinearPipeline(benchmark::State &state) {
  const int numNodes = state.range(0);
  const int64_t delayMicros = state.range(1);

  // Setup logger (only once)
  static bool loggerInitialized = false;
  if (!loggerInitialized) {
    ai_pipe::logging::LoggerConfig cfg;
    cfg.async_enabled = true;
    cfg.json_output = true;
    ai_pipe::logging::Logger::instance().configure(cfg);
  }

  // Create graph
  auto graph = createLinearGraph(numNodes, delayMicros);

  // Create engine
  DefaultExecutionEngine engine;
  const uint8_t numWorkers = 4;
  if (!engine.initialize(&graph, numWorkers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  // Prepare input
  PortDataMap inputs;
  auto inputData = std::make_shared<PortData>();
  inputData->setParam<int>("data", 42);
  inputs["Node0"] = inputData;

  // Benchmark loop
  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  // Report custom metrics
  state.SetItemsProcessed(state.iterations() * numNodes);
  state.SetLabel("nodes=" + std::to_string(numNodes) +
                 ",delay=" + std::to_string(delayMicros) + "us");
}

// Register linear pipeline benchmarks
BENCHMARK(BM_LinearPipeline)
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
static void BM_ParallelExecution(benchmark::State &state) {
  const int numParallel = state.range(0);
  const int64_t delayMicros = state.range(1);

  auto graph = createParallelGraph(numParallel, delayMicros);

  DefaultExecutionEngine engine;
  const uint8_t numWorkers = 8; // More workers for parallel execution
  if (!engine.initialize(&graph, numWorkers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto inputData = std::make_shared<PortData>();
  inputData->setParam<int>("data", 42);
  inputs["Source"] = inputData;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          (numParallel + 2)); // +source +sink
  state.SetLabel("parallel=" + std::to_string(numParallel) +
                 ",delay=" + std::to_string(delayMicros) + "us");
}

BENCHMARK(BM_ParallelExecution)
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
static void BM_WorkerThreadScaling(benchmark::State &state) {
  const uint8_t numWorkers = state.range(0);
  const int numParallel = 8; // Fixed parallel workload
  const int64_t delayMicros = 100;

  auto graph = createParallelGraph(numParallel, delayMicros);

  DefaultExecutionEngine engine;
  if (!engine.initialize(&graph, numWorkers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto inputData = std::make_shared<PortData>();
  inputData->setParam<int>("data", 42);
  inputs["Source"] = inputData;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetLabel("workers=" + std::to_string(numWorkers));
}

BENCHMARK(BM_WorkerThreadScaling)
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
static void BM_DiamondPattern(benchmark::State &state) {
  const int depth = state.range(0);
  const int64_t delayMicros = state.range(1);

  auto graph = createDiamondGraph(depth, delayMicros);

  DefaultExecutionEngine engine;
  const uint8_t numWorkers = 4;
  if (!engine.initialize(&graph, numWorkers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto inputData = std::make_shared<PortData>();
  inputData->setParam<int>("data", 42);
  inputs["Source"] = inputData;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  int totalNodes = 1 + 2 * depth + 1; // source + 2 branches + sink
  state.SetItemsProcessed(state.iterations() * totalNodes);
  state.SetLabel("depth=" + std::to_string(depth) +
                 ",delay=" + std::to_string(delayMicros) + "us");
}

BENCHMARK(BM_DiamondPattern)
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
static void BM_Throughput(benchmark::State &state) {
  const int numNodes = 5;
  const int64_t delayMicros = 10; // Small delay to simulate real work

  auto graph = createLinearGraph(numNodes, delayMicros);

  DefaultExecutionEngine engine;
  const uint8_t numWorkers = 4;
  if (!engine.initialize(&graph, numWorkers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto inputData = std::make_shared<PortData>();
  inputData->setParam<int>("data", 42);
  inputs["Node0"] = inputData;

  int64_t totalExecutions = 0;

  for (auto _ : state) {
    // Execute multiple times per iteration to measure throughput
    for (int i = 0; i < 10; ++i) {
      engine.execute(inputs, true);
      engine.reset();
      totalExecutions++;
    }
  }

  state.SetItemsProcessed(totalExecutions);
  state.SetLabel("executions_per_iter=10");
}

BENCHMARK(BM_Throughput)->Unit(benchmark::kMillisecond);

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
static void BM_SchedulingOverhead(benchmark::State &state) {
  const int numNodes = state.range(0);

  auto graph = createLinearGraph(numNodes, 0); // Zero delay

  DefaultExecutionEngine engine;
  const uint8_t numWorkers = 4;
  if (!engine.initialize(&graph, numWorkers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto inputData = std::make_shared<PortData>();
  inputData->setParam<int>("data", 42);
  inputs["Node0"] = inputData;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetItemsProcessed(state.iterations() * numNodes);
  state.SetLabel("nodes=" + std::to_string(numNodes) + ",overhead_only");
}

BENCHMARK(BM_SchedulingOverhead)
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
static void BM_StateTransitions(benchmark::State &state) {
  const int numNodes = 10;

  for (auto _ : state) {
    state.PauseTiming();
    auto graph = createLinearGraph(numNodes, 0);
    DefaultExecutionEngine engine;
    state.ResumeTiming();

    engine.initialize(&graph, 4);

    state.PauseTiming();
    PortDataMap inputs;
    auto inputData = std::make_shared<PortData>();
    inputData->setParam<int>("data", 42);
    inputs["Node0"] = inputData;
    state.ResumeTiming();

    engine.execute(inputs, true);
    engine.reset();
  }

  state.SetLabel("init+execute+reset");
}

BENCHMARK(BM_StateTransitions)->Unit(benchmark::kMicrosecond);

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
static void BM_ComplexGraph(benchmark::State &state) {
  const int64_t delayMicros = state.range(0);

  Graph graph;

  // Stage 1: Single node
  auto stage1 = std::make_shared<BenchmarkNode>("Stage1", delayMicros);
  graph.addNode(stage1);

  // Stage 2: Fan-out to 3 nodes
  std::vector<std::shared_ptr<BenchmarkNode>> stage2Nodes;
  for (int i = 0; i < 3; ++i) {
    auto node = std::make_shared<BenchmarkNode>("Stage2_" + std::to_string(i),
                                                delayMicros);
    stage2Nodes.push_back(node);
    graph.addNode(node);
    graph.addEdge("Stage1", "output", "Stage2_" + std::to_string(i), "input");
  }

  // Stage 3: Each stage2 node fans out to 2 nodes
  std::vector<std::shared_ptr<BenchmarkNode>> stage3Nodes;
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 2; ++j) {
      auto node = std::make_shared<BenchmarkNode>(
          "Stage3_" + std::to_string(i) + "_" + std::to_string(j), delayMicros);
      stage3Nodes.push_back(node);
      graph.addNode(node);
      graph.addEdge("Stage2_" + std::to_string(i), "output",
                    "Stage3_" + std::to_string(i) + "_" + std::to_string(j),
                    "input");
    }
  }

  // Stage 4: Converge to single node
  auto stage4 = std::make_shared<BenchmarkNode>("Stage4", delayMicros);
  graph.addNode(stage4);
  for (int i = 0; i < 3; ++i) {
    for (int j = 0; j < 2; ++j) {
      graph.addEdge("Stage3_" + std::to_string(i) + "_" + std::to_string(j),
                    "output", "Stage4", "input");
    }
  }

  DefaultExecutionEngine engine;
  const uint8_t numWorkers = 8;
  if (!engine.initialize(&graph, numWorkers)) {
    state.SkipWithError("Failed to initialize engine");
    return;
  }

  PortDataMap inputs;
  auto inputData = std::make_shared<PortData>();
  inputData->setParam<int>("data", 42);
  inputs["Stage1"] = inputData;

  for (auto _ : state) {
    engine.execute(inputs, true);
    engine.reset();
  }

  int totalNodes = 1 + 3 + 6 + 1; // stage1 + stage2 + stage3 + stage4
  state.SetItemsProcessed(state.iterations() * totalNodes);
  state.SetLabel("delay=" + std::to_string(delayMicros) +
                 "us,nodes=" + std::to_string(totalNodes));
}

BENCHMARK(BM_ComplexGraph)
    ->Arg(0)   // No delay
    ->Arg(50)  // 50us delay
    ->Arg(100) // 100us delay
    ->Unit(benchmark::kMicrosecond);
