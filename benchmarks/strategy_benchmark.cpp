/**
 * @file strategy_benchmark.cpp
 * @brief Benchmark tests comparing different strategies
 *
 * This file contains performance tests comparing scheduler strategies,
 * sync strategies, and execution modes.
 */

#include "benchmark_utils.hpp"
#include "scheduler_strategies.hpp"
#include "sync_strategies.hpp"
#include <benchmark/benchmark.h>

namespace ai_pipe::benchmark {

// =============================================================================
// Scheduler Strategy Comparisons
// =============================================================================

/**
 * @brief Compare scheduler strategies in batch-like workload
 */
static void BM_Strategy_Scheduler_Batch(::benchmark::State &state) {
  const auto strategy_type = state.range(0); // 0=Batch, 1=Stream, 2=Hybrid
  const std::size_t workers = 4;
  const std::size_t depth = 8;

  auto topology = buildLinearPipeline(depth);

  std::unique_ptr<ExecutionEngine> engine;
  std::string strategy_name;

  switch (strategy_type) {
  case 0:
    engine = createBenchmarkBatchEngine(workers);
    strategy_name = "Batch";
    break;
  case 1:
    engine = createBenchmarkStreamEngine(workers, 32, false);
    strategy_name = "Stream";
    break;
  case 2:
    engine = createBenchmarkHybridEngine(workers, 32);
    strategy_name = "Hybrid";
    break;
  default:
    engine = createBenchmarkBatchEngine(workers);
    strategy_name = "Batch";
  }

  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    auto input = createSourceInput(0, 1024);
    engine->execute(input, true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations());
  state.SetLabel(strategy_name);
}

BENCHMARK(BM_Strategy_Scheduler_Batch)
    ->Args({0}) // Batch
    ->Args({1}) // Stream
    ->Args({2}) // Hybrid
    ->Unit(::benchmark::kMicrosecond);

/**
 * @brief Compare scheduler strategies in streaming workload
 */
static void BM_Strategy_Scheduler_Stream(::benchmark::State &state) {
  const auto strategy_type = state.range(0);
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t frames = 500;

  auto topology = buildLinearPipeline(depth);

  std::unique_ptr<ExecutionEngine> engine;
  std::string strategy_name;

  auto config = EngineConfig::stream(workers, 32);
  engine = ExecutionEngine::create(config);

  switch (strategy_type) {
  case 0:
    engine->setSchedulerStrategy(std::make_unique<BatchSchedulerStrategy>());
    strategy_name = "BatchScheduler";
    break;
  case 1:
    engine->setSchedulerStrategy(std::make_unique<StreamSchedulerStrategy>());
    strategy_name = "StreamScheduler";
    break;
  case 2:
    engine->setSchedulerStrategy(std::make_unique<HybridSchedulerStrategy>());
    strategy_name = "HybridScheduler";
    break;
  default:
    strategy_name = "Default";
  }

  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);
      std::this_thread::sleep_for(std::chrono::microseconds{100});
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);

    auto stats = sink->getLatencyStats();
    recordLatencyCounters(state, stats);

    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames));
  state.SetLabel(strategy_name);
}

BENCHMARK(BM_Strategy_Scheduler_Stream)
    ->Args({0})
    ->Args({1})
    ->Args({2})
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// Sync Strategy Comparisons
// =============================================================================

/**
 * @brief Compare sync strategies in fork-join topology
 */
static void BM_Strategy_Sync_ForkJoin(::benchmark::State &state) {
  const auto sync_type = state.range(0); // 0=NoSync, 1=JoinAware
  const auto branches = static_cast<std::size_t>(state.range(1));
  const std::size_t workers = 8;
  const auto branch_delay = std::chrono::microseconds{100};

  auto topology = buildDiamondPipeline(branches, branch_delay);

  auto config = EngineConfig::stream(workers, 32);
  auto engine = ExecutionEngine::create(config);

  std::string sync_name;
  if (sync_type == 0) {
    engine->setSyncStrategy(createNoSyncStrategy());
    sync_name = "NoSync";
  } else {
    engine->setSyncStrategy(createJoinAwareSyncStrategy());
    sync_name = "JoinAware";
  }

  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());
  const std::size_t frames = 200;

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);
      std::this_thread::sleep_for(std::chrono::microseconds{200});
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);

    auto stats = sink->getLatencyStats();
    recordLatencyCounters(state, stats);

    auto engine_stats = engine->statistics();
    state.counters["drop_rate"] = engine_stats.drop_rate;

    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames));
  state.counters["branches"] = static_cast<double>(branches);
  state.SetLabel(sync_name + "_" + std::to_string(branches) + "branches");
}

BENCHMARK(BM_Strategy_Sync_ForkJoin)
    ->Args({0, 2}) // NoSync, 2 branches
    ->Args({1, 2}) // JoinAware, 2 branches
    ->Args({0, 4}) // NoSync, 4 branches
    ->Args({1, 4}) // JoinAware, 4 branches
    ->Args({0, 8}) // NoSync, 8 branches
    ->Args({1, 8}) // JoinAware, 8 branches
    ->Unit(::benchmark::kMillisecond);

/**
 * @brief Compare sync strategies with unbalanced branch delays
 */
static void BM_Strategy_Sync_Unbalanced(::benchmark::State &state) {
  const auto sync_type = state.range(0);
  const auto slow_delay_us = static_cast<std::size_t>(state.range(1));
  const std::size_t workers = 8;
  const std::size_t branches = 4;

  // Build custom unbalanced diamond
  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source", 1024);
  result.nodes.push_back(source);
  result.graph->addNode(source);
  result.source_node = "source";

  auto fork = std::make_shared<FanOutNode>("fork", branches);
  result.nodes.push_back(fork);
  result.graph->addNode(fork);
  result.graph->addEdge("source", "output", "fork", "input");

  // Create unbalanced branches: first branch is slow, others are fast
  for (std::size_t i = 0; i < branches; ++i) {
    std::string name = "branch_" + std::to_string(i);
    auto delay = (i == 0) ? std::chrono::microseconds{slow_delay_us}
                          : std::chrono::microseconds{100};
    auto branch = std::make_shared<DelayNode>(name, delay);
    result.nodes.push_back(branch);
    result.graph->addNode(branch);
    result.graph->addEdge("fork", "output_" + std::to_string(i), name, "input");
  }

  auto join = std::make_shared<AggregatorNode>("join", branches);
  result.nodes.push_back(join);
  result.graph->addNode(join);
  for (std::size_t i = 0; i < branches; ++i) {
    result.graph->addEdge("branch_" + std::to_string(i), "output", "join",
                          "input_" + std::to_string(i));
  }

  auto sink = std::make_shared<SinkNode>("sink");
  result.nodes.push_back(sink);
  result.graph->addNode(sink);
  result.graph->addEdge("join", "output", "sink", "input");

  auto config = EngineConfig::stream(workers, 32);
  auto engine = ExecutionEngine::create(config);

  if (sync_type == 0) {
    engine->setSyncStrategy(createNoSyncStrategy());
  } else {
    engine->setSyncStrategy(createJoinAwareSyncStrategy());
  }

  engine->initialize(result.graph.get(), workers);

  const std::size_t frames = 150;

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(result.source_node, "output", packet);
      std::this_thread::sleep_for(std::chrono::microseconds{200});
    }

    engine->waitForDrain(0, std::chrono::milliseconds{10000});
    engine->stopStreaming(true);

    auto stats = engine->statistics();
    state.counters["processed"] =
        static_cast<double>(stats.total_frames_processed);
    state.counters["dropped"] = static_cast<double>(stats.total_frames_dropped);

    engine->reset();
  }

  state.counters["slow_delay_us"] = static_cast<double>(slow_delay_us);
}

BENCHMARK(BM_Strategy_Sync_Unbalanced)
    ->Args({0, 100})  // NoSync, 100us slow
    ->Args({1, 100})  // JoinAware, 100us slow
    ->Args({0, 500})  // NoSync, 500us slow
    ->Args({1, 500})  // JoinAware, 500us slow
    ->Args({0, 1000}) // NoSync, 1ms slow
    ->Args({1, 1000}) // JoinAware, 1ms slow
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// Strategy Overhead Measurement
// =============================================================================

/**
 * @brief Measure pure sync strategy overhead
 */
static void BM_Strategy_SyncOverhead(::benchmark::State &state) {
  const auto sync_type = state.range(0);
  const std::size_t branches = 8;

  auto topology = buildDiamondPipeline(branches, std::chrono::microseconds{0});

  std::unique_ptr<ISyncStrategy> sync;
  if (sync_type == 0) {
    sync = createNoSyncStrategy();
  } else {
    sync = createJoinAwareSyncStrategy();
  }

  // Measure initialization time
  for (auto _ : state) {
    sync->initialize(topology.graph.get());
    sync->reset();
  }

  state.counters["sync_type"] = static_cast<double>(sync_type);
  state.counters["branches"] = static_cast<double>(branches);
}

BENCHMARK(BM_Strategy_SyncOverhead)
    ->Args({0}) // NoSync
    ->Args({1}) // JoinAware
    ->Unit(::benchmark::kNanosecond);

/**
 * @brief Measure scheduler decision overhead
 */
static void BM_Strategy_SchedulerDecisionOverhead(::benchmark::State &state) {
  const auto strategy_type = state.range(0);

  std::unique_ptr<ISchedulerStrategy> scheduler;
  switch (strategy_type) {
  case 0:
    scheduler = std::make_unique<BatchSchedulerStrategy>();
    break;
  case 1:
    scheduler = std::make_unique<StreamSchedulerStrategy>();
    break;
  case 2:
    scheduler = std::make_unique<HybridSchedulerStrategy>();
    break;
  default:
    scheduler = std::make_unique<BatchSchedulerStrategy>();
  }

  // Create mock scheduling context with a real node
  auto mock_node = std::make_shared<PassthroughNode>("test_node");

  SchedulingContext ctx;
  ctx.node = mock_node;
  ctx.is_source_node = false;
  ctx.has_initial_input = false;
  ctx.expected_input_ports = {"input_0", "input_1", "input_2"};
  ctx.ready_input_ports = {"input_0", "input_1"};
  ctx.execution_count = 5;
  ctx.last_execution_time =
      std::chrono::steady_clock::now() - std::chrono::milliseconds{10};

  for (auto _ : state) {
    ::benchmark::DoNotOptimize(scheduler->shouldSchedule(ctx));
  }

  state.counters["strategy_type"] = static_cast<double>(strategy_type);
}

BENCHMARK(BM_Strategy_SchedulerDecisionOverhead)
    ->Args({0}) // Batch
    ->Args({1}) // Stream
    ->Args({2}) // Hybrid
    ->Unit(::benchmark::kNanosecond);

// =============================================================================
// Combined Strategy Analysis
// =============================================================================

/**
 * @brief Compare all scheduler + sync combinations
 */
static void BM_Strategy_Combinations(::benchmark::State &state) {
  const auto scheduler_type = state.range(0);
  const auto sync_type = state.range(1);
  const std::size_t workers = 4;
  const std::size_t branches = 4;
  const std::size_t frames = 200;

  auto topology =
      buildDiamondPipeline(branches, std::chrono::microseconds{100});

  auto config = EngineConfig::stream(workers, 32);
  auto engine = ExecutionEngine::create(config);

  // Set scheduler
  switch (scheduler_type) {
  case 0:
    engine->setSchedulerStrategy(std::make_unique<BatchSchedulerStrategy>());
    break;
  case 1:
    engine->setSchedulerStrategy(std::make_unique<StreamSchedulerStrategy>());
    break;
  case 2:
    engine->setSchedulerStrategy(std::make_unique<HybridSchedulerStrategy>());
    break;
  }

  // Set sync
  if (sync_type == 0) {
    engine->setSyncStrategy(createNoSyncStrategy());
  } else {
    engine->setSyncStrategy(createJoinAwareSyncStrategy());
  }

  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);
      std::this_thread::sleep_for(std::chrono::microseconds{150});
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);

    auto stats = sink->getLatencyStats();
    recordLatencyCounters(state, stats);

    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames));
  state.counters["scheduler"] = static_cast<double>(scheduler_type);
  state.counters["sync"] = static_cast<double>(sync_type);
}

BENCHMARK(BM_Strategy_Combinations)
    ->Args({0, 0}) // Batch + NoSync
    ->Args({0, 1}) // Batch + JoinAware
    ->Args({1, 0}) // Stream + NoSync
    ->Args({1, 1}) // Stream + JoinAware
    ->Args({2, 0}) // Hybrid + NoSync
    ->Args({2, 1}) // Hybrid + JoinAware
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// Execution Mode Comparison
// =============================================================================

/**
 * @brief Direct comparison of execution modes (batch vs stream vs hybrid)
 */
static void BM_Strategy_ExecutionMode(::benchmark::State &state) {
  const auto mode = static_cast<ExecutionMode>(state.range(0));
  const std::size_t workers = 4;
  const std::size_t depth = 8;
  const auto node_delay = std::chrono::microseconds{50};

  auto topology = buildLinearPipeline(depth, node_delay);

  std::unique_ptr<ExecutionEngine> engine;
  std::string mode_name;

  switch (mode) {
  case ExecutionMode::BATCH:
    engine = createBenchmarkBatchEngine(workers);
    mode_name = "BATCH";
    break;
  case ExecutionMode::STREAM:
    engine = createBenchmarkStreamEngine(workers, 32, true);
    mode_name = "STREAM";
    break;
  case ExecutionMode::HYBRID:
    engine = createBenchmarkHybridEngine(workers, 32);
    mode_name = "HYBRID";
    break;
  }

  engine->initialize(topology.graph.get(), workers);

  if (mode == ExecutionMode::BATCH) {
    // Batch mode: single execution per iteration
    for (auto _ : state) {
      auto input = createSourceInput(0, 1024);
      engine->execute(input, true);
      engine->reset();
    }
  } else {
    // Stream/Hybrid: streaming with multiple frames
    const std::size_t frames = 100;
    for (auto _ : state) {
      engine->startStreaming();

      for (std::size_t i = 0; i < frames; ++i) {
        auto packet = std::make_shared<PortData>();
        packet->id = i;
        (void)engine->pushInput(topology.source_node, "output", packet);
        std::this_thread::sleep_for(std::chrono::microseconds{100});
      }

      engine->waitForDrain(0, std::chrono::milliseconds{5000});
      engine->stopStreaming(true);
      engine->reset();
    }
    state.SetItemsProcessed(state.iterations() *
                            static_cast<std::int64_t>(frames));
  }

  state.SetLabel(mode_name);
}

BENCHMARK(BM_Strategy_ExecutionMode)
    ->Args({0}) // BATCH
    ->Args({1}) // STREAM
    ->Args({2}) // HYBRID
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// StreamScheduler Configuration Impact
// =============================================================================

/**
 * @brief Measure impact of StreamScheduler configuration options
 */
static void BM_Strategy_StreamConfig(::benchmark::State &state) {
  const auto allow_partial = state.range(0);
  const auto min_interval_ms = state.range(1);
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t frames = 300;

  auto topology = buildLinearPipeline(depth);

  StreamSchedulerConfig config;
  config.allow_partial_inputs = (allow_partial != 0);
  config.auto_reschedule = true;
  config.min_interval = std::chrono::milliseconds{min_interval_ms};

  auto engine_config = EngineConfig::stream(workers, 32);
  auto engine = ExecutionEngine::create(engine_config);
  engine->setSchedulerStrategy(
      std::make_unique<StreamSchedulerStrategy>(config));
  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);
      std::this_thread::sleep_for(std::chrono::microseconds{50});
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);

    auto stats = sink->getLatencyStats();
    recordLatencyCounters(state, stats);

    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames));
  state.counters["allow_partial"] = static_cast<double>(allow_partial);
  state.counters["min_interval_ms"] = static_cast<double>(min_interval_ms);
}

BENCHMARK(BM_Strategy_StreamConfig)
    ->Args({0, 0})  // No partial, no interval
    ->Args({1, 0})  // Allow partial, no interval
    ->Args({0, 10}) // No partial, 10ms interval
    ->Args({1, 10}) // Allow partial, 10ms interval
    ->Unit(::benchmark::kMillisecond);

} // namespace ai_pipe::benchmark
