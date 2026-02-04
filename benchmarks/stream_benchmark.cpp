/**
 * @file stream_benchmark.cpp
 * @brief Benchmark tests for streaming execution mode
 *
 * This file contains performance tests for streaming scenarios,
 * measuring sustained throughput, latency, and backpressure handling.
 */

#include "benchmark_utils.hpp"
#include <atomic>
#include <benchmark/benchmark.h>
#include <thread>

namespace ai_pipe::benchmark {

// =============================================================================
// Sustained Throughput Benchmarks
// =============================================================================

/**
 * @brief Measure sustained streaming throughput
 */
static void BM_Stream_SustainedThroughput(::benchmark::State &state) {
  const auto workers = static_cast<std::uint8_t>(state.range(0));
  const std::size_t depth = 4;
  const std::size_t frames_per_iter = 1000;
  const std::size_t queue_capacity = 32;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  // Get sink for tracking
  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    // Push frames
    for (std::size_t i = 0; i < frames_per_iter; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);
    }

    // Wait for drain
    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames_per_iter));
  state.counters["workers"] = static_cast<double>(workers);
  state.counters["frames_per_iter"] = static_cast<double>(frames_per_iter);

  // Record latency stats
  auto stats = sink->getLatencyStats();
  recordLatencyCounters(state, stats);
}

BENCHMARK(BM_Stream_SustainedThroughput)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Unit(::benchmark::kMillisecond)
    ->MinTime(3.0);

// =============================================================================
// Queue Capacity Impact
// =============================================================================

/**
 * @brief Measure impact of queue capacity on throughput
 */
static void BM_Stream_QueueCapacity(::benchmark::State &state) {
  const auto queue_capacity = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t frames = 500;
  const auto delay = std::chrono::microseconds{50};

  auto topology = buildLinearPipeline(depth, delay);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    std::atomic<std::size_t> pushed{0};
    std::atomic<std::size_t> dropped{0};

    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());

      auto result = engine->pushInput(topology.source_node, "output", packet);
      if (result.isOk()) {
        pushed.fetch_add(1, std::memory_order_relaxed);
      }
      if (result.isDropped()) {
        dropped.fetch_add(1, std::memory_order_relaxed);
      }
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);
    engine->reset();

    state.counters["pushed"] = static_cast<double>(pushed.load());
    state.counters["dropped"] = static_cast<double>(dropped.load());
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames));
  state.counters["queue_capacity"] = static_cast<double>(queue_capacity);
}

BENCHMARK(BM_Stream_QueueCapacity)
    ->Apply(QueueCapacityArgs)
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// End-to-End Latency
// =============================================================================

/**
 * @brief Measure end-to-end latency distribution
 */
static void BM_Stream_EndToEndLatency(::benchmark::State &state) {
  const auto depth = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;
  const std::size_t queue_capacity = 16;
  const std::size_t frames = 200;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
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

      // Small delay to prevent queue flooding
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
  state.counters["depth"] = static_cast<double>(depth);
}

BENCHMARK(BM_Stream_EndToEndLatency)
    ->Args({1})
    ->Args({4})
    ->Args({8})
    ->Args({16})
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// Backpressure Benchmarks
// =============================================================================

/**
 * @brief Measure behavior under backpressure (producer faster than consumer)
 */
static void BM_Stream_Backpressure(::benchmark::State &state) {
  const auto consumer_delay_us = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t queue_capacity = 16;
  const std::size_t frames = 300;

  auto topology =
      buildLinearPipeline(depth, std::chrono::microseconds{consumer_delay_us});
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  engine->setDropCallback(
      [&state](const std::string &, std::uint64_t, const std::string &) {
        state.counters["drops"]++;
      });

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  for (auto _ : state) {
    sink->reset();
    state.counters["drops"] = 0;
    engine->startStreaming();

    // Fast producer
    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);

      // Much faster than consumer
      std::this_thread::sleep_for(std::chrono::microseconds{10});
    }

    engine->waitForDrain(0, std::chrono::milliseconds{10000});
    engine->stopStreaming(true);
  }

  auto stats = engine->statistics();
  state.counters["processed"] =
      static_cast<double>(stats.total_frames_processed);
  state.counters["drop_rate"] = stats.drop_rate;
  state.counters["consumer_delay_us"] = static_cast<double>(consumer_delay_us);
  engine->reset();
}

BENCHMARK(BM_Stream_Backpressure)
    ->Args({100})  // 100us delay
    ->Args({500})  // 500us delay
    ->Args({1000}) // 1ms delay
    ->Args({5000}) // 5ms delay
    ->Unit(::benchmark::kMillisecond)
    ->MinTime(2.0);

// =============================================================================
// Multi-Producer Benchmarks
// =============================================================================

/**
 * @brief Measure performance with multiple concurrent producers
 */
static void BM_Stream_MultiProducer(::benchmark::State &state) {
  const auto producer_count = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 8;
  const std::size_t depth = 4;
  const std::size_t queue_capacity = 64;
  const std::size_t frames_per_producer = 200;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    std::vector<std::thread> producers;
    std::atomic<std::size_t> total_pushed{0};

    for (std::size_t p = 0; p < producer_count; ++p) {
      producers.emplace_back([&, p]() {
        for (std::size_t i = 0; i < frames_per_producer; ++i) {
          auto packet = std::make_shared<PortData>();
          packet->id = p * frames_per_producer + i;
          packet->setParam("timestamp", std::chrono::steady_clock::now());

          auto result =
              engine->pushInput(topology.source_node, "output", packet);
          if (result.isOk()) {
            total_pushed.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
    }

    for (auto &t : producers) {
      t.join();
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);
    engine->reset();

    state.counters["total_pushed"] = static_cast<double>(total_pushed.load());
  }

  auto total_frames = producer_count * frames_per_producer;
  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(total_frames));
  state.counters["producers"] = static_cast<double>(producer_count);
}

BENCHMARK(BM_Stream_MultiProducer)
    ->Args({1})
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// Variable Rate Benchmarks
// =============================================================================

/**
 * @brief Measure streaming at various target frame rates
 */
static void BM_Stream_VariableRate(::benchmark::State &state) {
  const auto target_fps = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t queue_capacity = 32;
  const std::size_t duration_ms = 1000;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());

  auto frame_interval = std::chrono::microseconds{1000000 / target_fps};
  auto expected_frames = target_fps * duration_ms / 1000;

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    auto start = std::chrono::steady_clock::now();
    std::size_t frame_count = 0;

    while (std::chrono::steady_clock::now() - start <
           std::chrono::milliseconds{duration_ms}) {
      auto packet = std::make_shared<PortData>();
      packet->id = frame_count++;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);

      std::this_thread::sleep_for(frame_interval);
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);

    auto elapsed = std::chrono::steady_clock::now() - start;
    auto actual_fps = static_cast<double>(frame_count) /
                      std::chrono::duration<double>(elapsed).count();

    state.counters["actual_fps"] = actual_fps;
    state.counters["frames_sent"] = static_cast<double>(frame_count);

    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(expected_frames));
  state.counters["target_fps"] = static_cast<double>(target_fps);
}

BENCHMARK(BM_Stream_VariableRate)
    ->Args({30})   // 30 FPS
    ->Args({60})   // 60 FPS
    ->Args({120})  // 120 FPS
    ->Args({500})  // 500 FPS
    ->Args({1000}) // 1000 FPS
    ->Unit(::benchmark::kMillisecond)
    ->MinTime(2.0);

// =============================================================================
// Fork-Join Streaming
// =============================================================================

/**
 * @brief Measure fork-join streaming with synchronization
 */
static void BM_Stream_ForkJoin_Sync(::benchmark::State &state) {
  const auto branches = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 8;
  const std::size_t queue_capacity = 32;
  const std::size_t frames = 300;
  const auto branch_delay = std::chrono::microseconds{100};

  auto topology = buildDiamondPipeline(branches, branch_delay);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
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

      std::this_thread::sleep_for(std::chrono::microseconds{200});
    }

    engine->waitForDrain(0, std::chrono::milliseconds{5000});
    engine->stopStreaming(true);

    auto stats = sink->getLatencyStats();
    recordLatencyCounters(state, stats);

    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames));
  state.counters["branches"] = static_cast<double>(branches);
}

BENCHMARK(BM_Stream_ForkJoin_Sync)
    ->Args({2})
    ->Args({4})
    ->Args({8})
    ->Unit(::benchmark::kMillisecond);

// =============================================================================
// Long Running Stability
// =============================================================================

/**
 * @brief Measure stability over long running streaming
 */
static void BM_Stream_LongRunning(::benchmark::State &state) {
  const auto duration_seconds = static_cast<std::size_t>(state.range(0));
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t queue_capacity = 32;
  const std::size_t target_fps = 100;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  auto sink = std::dynamic_pointer_cast<SinkNode>(topology.nodes.back());
  auto frame_interval = std::chrono::microseconds{1000000 / target_fps};

  for (auto _ : state) {
    sink->reset();
    engine->startStreaming();

    auto start = std::chrono::steady_clock::now();
    std::size_t frame_count = 0;

    while (std::chrono::steady_clock::now() - start <
           std::chrono::seconds{duration_seconds}) {
      auto packet = std::make_shared<PortData>();
      packet->id = frame_count++;
      packet->setParam("timestamp", std::chrono::steady_clock::now());
      (void)engine->pushInput(topology.source_node, "output", packet);

      std::this_thread::sleep_for(frame_interval);
    }

    engine->waitForDrain(0, std::chrono::milliseconds{10000});
    engine->stopStreaming(true);

    auto stats = engine->statistics();
    state.counters["total_processed"] =
        static_cast<double>(stats.total_frames_processed);
    state.counters["success_rate"] = stats.success_rate;
    state.counters["throughput_fps"] = stats.throughput;

    auto latency_stats = sink->getLatencyStats();
    recordLatencyCounters(state, latency_stats);

    engine->reset();
  }

  state.counters["duration_seconds"] = static_cast<double>(duration_seconds);
}

BENCHMARK(BM_Stream_LongRunning)
    ->Args({5})
    ->Args({10})
    ->Args({30})
    ->Unit(::benchmark::kSecond)
    ->Iterations(1);

// =============================================================================
// Start/Stop Overhead
// =============================================================================

/**
 * @brief Measure streaming start/stop overhead
 */
static void BM_Stream_StartStopOverhead(::benchmark::State &state) {
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t queue_capacity = 16;
  const std::size_t frames_per_cycle = 50;

  auto topology = buildLinearPipeline(depth);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  for (auto _ : state) {
    engine->startStreaming();

    for (std::size_t i = 0; i < frames_per_cycle; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      (void)engine->pushInput(topology.source_node, "output", packet);
    }

    engine->stopStreaming(true);
    engine->reset();
  }

  state.SetItemsProcessed(state.iterations() *
                          static_cast<std::int64_t>(frames_per_cycle));
}

BENCHMARK(BM_Stream_StartStopOverhead)
    ->Unit(::benchmark::kMillisecond)
    ->MinTime(2.0);

// =============================================================================
// Queue Depth Under Load
// =============================================================================

/**
 * @brief Measure queue depth behavior under varying load
 */
static void BM_Stream_QueueDepthUnderLoad(::benchmark::State &state) {
  const auto load_factor = state.range(0) / 10.0; // 0.5, 1.0, 1.5, 2.0, 3.0
  const std::size_t workers = 4;
  const std::size_t depth = 4;
  const std::size_t queue_capacity = 32;
  const auto consumer_delay = std::chrono::microseconds{100};
  const std::size_t frames = 500;

  auto topology = buildLinearPipeline(depth, consumer_delay);
  auto engine = createBenchmarkStreamEngine(workers, queue_capacity, true);
  engine->initialize(topology.graph.get(), workers);

  // Producer delay = consumer_delay / load_factor
  auto producer_delay = std::chrono::microseconds{
      static_cast<std::int64_t>(consumer_delay.count() / load_factor)};

  for (auto _ : state) {
    engine->startStreaming();

    std::size_t max_queue_depth = 0;
    std::size_t total_queue_samples = 0;
    double avg_queue_depth = 0;

    for (std::size_t i = 0; i < frames; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = i;
      (void)engine->pushInput(topology.source_node, "output", packet);

      std::string monitor_node = (topology.nodes.size() > 1)
                                     ? topology.nodes[1]->getName()
                                     : topology.sink_node;
      auto current_depth = engine->queueDepth(monitor_node, "input");
      max_queue_depth = std::max(max_queue_depth, current_depth);
      avg_queue_depth += static_cast<double>(current_depth);
      total_queue_samples++;

      std::this_thread::sleep_for(producer_delay);
    }

    engine->waitForDrain(0, std::chrono::milliseconds{10000});
    engine->stopStreaming(true);

    state.counters["max_queue_depth"] = static_cast<double>(max_queue_depth);
    state.counters["avg_queue_depth"] =
        avg_queue_depth / static_cast<double>(total_queue_samples);

    engine->reset();
  }

  state.counters["load_factor"] = load_factor;
  state.counters["producer_delay_us"] =
      static_cast<double>(producer_delay.count());
}

BENCHMARK(BM_Stream_QueueDepthUnderLoad)
    ->Args({5})  // 0.5x load
    ->Args({10}) // 1.0x load
    ->Args({15}) // 1.5x load
    ->Args({20}) // 2.0x load
    ->Args({30}) // 3.0x load
    ->Unit(::benchmark::kMillisecond);

} // namespace ai_pipe::benchmark
