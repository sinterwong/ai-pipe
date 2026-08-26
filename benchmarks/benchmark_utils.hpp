#ifndef BENCHMARK_UTILS_HPP
#define BENCHMARK_UTILS_HPP

#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "benchmark_nodes.hpp"
#include "join_aware_sync_strategy.hpp"

#include <benchmark/benchmark.h>
#include <chrono>
#include <memory>
#include <string>
#include <vector>

namespace ai_pipe::benchmark {

// Topology Builders

// Result of topology building
struct TopologyResult {
  std::unique_ptr<Graph> graph;
  std::vector<std::shared_ptr<ILogicNode>> nodes;
  std::string source_node;
  std::string sink_node;
  std::vector<std::string> source_nodes; // For multi-source topologies
  std::vector<std::string> sink_nodes;   // For multi-sink topologies
};

// Build a linear pipeline: Source → [Node1 → ... → NodeN] → Sink
inline TopologyResult buildLinearPipeline(
    std::size_t depth,
    std::chrono::microseconds node_delay = std::chrono::microseconds{0}) {
  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  // Source
  auto source = std::make_shared<SourceNode>("source", 1024);
  result.nodes.push_back(source);
  result.graph->addNode(source);
  result.source_node = "source";
  result.source_nodes.push_back("source");

  std::string prev_node = "source";

  // Intermediate nodes
  for (std::size_t i = 0; i < depth; ++i) {
    std::string name = "node_" + std::to_string(i);
    std::shared_ptr<ILogicNode> node;

    if (node_delay.count() > 0) {
      node = std::make_shared<DelayNode>(name, node_delay);
    } else {
      node = std::make_shared<PassthroughNode>(name);
    }

    result.nodes.push_back(node);
    result.graph->addNode(node);
    result.graph->addEdge(prev_node, "output", name, "input");
    prev_node = name;
  }

  // Sink
  auto sink = std::make_shared<SinkNode>("sink");
  result.nodes.push_back(sink);
  result.graph->addNode(sink);
  result.graph->addEdge(prev_node, "output", "sink", "input");
  result.sink_node = "sink";
  result.sink_nodes.push_back("sink");

  return result;
}

// Build a compute-intensive linear pipeline
inline TopologyResult buildComputePipeline(std::size_t depth,
                                           std::size_t iterations_per_node) {
  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source", 1024);
  result.nodes.push_back(source);
  result.graph->addNode(source);
  result.source_node = "source";
  result.source_nodes.push_back("source");

  std::string prev_node = "source";

  for (std::size_t i = 0; i < depth; ++i) {
    std::string name = "compute_" + std::to_string(i);
    auto node = std::make_shared<ComputeNode>(name, iterations_per_node);
    result.nodes.push_back(node);
    result.graph->addNode(node);
    result.graph->addEdge(prev_node, "output", name, "input");
    prev_node = name;
  }

  auto sink = std::make_shared<SinkNode>("sink");
  result.nodes.push_back(sink);
  result.graph->addNode(sink);
  result.graph->addEdge(prev_node, "output", "sink", "input");
  result.sink_node = "sink";
  result.sink_nodes.push_back("sink");

  return result;
}

// Build a diamond (fork-join) topology
//
//        Source
//          |
//       [Fork]
//      /  |   \
//     B1  B2 ... Bn
//      \  |   /
//       [Join]
//          |
//        Sink
inline TopologyResult buildDiamondPipeline(
    std::size_t branch_count,
    std::chrono::microseconds branch_delay = std::chrono::microseconds{100}) {
  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  // Source
  auto source = std::make_shared<SourceNode>("source", 1024);
  result.nodes.push_back(source);
  result.graph->addNode(source);
  result.source_node = "source";
  result.source_nodes.push_back("source");

  // Fork node
  auto fork = std::make_shared<FanOutNode>("fork", branch_count);
  result.nodes.push_back(fork);
  result.graph->addNode(fork);
  result.graph->addEdge("source", "output", "fork", "input");

  // Branch nodes
  for (std::size_t i = 0; i < branch_count; ++i) {
    std::string name = "branch_" + std::to_string(i);
    auto branch = std::make_shared<DelayNode>(name, branch_delay);
    result.nodes.push_back(branch);
    result.graph->addNode(branch);
    result.graph->addEdge("fork", "output_" + std::to_string(i), name, "input");
  }

  // Join node
  auto join = std::make_shared<AggregatorNode>("join", branch_count);
  result.nodes.push_back(join);
  result.graph->addNode(join);
  for (std::size_t i = 0; i < branch_count; ++i) {
    result.graph->addEdge("branch_" + std::to_string(i), "output", "join",
                          "input_" + std::to_string(i));
  }

  // Sink
  auto sink = std::make_shared<SinkNode>("sink");
  result.nodes.push_back(sink);
  result.graph->addNode(sink);
  result.graph->addEdge("join", "output", "sink", "input");
  result.sink_node = "sink";
  result.sink_nodes.push_back("sink");

  return result;
}

// Build a multi-stage fork-join pipeline
inline TopologyResult buildMultiStagePipeline(
    std::size_t stages, std::size_t branches_per_stage,
    std::chrono::microseconds branch_delay = std::chrono::microseconds{50}) {
  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  auto source = std::make_shared<SourceNode>("source", 1024);
  result.nodes.push_back(source);
  result.graph->addNode(source);
  result.source_node = "source";
  result.source_nodes.push_back("source");

  std::string prev_node = "source";

  for (std::size_t s = 0; s < stages; ++s) {
    std::string fork_name = "fork_" + std::to_string(s);
    std::string join_name = "join_" + std::to_string(s);

    // Fork
    auto fork = std::make_shared<FanOutNode>(fork_name, branches_per_stage);
    result.nodes.push_back(fork);
    result.graph->addNode(fork);
    result.graph->addEdge(prev_node, "output", fork_name, "input");

    // Branches
    for (std::size_t b = 0; b < branches_per_stage; ++b) {
      std::string branch_name =
          "s" + std::to_string(s) + "_b" + std::to_string(b);
      auto branch = std::make_shared<DelayNode>(branch_name, branch_delay);
      result.nodes.push_back(branch);
      result.graph->addNode(branch);
      result.graph->addEdge(fork_name, "output_" + std::to_string(b),
                            branch_name, "input");
    }

    // Join
    auto join = std::make_shared<AggregatorNode>(join_name, branches_per_stage);
    result.nodes.push_back(join);
    result.graph->addNode(join);
    for (std::size_t b = 0; b < branches_per_stage; ++b) {
      std::string branch_name =
          "s" + std::to_string(s) + "_b" + std::to_string(b);
      result.graph->addEdge(branch_name, "output", join_name,
                            "input_" + std::to_string(b));
    }

    prev_node = join_name;
  }

  auto sink = std::make_shared<SinkNode>("sink");
  result.nodes.push_back(sink);
  result.graph->addNode(sink);
  result.graph->addEdge(prev_node, "output", "sink", "input");
  result.sink_node = "sink";
  result.sink_nodes.push_back("sink");

  return result;
}

// Build parallel independent pipelines
inline TopologyResult buildParallelPipelines(
    std::size_t pipeline_count, std::size_t depth_per_pipeline,
    std::chrono::microseconds node_delay = std::chrono::microseconds{10}) {
  TopologyResult result;
  result.graph = std::make_unique<Graph>();

  for (std::size_t p = 0; p < pipeline_count; ++p) {
    std::string prefix = "p" + std::to_string(p) + "_";

    // Source
    auto source = std::make_shared<SourceNode>(prefix + "source", 1024);
    result.nodes.push_back(source);
    result.graph->addNode(source);
    result.source_nodes.push_back(prefix + "source");

    std::string prev_node = prefix + "source";

    // Chain
    for (std::size_t d = 0; d < depth_per_pipeline; ++d) {
      std::string name = prefix + "node_" + std::to_string(d);
      auto node = std::make_shared<DelayNode>(name, node_delay);
      result.nodes.push_back(node);
      result.graph->addNode(node);
      result.graph->addEdge(prev_node, "output", name, "input");
      prev_node = name;
    }

    // Sink
    auto sink = std::make_shared<SinkNode>(prefix + "sink");
    result.nodes.push_back(sink);
    result.graph->addNode(sink);
    result.graph->addEdge(prev_node, "output", prefix + "sink", "input");
    result.sink_nodes.push_back(prefix + "sink");
  }

  if (!result.source_nodes.empty()) {
    result.source_node = result.source_nodes[0];
    result.sink_node = result.sink_nodes[0];
  }

  return result;
}

// Engine Factories

inline std::unique_ptr<ExecutionEngine>
createBenchmarkBatchEngine(std::uint8_t workers = 4) {
  auto config = EngineConfig::batch(workers);
  config.enable_statistics = true;
  return ExecutionEngine::create(config);
}

inline std::unique_ptr<ExecutionEngine>
createBenchmarkStreamEngine(std::uint8_t workers = 4,
                            std::size_t queue_capacity = 16,
                            bool use_join_aware_sync = true) {

  auto config = EngineConfig::stream(workers, queue_capacity);
  config.enable_statistics = true;
  config.enable_sync_coordination = true;

  auto engine = ExecutionEngine::create(config);

  if (use_join_aware_sync) {
    engine->setSyncStrategy(createJoinAwareSyncStrategy());
  }

  return engine;
}

// Measurement Helpers

// Record throughput metrics
inline void recordThroughput(::benchmark::State &state,
                             std::uint64_t items_processed,
                             std::size_t bytes_per_item = 0) {
  state.SetItemsProcessed(static_cast<std::int64_t>(items_processed));

  if (bytes_per_item > 0) {
    state.SetBytesProcessed(
        static_cast<std::int64_t>(items_processed * bytes_per_item));
  }
}

// Record custom counters
inline void recordCustomCounters(
    ::benchmark::State &state, const std::string &name, double value,
    ::benchmark::Counter::Flags flags = ::benchmark::Counter::kDefaults) {
  state.counters[name] = ::benchmark::Counter(value, flags);
}

// Record latency percentile counters
inline void recordLatencyCounters(::benchmark::State &state,
                                  const SinkNode::LatencyStats &stats) {
  state.counters["latency_avg_us"] = stats.avg;
  state.counters["latency_p50_us"] = stats.p50;
  state.counters["latency_p99_us"] = stats.p99;
  state.counters["latency_max_us"] = stats.max;
}

// Benchmark Argument Generators

// Generate worker count arguments: 1, 2, 4, 8, 16
inline void WorkerCountArgs(::benchmark::internal::Benchmark *b) {
  for (int workers = 1; workers <= 16; workers *= 2) {
    b->Args({workers});
  }
}

// Generate pipeline depth arguments: 1, 2, 4, 8, 16, 32
inline void PipelineDepthArgs(::benchmark::internal::Benchmark *b) {
  for (int depth = 1; depth <= 32; depth *= 2) {
    b->Args({depth});
  }
}

// Generate queue capacity arguments: 8, 16, 32, 64, 128, 256
inline void QueueCapacityArgs(::benchmark::internal::Benchmark *b) {
  for (int capacity = 8; capacity <= 256; capacity *= 2) {
    b->Args({capacity});
  }
}

// Generate payload size arguments: 64B, 256B, 1KB, 4KB, 16KB, 64KB
inline void PayloadSizeArgs(::benchmark::internal::Benchmark *b) {
  for (int size = 64; size <= 65536; size *= 4) {
    b->Args({size});
  }
}

// Generate combined workers x depth arguments
inline void WorkerDepthArgs(::benchmark::internal::Benchmark *b) {
  for (int workers = 1; workers <= 8; workers *= 2) {
    for (int depth = 1; depth <= 16; depth *= 2) {
      b->Args({workers, depth});
    }
  }
}

// Generate branch count arguments for fork-join: 2, 4, 8, 16
inline void BranchCountArgs(::benchmark::internal::Benchmark *b) {
  for (int branches = 2; branches <= 16; branches *= 2) {
    b->Args({branches});
  }
}

// Test Data Helpers

inline PortDataMap createSourceInput(std::uint64_t frame_id = 0,
                                     std::size_t payload_size = 1024) {
  PortDataMap inputs;
  auto packet = std::make_shared<PortData>();
  packet->id = frame_id;
  packet->setParam("payload", BenchmarkPayload(frame_id, payload_size));
  packet->setParam("timestamp", std::chrono::steady_clock::now());
  inputs["input"] = packet;
  return inputs;
}

inline PortDataMap
createMultiSourceInput(const std::vector<std::string> &source_nodes,
                       std::uint64_t frame_id = 0,
                       std::size_t payload_size = 1024) {
  PortDataMap inputs;
  for (const auto &source : source_nodes) {
    auto packet = std::make_shared<PortData>();
    packet->id = frame_id;
    packet->setParam("payload", BenchmarkPayload(frame_id, payload_size));
    packet->setParam("timestamp", std::chrono::steady_clock::now());
    inputs[source + ":input"] = packet;
  }
  return inputs;
}

} // namespace ai_pipe::benchmark

#endif // BENCHMARK_UTILS_HPP
