/**
 * @file benchmark_nodes.hpp
 * @brief Reusable test node implementations for benchmarking
 *
 * This file provides various test node implementations that simulate
 * different workload patterns for accurate performance measurement.
 */

#ifndef BENCHMARK_NODES_HPP
#define BENCHMARK_NODES_HPP

#include "ai_pipe/data_types.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <mutex>
#include <numeric>
#include <random>
#include <thread>
#include <vector>

namespace ai_pipe::benchmark {

// =============================================================================
// Benchmark Payload
// =============================================================================

/**
 * @brief Configurable payload for benchmark testing
 */
struct BenchmarkPayload {
  std::uint64_t frame_id{0};
  std::chrono::steady_clock::time_point timestamp;
  std::vector<std::uint8_t> data;

  BenchmarkPayload() = default;

  explicit BenchmarkPayload(std::size_t size_bytes)
      : timestamp(std::chrono::steady_clock::now()), data(size_bytes, 0xAB) {}

  BenchmarkPayload(std::uint64_t id, std::size_t size_bytes)
      : frame_id(id), timestamp(std::chrono::steady_clock::now()),
        data(size_bytes, 0xAB) {}
};

// =============================================================================
// Passthrough Node - Minimal Overhead
// =============================================================================

/**
 * @brief Zero-overhead passthrough node for measuring framework overhead
 */
class PassthroughNode : public ILogicNode {
public:
  explicit PassthroughNode(const std::string &name,
                           const std::string &input_port = "input",
                           const std::string &output_port = "output")
      : ILogicNode(name), m_input_port(input_port), m_output_port(output_port) {
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    auto it = inputs.find(m_input_port);
    if (it != inputs.end()) {
      outputs[m_output_port] = it->second;
    }
    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {m_input_port};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {m_output_port};
  }

  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

private:
  std::string m_input_port;
  std::string m_output_port;
  std::atomic<std::uint64_t> m_execution_count{0};
};

// =============================================================================
// Delay Node - IO-Bound Simulation
// =============================================================================

/**
 * @brief Node that simulates IO-bound operations with configurable delay
 */
class DelayNode : public ILogicNode {
public:
  explicit DelayNode(const std::string &name, std::chrono::microseconds delay,
                     const std::string &input_port = "input",
                     const std::string &output_port = "output")
      : ILogicNode(name), m_delay(delay), m_input_port(input_port),
        m_output_port(output_port) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
    }

    auto it = inputs.find(m_input_port);
    if (it != inputs.end()) {
      outputs[m_output_port] = it->second;
    }
    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {m_input_port};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {m_output_port};
  }

  void setDelay(std::chrono::microseconds delay) { m_delay = delay; }
  std::chrono::microseconds delay() const { return m_delay; }
  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

private:
  std::chrono::microseconds m_delay;
  std::string m_input_port;
  std::string m_output_port;
  std::atomic<std::uint64_t> m_execution_count{0};
};

// =============================================================================
// Compute Node - CPU-Bound Simulation
// =============================================================================

/**
 * @brief Node that simulates CPU-intensive operations
 */
class ComputeNode : public ILogicNode {
public:
  explicit ComputeNode(const std::string &name, std::size_t iterations = 10000,
                       const std::string &input_port = "input",
                       const std::string &output_port = "output")
      : ILogicNode(name), m_iterations(iterations), m_input_port(input_port),
        m_output_port(output_port) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    // CPU-intensive work
    volatile double result = 0.0;
    for (std::size_t i = 0; i < m_iterations; ++i) {
      result +=
          std::sin(static_cast<double>(i)) * std::cos(static_cast<double>(i));
    }

    // Prevent optimization
    m_sink = result;

    auto it = inputs.find(m_input_port);
    if (it != inputs.end()) {
      outputs[m_output_port] = it->second;
    }
    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {m_input_port};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {m_output_port};
  }

  void setIterations(std::size_t iterations) { m_iterations = iterations; }
  std::size_t iterations() const { return m_iterations; }
  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

private:
  std::size_t m_iterations;
  std::string m_input_port;
  std::string m_output_port;
  volatile double m_sink{0.0};
  std::atomic<std::uint64_t> m_execution_count{0};
};

// =============================================================================
// Memory Node - Memory Bandwidth Testing
// =============================================================================

/**
 * @brief Node that simulates memory-intensive operations
 */
class MemoryNode : public ILogicNode {
public:
  explicit MemoryNode(const std::string &name,
                      std::size_t buffer_size = 1024 * 1024,
                      std::size_t copy_count = 10,
                      const std::string &input_port = "input",
                      const std::string &output_port = "output")
      : ILogicNode(name), m_buffer_size(buffer_size), m_copy_count(copy_count),
        m_input_port(input_port), m_output_port(output_port),
        m_buffer_a(buffer_size, 0xAA), m_buffer_b(buffer_size, 0xBB) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    // Memory-intensive work
    for (std::size_t i = 0; i < m_copy_count; ++i) {
      std::copy(m_buffer_a.begin(), m_buffer_a.end(), m_buffer_b.begin());
      std::swap(m_buffer_a, m_buffer_b);
    }

    auto it = inputs.find(m_input_port);
    if (it != inputs.end()) {
      outputs[m_output_port] = it->second;
    }
    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {m_input_port};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {m_output_port};
  }

  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

private:
  std::size_t m_buffer_size;
  std::size_t m_copy_count;
  std::string m_input_port;
  std::string m_output_port;
  std::vector<std::uint8_t> m_buffer_a;
  std::vector<std::uint8_t> m_buffer_b;
  std::atomic<std::uint64_t> m_execution_count{0};
};

// =============================================================================
// Fan-Out Node - Fork Point Simulation
// =============================================================================

/**
 * @brief Node that splits input to multiple outputs (fork point)
 */
class FanOutNode : public ILogicNode {
public:
  explicit FanOutNode(const std::string &name, std::size_t output_count,
                      const std::string &input_port = "input")
      : ILogicNode(name), m_input_port(input_port) {
    for (std::size_t i = 0; i < output_count; ++i) {
      m_output_ports.push_back("output_" + std::to_string(i));
    }
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    auto it = inputs.find(m_input_port);
    if (it != inputs.end()) {
      for (const auto &port : m_output_ports) {
        outputs[port] = it->second;
      }
    }
    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {m_input_port};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return m_output_ports;
  }

  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

private:
  std::string m_input_port;
  std::vector<std::string> m_output_ports;
  std::atomic<std::uint64_t> m_execution_count{0};
};

// =============================================================================
// Aggregator Node - Join Point Simulation
// =============================================================================

/**
 * @brief Node that joins multiple inputs (join point)
 */
class AggregatorNode : public ILogicNode {
public:
  explicit AggregatorNode(const std::string &name, std::size_t input_count,
                          const std::string &output_port = "output")
      : ILogicNode(name), m_output_port(output_port) {
    for (std::size_t i = 0; i < input_count; ++i) {
      m_input_ports.push_back("input_" + std::to_string(i));
    }
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    // Simply forward first available input for benchmarking
    for (const auto &port : m_input_ports) {
      auto it = inputs.find(port);
      if (it != inputs.end()) {
        outputs[m_output_port] = it->second;
        break;
      }
    }
    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return m_input_ports;
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {m_output_port};
  }

  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

private:
  std::vector<std::string> m_input_ports;
  std::string m_output_port;
  std::atomic<std::uint64_t> m_execution_count{0};
};

// =============================================================================
// Source Node - Data Generator
// =============================================================================

/**
 * @brief Source node that generates benchmark data
 */
class SourceNode : public ILogicNode {
public:
  explicit SourceNode(const std::string &name, std::size_t payload_size = 1024,
                      const std::string &output_port = "output")
      : ILogicNode(name), m_payload_size(payload_size),
        m_output_port(output_port) {}

  void process(const PortDataMap & /*inputs*/, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    auto packet = std::make_shared<PortData>();
    auto frame_id = m_frame_counter.fetch_add(1, std::memory_order_relaxed);
    packet->id = frame_id;
    packet->setParam("payload", BenchmarkPayload(frame_id, m_payload_size));
    packet->setParam("timestamp", std::chrono::steady_clock::now());

    outputs[m_output_port] = packet;
  }

  std::vector<std::string> getExpectedInputPorts() const override { return {}; }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {m_output_port};
  }

  std::uint64_t frameCount() const {
    return m_frame_counter.load(std::memory_order_relaxed);
  }

  void reset() { m_frame_counter.store(0, std::memory_order_relaxed); }

private:
  std::size_t m_payload_size;
  std::string m_output_port;
  std::atomic<std::uint64_t> m_frame_counter{0};
};

// =============================================================================
// Sink Node - Data Consumer with Latency Tracking
// =============================================================================

/**
 * @brief Sink node that consumes data and tracks latency
 */
class SinkNode : public ILogicNode {
public:
  explicit SinkNode(const std::string &name,
                    const std::string &input_port = "input")
      : ILogicNode(name), m_input_port(input_port) {}

  void process(const PortDataMap &inputs, PortDataMap & /*outputs*/,
               std::shared_ptr<PipelineContext> /*context*/) override {
    auto now = std::chrono::steady_clock::now();

    auto it = inputs.find(m_input_port);
    if (it != inputs.end() && it->second) {
      auto ts =
          it->second->getOptionalParam<std::chrono::steady_clock::time_point>(
              "timestamp");
      if (ts) {
        auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(now - *ts)
                .count();
        std::lock_guard<std::mutex> lock(m_mutex);
        m_latencies.push_back(latency);
      }
    }

    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {m_input_port};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

  // Latency statistics in microseconds
  struct LatencyStats {
    double avg{0};
    double min{0};
    double max{0};
    double p50{0};
    double p99{0};
    std::size_t count{0};
  };

  LatencyStats getLatencyStats() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    LatencyStats stats;
    stats.count = m_latencies.size();

    if (m_latencies.empty()) {
      return stats;
    }

    std::vector<std::int64_t> sorted = m_latencies;
    std::sort(sorted.begin(), sorted.end());

    stats.min = static_cast<double>(sorted.front());
    stats.max = static_cast<double>(sorted.back());
    stats.avg = std::accumulate(sorted.begin(), sorted.end(), 0.0) /
                static_cast<double>(sorted.size());

    auto p50_idx = sorted.size() / 2;
    auto p99_idx = static_cast<std::size_t>(sorted.size() * 0.99);

    stats.p50 = static_cast<double>(sorted[p50_idx]);
    stats.p99 =
        static_cast<double>(sorted[std::min(p99_idx, sorted.size() - 1)]);

    return stats;
  }

  void reset() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_latencies.clear();
    m_execution_count.store(0, std::memory_order_relaxed);
  }

private:
  std::string m_input_port;
  std::atomic<std::uint64_t> m_execution_count{0};
  mutable std::mutex m_mutex;
  std::vector<std::int64_t> m_latencies;
};

// =============================================================================
// Variable Latency Node - Realistic Simulation
// =============================================================================

/**
 * @brief Node with variable latency for realistic simulation
 */
class VariableLatencyNode : public ILogicNode {
public:
  explicit VariableLatencyNode(const std::string &name,
                               std::chrono::microseconds min_delay,
                               std::chrono::microseconds max_delay,
                               const std::string &input_port = "input",
                               const std::string &output_port = "output")
      : ILogicNode(name), m_min_delay(min_delay), m_max_delay(max_delay),
        m_input_port(input_port), m_output_port(output_port),
        m_rng(std::random_device{}()) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> /*context*/) override {
    std::uniform_int_distribution<std::int64_t> dist(m_min_delay.count(),
                                                     m_max_delay.count());
    auto delay = std::chrono::microseconds(dist(m_rng));

    std::this_thread::sleep_for(delay);

    auto it = inputs.find(m_input_port);
    if (it != inputs.end()) {
      outputs[m_output_port] = it->second;
    }
    m_execution_count.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {m_input_port};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {m_output_port};
  }

  std::uint64_t executionCount() const {
    return m_execution_count.load(std::memory_order_relaxed);
  }

private:
  std::chrono::microseconds m_min_delay;
  std::chrono::microseconds m_max_delay;
  std::string m_input_port;
  std::string m_output_port;
  std::mt19937 m_rng;
  std::atomic<std::uint64_t> m_execution_count{0};
};

} // namespace ai_pipe::benchmark

#endif // BENCHMARK_NODES_HPP
