/**
 * @file benchmark_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Benchmark helper node with configurable processing delay
 * @version 0.1
 * @date 2025-11-15
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_BENCHMARK_NODE_HPP
#define AI_PIPE_BENCHMARK_NODE_HPP

#include "ai_pipe/context.hpp"
#include "ai_pipe/node_base.hpp"
#include "ai_pipe/types.hpp"
#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace ai_pipe::bench {

/**
 * @brief Benchmark node that simulates processing delay
 *
 * This node is used for performance testing. It can simulate
 * different workload intensities by sleeping for a configurable duration.
 */
class BenchmarkNode : public ILogicNode {
public:
  /**
   * @brief Construct a new Benchmark Node
   *
   * @param name Node name
   * @param delayMicros Processing delay in microseconds (default: 0)
   */
  explicit BenchmarkNode(const std::string &name,
                         int64_t delayMicros = 0)
      : ILogicNode(name), m_delayMicros(delayMicros) {}

  ~BenchmarkNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override {
    // Simulate processing delay
    if (m_delayMicros > 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(m_delayMicros));
    }

    // Pass through the input data
    const std::string inputPortName = "input";
    const std::string outputPortName = "output";

    if (inputs.find(inputPortName) != inputs.end()) {
      const auto &inputDataPacket = inputs.at(inputPortName);
      auto outputDataPacket = std::make_shared<PortData>();
      *outputDataPacket = *inputDataPacket;
      outputs[outputPortName] = outputDataPacket;
    } else {
      // Source node - create dummy output
      auto outputDataPacket = std::make_shared<PortData>();
      outputDataPacket->setParam<int>("data", 42);
      outputs[outputPortName] = outputDataPacket;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  /**
   * @brief Set the processing delay
   *
   * @param delayMicros Delay in microseconds
   */
  void setDelay(int64_t delayMicros) { m_delayMicros = delayMicros; }

  /**
   * @brief Get the current processing delay
   *
   * @return int64_t Delay in microseconds
   */
  int64_t getDelay() const { return m_delayMicros; }

private:
  int64_t m_delayMicros; // Processing delay in microseconds
};

} // namespace ai_pipe::bench

#endif // AI_PIPE_BENCHMARK_NODE_HPP
