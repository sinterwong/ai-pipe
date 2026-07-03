/**
 * @file helper_nodes.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Reusable test node implementations (pass-through, source, sink,
 * failable, join, slow)
 * @version 0.1
 * @date 2026-01-21
 *
 * @copyright Copyright (c) 2026
 *
 */
#ifndef AI_PIPE_UNIT_TEST_HELPER_NODES
#define AI_PIPE_UNIT_TEST_HELPER_NODES

#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/logger.hpp"

using namespace ai_pipe;
using namespace std::chrono_literals;

namespace ai_pipe_unit_test {
class PassThroughNode : public ILogicNode {
public:
  explicit PassThroughNode(const std::string &name,
                           std::chrono::milliseconds delay = 0ms)
      : ILogicNode(name), m_delay(delay) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
      LOG_TRACE_S << m_name << " delaying for " << m_delay.count() << "ms";
    }
    m_processCount.fetch_add(1, std::memory_order_relaxed);

    // Pass through all inputs to outputs
    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int processCount() const {
    return m_processCount.load(std::memory_order_relaxed);
  }

  void resetCount() { m_processCount.store(0, std::memory_order_relaxed); }

private:
  std::chrono::milliseconds m_delay;
  std::atomic<int> m_processCount{0};
};

class SourceNode : public ILogicNode {
public:
  explicit SourceNode(const std::string &name,
                      std::chrono::milliseconds delay = 0ms)
      : ILogicNode(name), m_delay(delay) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
    }

    m_processCount.fetch_add(1, std::memory_order_relaxed);

    auto output = std::make_shared<PortData>();
    output->id = m_nextId.fetch_add(1, std::memory_order_relaxed);

    if (!inputs.empty()) {
      auto it = inputs.begin();
      if (it->second) {
        output->id = it->second->id;
      }
    }

    output->setParam("source", getName());
    outputs["output"] = output;
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int processCount() const {
    return m_processCount.load(std::memory_order_relaxed);
  }

  void resetCount() { m_processCount.store(0, std::memory_order_relaxed); }

private:
  std::chrono::milliseconds m_delay;
  std::atomic<int> m_processCount{0};
  std::atomic<uint64_t> m_nextId{1};
};

class SinkNode : public ILogicNode {
public:
  explicit SinkNode(const std::string &name,
                    std::chrono::milliseconds delay = 0ms)
      : ILogicNode(name), m_delay(delay) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
    }
    m_processCount.fetch_add(1, std::memory_order_relaxed);

    // collect received data
    std::lock_guard<std::mutex> lock(m_mutex);
    for (const auto &[port, data] : inputs) {
      if (data) {
        m_receivedData.push_back(data);
      }
    }

    // mark as sink output
    for (const auto &[port, data] : inputs) {
      outputs[port + "_result"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"input_result"};
  }

  int processCount() const {
    return m_processCount.load(std::memory_order_relaxed);
  }

  std::vector<PortDataPtr> getReceivedData() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_receivedData;
  }

  void resetCount() {
    m_processCount.store(0, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(m_mutex);
    m_receivedData.clear();
  }

private:
  std::chrono::milliseconds m_delay;
  std::atomic<int> m_processCount{0};
  mutable std::mutex m_mutex;
  std::vector<PortDataPtr> m_receivedData;
};

class FailableNode : public ILogicNode {
public:
  explicit FailableNode(const std::string &name, bool should_fail = false)
      : ILogicNode(name), m_shouldFail(should_fail) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    m_processCount.fetch_add(1, std::memory_order_relaxed);

    if (m_shouldFail.load(std::memory_order_relaxed)) {
      throw std::runtime_error("Intentional failure in " + getName());
    }

    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  void setShouldFail(bool fail) {
    m_shouldFail.store(fail, std::memory_order_relaxed);
  }

  int processCount() const {
    return m_processCount.load(std::memory_order_relaxed);
  }

private:
  std::atomic<bool> m_shouldFail;
  std::atomic<int> m_processCount{0};
};

class JoinNode : public ILogicNode {
public:
  explicit JoinNode(const std::string &name,
                    const std::vector<std::string> &input_ports)
      : ILogicNode(name), m_inputPorts(input_ports) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    m_processCount.fetch_add(1, std::memory_order_relaxed);

    // combine all inputs
    auto output = std::make_shared<PortData>();
    output->id = 0;
    for (const auto &[port, data] : inputs) {
      if (data) {
        output->setParam(port, data->id);
        if (data->id > output->id) {
          output->id = data->id;
        }
      }
    }
    outputs["output"] = output;
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return m_inputPorts;
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int processCount() const {
    return m_processCount.load(std::memory_order_relaxed);
  }

private:
  std::vector<std::string> m_inputPorts;
  std::atomic<int> m_processCount{0};
};

class SlowNode : public ILogicNode {
public:
  explicit SlowNode(const std::string &name, std::chrono::milliseconds delay)
      : ILogicNode(name), m_delay(delay) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    std::this_thread::sleep_for(m_delay);
    m_processCount.fetch_add(1, std::memory_order_relaxed);

    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int processCount() const {
    return m_processCount.load(std::memory_order_relaxed);
  }

private:
  std::chrono::milliseconds m_delay;
  std::atomic<int> m_processCount{0};
};

} // namespace ai_pipe_unit_test

#endif