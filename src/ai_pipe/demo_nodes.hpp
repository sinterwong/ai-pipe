/**
 * @file demo_nodes.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-02
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __DEMO_NODES_HPP__
#define __DEMO_NODES_HPP__

#include "node_base.hpp"
#include "node_param_types.hpp"

#include <logger.hpp>

namespace ai_pipe {

class DemoSourceNode : public NodeBase {
public:
  DemoSourceNode(const std::string &name, const DemoSourceNodeParams &params)
      : NodeBase(name), params_(params), valueData_(params.source_id) {
    LOG_INFOS << "[" << getName()
              << "] Constructed with source_id: " << params_.source_id;
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context) override {
    if (!inputs.empty()) {
      LOG_ERRORS << "Source node " << name_ << " received unexpected input.";
      throw std::runtime_error("Source node " + name_ +
                               " received unexpected input.");
      return;
    }

    PortData outputPacket0;
    outputPacket0.id = 100000 + valueData_;
    outputPacket0.setParam<int>("original_data", valueData_);
    LOG_INFOS << "[" << getName()
              << "] Emitting data 0: value = " << valueData_;

    PortData outputPacket1;
    outputPacket1.id = 100001 + valueData_;
    outputPacket1.setParam<int>("processed_data", valueData_ + 1);
    LOG_INFOS << "[" << getName()
              << "] Emitting data 1: value = " << (valueData_ + 1);

    valueData_++;

    outputs["demo_source_output_0"] = std::make_shared<PortData>(outputPacket0);
    outputs["demo_source_output_1"] = std::make_shared<PortData>(outputPacket1);
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"demo_source_output_0", "demo_source_output_1"};
  }

  std::vector<std::string> getExpectedInputPorts() const override { return {}; }

private:
  DemoSourceNodeParams params_;
  int valueData_;
};

class DemoProcessingNode : public NodeBase {
public:
  DemoProcessingNode(const std::string &name,
                     const DemoProcessingNodeParams &params)
      : NodeBase(name), params_(params) {
    LOG_INFOS << "[" << getName() << "] Constructed with processing_threshold: "
              << params_.processing_threshold;
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"demo_process_input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"demo_process_output"};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context) override {
    auto it = inputs.find("demo_process_input");
    if (it == inputs.end() || !it->second) {
      LOG_ERRORS << "Processing node " << name_
                 << " missing or invalid input data for 'demo_process_input'.";
      throw std::runtime_error("DemoProcessingNode:" + name_ +
                               " missing input for 'demo_process_input'.");
    }

    const auto inputPacket = it->second;
    PortData outputPacket;
    outputPacket.id = inputPacket->id + 100;

    try {
      auto data = inputPacket->getParam<int>("original_data");
      int processedValue =
          data + static_cast<int>(params_.processing_threshold);

      outputPacket.setParam<int>("processed_data", processedValue);
      outputPacket.setParam<uint64_t>("original_id", inputPacket->id);

      LOG_INFOS << "[" << getName() << "] Input: " << data
                << ", Threshold: " << params_.processing_threshold
                << ", Emitting data: " << processedValue;

      outputs["demo_process_output"] = std::make_shared<PortData>(outputPacket);
    } catch (const std::runtime_error &e) {
      LOG_ERRORS << "[" << getName()
                 << "] Failed to get 'original_data' from input or process: "
                 << e.what();
      throw std::runtime_error("DemoProcessingNode:" + name_ +
                               " failed during processing.");
    }
  }

private:
  DemoProcessingNodeParams params_;
};

class DemoSinkNode : public NodeBase {
public:
  DemoSinkNode(const std::string &name, const DemoSinkNodeParams &params)
      : NodeBase(name), params_(params) {
    // For DemoSinkNode, inputPorts might be part of its configuration or
    // determined dynamically. If determined by params, use params_.input_ports
    // or similar. For now, we'll keep it simple and assume it still needs to
    // know its expected ports. This might need adjustment based on how graph
    // connections define this. For this example, let's assume input_ports are
    // still passed separately or part of a base config. If
    // `params_.expected_input_ports` were a field, you'd use it here. For now,
    // we'll use a fixed list as before, but acknowledge params are available.
    LOG_INFOS << "[" << getName()
              << "] Constructed. Output path (from params): "
              << params_.output_path;
  }

  // The sink node might still need to declare which ports it *can* receive on,
  // even if the actual connections are made by the graph.
  // Let's assume for now the test will connect to "demo_sink_input".
  std::vector<std::string> getExpectedInputPorts() const override {
    // This could be configured via params_ if needed.
    // For example: return params_.expected_ports;
    return {"demo_sink_input_1", "demo_sink_input_2"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context) override {
    if (!outputs.empty()) {
      LOG_ERRORS << "Sink node " << name_ << " produced unexpected output.";
      throw std::runtime_error("Sink node " + name_ +
                               " produced unexpected output.");
      return;
    }
    LOG_INFOS << "[" << getName()
              << "] Processing inputs. Configured output path: "
              << params_.output_path;

    for (const auto &port_pair : inputs) {
      const std::string &portName = port_pair.first;
      const auto &inputPacket = port_pair.second;

      if (!inputPacket) {
        LOG_WARNINGS << "  Port: " << portName
                     << " - Null data packet received.";
        continue;
      }

      LOG_INFOS << "  Port: " << portName << ", Packet ID: " << inputPacket->id;
      try {
        auto processedDataOpt =
            inputPacket->getOptionalParam<int>("processed_data");
        if (processedDataOpt.has_value()) {
          LOG_INFOS << "    Processed Data: " << processedDataOpt.value();
        } else {
          auto originalDataOpt =
              inputPacket->getOptionalParam<int>("original_data");
          if (originalDataOpt.has_value()) {
            LOG_INFOS << "    Original Data: " << originalDataOpt.value();
          } else {
            LOG_INFOS << "    No 'processed_data' or 'original_data' found.";
          }
        }
      } catch (const std::runtime_error &e) {
        LOG_WARNINGS << "    Failed to access data on port " << portName << ": "
                     << e.what();
      }
    }
    // Here you could, for example, write something to params_.output_path
  }

private:
  DemoSinkNodeParams params_;
  // // This might be redundant if defined by graph connections
  // std::vector<std::string> inputPorts_;
};

} // namespace ai_pipe
#endif // __DEMO_NODES_HPP__