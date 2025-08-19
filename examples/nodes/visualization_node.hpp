#ifndef __VISUALIZATION_NODE_HPP__
#define __VISUALIZATION_NODE_HPP__

#include "ai_core/algo_data_types.hpp"
#include "ai_pipe/node_base.hpp"
#include <filesystem>
#include <nlohmann/json.hpp>
#include <opencv2/opencv.hpp>

namespace ai_pipe::examples {

struct VisualizationNodeParams {
  std::string outputDir;
  std::string prefixName;
};

void from_json(const nlohmann::json &j, VisualizationNodeParams &p);

class VisualizationNode : public NodeBase {
public:
  VisualizationNode(const std::string &name,
                    const VisualizationNodeParams &params);
  ~VisualizationNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  cv::Mat visualizeAccordOutput(ai_core::AlgoOutput &algoOutput,
                                const cv::Mat &originalImage) const;

private:
  VisualizationNodeParams mParams;
  std::filesystem::path mOutputDirPath;
};

} // namespace ai_pipe::examples

#endif // __VISUALIZATION_NODE_HPP__
