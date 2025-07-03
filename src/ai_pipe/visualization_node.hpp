#ifndef __VISUALIZATION_NODE_HPP__
#define __VISUALIZATION_NODE_HPP__

#include "node_base.hpp"
#include "node_param_types.hpp"
#include "pipe_types.hpp"

#include <opencv2/opencv.hpp>
namespace ai_pipe {

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
  VisualizationNodeParams params_;
  std::filesystem::path outputDir_;
};

} // namespace ai_pipe

#endif // __VISUALIZATION_NODE_HPP__
