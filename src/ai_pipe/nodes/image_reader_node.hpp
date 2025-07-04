/**
 * @file image_reader_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-22
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __IMAGE_READER_NODE_HPP__
#define __IMAGE_READER_NODE_HPP__

#include "node_base.hpp"
#include "node_param_types.hpp"
#include "pipe_types.hpp"

#include <opencv2/opencv.hpp>

namespace ai_pipe {

class ImageReaderNode : public NodeBase {
public:
  ImageReaderNode(const std::string &name, const ImageReaderNodeParams &params);
  ~ImageReaderNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  cv::Mat convertImageColorType(const cv::Mat &image) const;

private:
  uint64_t m_frameIndex_;
  ImageReaderNodeParams params_;
};

} // namespace ai_pipe

#endif // __IMAGE_READER_NODE_HPP__
