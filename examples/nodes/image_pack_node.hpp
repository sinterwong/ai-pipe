/**
 * @file image_pack_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-22
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __IMAGE_PACKING_NODE_HPP__
#define __IMAGE_PACKING_NODE_HPP__

#include "ai_pipe/node_base.hpp"
#include "common_types.hpp"
#include <nlohmann/json.hpp>
#include <opencv2/opencv.hpp>

namespace ai_pipe::examples {

struct ImagePackNodeParams {
  ColorType colorType;
};

void from_json(const nlohmann::json &j, ImagePackNodeParams &p);

class ImagePackNode : public NodeBase {
public:
  ImagePackNode(const std::string &name, const ImagePackNodeParams &params);
  ~ImagePackNode() override = default;

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  std::vector<std::string> getExpectedInputPorts() const override;
  std::vector<std::string> getExpectedOutputPorts() const override;

private:
  cv::Mat convertBgrImageToColorType(const cv::Mat &image,
                                     const ColorType colorType) const;

private:
  uint64_t mCounter;
  ImagePackNodeParams mParams;
};
} // namespace ai_pipe::examples

#endif // __IMAGE_READER_NODE_HPP__
