
#include "image_reader_node.hpp"
#include "logic_types/pipe_common_types.hpp"
#include "logic_types/pipe_data_types.hpp"

#include <logger.hpp>
#include <mexception.hpp>
#include <opencv2/imgcodecs.hpp>
#include <time_utils.hpp>
namespace ai_pipe {
using namespace common_utils::exception;

ImageReaderNode::ImageReaderNode(const std::string &name,
                                 const ImageReaderNodeParams &params)
    : NodeBase(name), m_frameIndex_(0), params_(params) {}

void ImageReaderNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                              std::shared_ptr<PipelineContext> context) {
  const std::string inputPortName = getExpectedInputPorts()[0];
  const std::string outputPortName = getExpectedOutputPorts()[0];
  const std::string outputPortNameWithPath = getExpectedOutputPorts()[1];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "ImageReaderNode: Missing '" << inputPortName << "' input.";
    throw InvalidValueException("ImageReaderNode: Missing '" + inputPortName +
                                "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);
  if (!inputDataPacket->has<std::string>("image_path")) {
    LOG_ERRORS << "ImageReaderNode: '" << inputPortName
               << "' input is not a string.";
    throw InvalidValueException("ImageReaderNode: '" + inputPortName +
                                "' input is not a string.");
  }

  const std::string imagePath =
      inputDataPacket->getParam<std::string>("image_path");

  cv::Mat image = cv::imread(imagePath, cv::IMREAD_COLOR);

  if (image.empty()) {
    LOG_ERRORS << "ImageReaderNode: Failed to read image from path: "
               << imagePath;
    throw FileOperationException(
        "ImageReaderNode: Failed to read image from path: " + imagePath);
  }

  auto imageFramePacket = std::make_shared<PortData>();
  ImageFrame imageFrame;
  imageFrame.colorType = params_.colorType;
  imageFrame.data = convertImageColorType(image);
  imageFrame.timestamp = common_utils::getCurrentTimestamp();
  imageFrame.frameId = m_frameIndex_;

  imageFramePacket->setParam<ImageFramePtr>(
      "image_data", std::make_shared<ImageFrame>(imageFrame));
  outputs[outputPortName] = imageFramePacket;

  auto imageFrameRawPacket = std::make_shared<PortData>();
  ImageFrame imageRawFrame;
  imageFrame.colorType = ColorType::BGR888;
  imageFrame.data = image;
  imageFrame.timestamp = common_utils::getCurrentTimestamp();
  imageFrame.frameId = m_frameIndex_;
  imageFrameRawPacket->setParam<ImageFramePtr>(
      "image_data", std::make_shared<ImageFrame>(imageFrame));
  imageFrameRawPacket->setParam<std::string>("image_path", imagePath);
  outputs[outputPortNameWithPath] = imageFrameRawPacket;

  m_frameIndex_++;
}

std::vector<std::string> ImageReaderNode::getExpectedInputPorts() const {
  return {"image_input_path"};
}

std::vector<std::string> ImageReaderNode::getExpectedOutputPorts() const {
  return {"image_output_data", "image_output_data_with_path"};
}

cv::Mat ImageReaderNode::convertImageColorType(const cv::Mat &image) const {
  cv::Mat converted_image;
  switch (params_.colorType) {
  case ColorType::RGB888:
    cv::cvtColor(image, converted_image, cv::COLOR_BGR2RGB);
    break;
  case ColorType::BGR888:
    converted_image = image.clone();
    break;
  case ColorType::GRAY:
    cv::cvtColor(image, converted_image, cv::COLOR_BGR2GRAY);
    break;
  case ColorType::YUV:
    cv::cvtColor(image, converted_image, cv::COLOR_BGR2YUV);
    break;
  case ColorType::YUV_I420:
    cv::cvtColor(image, converted_image, cv::COLOR_BGR2YUV_I420);
    break;
  default:
    LOG_WARNINGS << "ImageReaderNode: Unknown color type, returning original "
                    "image.";
    throw InvalidValueException(
        "ImageReaderNode: Unknown color type specified.");
  }
  return converted_image;
}

} // namespace ai_pipe
