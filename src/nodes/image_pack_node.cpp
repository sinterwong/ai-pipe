
#include "image_pack_node.hpp"
#include "logic_types/pipe_common_types.hpp"
#include "logic_types/pipe_data_types.hpp"

#include <logger.hpp>
#include <mexception.hpp>
#include <opencv2/imgcodecs.hpp>
#include <time_utils.hpp>
namespace ai_pipe {
using namespace common_utils::exception;

ImagePackNode::ImagePackNode(const std::string &name,
                             const ImagePackNodeParams &params)
    : NodeBase(name), mCounter(0), mParams(params) {}

void ImagePackNode::process(const PortDataMap &inputs, PortDataMap &outputs,
                            std::shared_ptr<PipelineContext> context) {
  const std::string inputPortName = getExpectedInputPorts()[0];
  const std::string outputPortName = getExpectedOutputPorts()[0];
  const std::string outputPortNameWithPath = getExpectedOutputPorts()[1];

  if (inputs.find(inputPortName) == inputs.end()) {
    LOG_ERRORS << "ImagePackNode: Missing '" << inputPortName << "' input.";
    throw InvalidValueException("ImagePackNode: Missing '" + inputPortName +
                                "' input.");
  }

  const auto &inputDataPacket = inputs.at(inputPortName);

  cv::Mat imageBgr;
  if (inputDataPacket->has<std::string>("image_path")) {
    const std::string imagePath =
        inputDataPacket->getParam<std::string>("image_path");
    imageBgr = cv::imread(imagePath, cv::IMREAD_COLOR);
    if (imageBgr.empty()) {
      LOG_ERRORS << "ImagePackNode: Failed to read image from path: "
                 << imagePath;
      throw FileOperationException(
          "ImagePackNode: Failed to read image from path: " + imagePath);
    }
  } else if (inputDataPacket->has<cv::Mat>("cv_bgr_frame")) {
    imageBgr = inputDataPacket->getParam<cv::Mat>("cv_bgr_frame");
    if (imageBgr.empty()) {
      LOG_ERRORS << "ImagePackNode: Received empty cv::Mat frame.";
      throw InvalidValueException(
          "ImagePackNode: Received empty cv::Mat frame.");
    }
  } else if (inputDataPacket->has<std::vector<uchar>>("encoded_png_data")) {
    const std::vector<uchar> &encodedData =
        inputDataPacket->getParam<std::vector<uchar>>("encoded_png_data");
    imageBgr = cv::imdecode(encodedData, cv::IMREAD_COLOR);
    if (imageBgr.empty()) {
      LOG_ERRORS << "ImagePackNode: Failed to decode image from encoded data.";
      throw InvalidValueException(
          "ImagePackNode: Failed to decode image from encoded data.");
    }
  } else {
    LOG_ERRORS << "ImagePackNode: Input data type is not supported for "
                  "ImagePackNode. Expected 'image_path', 'cv_bgr_frame', or "
                  "'encoded_png_data'.";
    throw InvalidValueException(
        "ImagePackNode: Input data type is not supported.");
  }

  auto imageFramePacket = std::make_shared<PortData>();
  ImageFrame imageFrame;
  imageFrame.colorType = mParams.colorType;
  imageFrame.data = convertBgrImageToColorType(imageBgr, mParams.colorType);
  imageFrame.timestamp = common_utils::getCurrentTimestamp();
  imageFrame.frameId = mCounter;

  imageFramePacket->setParam<ImageFramePtr>(
      "image_data", std::make_shared<ImageFrame>(imageFrame));
  outputs[outputPortName] = imageFramePacket;

  auto imageFrameRawPacket = std::make_shared<PortData>();
  ImageFrame imageRawFrame;
  imageFrame.colorType = ColorType::BGR888;
  imageFrame.data = imageBgr;
  imageFrame.timestamp = common_utils::getCurrentTimestamp();
  imageFrame.frameId = mCounter;
  imageFrameRawPacket->setParam<ImageFramePtr>(
      "image_data", std::make_shared<ImageFrame>(imageFrame));
  outputs[outputPortNameWithPath] = imageFrameRawPacket;

  mCounter++;
}

std::vector<std::string> ImagePackNode::getExpectedInputPorts() const {
  return {"image_input_path"};
}

std::vector<std::string> ImagePackNode::getExpectedOutputPorts() const {
  return {"image_output_data", "image_output_data_with_path"};
}

cv::Mat
ImagePackNode::convertBgrImageToColorType(const cv::Mat &image,
                                          const ColorType colorType) const {
  cv::Mat convertedImage;
  switch (colorType) {
  case ColorType::RGB888:
    cv::cvtColor(image, convertedImage, cv::COLOR_BGR2RGB);
    break;
  case ColorType::BGR888:
    convertedImage = image;
    break;
  case ColorType::GRAY:
    cv::cvtColor(image, convertedImage, cv::COLOR_BGR2GRAY);
    break;
  case ColorType::YUV:
    cv::cvtColor(image, convertedImage, cv::COLOR_BGR2YUV);
    break;
  case ColorType::YUV_I420:
    cv::cvtColor(image, convertedImage, cv::COLOR_BGR2YUV_I420);
    break;
  default:
    LOG_WARNINGS << "ImagePackNode: Unknown color type, returning original "
                    "image.";
    throw InvalidValueException("ImagePackNode: Unknown color type specified.");
  }
  return convertedImage;
}

} // namespace ai_pipe
