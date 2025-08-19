
#include "image_pack_node.hpp"
#include "ai_pipe/exception.hpp"
#include "ai_pipe/node_base.hpp"
#include "ai_pipe/node_registrar.hpp"
#include "common_types.hpp"
#include "data_types.hpp"
#include "time_utils.hpp"
#include <logger.hpp>
#include <nlohmann/json.hpp>
#include <opencv2/imgcodecs.hpp>

namespace ai_pipe::examples {
using namespace exception;

void from_json(const nlohmann::json &j, ImagePackNodeParams &p) {
  if (j.contains("color_type")) {
    std::string color_type_str = j.at("color_type").get<std::string>();
    if (color_type_str == "RGB888") {
      p.colorType = ColorType::RGB888;
    } else if (color_type_str == "BGR888") {
      p.colorType = ColorType::BGR888;
    } else if (color_type_str == "GRAY") {
      p.colorType = ColorType::GRAY;
    } else if (color_type_str == "YUV") {
      p.colorType = ColorType::YUV;
    } else if (color_type_str == "YUV_I420") {
      p.colorType = ColorType::YUV_I420;
    } else if (color_type_str == "YUV_YV12") {
      p.colorType = ColorType::YUV_YV12;
    } else {
      throw std::runtime_error("Unknown color_type: " + color_type_str);
    }
  } else {
    LOG_WARNINGS << "Missing 'color_type' in ImagePackNodeParams JSON. "
                    "Defaulting to BGR888.";
    p.colorType = ColorType::BGR888;
  }
}

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
  imageFrame.timestamp = utils::getCurrentTimestamp();
  imageFrame.frameId = mCounter;

  imageFramePacket->setParam<ImageFramePtr>(
      "image_data", std::make_shared<ImageFrame>(imageFrame));
  outputs[outputPortName] = imageFramePacket;

  auto imageFrameRawPacket = std::make_shared<PortData>();
  ImageFrame imageRawFrame;
  imageFrame.colorType = ColorType::BGR888;
  imageFrame.data = imageBgr;
  imageFrame.timestamp = utils::getCurrentTimestamp();
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

AI_PIPE_REGISTER_NODE(ImagePackNode, ImagePackNodeParams);
} // namespace ai_pipe::examples
