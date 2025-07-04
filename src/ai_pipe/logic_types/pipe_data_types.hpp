/**
 * @file pipe_data_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-22
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef __PIPE_DATA_TYPES_HPP__
#define __PIPE_DATA_TYPES_HPP__

#include "pipe_common_types.hpp"
#include <memory>
#include <opencv2/opencv.hpp>

namespace ai_pipe {

struct ImageFrame {
  cv::Mat data;
  ColorType colorType;
  uint64_t timestamp;
  uint32_t frameId;
};

using ImageFramePtr = std::shared_ptr<ImageFrame>;

} // namespace ai_pipe

#endif // __PIPE_DATA_TYPES_HPP__