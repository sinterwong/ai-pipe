/**
 * @file pipe_common_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-06-22
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __PIPE_COMMON_TYPES_HPP__
#define __PIPE_COMMON_TYPES_HPP__

#include <opencv2/opencv.hpp>

namespace ai_pipe {
enum class ColorType : uint8_t {
  RGB888 = 0,
  BGR888,
  GRAY,
  YUV,
  YUV_I420,
  YUV_YV12
};
} // namespace ai_pipe

#endif // __PIPE_COMMON_TYPES_HPP__
