/**
 * @file config.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-08-18
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef _AI_PIPE_CONFIG_HPP__
#define _AI_PIPE_CONFIG_HPP__

#include <cstdint>
#include <string>

namespace ai_pipe {

struct PipelineConfig {
  std::string graphConfigPath;
  uint8_t numWorkers = 4;
};
} // namespace ai_pipe

#endif // AI_PIPE_CONFIG_