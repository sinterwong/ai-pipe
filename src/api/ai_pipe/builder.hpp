/**
 * @file builder.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-09-01
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_PIPELINE_BUILDER_HPP
#define AI_PIPE_PIPELINE_BUILDER_HPP
#include "ai_pipe/graph.hpp"
#include <string>

namespace ai_pipe {

class PipelineBuilder {
public:
  static Graph buildGraphFromConfig(const std::string &configPath);
};

} // namespace ai_pipe

#endif // __AI_PIPE_PIPELINE_BUILDER_HPP__
