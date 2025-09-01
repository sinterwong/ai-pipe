/**
 * @file pipeline_builder.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-09-01
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_EXAMPLE_PIPELINE_BUILDER_HPP
#define AI_PIPE_EXAMPLE_PIPELINE_BUILDER_HPP
#include "ai_pipe/graph.hpp"
#include "ai_pipe/pipeline.hpp"
#include <string>

namespace ai_pipe::examples {

class PipelineBuilder {
public:
  static Graph buildGraphFromConfig(const std::string &configPath);

  static Pipeline
  buildPipelineFromConfig(const std::string &configPath,
                          std::shared_ptr<PipelineContext> context = nullptr,
                          uint8_t numWorkers = 1);
};

} // namespace ai_pipe::examples

#endif // __AI_PIPE_PIPELINE_BUILDER_HPP__
