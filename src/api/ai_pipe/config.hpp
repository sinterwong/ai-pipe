/**
 * @file config.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Pipeline configuration structures
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

/**
 * @brief Configuration for Pipeline initialization
 *
 * This structure holds configuration parameters that control
 * Pipeline behavior and engine selection.
 */
struct PipelineConfig {
  /**
   * @brief Type of execution engine to use
   *
   * Specifies which ExecutionEngine implementation to instantiate.
   * Examples: "DefaultExecutionEngine", "BackpressureEngine", etc.
   * If empty, defaults to "DefaultExecutionEngine".
   */
  std::string engineType = "DefaultExecutionEngine";

  /**
   * @brief Number of worker threads for the execution engine
   *
   * Controls the thread pool size for parallel node execution.
   * Default is 1 (single-threaded execution).
   */
  uint8_t numWorkers = 1;

  /**
   * @brief Default constructor with sensible defaults
   */
  PipelineConfig() = default;

  /**
   * @brief Constructor with parameters
   * @param type The execution engine type name
   * @param workers Number of worker threads
   */
  PipelineConfig(const std::string &type, uint8_t workers = 1)
      : engineType(type), numWorkers(workers) {}
};

} // namespace ai_pipe

#endif // _AI_PIPE_CONFIG_HPP__