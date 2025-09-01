/**
 * @file execution_engine_base.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-09-01
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_I_EXECUTION_ENGINE_HPP
#define AI_PIPE_I_EXECUTION_ENGINE_HPP
#include "ai_pipe/types.hpp"
#include "graph.hpp"
#include <functional>
#include <memory>
#include <string>

namespace ai_pipe {

class IExecutionEngine {
public:
  virtual ~IExecutionEngine() = default;

  virtual bool initialize(Graph *graph, uint8_t numWorkers = 4) = 0;

  virtual bool execute(const PortDataMap &initialInputs,
                       bool waitForCompletion = true,
                       std::shared_ptr<PipelineContext> context = nullptr) = 0;

  virtual void stopExecutionAsync() = 0;

  virtual void stopExecutionSync() = 0;

  virtual void reset() = 0;

  virtual EngineState getState() const = 0;

  virtual void setPipelineResultCallback(
      std::function<void(const PortDataMap &finalResults)> callback) = 0;

  virtual void
  setPipelineErrorCallback(std::function<void(const std::string &errorMsg,
                                              const std::string &nodeName)>
                               callback) = 0;

  virtual std::unordered_map<std::string, NodeExecutionState>
  getNodeStates() const = 0;
};

} // namespace ai_pipe

#endif
