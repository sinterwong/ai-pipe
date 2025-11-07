/**
 * @file node_base.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __PIPE_NODE_BASE_HPP_
#define __PIPE_NODE_BASE_HPP_
#include <string>
#include <vector>

#include "ai_pipe/context.hpp"
#include "ai_pipe/types.hpp"

namespace ai_pipe {

class ILogicNode {
public:
  ILogicNode(const std::string name) : name_(name) {}
  virtual ~ILogicNode() {}

  const std::string &getName() const { return name_; }

  virtual void process(const PortDataMap &inputs, PortDataMap &outputs,
                       std::shared_ptr<PipelineContext> context = nullptr) = 0;

  virtual std::vector<std::string> getExpectedInputPorts() const { return {}; }

  virtual std::vector<std::string> getExpectedOutputPorts() const { return {}; }

protected:
  std::string name_;
};
} // namespace ai_pipe

#endif
