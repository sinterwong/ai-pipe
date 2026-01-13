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
#ifndef AI_PIPE_I_LOGIC_NODE_HPP
#define AI_PIPE_I_LOGIC_NODE_HPP
#include <string>
#include <vector>

#include "ai_pipe/context.hpp"
#include "ai_pipe/types.hpp"

namespace ai_pipe {

class ILogicNode {
public:
  ILogicNode(const std::string name) : m_name(name) {}
  virtual ~ILogicNode() {}

  const std::string &getName() const { return m_name; }

  virtual void process(const PortDataMap &inputs, PortDataMap &outputs,
                       std::shared_ptr<PipelineContext> context = nullptr) = 0;

  virtual std::vector<std::string> getExpectedInputPorts() const { return {}; }

  virtual std::vector<std::string> getExpectedOutputPorts() const { return {}; }

protected:
  std::string m_name;
};
} // namespace ai_pipe

#endif
