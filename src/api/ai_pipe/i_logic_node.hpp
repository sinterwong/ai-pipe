/**
 * @file i_logic_node.hpp
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
#include <typeindex>
#include <vector>

#include "ai_pipe/context.hpp"
#include "ai_pipe/data_types.hpp"

namespace ai_pipe {

class ILogicNode {
public:
  explicit ILogicNode(std::string name) : m_name(std::move(name)) {}
  virtual ~ILogicNode() = default;

  const std::string &getName() const { return m_name; }

  virtual void process(const PortDataMap &inputs, PortDataMap &outputs,
                       std::shared_ptr<PipelineContext> context = nullptr) = 0;

  virtual std::vector<std::string> getExpectedInputPorts() const { return {}; }

  virtual std::vector<std::string> getExpectedOutputPorts() const { return {}; }

  /**
   * @brief Declare the semantic payload type carried on a port
   *
   * Optional typing hook: return typeid(T) for the primary payload a
   * port produces (output) or expects (input). Graph::addEdge rejects a
   * connection whose two endpoints both declare a type and disagree,
   * catching wiring mistakes at build time instead of at runtime inside
   * process().
   *
   * The default typeid(void) means "untyped" and opts the port out of
   * validation; untyped-to-typed connections are always allowed.
   *
   * @param port_name The port being queried (input or output)
   */
  virtual std::type_index
  portPayloadType(const std::string &port_name) const {
    (void)port_name;
    return typeid(void);
  }

protected:
  std::string m_name;
};
} // namespace ai_pipe

#endif
