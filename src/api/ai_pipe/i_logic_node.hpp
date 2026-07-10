/**
 * @file i_logic_node.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Abstract processing node interface: ports, lifecycle, and process()
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
#include "ai_pipe/error.hpp"

namespace ai_pipe {

class ILogicNode {
public:
  explicit ILogicNode(std::string name) : m_name(std::move(name)) {}
  virtual ~ILogicNode() = default;

  const std::string &getName() const { return m_name; }

  virtual void process(const PortDataMap &inputs, PortDataMap &outputs,
                       std::shared_ptr<PipelineContext> context = nullptr) = 0;

  /**
   * @brief One-time initialization before any process() call
   *
   * The engine invokes setup() for every node (in topological order)
   * when execution first starts - the place for expensive work like
   * loading models or allocating device memory, with access to the
   * pipeline context's resources/services. A failure aborts the run
   * and tears down already-set-up nodes in reverse order.
   *
   * Default: no-op success, so existing nodes are unaffected.
   *
   * The by-value shared_ptr is part of the stable public signature
   * (overriders may keep the context); changing it would break every
   * existing node.
   */
  // NOLINTNEXTLINE(performance-unnecessary-value-param)
  virtual Result<void> setup(std::shared_ptr<PipelineContext> context) {
    (void)context;
    return Result<void>::ok();
  }

  /**
   * @brief Release resources acquired in setup()
   *
   * Invoked in reverse topological order on engine reset() and
   * destruction, after all in-flight tasks have completed. Must not
   * throw.
   */
  virtual void teardown() noexcept {}

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
  virtual std::type_index portPayloadType(const std::string &port_name) const {
    (void)port_name;
    return typeid(void);
  }

protected:
  std::string m_name;
};
} // namespace ai_pipe

#endif
