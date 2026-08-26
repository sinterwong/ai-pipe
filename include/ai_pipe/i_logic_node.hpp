#ifndef AI_PIPE_I_LOGIC_NODE_HPP
#define AI_PIPE_I_LOGIC_NODE_HPP
#include <string>
#include <typeindex>
#include <vector>

#include "ai_pipe/context.hpp"
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/error.hpp"

namespace ai_pipe {

/**
 * Processing stage with named input and output ports.
 *
 * The engine serializes lifecycle hooks and `process()` calls for a given node.
 * Different nodes may execute concurrently, so shared external state still
 * requires synchronization. Inputs provide shared ownership without mutable
 * access; output handles transfer shared ownership to the engine.
 */
class ILogicNode {
public:
  explicit ILogicNode(std::string name) : m_name(std::move(name)) {}
  virtual ~ILogicNode() = default;

  const std::string &getName() const { return m_name; }

  /**
   * Processes one ready input set.
   *
   * The engine reports thrown exceptions as node failures. Implementations may
   * retain the shared context but must not mutate packets in `inputs`.
   */
  virtual void process(const PortDataMap &inputs, PortDataMap &outputs,
                       std::shared_ptr<PipelineContext> context = nullptr) = 0;

  /**
   * Initializes resources once before the first `process()` call.
   *
   * Nodes are initialized in topological order. A failure aborts the run and
   * tears down the successfully initialized prefix in reverse order. The
   * default implementation succeeds without side effects.
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
   * Releases resources acquired by `setup()` after in-flight work completes.
   * Nodes are torn down in reverse topological order. The default is a no-op.
   */
  virtual void teardown() noexcept {}

  /**
   * Flushes buffered output after every input port reaches end of stream.
   *
   * Called at most once per run, after the last packet on the node's
   * last open input port has been processed and before EOS propagates
   * to the node's downstream ports. This is where a node with internal
   * buffering (batch accumulator, temporal smoother, sliding window)
   * emits the residue it would otherwise strand at the end of a finite
   * stream.
   *
   * Anything written to `outputs` is propagated exactly like a
   * process() result: frame identity is inherited, packets are enqueued
   * downstream, and a sink's outputs are collected as pipeline results.
   * Leave @p outputs empty when there is nothing to flush.
   *
   * The engine guarantees this never runs concurrently with process()
   * on the same node, so it may touch the node's internal state freely.
   *
   * Throwing is treated as a node failure (reported through the error
   * callback and statistics) but does NOT stop EOS from propagating -
   * a flush bug must not strand the rest of the graph waiting for an
   * end of stream that never arrives.
   *
   * The default implementation emits nothing.
   *
   * The by-value shared_ptr matches process() and setup(): overriders
   * may keep the context, and a node interface where one hook took a
   * reference and its neighbours took values would be a trap.
   *
   * @see docs/design/eos_flush.md
   */
  virtual void
  onEndOfStream(PortDataMap &outputs,
                // NOLINTNEXTLINE(performance-unnecessary-value-param)
                std::shared_ptr<PipelineContext> context) {
    (void)outputs;
    (void)context;
  }

  /** Declares required input port names; an empty list denotes a source node.
   */
  virtual std::vector<std::string> getExpectedInputPorts() const { return {}; }

  /** Declares output port names; an empty list denotes a sink node. */
  virtual std::vector<std::string> getExpectedOutputPorts() const { return {}; }

  /**
   * Declares the semantic payload type carried on a port.
   *
   * Return `typeid(T)` for the primary payload a port produces or expects.
   * `Graph::addEdge()` rejects a
   * connection whose two endpoints both declare a type and disagree,
   * catching wiring mistakes at build time instead of at runtime inside
   * process().
   *
   * The default `typeid(void)` means untyped and opts the port out of
   * validation; untyped-to-typed connections are always allowed.
   *
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
