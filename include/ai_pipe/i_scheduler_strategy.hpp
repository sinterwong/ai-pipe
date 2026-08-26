#ifndef AI_PIPE_I_SCHEDULER_STRATEGY_HPP
#define AI_PIPE_I_SCHEDULER_STRATEGY_HPP

#include "ai_pipe/data_types.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace ai_pipe {

// Forward declaration; full definition in ai_pipe/compiled_graph.hpp
class CompiledGraph;

// Scheduling Types

/**
 * @brief Result of a scheduling decision
 */
enum class ScheduleDecision {
  ScheduleNow,   ///< Node should be scheduled immediately
  WaitForInputs, ///< Wait for more inputs before scheduling
  SkipExecution, ///< Skip this execution cycle
  /**
   * Defer scheduling by ScheduleResult::retry_delay (R3.2): the engine
   * arms its defer timer and re-runs the scheduling decision when the
   * delay expires, so a deferred node is re-evaluated even if no
   * further data event arrives.
   */
  DeferToNextCycle
};

/**
 * @brief Context information for making scheduling decisions
 */
struct SchedulingContext {
  std::shared_ptr<ILogicNode> node;

  /// Number of input ports the node declares
  std::uint32_t expected_input_count{0};

  /// Number of input ports that currently have data queued
  std::uint32_t ready_input_count{0};

  /// Bit i set = the i-th declared input port (in
  /// getExpectedInputPorts() order) has data. Only the first 64 ports
  /// are represented; the counts above are always exact.
  std::uint64_t ready_port_mask{0};

  std::size_t pending_predecessor_count{0};
  bool is_source_node{false};
  bool is_sink_node{false};
  bool has_initial_input{false};
  std::chrono::steady_clock::time_point last_execution_time;
  std::uint64_t execution_count{0};

  [[nodiscard]] bool allInputsReady() const {
    return ready_input_count >= expected_input_count;
  }

  [[nodiscard]] bool hasReadyInput() const { return ready_input_count > 0; }

  [[nodiscard]] double inputReadinessRatio() const {
    if (expected_input_count == 0)
      return 1.0;
    return static_cast<double>(ready_input_count) /
           static_cast<double>(expected_input_count);
  }
};

/**
 * @brief Scheduling result with additional metadata
 */
struct ScheduleResult {
  ScheduleDecision decision{ScheduleDecision::WaitForInputs};
  std::optional<std::chrono::milliseconds> retry_delay;
  std::string reason;

  static ScheduleResult scheduleNow(const std::string &reason = "") {
    return {ScheduleDecision::ScheduleNow, std::nullopt, reason};
  }

  static ScheduleResult waitForInputs(const std::string &reason = "") {
    return {ScheduleDecision::WaitForInputs, std::nullopt, reason};
  }

  static ScheduleResult skip(const std::string &reason = "") {
    return {ScheduleDecision::SkipExecution, std::nullopt, reason};
  }

  static ScheduleResult
  defer(std::chrono::milliseconds delay,
        const std::string &reason = "deferred to next cycle") {
    return {ScheduleDecision::DeferToNextCycle, delay, reason};
  }
};

/**
 * @brief Defines what "completion" means for an execution
 */
enum class CompletionSemantics {
  SinglePass,     ///< Complete when all sinks execute once
  Continuous,     ///< Never complete (streaming mode)
  ConditionBased, ///< Complete when a condition is met
  TimeoutBased    ///< Complete after timeout
};

/**
 * @brief Completion check result
 */
struct CompletionStatus {
  bool is_complete{false};
  std::string reason;
  std::optional<std::chrono::milliseconds> time_to_completion;
};

/**
 * @brief One sink node's execution count for completion checks (R4.5)
 *
 * The name view aliases engine-owned storage and is only valid for the
 * duration of the checkCompletion() call - copy it if a strategy needs
 * to keep it (none of the built-ins do).
 */
struct SinkExecutionCount {
  std::string_view name;
  std::uint64_t executions{0};
};

// Scheduler Strategy Interface

/**
 * @brief Abstract interface for scheduling strategies
 *
 * Strategies determine:
 * - When a node should be scheduled
 * - What "completion" means for the execution
 * - Whether nodes should be rescheduled after completion
 *
 * To create a custom strategy, inherit from this interface and implement
 * all pure virtual methods.
 */
class ISchedulerStrategy {
public:
  virtual ~ISchedulerStrategy() = default;

  /**
   * @brief Decide whether a node should be scheduled
   * @param context Scheduling context with node state
   * @return Schedule result with decision and reason
   */
  [[nodiscard]] virtual ScheduleResult
  shouldSchedule(const SchedulingContext &context) const = 0;

  /**
   * @brief Handle node completion
   * @param node The completed node
   * @param success Whether execution succeeded
   * @param outputs The outputs produced
   * @return true if node should be rescheduled
   */
  [[nodiscard]] virtual bool
  onNodeComplete(const std::shared_ptr<ILogicNode> &node, bool success,
                 const PortDataMap &outputs) = 0;

  /**
   * @brief Check if execution is complete
   * @param active_node_count Number of currently active nodes
   * @param pending_node_count Number of pending nodes
   * @param sink_execution_counts Execution counts for sink nodes (R4.5:
   *        a reused snapshot vector - the engine calls this on every
   *        task completion in batch mode, so the parameter type avoids
   *        per-call map/string allocations)
   * @return Completion status
   */
  [[nodiscard]] virtual CompletionStatus checkCompletion(
      std::size_t active_node_count, std::size_t pending_node_count,
      const std::vector<SinkExecutionCount> &sink_execution_counts) const = 0;

  /**
   * @brief Get completion semantics for this strategy
   */
  [[nodiscard]] virtual CompletionSemantics completionSemantics() const = 0;

  /**
   * @brief Check if this strategy supports streaming
   */
  [[nodiscard]] virtual bool supportsStreaming() const = 0;

  /**
   * @brief Get strategy name for logging
   */
  [[nodiscard]] virtual std::string name() const = 0;

  /**
   * @brief Initialize from the compiled topology snapshot (optional)
   *
   * R4.2: strategies initialize from the engine-owned CompiledGraph
   * (an immutable indexed snapshot holding node ownership) instead of
   * a raw Graph pointer, so no strategy depends on the caller keeping
   * the mutable Graph alive after engine initialization.
   */
  virtual void initialize(const CompiledGraph &graph) { (void)graph; }

  /**
   * @brief Reset strategy state
   */
  virtual void reset() {}

  /**
   * @brief Clone this strategy
   */
  [[nodiscard]] virtual std::unique_ptr<ISchedulerStrategy> clone() const = 0;
};

} // namespace ai_pipe

#endif // AI_PIPE_I_SCHEDULER_STRATEGY_HPP
