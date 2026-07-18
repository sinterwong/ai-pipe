/**
 * @file scheduler_strategies.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Built-in scheduler strategy implementations
 * @version 1.0
 * @date 2025-12-24
 *
 * This file provides built-in scheduler strategy implementations that
 * do not depend on any internal headers. Users can include this file
 * to access standard strategies or use them as examples for custom ones.
 *
 * Strategies provided:
 * - BatchSchedulerStrategy: Traditional batch processing
 * - StreamSchedulerStrategy: Continuous streaming with backpressure
 * - HybridSchedulerStrategy: Mixed batch/stream behavior
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_SCHEDULER_STRATEGIES_HPP
#define AI_PIPE_SCHEDULER_STRATEGIES_HPP

#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/i_scheduler_strategy.hpp"

namespace ai_pipe {

// =============================================================================
// Batch Scheduler Strategy
// =============================================================================

/**
 * @brief Traditional batch processing scheduler
 *
 * Characteristics:
 * - Nodes execute when all inputs are ready
 * - Execution completes when all sinks execute once
 * - No automatic rescheduling
 */
class BatchSchedulerStrategy final : public ISchedulerStrategy {
public:
  [[nodiscard]] ScheduleResult
  shouldSchedule(const SchedulingContext &context) const override {
    if (context.is_source_node && context.has_initial_input) {
      return ScheduleResult::scheduleNow("source node with initial input");
    }

    if (context.is_source_node && context.expected_input_count == 0) {
      return ScheduleResult::scheduleNow("source node with no expected inputs");
    }

    if (context.allInputsReady()) {
      return ScheduleResult::scheduleNow("all inputs ready");
    }

    return ScheduleResult::waitForInputs(
        "waiting for " +
        std::to_string(context.expected_input_count -
                       context.ready_input_count) +
        " more inputs");
  }

  [[nodiscard]] bool
  onNodeComplete(const std::shared_ptr<ILogicNode> & /*node*/, bool /*success*/,
                 const PortDataMap & /*outputs*/) override {
    return false; // Batch mode: no reschedule
  }

  [[nodiscard]] CompletionStatus
  checkCompletion(std::size_t active_node_count,
                  std::size_t /*pending_node_count*/,
                  const std::unordered_map<std::string, std::uint64_t>
                      &sink_execution_counts) const override {
    if (active_node_count > 0) {
      return {false, "active tasks remaining", std::nullopt};
    }

    for (const auto &[sink_name, count] : sink_execution_counts) {
      if (count == 0) {
        return {false, "sink '" + sink_name + "' has not executed",
                std::nullopt};
      }
    }

    return {true, "all sinks executed", std::nullopt};
  }

  [[nodiscard]] CompletionSemantics completionSemantics() const override {
    return CompletionSemantics::SinglePass;
  }

  [[nodiscard]] bool supportsStreaming() const override { return false; }

  [[nodiscard]] std::string name() const override {
    return "BatchSchedulerStrategy";
  }

  [[nodiscard]] std::unique_ptr<ISchedulerStrategy> clone() const override {
    return std::make_unique<BatchSchedulerStrategy>(*this);
  }
};

// =============================================================================
// Stream Scheduler Strategy
// =============================================================================

/**
 * @brief Configuration for stream scheduling
 */
struct StreamSchedulerConfig {
  bool allow_partial_inputs = false; ///< Allow scheduling with partial inputs
  double min_input_ratio = 1.0;      ///< Minimum input readiness ratio
  bool auto_reschedule = true;       ///< Automatically reschedule on completion
  std::chrono::milliseconds min_interval{
      0}; ///< Minimum interval between executions
};

/**
 * @brief Continuous streaming scheduler
 *
 * Characteristics:
 * - Supports continuous data flow
 * - Automatic rescheduling on completion
 * - Optional partial input execution
 * - Rate limiting support
 */
class StreamSchedulerStrategy final : public ISchedulerStrategy {
public:
  explicit StreamSchedulerStrategy(StreamSchedulerConfig config = {})
      : m_config(config) {}

  [[nodiscard]] ScheduleResult
  shouldSchedule(const SchedulingContext &context) const override {
    // Rate limiting gates every data-ready path below (R3.2): a node
    // that executed too recently defers instead of scheduling, and the
    // engine's defer timer re-evaluates it once the interval elapses.
    // (This check previously sat below the ready-input returns, where
    // it was unreachable exactly when inputs were ready - min_interval
    // never limited anything.)
    if (m_config.min_interval.count() > 0 && context.execution_count > 0 &&
        hasReadyWork(context)) {
      const auto elapsed =
          std::chrono::steady_clock::now() - context.last_execution_time;
      if (elapsed < m_config.min_interval) {
        const auto remaining =
            m_config.min_interval -
            std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
        return ScheduleResult::defer(
            std::max(remaining, std::chrono::milliseconds{1}), "rate limited");
      }
    }

    // Source nodes with initial input can be scheduled
    if (context.is_source_node && context.has_initial_input) {
      return ScheduleResult::scheduleNow("source node with input ready");
    }

    if (m_config.allow_partial_inputs) {
      if (context.inputReadinessRatio() >= m_config.min_input_ratio &&
          context.hasReadyInput()) {
        return ScheduleResult::scheduleNow("partial inputs ready");
      }
    } else {
      if (context.allInputsReady()) {
        return ScheduleResult::scheduleNow("all inputs ready");
      }
    }

    return ScheduleResult::waitForInputs("waiting for inputs in stream mode");
  }

  [[nodiscard]] bool
  onNodeComplete(const std::shared_ptr<ILogicNode> & /*node*/, bool success,
                 const PortDataMap & /*outputs*/) override {
    return success && m_config.auto_reschedule;
  }

  [[nodiscard]] CompletionStatus
  checkCompletion(std::size_t /*active_node_count*/,
                  std::size_t /*pending_node_count*/,
                  const std::unordered_map<std::string, std::uint64_t>
                      & /*sink_execution_counts*/) const override {
    return {false, "streaming mode - continuous execution", std::nullopt};
  }

  [[nodiscard]] CompletionSemantics completionSemantics() const override {
    return CompletionSemantics::Continuous;
  }

  [[nodiscard]] bool supportsStreaming() const override { return true; }

  [[nodiscard]] std::string name() const override {
    return "StreamSchedulerStrategy";
  }

  [[nodiscard]] std::unique_ptr<ISchedulerStrategy> clone() const override {
    return std::make_unique<StreamSchedulerStrategy>(*this);
  }

  // Configuration accessors
  void setConfig(const StreamSchedulerConfig &config) { m_config = config; }
  [[nodiscard]] const StreamSchedulerConfig &config() const { return m_config; }

private:
  /** @brief Would the ready-input paths schedule this node right now? */
  [[nodiscard]] bool hasReadyWork(const SchedulingContext &context) const {
    if (context.is_source_node && context.has_initial_input) {
      return true;
    }
    if (m_config.allow_partial_inputs) {
      return context.hasReadyInput() &&
             context.inputReadinessRatio() >= m_config.min_input_ratio;
    }
    return context.allInputsReady();
  }

  StreamSchedulerConfig m_config;
};

// =============================================================================
// Hybrid Scheduler Strategy
// =============================================================================

/**
 * @brief Hybrid scheduler combining batch and stream behaviors
 *
 * Allows per-node configuration of scheduling behavior.
 */
class HybridSchedulerStrategy final : public ISchedulerStrategy {
public:
  [[nodiscard]] ScheduleResult
  shouldSchedule(const SchedulingContext &context) const override {
    // Default to stream-like behavior
    if (context.is_source_node && context.has_initial_input) {
      return ScheduleResult::scheduleNow("source node with input ready");
    }

    if (context.allInputsReady()) {
      return ScheduleResult::scheduleNow("all inputs ready");
    }

    return ScheduleResult::waitForInputs("waiting for inputs");
  }

  [[nodiscard]] bool
  onNodeComplete(const std::shared_ptr<ILogicNode> & /*node*/, bool success,
                 const PortDataMap & /*outputs*/) override {
    return success; // Reschedule on success
  }

  [[nodiscard]] CompletionStatus
  checkCompletion(std::size_t /*active_node_count*/,
                  std::size_t /*pending_node_count*/,
                  const std::unordered_map<std::string, std::uint64_t>
                      & /*sink_execution_counts*/) const override {
    return {false, "hybrid mode - continuous execution", std::nullopt};
  }

  [[nodiscard]] CompletionSemantics completionSemantics() const override {
    return CompletionSemantics::Continuous;
  }

  [[nodiscard]] bool supportsStreaming() const override { return true; }

  [[nodiscard]] std::string name() const override {
    return "HybridSchedulerStrategy";
  }

  [[nodiscard]] std::unique_ptr<ISchedulerStrategy> clone() const override {
    return std::make_unique<HybridSchedulerStrategy>(*this);
  }
};

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * @brief Create a scheduler strategy for the given execution mode
 */
inline std::unique_ptr<ISchedulerStrategy>
createSchedulerStrategy(ExecutionMode mode,
                        const StreamSchedulerConfig &stream_config = {}) {
  switch (mode) {
  case ExecutionMode::BATCH:
    return std::make_unique<BatchSchedulerStrategy>();
  case ExecutionMode::STREAM:
    return std::make_unique<StreamSchedulerStrategy>(stream_config);
  case ExecutionMode::HYBRID:
    return std::make_unique<HybridSchedulerStrategy>();
  }
  return std::make_unique<BatchSchedulerStrategy>();
}

} // namespace ai_pipe

#endif // AI_PIPE_SCHEDULER_STRATEGIES_HPP
