/**
 * @file strategies.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Configuration and factories for the built-in strategies
 * @version 1.0
 * @date 2026-08-01
 *
 * ISchedulerStrategy and ISyncStrategy are public interfaces, but the
 * built-in implementations (BatchSchedulerStrategy,
 * StreamSchedulerStrategy, NoSyncStrategy, JoinAwareSyncStrategy) live
 * in private headers under src/ and are not installed. Without this
 * header a find_package() consumer could only reimplement a strategy
 * from scratch or fall back to whatever PipelineOptions selects.
 *
 * This header publishes the tuning surface — StreamSchedulerConfig —
 * and out-of-line factories that hand back the built-ins behind their
 * interface pointers. The concrete classes stay private on purpose: the
 * ABI promise made at 1.0 then covers the interfaces and this config
 * struct, not the scheduling internals, which stay free to change.
 *
 * Consumers who need behaviour the built-ins do not offer still
 * implement ISchedulerStrategy / ISyncStrategy directly; these
 * factories are for configuring and composing what already ships.
 *
 * Typical use:
 * @code
 *   StreamSchedulerConfig cfg;
 *   cfg.min_interval = std::chrono::milliseconds{33}; // ~30 fps cap
 *
 *   auto pipeline = Pipeline::create()
 *                       .withGraph(std::move(graph))
 *                       .withOptions(PipelineOptions::stream())
 *                       .withSchedulerStrategy(createStreamScheduler(cfg))
 *                       .withSyncStrategy(createJoinAwareSyncStrategy())
 *                       .build();
 * @endcode
 *
 * @copyright Copyright (c) 2026
 */

#ifndef AI_PIPE_STRATEGIES_HPP
#define AI_PIPE_STRATEGIES_HPP

#include "ai_pipe/execution_types.hpp"
#include "ai_pipe/i_scheduler_strategy.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include <chrono>
#include <memory>

namespace ai_pipe {

// =============================================================================
// Stream Scheduler Configuration
// =============================================================================

/**
 * @brief Tuning knobs for the built-in streaming scheduler
 *
 * Defaults match what ExecutionMode::STREAM installs when no strategy is
 * supplied, so a default-constructed config reproduces stock behaviour.
 */
struct StreamSchedulerConfig {
  /**
   * Allow a multi-input node to schedule before every port has data.
   * min_input_ratio is consulted only when this is true: a node
   * schedules once ready_inputs/expected_inputs >= min_input_ratio and
   * at least one input is ready. The default of 0.0 means "any ready
   * input schedules" - raising the ratio raises the bar. (R3.4: the
   * previous default of 1.0 made enabling partial inputs a no-op
   * unless the ratio was also lowered.)
   */
  bool allow_partial_inputs = false;
  double min_input_ratio = 0.0;

  bool auto_reschedule = true; ///< Automatically reschedule on completion
  std::chrono::milliseconds min_interval{
      0}; ///< Minimum interval between executions
};

// =============================================================================
// Scheduler Factories
// =============================================================================

/**
 * @brief Create the built-in batch scheduler
 *
 * Nodes execute once all inputs are ready; the run completes when every
 * sink has executed once. No automatic rescheduling.
 */
[[nodiscard]] std::unique_ptr<ISchedulerStrategy> createBatchScheduler();

/**
 * @brief Create the built-in streaming scheduler
 * @param config Tuning knobs; defaults reproduce stock stream behaviour
 *
 * Continuous data flow with automatic rescheduling, optional partial
 * input execution, and optional rate limiting.
 */
[[nodiscard]] std::unique_ptr<ISchedulerStrategy>
createStreamScheduler(const StreamSchedulerConfig &config = {});

/**
 * @brief Create the built-in scheduler matching an execution mode
 * @param stream_config Consulted only for ExecutionMode::STREAM
 */
[[nodiscard]] std::unique_ptr<ISchedulerStrategy>
createSchedulerStrategy(ExecutionMode mode,
                        const StreamSchedulerConfig &stream_config = {});

// =============================================================================
// Sync Factories
// =============================================================================

/**
 * @brief Create the no-op sync strategy
 *
 * Performs no cross-branch coordination — what batch mode uses, and what
 * stream mode uses when enable_sync_coordination is false.
 */
[[nodiscard]] std::unique_ptr<ISyncStrategy> createNoSyncStrategy();

/**
 * @brief Create the fork-join-aware sync strategy
 *
 * Analyses the compiled topology to find fork/join pairs and coordinates
 * frame drops only across branches that actually reconverge. This is
 * what stream mode installs when enable_sync_coordination is true.
 *
 * The returned strategy derives its sync groups in initialize(), which
 * ExecutionEngine::initialize() calls — install it before initializing
 * the engine (the PipelineBuilder path does this for you) or it will
 * hold no sync groups. See docs/TODO.md N2.
 */
[[nodiscard]] std::unique_ptr<ISyncStrategy> createJoinAwareSyncStrategy();

} // namespace ai_pipe

#endif // AI_PIPE_STRATEGIES_HPP
