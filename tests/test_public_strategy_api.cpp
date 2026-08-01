/**
 * @file test_public_strategy_api.cpp
 * @brief Contract tests for ai_pipe/strategies.hpp
 *
 * These tests deliberately include ONLY the public header — no private
 * src/ header — so they exercise exactly what a find_package() consumer
 * can reach. test_scheduler_strategies.cpp and test_sync_strategies.cpp
 * cover the same strategies from the inside (with the concrete types
 * visible); this file guards the boundary itself: if
 * StreamSchedulerConfig or a factory moves back behind a private header,
 * or a factory stops forwarding its config, this TU fails.
 */

#include "ai_pipe/strategies.hpp"
#include <gtest/gtest.h>

namespace ai_pipe {
namespace {

// =============================================================================
// Factories are reachable and return the built-ins
// =============================================================================

TEST(PublicStrategyApiTest, CreateBatchScheduler) {
  auto strategy = createBatchScheduler();
  ASSERT_NE(strategy, nullptr);
  EXPECT_EQ(strategy->name(), "BatchSchedulerStrategy");
  EXPECT_FALSE(strategy->supportsStreaming());
  EXPECT_EQ(strategy->completionSemantics(), CompletionSemantics::SinglePass);
}

TEST(PublicStrategyApiTest, CreateStreamScheduler) {
  auto strategy = createStreamScheduler();
  ASSERT_NE(strategy, nullptr);
  EXPECT_EQ(strategy->name(), "StreamSchedulerStrategy");
  EXPECT_TRUE(strategy->supportsStreaming());
  EXPECT_EQ(strategy->completionSemantics(), CompletionSemantics::Continuous);
}

TEST(PublicStrategyApiTest, CreateSchedulerStrategyDispatchesOnMode) {
  EXPECT_EQ(createSchedulerStrategy(ExecutionMode::BATCH)->name(),
            "BatchSchedulerStrategy");
  EXPECT_EQ(createSchedulerStrategy(ExecutionMode::STREAM)->name(),
            "StreamSchedulerStrategy");
}

TEST(PublicStrategyApiTest, CreateSyncStrategies) {
  auto nosync = createNoSyncStrategy();
  ASSERT_NE(nosync, nullptr);
  EXPECT_EQ(nosync->name(), "NoSyncStrategy");
  EXPECT_FALSE(nosync->isEnabled());

  auto join_aware = createJoinAwareSyncStrategy();
  ASSERT_NE(join_aware, nullptr);
  EXPECT_EQ(join_aware->name(), "JoinAwareSyncStrategy");
  EXPECT_TRUE(join_aware->isEnabled());
}

TEST(PublicStrategyApiTest, FactoriesReturnIndependentInstances) {
  auto a = createStreamScheduler();
  auto b = createStreamScheduler();
  EXPECT_NE(a.get(), b.get());
}

// =============================================================================
// The config a consumer passes actually reaches the strategy
// =============================================================================

/** A node with one of two inputs ready, having just executed. */
SchedulingContext partiallyReadyContext() {
  SchedulingContext context;
  context.expected_input_count = 2;
  context.ready_input_count = 1;
  context.execution_count = 1;
  context.last_execution_time = std::chrono::steady_clock::now();
  return context;
}

TEST(PublicStrategyApiTest, DefaultConfigWaitsForAllInputs) {
  auto strategy = createStreamScheduler();
  const auto result = strategy->shouldSchedule(partiallyReadyContext());
  EXPECT_EQ(result.decision, ScheduleDecision::WaitForInputs);
}

TEST(PublicStrategyApiTest, PartialInputConfigIsHonored) {
  StreamSchedulerConfig config;
  config.allow_partial_inputs = true;
  config.min_input_ratio = 0.5; // 1 of 2 ready clears the bar

  auto strategy = createStreamScheduler(config);
  const auto result = strategy->shouldSchedule(partiallyReadyContext());
  EXPECT_EQ(result.decision, ScheduleDecision::ScheduleNow);
}

TEST(PublicStrategyApiTest, MinInputRatioAboveReadinessStillWaits) {
  StreamSchedulerConfig config;
  config.allow_partial_inputs = true;
  config.min_input_ratio = 0.9; // 1 of 2 ready does not clear the bar

  auto strategy = createStreamScheduler(config);
  const auto result = strategy->shouldSchedule(partiallyReadyContext());
  EXPECT_EQ(result.decision, ScheduleDecision::WaitForInputs);
}

TEST(PublicStrategyApiTest, MinIntervalRateLimitIsHonored) {
  StreamSchedulerConfig config;
  config.allow_partial_inputs = true;
  config.min_input_ratio = 0.5;
  config.min_interval = std::chrono::milliseconds{500};

  auto strategy = createStreamScheduler(config);
  // Inputs are ready, but the node executed just now: defer, don't run.
  const auto result = strategy->shouldSchedule(partiallyReadyContext());
  EXPECT_EQ(result.decision, ScheduleDecision::DeferToNextCycle);
  ASSERT_TRUE(result.retry_delay.has_value());
  EXPECT_GT(result.retry_delay->count(), 0);
}

TEST(PublicStrategyApiTest, ModeFactoryForwardsStreamConfig) {
  StreamSchedulerConfig config;
  config.allow_partial_inputs = true;
  config.min_input_ratio = 0.5;

  auto strategy = createSchedulerStrategy(ExecutionMode::STREAM, config);
  const auto result = strategy->shouldSchedule(partiallyReadyContext());
  EXPECT_EQ(result.decision, ScheduleDecision::ScheduleNow);
}

TEST(PublicStrategyApiTest, CloneCarriesConfig) {
  StreamSchedulerConfig config;
  config.allow_partial_inputs = true;
  config.min_input_ratio = 0.5;

  auto original = createStreamScheduler(config);
  auto cloned = original->clone();
  ASSERT_NE(cloned, nullptr);

  const auto result = cloned->shouldSchedule(partiallyReadyContext());
  EXPECT_EQ(result.decision, ScheduleDecision::ScheduleNow);
}

TEST(PublicStrategyApiTest, AutoRescheduleConfigIsHonored) {
  StreamSchedulerConfig config;
  config.auto_reschedule = false;

  auto strategy = createStreamScheduler(config);
  EXPECT_FALSE(strategy->onNodeComplete(nullptr, true, PortDataMap{}));

  auto rescheduling = createStreamScheduler(); // auto_reschedule defaults true
  EXPECT_TRUE(rescheduling->onNodeComplete(nullptr, true, PortDataMap{}));
}

} // namespace
} // namespace ai_pipe
