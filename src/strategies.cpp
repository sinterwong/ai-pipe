#include "ai_pipe/strategies.hpp"

#include "join_aware_sync_strategy.hpp"
#include "scheduler_strategies.hpp"
#include "sync_strategies.hpp"

namespace ai_pipe {

std::unique_ptr<ISchedulerStrategy> createBatchScheduler() {
  return std::make_unique<BatchSchedulerStrategy>();
}

std::unique_ptr<ISchedulerStrategy>
createStreamScheduler(const StreamSchedulerConfig &config) {
  return std::make_unique<StreamSchedulerStrategy>(config);
}

std::unique_ptr<ISchedulerStrategy>
createSchedulerStrategy(ExecutionMode mode,
                        const StreamSchedulerConfig &stream_config) {
  switch (mode) {
  case ExecutionMode::BATCH:
    return createBatchScheduler();
  case ExecutionMode::STREAM:
    return createStreamScheduler(stream_config);
  }
  return createBatchScheduler();
}

std::unique_ptr<ISyncStrategy> createNoSyncStrategy() {
  return std::make_unique<NoSyncStrategy>();
}

std::unique_ptr<ISyncStrategy> createJoinAwareSyncStrategy() {
  return std::make_unique<JoinAwareSyncStrategy>();
}

} // namespace ai_pipe
