/**
 * @file sync_strategies.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Built-in synchronization strategy implementations
 * @version 1.0
 * @date 2025-12-24
 *
 * Built-in sync strategy implementations. These classes are private:
 * consumers reach them through the factories in ai_pipe/strategies.hpp,
 * so their layout is not part of the installed ABI.
 *
 * Strategies provided:
 * - NoSyncStrategy: No synchronization (for batch processing)
 *
 * The fork-join-aware counterpart lives in join_aware_sync_strategy.hpp
 * and is what stream mode installs when sync coordination is enabled.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_SYNC_STRATEGIES_HPP
#define AI_PIPE_SYNC_STRATEGIES_HPP

#include "ai_pipe/i_sync_strategy.hpp"
#include "ai_pipe/strategies.hpp"

namespace ai_pipe {

// =============================================================================
// No Sync Strategy
// =============================================================================

/**
 * @brief Null sync strategy for batch processing
 *
 * This strategy does no synchronization - suitable for batch processing
 * where frames don't need to be aligned across branches.
 */
class NoSyncStrategy final : public ISyncStrategy {
public:
  void initialize(const CompiledGraph & /*graph*/) override {}
  void reset() override {}

  void registerSyncGroup(const SyncGroupId & /*group_id*/,
                         const std::vector<BranchId> & /*branch_ids*/,
                         const std::string & /*join_node*/) override {}

  void mapNodeToGroup(const std::string & /*node_name*/,
                      const SyncGroupId & /*group_id*/,
                      const BranchId & /*branch_id*/) override {}

  [[nodiscard]] std::vector<BranchId>
  reportDrop(const std::string & /*node_name*/, FrameId /*frame_id*/,
             const std::string & /*reason*/) override {
    return {};
  }

  [[nodiscard]] bool shouldDrop(const std::string & /*node_name*/,
                                FrameId /*frame_id*/) const override {
    return false;
  }

  void markProcessed(const std::string & /*node_name*/,
                     FrameId /*frame_id*/) override {}

  [[nodiscard]] FrameId
  getWatermark(const SyncGroupId & /*group_id*/) const override {
    return 0;
  }

  [[nodiscard]] bool isEnabled() const override { return false; }

  [[nodiscard]] std::string name() const override { return "NoSyncStrategy"; }

  [[nodiscard]] std::unique_ptr<ISyncStrategy> clone() const override {
    return std::make_unique<NoSyncStrategy>();
  }
};

} // namespace ai_pipe

#endif // AI_PIPE_SYNC_STRATEGIES_HPP
