#ifndef AI_PIPE_I_SYNC_STRATEGY_HPP
#define AI_PIPE_I_SYNC_STRATEGY_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace ai_pipe {

// Forward declaration; full definition in ai_pipe/compiled_graph.hpp
class CompiledGraph;

// Sync Types

/**
 * @brief Unique identifier for a synchronization group.
 */
using SyncGroupId = std::string;

/**
 * @brief Branch identifier within a sync group.
 */
using BranchId = std::string;

/**
 * @brief Frame identifier for synchronization.
 */
using FrameId = std::uint64_t;

// Sync Strategy Interface

/**
 * @brief Abstract interface for synchronization strategies.
 *
 * Synchronization strategies handle:
 * - Registration of sync groups (parallel branches)
 * - Drop propagation across branches
 * - Watermark tracking
 *
 * To create a custom strategy, inherit from this interface and implement
 * all pure virtual methods.
 */
class ISyncStrategy {
public:
  virtual ~ISyncStrategy() = default;

  /**
   * Initializes state from the immutable topology snapshot.
   *
   * The snapshot owns its nodes, so a strategy can retain derived topology
   * state without depending on the caller's mutable `Graph` lifetime.
   */
  virtual void initialize(const CompiledGraph &graph) = 0;

  /**
   * @brief Reset strategy state.
   */
  virtual void reset() = 0;

  /**
   * @brief Register a synchronization group.
   * @param group_id Unique identifier for the sync group
   * @param branch_ids IDs of branches in this group
   * @param join_node Name of the join node (optional)
   */
  virtual void registerSyncGroup(const SyncGroupId &group_id,
                                 const std::vector<BranchId> &branch_ids,
                                 const std::string &join_node = "") = 0;

  /**
   * @brief Map a node to a sync group and branch.
   */
  virtual void mapNodeToGroup(const std::string &node_name,
                              const SyncGroupId &group_id,
                              const BranchId &branch_id) = 0;

  /**
   * @brief Report a frame drop and get affected branches.
   * @return List of branches that should also drop this frame
   */
  [[nodiscard]] virtual std::vector<BranchId>
  reportDrop(const std::string &node_name, FrameId frame_id,
             const std::string &reason) = 0;

  /**
   * @brief Check if a frame should be dropped.
   */
  [[nodiscard]] virtual bool shouldDrop(const std::string &node_name,
                                        FrameId frame_id) const = 0;

  /**
   * @brief Mark a frame as processed by a node.
   */
  virtual void markProcessed(const std::string &node_name,
                             FrameId frame_id) = 0;

  /**
   * @brief Get the watermark for a sync group.
   */
  [[nodiscard]] virtual FrameId
  getWatermark(const SyncGroupId &group_id) const = 0;

  /**
   * @brief Check if synchronization is enabled.
   */
  [[nodiscard]] virtual bool isEnabled() const = 0;

  /**
   * @brief Whether this strategy tracks the given node.
   *
   * The engine consults this once at initialize time to decide which
   * nodes get per-frame shouldDrop()/markProcessed() calls. Strategies
   * with topology knowledge (e.g. JoinAwareSyncStrategy) return true
   * only for nodes mapped into a sync group, sparing untracked nodes
   * the per-frame strategy lock.
   */
  [[nodiscard]] virtual bool tracksNode(const std::string &node_name) const {
    (void)node_name;
    return isEnabled();
  }

  /**
   * @brief Get strategy name for logging.
   */
  [[nodiscard]] virtual std::string name() const = 0;

  /**
   * @brief Clone this strategy.
   */
  [[nodiscard]] virtual std::unique_ptr<ISyncStrategy> clone() const = 0;
};

} // namespace ai_pipe

#endif // AI_PIPE_I_SYNC_STRATEGY_HPP
