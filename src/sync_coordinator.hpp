/**
 * @file sync_coordinator.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-12-24
 *
 * This is the most critical component for handling the DAG branch/join
 * synchronization problem. When one branch drops a frame due to backpressure,
 * parallel branches must also drop the corresponding frame to maintain
 * alignment at join points.
 *
 * Key concepts:
 * - Sync Group: A set of parallel branches that share a common join point
 * - Drop Propagation: When one branch drops frame N, all branches drop frame N
 * - Watermark: The minimum frame ID that has been fully processed by all
 * branches
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_SYNC_COORDINATOR_HPP
#define AI_PIPE_SYNC_COORDINATOR_HPP

#include "bounded_drop_queue.hpp"
#include "frame_metadata.hpp"
#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ai_pipe {

// =============================================================================
// Sync Group Definitions
// =============================================================================

/**
 * @brief Unique identifier for a synchronization group
 */
using SyncGroupId = std::string;

/**
 * @brief Branch identifier within a sync group
 */
using BranchId = std::string;

/**
 * @brief Callback for coordinated drop events
 */
using CoordinatedDropCallback =
    std::function<void(SyncGroupId group_id, BranchId branch_id,
                       FrameId frame_id, const std::string &reason)>;

// =============================================================================
// Branch State
// =============================================================================

/**
 * @brief State tracking for a single branch in a sync group
 */
struct BranchState {
  BranchId branch_id;
  FrameId latest_frame{frame_constants::kInvalidFrameId}; ///< Latest frame seen
  FrameId processed_frame{
      frame_constants::kInvalidFrameId};      ///< Latest frame processed
  std::unordered_set<FrameId> dropped_frames; ///< Frames dropped by this branch
  std::unordered_set<FrameId>
      pending_sync_drops;         ///< Drops pending from other branches
  std::atomic<bool> active{true}; ///< Whether this branch is still active

  BranchState() = default;
  explicit BranchState(BranchId id) : branch_id(std::move(id)) {}

  BranchState(const BranchState &other)
      : branch_id(other.branch_id), latest_frame(other.latest_frame),
        processed_frame(other.processed_frame),
        dropped_frames(other.dropped_frames),
        pending_sync_drops(other.pending_sync_drops),
        active(other.active.load()) {}

  BranchState &operator=(const BranchState &other) {
    if (this != &other) {
      branch_id = other.branch_id;
      latest_frame = other.latest_frame;
      processed_frame = other.processed_frame;
      dropped_frames = other.dropped_frames;
      pending_sync_drops = other.pending_sync_drops;
      active.store(other.active.load());
    }
    return *this;
  }
};

// =============================================================================
// Sync Group
// =============================================================================

/**
 * @brief Represents a group of branches that must synchronize drops
 *
 * A sync group is typically created at a branch point in the DAG and
 * includes all paths that converge at a common join node.
 */
class SyncGroup {
public:
  explicit SyncGroup(SyncGroupId id) : m_groupId(std::move(id)) {}

  // -------------------------------------------------------------------------
  // Branch Management
  // -------------------------------------------------------------------------

  /**
   * @brief Register a branch in this sync group
   */
  void addBranch(const BranchId &branch_id) {
    std::unique_lock lock(m_mutex);
    if (m_branches.find(branch_id) == m_branches.end()) {
      m_branches[branch_id] = std::make_unique<BranchState>(branch_id);
    }
  }

  /**
   * @brief Remove a branch from the sync group
   */
  void removeBranch(const BranchId &branch_id) {
    std::unique_lock lock(m_mutex);
    auto it = m_branches.find(branch_id);
    if (it != m_branches.end()) {
      it->second->active = false;
      m_branches.erase(it);
    }
  }

  /**
   * @brief Check if a branch exists
   */
  [[nodiscard]] bool hasBranch(const BranchId &branch_id) const {
    std::shared_lock lock(m_mutex);
    return m_branches.find(branch_id) != m_branches.end();
  }

  /**
   * @brief Get all branch IDs
   */
  [[nodiscard]] std::vector<BranchId> branchIds() const {
    std::shared_lock lock(m_mutex);
    std::vector<BranchId> ids;
    ids.reserve(m_branches.size());
    for (const auto &[id, state] : m_branches) {
      ids.push_back(id);
    }
    return ids;
  }

  /**
   * @brief Get number of branches
   */
  [[nodiscard]] std::size_t branchCount() const {
    std::shared_lock lock(m_mutex);
    return m_branches.size();
  }

  // -------------------------------------------------------------------------
  // Frame Tracking
  // -------------------------------------------------------------------------

  /**
   * @brief Report that a branch received a frame
   * @return The current watermark (minimum processed frame across all branches)
   */
  FrameId reportFrameReceived(const BranchId &branch_id, FrameId frame_id) {
    std::unique_lock lock(m_mutex);

    auto it = m_branches.find(branch_id);
    if (it == m_branches.end()) {
      return m_watermark;
    }

    auto &state = *it->second;
    if (frame_id > state.latest_frame) {
      state.latest_frame = frame_id;
    }

    // Check if this frame needs to be dropped due to sync requirements
    if (m_globalDrops.count(frame_id) > 0) {
      state.pending_sync_drops.insert(frame_id);
    }

    return m_watermark;
  }

  /**
   * @brief Report that a branch processed a frame
   */
  void reportFrameProcessed(const BranchId &branch_id, FrameId frame_id) {
    std::unique_lock lock(m_mutex);

    auto it = m_branches.find(branch_id);
    if (it == m_branches.end()) {
      return;
    }

    auto &state = *it->second;
    if (frame_id > state.processed_frame) {
      state.processed_frame = frame_id;
    }

    updateWatermarkLocked();
  }

  /**
   * @brief Report that a branch dropped a frame
   *
   * This triggers coordinated drops across all other branches in the group.
   *
   * @param branch_id The branch that initiated the drop
   * @param frame_id The frame ID that was dropped
   * @param reason The reason for dropping
   * @return List of branches that need to drop this frame
   */
  std::vector<BranchId> reportFrameDropped(const BranchId &branch_id,
                                           FrameId frame_id,
                                           const std::string &reason) {
    std::unique_lock lock(m_mutex);

    auto it = m_branches.find(branch_id);
    if (it == m_branches.end()) {
      return {};
    }

    auto &state = *it->second;
    state.dropped_frames.insert(frame_id);

    // Add to global drops
    m_globalDrops.insert(frame_id);
    m_dropReasons[frame_id] = reason;

    // Collect branches that need to drop this frame
    std::vector<BranchId> affected_branches;
    for (auto &[other_id, other_state] : m_branches) {
      if (other_id != branch_id) {
        // If this branch hasn't processed this frame yet, mark for sync drop
        if (other_state->processed_frame < frame_id ||
            other_state->processed_frame == frame_constants::kInvalidFrameId) {
          other_state->pending_sync_drops.insert(frame_id);
          affected_branches.push_back(other_id);
        }
      }
    }

    return affected_branches;
  }

  // -------------------------------------------------------------------------
  // Sync Drop Queries
  // -------------------------------------------------------------------------

  /**
   * @brief Check if a frame should be dropped by a branch
   */
  [[nodiscard]] bool shouldDropFrame(const BranchId &branch_id,
                                     FrameId frame_id) const {
    std::shared_lock lock(m_mutex);

    // Check if globally marked for drop
    if (m_globalDrops.count(frame_id) > 0) {
      return true;
    }

    // Check branch-specific pending drops
    auto it = m_branches.find(branch_id);
    if (it != m_branches.end()) {
      return it->second->pending_sync_drops.count(frame_id) > 0;
    }

    return false;
  }

  /**
   * @brief Get pending sync drops for a branch
   */
  [[nodiscard]] std::unordered_set<FrameId>
  getPendingSyncDrops(const BranchId &branch_id) const {
    std::shared_lock lock(m_mutex);

    auto it = m_branches.find(branch_id);
    if (it == m_branches.end()) {
      return {};
    }

    return it->second->pending_sync_drops;
  }

  /**
   * @brief Clear a pending sync drop after it's been handled
   */
  void clearPendingSyncDrop(const BranchId &branch_id, FrameId frame_id) {
    std::unique_lock lock(m_mutex);

    auto it = m_branches.find(branch_id);
    if (it != m_branches.end()) {
      it->second->pending_sync_drops.erase(frame_id);
      it->second->dropped_frames.insert(frame_id);
    }
  }

  /**
   * @brief Clear all pending sync drops up to a frame ID
   */
  void clearPendingSyncDropsBefore(const BranchId &branch_id,
                                   FrameId frame_id) {
    std::unique_lock lock(m_mutex);

    auto it = m_branches.find(branch_id);
    if (it == m_branches.end()) {
      return;
    }

    auto &pending = it->second->pending_sync_drops;
    auto &dropped = it->second->dropped_frames;

    for (auto frame_it = pending.begin(); frame_it != pending.end();) {
      if (*frame_it < frame_id) {
        dropped.insert(*frame_it);
        frame_it = pending.erase(frame_it);
      } else {
        ++frame_it;
      }
    }
  }

  // -------------------------------------------------------------------------
  // Watermark
  // -------------------------------------------------------------------------

  /**
   * @brief Get the current watermark
   *
   * The watermark is the minimum frame ID that has been fully processed
   * by all branches (either processed or dropped).
   */
  [[nodiscard]] FrameId watermark() const {
    std::shared_lock lock(m_mutex);
    return m_watermark;
  }

  /**
   * @brief Get the minimum latest frame across all branches
   */
  [[nodiscard]] FrameId minLatestFrame() const {
    std::shared_lock lock(m_mutex);

    FrameId min_latest = frame_constants::kEndOfStreamFrameId;
    for (const auto &[id, state] : m_branches) {
      if (state->active && state->latest_frame < min_latest) {
        min_latest = state->latest_frame;
      }
    }
    return min_latest;
  }

  // -------------------------------------------------------------------------
  // Cleanup
  // -------------------------------------------------------------------------

  /**
   * @brief Clean up old drop records below the watermark
   */
  void cleanup() {
    std::unique_lock lock(m_mutex);

    // Remove old global drops
    for (auto it = m_globalDrops.begin(); it != m_globalDrops.end();) {
      if (*it < m_watermark) {
        m_dropReasons.erase(*it);
        it = m_globalDrops.erase(it);
      } else {
        ++it;
      }
    }

    // Clean up branch-specific drops
    for (auto &[id, state] : m_branches) {
      for (auto it = state->dropped_frames.begin();
           it != state->dropped_frames.end();) {
        if (*it < m_watermark) {
          it = state->dropped_frames.erase(it);
        } else {
          ++it;
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Info
  // -------------------------------------------------------------------------

  [[nodiscard]] const SyncGroupId &id() const { return m_groupId; }

  [[nodiscard]] std::string toString() const {
    std::shared_lock lock(m_mutex);

    std::string result =
        "SyncGroup{id=" + m_groupId +
        ", branches=" + std::to_string(m_branches.size()) +
        ", watermark=" + std::to_string(m_watermark) +
        ", globalDrops=" + std::to_string(m_globalDrops.size());

    for (const auto &[id, state] : m_branches) {
      result +=
          ", " + id + ":(latest=" + std::to_string(state->latest_frame) +
          ", processed=" + std::to_string(state->processed_frame) +
          ", pending=" + std::to_string(state->pending_sync_drops.size()) + ")";
    }

    result += "}";
    return result;
  }

private:
  void updateWatermarkLocked() {
    // Find minimum processed frame across all active branches
    FrameId new_watermark = frame_constants::kEndOfStreamFrameId;

    for (const auto &[id, state] : m_branches) {
      if (!state->active)
        continue;

      // Effective processed = max of processed frame and max dropped frame
      // below it
      FrameId effective_processed = state->processed_frame;
      for (FrameId dropped : state->dropped_frames) {
        if (dropped > effective_processed ||
            effective_processed == frame_constants::kInvalidFrameId) {
          // Consider dropped frames as "processed" for watermark calculation
          effective_processed = std::max(effective_processed, dropped);
        }
      }

      if (effective_processed != frame_constants::kInvalidFrameId &&
          (new_watermark == frame_constants::kEndOfStreamFrameId ||
           effective_processed < new_watermark)) {
        new_watermark = effective_processed;
      }
    }

    if (new_watermark != frame_constants::kEndOfStreamFrameId &&
        new_watermark > m_watermark) {
      m_watermark = new_watermark;
    }
  }

private:
  SyncGroupId m_groupId;

  // Branch states
  std::unordered_map<BranchId, std::unique_ptr<BranchState>> m_branches;

  // Global drops (frames that all branches should drop)
  std::unordered_set<FrameId> m_globalDrops;
  std::unordered_map<FrameId, std::string> m_dropReasons;

  // Watermark tracking
  FrameId m_watermark{frame_constants::kInvalidFrameId};

  // Thread safety
  mutable std::shared_mutex m_mutex;
};

// =============================================================================
// Synchronization Coordinator
// =============================================================================

/**
 * @brief Central coordinator for multi-stream synchronization
 *
 * Manages multiple sync groups and provides the interface for:
 * - Registering DAG branch/join structures
 * - Coordinating frame drops across parallel branches
 * - Tracking watermarks for each sync group
 *
 * Usage in BackpressureExecutionEngine:
 * @code
 *   auto coordinator = std::make_shared<SyncCoordinator>();
 *
 *   // When building the graph, identify branch/join structures
 *   coordinator->createSyncGroup("group1", {"branchA", "branchB"});
 *
 *   // When a node drops a frame
 *   if (node_in_sync_group) {
 *     coordinator->reportDrop("group1", "branchA", frame_id, "backpressure");
 *   }
 *
 *   // Before processing a frame, check if it should be dropped
 *   if (coordinator->shouldDropFrame("group1", "branchB", frame_id)) {
 *     // Skip this frame
 *   }
 * @endcode
 */
class SyncCoordinator {
public:
  SyncCoordinator() = default;
  ~SyncCoordinator() = default;

  // Non-copyable, non-movable (singleton-like usage)
  SyncCoordinator(const SyncCoordinator &) = delete;
  SyncCoordinator &operator=(const SyncCoordinator &) = delete;

  // -------------------------------------------------------------------------
  // Sync Group Management
  // -------------------------------------------------------------------------

  /**
   * @brief Create a new sync group
   * @param group_id Unique identifier for the group
   * @param branch_ids Initial set of branches
   * @return true if created successfully
   */
  bool createSyncGroup(const SyncGroupId &group_id,
                       const std::vector<BranchId> &branch_ids = {}) {
    std::unique_lock lock(m_mutex);

    if (m_groups.find(group_id) != m_groups.end()) {
      return false; // Already exists
    }

    auto group = std::make_unique<SyncGroup>(group_id);
    for (const auto &branch_id : branch_ids) {
      group->addBranch(branch_id);
    }

    m_groups[group_id] = std::move(group);
    return true;
  }

  /**
   * @brief Remove a sync group
   */
  void removeSyncGroup(const SyncGroupId &group_id) {
    std::unique_lock lock(m_mutex);
    m_groups.erase(group_id);
  }

  /**
   * @brief Check if a sync group exists
   */
  [[nodiscard]] bool hasSyncGroup(const SyncGroupId &group_id) const {
    std::shared_lock lock(m_mutex);
    return m_groups.find(group_id) != m_groups.end();
  }

  /**
   * @brief Get a sync group
   */
  [[nodiscard]] SyncGroup *getSyncGroup(const SyncGroupId &group_id) {
    std::shared_lock lock(m_mutex);
    auto it = m_groups.find(group_id);
    return it != m_groups.end() ? it->second.get() : nullptr;
  }

  /**
   * @brief Get all sync group IDs
   */
  [[nodiscard]] std::vector<SyncGroupId> syncGroupIds() const {
    std::shared_lock lock(m_mutex);
    std::vector<SyncGroupId> ids;
    ids.reserve(m_groups.size());
    for (const auto &[id, group] : m_groups) {
      ids.push_back(id);
    }
    return ids;
  }

  // -------------------------------------------------------------------------
  // Branch Management
  // -------------------------------------------------------------------------

  /**
   * @brief Add a branch to a sync group
   */
  bool addBranch(const SyncGroupId &group_id, const BranchId &branch_id) {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it == m_groups.end()) {
      return false;
    }

    it->second->addBranch(branch_id);
    return true;
  }

  /**
   * @brief Remove a branch from a sync group
   */
  void removeBranch(const SyncGroupId &group_id, const BranchId &branch_id) {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it != m_groups.end()) {
      it->second->removeBranch(branch_id);
    }
  }

  // -------------------------------------------------------------------------
  // Frame Reporting
  // -------------------------------------------------------------------------

  /**
   * @brief Report that a frame was received by a branch
   */
  FrameId reportFrameReceived(const SyncGroupId &group_id,
                              const BranchId &branch_id, FrameId frame_id) {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it == m_groups.end()) {
      return frame_constants::kInvalidFrameId;
    }

    return it->second->reportFrameReceived(branch_id, frame_id);
  }

  /**
   * @brief Report that a frame was processed by a branch
   */
  void reportFrameProcessed(const SyncGroupId &group_id,
                            const BranchId &branch_id, FrameId frame_id) {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it != m_groups.end()) {
      it->second->reportFrameProcessed(branch_id, frame_id);
    }
  }

  /**
   * @brief Report that a frame was dropped by a branch
   *
   * This is the key method for coordinated dropping. When called:
   * 1. Records the drop for the originating branch
   * 2. Notifies all other branches in the group to drop the same frame
   * 3. Calls the coordinated drop callback for each affected branch
   *
   * @param group_id The sync group
   * @param branch_id The branch that dropped the frame
   * @param frame_id The dropped frame ID
   * @param reason The reason for dropping
   */
  void reportDrop(const SyncGroupId &group_id, const BranchId &branch_id,
                  FrameId frame_id, const std::string &reason) {
    std::vector<BranchId> affected_branches;

    {
      std::shared_lock lock(m_mutex);

      auto it = m_groups.find(group_id);
      if (it == m_groups.end()) {
        return;
      }

      affected_branches =
          it->second->reportFrameDropped(branch_id, frame_id, reason);
    }

    // Notify affected branches via callback (outside lock)
    if (m_dropCallback) {
      for (const auto &affected : affected_branches) {
        m_dropCallback(group_id, affected, frame_id,
                       "Sync drop from " + branch_id + ": " + reason);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Sync Drop Queries
  // -------------------------------------------------------------------------

  /**
   * @brief Check if a frame should be dropped by a branch
   */
  [[nodiscard]] bool shouldDropFrame(const SyncGroupId &group_id,
                                     const BranchId &branch_id,
                                     FrameId frame_id) const {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it == m_groups.end()) {
      return false;
    }

    return it->second->shouldDropFrame(branch_id, frame_id);
  }

  /**
   * @brief Get all pending sync drops for a branch
   */
  [[nodiscard]] std::unordered_set<FrameId>
  getPendingSyncDrops(const SyncGroupId &group_id,
                      const BranchId &branch_id) const {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it == m_groups.end()) {
      return {};
    }

    return it->second->getPendingSyncDrops(branch_id);
  }

  /**
   * @brief Clear a pending sync drop
   */
  void clearPendingSyncDrop(const SyncGroupId &group_id,
                            const BranchId &branch_id, FrameId frame_id) {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it != m_groups.end()) {
      it->second->clearPendingSyncDrop(branch_id, frame_id);
    }
  }

  // -------------------------------------------------------------------------
  // Watermark
  // -------------------------------------------------------------------------

  /**
   * @brief Get the watermark for a sync group
   */
  [[nodiscard]] FrameId getWatermark(const SyncGroupId &group_id) const {
    std::shared_lock lock(m_mutex);

    auto it = m_groups.find(group_id);
    if (it == m_groups.end()) {
      return frame_constants::kInvalidFrameId;
    }

    return it->second->watermark();
  }

  // -------------------------------------------------------------------------
  // Callbacks
  // -------------------------------------------------------------------------

  /**
   * @brief Set callback for coordinated drop events
   */
  void setDropCallback(CoordinatedDropCallback callback) {
    m_dropCallback = std::move(callback);
  }

  // -------------------------------------------------------------------------
  // Maintenance
  // -------------------------------------------------------------------------

  /**
   * @brief Clean up old records in all sync groups
   */
  void cleanup() {
    std::shared_lock lock(m_mutex);

    for (auto &[id, group] : m_groups) {
      group->cleanup();
    }
  }

  /**
   * @brief Reset all sync groups
   */
  void reset() {
    std::unique_lock lock(m_mutex);
    m_groups.clear();
  }

  // -------------------------------------------------------------------------
  // Info
  // -------------------------------------------------------------------------

  [[nodiscard]] std::string toString() const {
    std::shared_lock lock(m_mutex);

    std::string result =
        "SyncCoordinator{groups=" + std::to_string(m_groups.size()) + ":\n";

    for (const auto &[id, group] : m_groups) {
      result += "  " + group->toString() + "\n";
    }

    result += "}";
    return result;
  }

private:
  // Sync groups by ID
  std::unordered_map<SyncGroupId, std::unique_ptr<SyncGroup>> m_groups;

  // Callback for coordinated drops
  CoordinatedDropCallback m_dropCallback;

  // Thread safety
  mutable std::shared_mutex m_mutex;
};

// =============================================================================
// Sync Context for Nodes
// =============================================================================

/**
 * @brief Helper class for nodes to interact with sync coordinator
 *
 * Provides a simplified interface for nodes that participate in sync groups.
 */
class NodeSyncContext {
public:
  NodeSyncContext(std::shared_ptr<SyncCoordinator> coordinator,
                  SyncGroupId group_id, BranchId branch_id)
      : m_coordinator(std::move(coordinator)), m_groupId(std::move(group_id)),
        m_branchId(std::move(branch_id)) {}

  /**
   * @brief Report frame received
   */
  void onFrameReceived(FrameId frame_id) {
    if (m_coordinator) {
      m_coordinator->reportFrameReceived(m_groupId, m_branchId, frame_id);
    }
  }

  /**
   * @brief Report frame processed
   */
  void onFrameProcessed(FrameId frame_id) {
    if (m_coordinator) {
      m_coordinator->reportFrameProcessed(m_groupId, m_branchId, frame_id);
    }
  }

  /**
   * @brief Report frame dropped
   */
  void onFrameDropped(FrameId frame_id, const std::string &reason) {
    if (m_coordinator) {
      m_coordinator->reportDrop(m_groupId, m_branchId, frame_id, reason);
    }
  }

  /**
   * @brief Check if a frame should be skipped
   */
  [[nodiscard]] bool shouldSkipFrame(FrameId frame_id) const {
    if (m_coordinator) {
      return m_coordinator->shouldDropFrame(m_groupId, m_branchId, frame_id);
    }
    return false;
  }

  /**
   * @brief Acknowledge a sync drop
   */
  void acknowledgeSyncDrop(FrameId frame_id) {
    if (m_coordinator) {
      m_coordinator->clearPendingSyncDrop(m_groupId, m_branchId, frame_id);
    }
  }

  [[nodiscard]] const SyncGroupId &groupId() const { return m_groupId; }
  [[nodiscard]] const BranchId &branchId() const { return m_branchId; }

private:
  std::shared_ptr<SyncCoordinator> m_coordinator;
  SyncGroupId m_groupId;
  BranchId m_branchId;
};

} // namespace ai_pipe

#endif // AI_PIPE_SYNC_COORDINATOR_HPP