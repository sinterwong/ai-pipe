/**
 * @file helper_sync_strategy.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2026-01-21
 *
 * @copyright Copyright (c) 2026
 *
 */
#ifndef AI_PIPE_UNIT_TEST_HELPER_SYNC_STRATEGY
#define AI_PIPE_UNIT_TEST_HELPER_SYNC_STRATEGY
#include "ai_pipe/i_sync_strategy.hpp"
#include <map>
#include <unordered_set>

using namespace ai_pipe;

namespace ai_pipe_unit_test {
class TestSyncStrategy : public ISyncStrategy {
public:
  TestSyncStrategy() = default;

  // Custom copy constructor (required due to mutex)
  TestSyncStrategy(const TestSyncStrategy &other) {
    std::lock_guard<std::mutex> lock(other.m_mutex);
    m_initialized = other.m_initialized;
    m_initGraph = other.m_initGraph;
    m_resetCount = other.m_resetCount;
    m_enabled = other.m_enabled;
    m_syncGroups = other.m_syncGroups;
    m_joinNodes = other.m_joinNodes;
    m_nodeMappings = other.m_nodeMappings;
    m_droppedFrames = other.m_droppedFrames;
    m_processedFrames = other.m_processedFrames;
    m_dropReasons = other.m_dropReasons;
    m_pendingDrops = other.m_pendingDrops;
    m_watermarks = other.m_watermarks;
  }

  void initialize(const Graph *graph) override {
    m_initialized = true;
    m_initGraph = graph;
  }

  void reset() override {
    m_resetCount++;
    m_droppedFrames.clear();
    m_processedFrames.clear();
  }

  void registerSyncGroup(const SyncGroupId &group_id,
                         const std::vector<BranchId> &branch_ids,
                         const std::string &join_node) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_syncGroups[group_id] = branch_ids;
    m_joinNodes[group_id] = join_node;
  }

  void mapNodeToGroup(const std::string &node_name, const SyncGroupId &group_id,
                      const BranchId &branch_id) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_nodeMappings[node_name] = {group_id, branch_id};
  }

  [[nodiscard]] std::vector<BranchId>
  reportDrop(const std::string &node_name, FrameId frame_id,
             const std::string &reason) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_droppedFrames[node_name].insert(frame_id);
    m_dropReasons[{node_name, frame_id}] = reason;

    // Find the sync group for this node
    auto it = m_nodeMappings.find(node_name);
    if (it == m_nodeMappings.end()) {
      return {};
    }

    const auto &[group_id, branch_id] = it->second;
    auto group_it = m_syncGroups.find(group_id);
    if (group_it == m_syncGroups.end()) {
      return {};
    }

    // Return other branches that should drop
    std::vector<BranchId> affected;
    for (const auto &bid : group_it->second) {
      if (bid != branch_id) {
        m_pendingDrops[bid].insert(frame_id);
        affected.push_back(bid);
      }
    }
    return affected;
  }

  [[nodiscard]] bool shouldDrop(const std::string &node_name,
                                FrameId frame_id) const override {
    std::lock_guard<std::mutex> lock(m_mutex);

    auto it = m_nodeMappings.find(node_name);
    if (it == m_nodeMappings.end()) {
      return false;
    }

    const auto &[group_id, branch_id] = it->second;
    auto pending_it = m_pendingDrops.find(branch_id);
    if (pending_it == m_pendingDrops.end()) {
      return false;
    }

    return pending_it->second.count(frame_id) > 0;
  }

  void markProcessed(const std::string &node_name, FrameId frame_id) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_processedFrames[node_name].insert(frame_id);
  }

  [[nodiscard]] FrameId
  getWatermark(const SyncGroupId &group_id) const override {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_watermarks.find(group_id);
    return it != m_watermarks.end() ? it->second : 0;
  }

  void setWatermark(const SyncGroupId &group_id, FrameId watermark) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_watermarks[group_id] = watermark;
  }

  [[nodiscard]] bool isEnabled() const override { return m_enabled; }
  void setEnabled(bool enabled) { m_enabled = enabled; }

  [[nodiscard]] std::string name() const override { return "TestSyncStrategy"; }

  [[nodiscard]] std::unique_ptr<ISyncStrategy> clone() const override {
    return std::make_unique<TestSyncStrategy>(*this);
  }

  // Test accessors
  bool wasInitialized() const { return m_initialized; }
  const Graph *getInitGraph() const { return m_initGraph; }
  int getResetCount() const { return m_resetCount; }

  std::map<SyncGroupId, std::vector<BranchId>> getSyncGroups() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_syncGroups;
  }

  std::unordered_set<FrameId> getDroppedFrames(const std::string &node) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_droppedFrames.find(node);
    return it != m_droppedFrames.end() ? it->second
                                       : std::unordered_set<FrameId>{};
  }

  std::unordered_set<FrameId>
  getProcessedFrames(const std::string &node) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_processedFrames.find(node);
    return it != m_processedFrames.end() ? it->second
                                         : std::unordered_set<FrameId>{};
  }

private:
  bool m_initialized{false};
  const Graph *m_initGraph{nullptr};
  int m_resetCount{0};
  bool m_enabled{true};

  mutable std::mutex m_mutex;
  std::map<SyncGroupId, std::vector<BranchId>> m_syncGroups;
  std::map<SyncGroupId, std::string> m_joinNodes;
  std::map<std::string, std::pair<SyncGroupId, BranchId>> m_nodeMappings;
  std::map<std::string, std::unordered_set<FrameId>> m_droppedFrames;
  std::map<std::string, std::unordered_set<FrameId>> m_processedFrames;
  std::map<std::pair<std::string, FrameId>, std::string> m_dropReasons;
  std::map<BranchId, std::unordered_set<FrameId>> m_pendingDrops;
  std::map<SyncGroupId, FrameId> m_watermarks;
};

} // namespace ai_pipe_unit_test

#endif