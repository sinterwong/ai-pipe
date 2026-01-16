/**
 * @file coordinated_sync_strategy.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Coordinated synchronization strategy implementation (internal)
 * @version 1.0
 * @date 2025-12-24
 *
 * This is an INTERNAL header file. Users should not include this directly.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_INTERNAL_COORDINATED_SYNC_STRATEGY_HPP
#define AI_PIPE_INTERNAL_COORDINATED_SYNC_STRATEGY_HPP

#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include "sync_coordinator.hpp"
#include <algorithm>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace ai_pipe {

/**
 * @brief Coordinated sync strategy using SyncCoordinator
 *
 * This strategy provides full synchronization support:
 * - Auto-detection of sync groups from graph topology
 * - Coordinated frame dropping across parallel branches
 * - Watermark tracking for progress monitoring
 *
 * @note This is an internal class. Users should not instantiate it directly.
 */
class CoordinatedSyncStrategy final : public ISyncStrategy {
public:
  CoordinatedSyncStrategy()
      : m_coordinator(std::make_shared<SyncCoordinator>()) {}

  void initialize(const Graph *graph) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_coordinator->reset();
    m_nodeMapping.clear();

    if (!graph)
      return;

    std::unordered_set<std::string> branch_points;
    std::unordered_set<std::string> join_points;

    for (const auto &node : graph->getNodes()) {
      int out_degree = graph->getOutDegree(node);
      int in_degree = graph->getInDegree(node);

      if (out_degree > 1) {
        branch_points.insert(node->getName());
      }
      if (in_degree > 1) {
        join_points.insert(node->getName());
      }
    }

    int group_counter = 0;
    for (const auto &branch_node_name : branch_points) {
      auto branch_node = graph->getNode(branch_node_name);
      if (!branch_node)
        continue;

      const auto &neighbors = graph->getOutgoingNeighbors(branch_node);
      if (neighbors.size() < 2)
        continue;

      std::string group_id = "sync_group_" + std::to_string(group_counter++);
      std::vector<BranchId> branch_ids;

      for (const auto &neighbor : neighbors) {
        branch_ids.push_back(neighbor->getName());
        m_nodeMapping[neighbor->getName()] = {group_id, neighbor->getName()};
      }

      m_coordinator->createSyncGroup(group_id, branch_ids);
    }
  }

  void reset() override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_coordinator->reset();
  }

  void registerSyncGroup(const SyncGroupId &group_id,
                         const std::vector<BranchId> &branch_ids,
                         const std::string & /*join_node*/) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_coordinator->createSyncGroup(group_id, branch_ids);
  }

  void mapNodeToGroup(const std::string &node_name, const SyncGroupId &group_id,
                      const BranchId &branch_id) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_nodeMapping[node_name] = {group_id, branch_id};
  }

  [[nodiscard]] std::vector<BranchId>
  reportDrop(const std::string &node_name, FrameId frame_id,
             const std::string &reason) override {
    std::lock_guard<std::mutex> lock(m_mutex);

    auto it = m_nodeMapping.find(node_name);
    if (it == m_nodeMapping.end()) {
      return {};
    }

    const auto &[group_id, branch_id] = it->second;
    m_coordinator->reportDrop(group_id, branch_id, frame_id, reason);

    auto group = m_coordinator->getSyncGroup(group_id);
    if (!group) {
      return {};
    }

    auto branch_ids = group->branchIds();
    branch_ids.erase(
        std::remove(branch_ids.begin(), branch_ids.end(), branch_id),
        branch_ids.end());

    return branch_ids;
  }

  [[nodiscard]] bool shouldDrop(const std::string &node_name,
                                FrameId frame_id) const override {
    std::lock_guard<std::mutex> lock(m_mutex);

    auto it = m_nodeMapping.find(node_name);
    if (it == m_nodeMapping.end()) {
      return false;
    }

    const auto &[group_id, branch_id] = it->second;
    auto pending = m_coordinator->getPendingSyncDrops(group_id, branch_id);

    return pending.count(frame_id) > 0;
  }

  void markProcessed(const std::string &node_name, FrameId frame_id) override {
    std::lock_guard<std::mutex> lock(m_mutex);

    auto it = m_nodeMapping.find(node_name);
    if (it == m_nodeMapping.end()) {
      return;
    }

    const auto &[group_id, branch_id] = it->second;
    m_coordinator->reportFrameProcessed(group_id, branch_id, frame_id);
  }

  [[nodiscard]] FrameId
  getWatermark(const SyncGroupId &group_id) const override {
    std::lock_guard<std::mutex> lock(m_mutex);

    auto group = m_coordinator->getSyncGroup(group_id);
    if (!group) {
      return 0;
    }

    return group->watermark();
  }

  [[nodiscard]] bool isEnabled() const override { return true; }

  [[nodiscard]] std::string name() const override {
    return "CoordinatedSyncStrategy";
  }

  [[nodiscard]] std::unique_ptr<ISyncStrategy> clone() const override {
    auto cloned = std::make_unique<CoordinatedSyncStrategy>();
    std::lock_guard<std::mutex> lock(m_mutex);
    cloned->m_nodeMapping = m_nodeMapping;
    return cloned;
  }

  std::shared_ptr<SyncCoordinator> coordinator() const { return m_coordinator; }

private:
  std::shared_ptr<SyncCoordinator> m_coordinator;
  std::unordered_map<std::string, std::pair<SyncGroupId, BranchId>>
      m_nodeMapping;
  mutable std::mutex m_mutex;
};

inline std::unique_ptr<ISyncStrategy> createCoordinatedSyncStrategy() {
  return std::make_unique<CoordinatedSyncStrategy>();
}

} // namespace ai_pipe

#endif // AI_PIPE_INTERNAL_COORDINATED_SYNC_STRATEGY_HPP
