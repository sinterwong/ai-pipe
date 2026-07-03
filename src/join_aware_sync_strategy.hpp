/**
 * @file join_aware_sync_strategy.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Fork-join-aware synchronization strategy built on topological analysis
 *
 *  * This strategy solves two critical problems of the naive
 * CoordinatedSyncStrategy:
 *
 * 1. FALSE COUPLING: Only branches that actually converge at a join point are
 *    synchronized. Divergent branches that never meet remain independent.
 *
 * 2. DEPTH BLINDNESS: The entire path from fork to join is mapped to a single
 *    logical BranchId, so any node along the path can trigger synchronized
 * drops.
 *
 * Algorithm:
 * - Identify all Join nodes (in-degree > 1)
 * - For each Join node, backtrack to find the Lowest Common Fork (LCF)
 * - Create a SyncGroup for each (Fork, Join) pair
 * - Map all nodes on the paths between Fork and Join to their logical branches
 *
 * @version 0.1
 * @date 2026-01-23
 *
 * @copyright Copyright (c) 2026
 *
 */
#ifndef AI_PIPE_INTERNAL_JOIN_AWARE_SYNC_STRATEGY_HPP
#define AI_PIPE_INTERNAL_JOIN_AWARE_SYNC_STRATEGY_HPP

#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include "sync_coordinator.hpp"
#include <algorithm>
#include <deque>
#include <memory>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ai_pipe {

/**
 * @brief Join-Aware sync strategy using topological analysis
 *
 * This advanced strategy provides intelligent synchronization support:
 * - Fork-Join pair detection using reverse BFS from join points
 * - Full path mapping from fork to join for complete branch coverage
 * - Nested fork-join structure support with multiple sync group membership
 * - Coordinated frame dropping only for truly convergent branches
 *
 */
class JoinAwareSyncStrategy final : public ISyncStrategy {
public:
  JoinAwareSyncStrategy()
      : m_coordinator(std::make_shared<SyncCoordinator>()) {}

  /**
   * @brief Initialize strategy with graph topology
   *
   * Performs intelligent topology analysis:
   * 1. Identify all join nodes (in-degree > 1)
   * 2. For each join node, find the Lowest Common Fork (LCF)
   * 3. Extract all paths between LCF and join
   * 4. Create sync groups and map all path nodes
   */
  void initialize(const Graph *graph) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_coordinator->reset();
    m_nodeMapping.clear();
    m_nodeMultiMapping.clear();
    m_debugInfo.clear();

    if (!graph)
      return;

    m_graph = graph;

    // Step 1: Find all join points (nodes with in-degree > 1)
    std::vector<std::string> join_points;

    for (const auto &node : graph->getNodes()) {
      int in_degree = graph->getInDegree(node);
      if (in_degree > 1) {
        join_points.push_back(node->getName());
      }
    }

    // Step 2: For each join point, find fork-join pairs and create sync groups
    int group_counter = 0;
    for (const auto &join_node_name : join_points) {
      auto result = findLowestCommonFork(graph, join_node_name);

      if (!result.valid) {
        continue; // No common fork found, skip
      }

      // Create a sync group for this fork-join pair
      std::string group_id = "sync_group_" + std::to_string(group_counter++) +
                             "_" + result.fork_node + "_to_" + join_node_name;

      std::vector<BranchId> branch_ids;
      branch_ids.reserve(result.paths.size());

      // Step 3: Map each path to a logical branch
      int branch_counter = 0;
      for (const auto &path : result.paths) {
        // Create a logical branch ID that represents this entire path
        std::string branch_id = "branch_" + std::to_string(branch_counter++);

        branch_ids.push_back(branch_id);

        // Map ALL nodes on this path to the same (group_id, branch_id)
        // This includes all intermediate nodes between fork and join
        for (const auto &node_name : path) {
          addNodeMapping(node_name, group_id, branch_id);
        }
      }

      // Register the sync group with the coordinator
      if (!branch_ids.empty()) {
        m_coordinator->createSyncGroup(group_id, branch_ids);

        // Store debug info
        std::stringstream ss;
        ss << "Group[" << group_id << "]: Fork=" << result.fork_node
           << ", Join=" << join_node_name << ", Branches=" << branch_ids.size()
           << " {";
        for (size_t i = 0; i < result.paths.size(); ++i) {
          ss << "[";
          for (size_t j = 0; j < result.paths[i].size(); ++j) {
            if (j > 0)
              ss << "->";
            ss << result.paths[i][j];
          }
          ss << "]";
          if (i < result.paths.size() - 1)
            ss << ", ";
        }
        ss << "}";
        m_debugInfo.push_back(ss.str());
      }
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
    addNodeMapping(node_name, group_id, branch_id);
  }

  /**
   * @brief Report a drop and propagate to all sibling branches
   *
   * When any node on a path reports a drop, all parallel branches
   * in the same sync group(s) are notified.
   */
  [[nodiscard]] std::vector<BranchId>
  reportDrop(const std::string &node_name, FrameId frame_id,
             const std::string &reason) override {
    std::lock_guard<std::mutex> lock(m_mutex);

    std::vector<BranchId> all_affected;

    // A node may belong to multiple sync groups (nested structures)
    auto multi_it = m_nodeMultiMapping.find(node_name);
    if (multi_it != m_nodeMultiMapping.end()) {
      for (const auto &[group_id, branch_id] : multi_it->second) {
        m_coordinator->reportDrop(group_id, branch_id, frame_id, reason);

        auto group = m_coordinator->getSyncGroup(group_id);
        if (group) {
          auto branch_ids = group->branchIds();
          for (const auto &bid : branch_ids) {
            if (bid != branch_id &&
                std::find(all_affected.begin(), all_affected.end(), bid) ==
                    all_affected.end()) {
              all_affected.push_back(bid);
            }
          }
        }
      }
    }

    return all_affected;
  }

  /**
   * @brief Check if a frame should be dropped
   *
   * Checks all sync groups that this node belongs to.
   */
  [[nodiscard]] bool shouldDrop(const std::string &node_name,
                                FrameId frame_id) const override {
    std::lock_guard<std::mutex> lock(m_mutex);

    auto multi_it = m_nodeMultiMapping.find(node_name);
    if (multi_it == m_nodeMultiMapping.end()) {
      return false;
    }

    // Check all groups this node belongs to
    for (const auto &[group_id, branch_id] : multi_it->second) {
      auto pending = m_coordinator->getPendingSyncDrops(group_id, branch_id);
      if (pending.count(frame_id) > 0) {
        return true;
      }
    }

    return false;
  }

  /**
   * @brief Mark a frame as processed
   *
   * Notifies all sync groups that this node belongs to.
   */
  void markProcessed(const std::string &node_name, FrameId frame_id) override {
    std::lock_guard<std::mutex> lock(m_mutex);

    auto multi_it = m_nodeMultiMapping.find(node_name);
    if (multi_it == m_nodeMultiMapping.end()) {
      return;
    }

    for (const auto &[group_id, branch_id] : multi_it->second) {
      m_coordinator->reportFrameProcessed(group_id, branch_id, frame_id);
    }
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

  [[nodiscard]] bool tracksNode(const std::string &node_name) const override {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_nodeMultiMapping.find(node_name) != m_nodeMultiMapping.end();
  }

  [[nodiscard]] std::string name() const override {
    return "JoinAwareSyncStrategy";
  }

  [[nodiscard]] std::unique_ptr<ISyncStrategy> clone() const override {
    auto cloned = std::make_unique<JoinAwareSyncStrategy>();
    std::lock_guard<std::mutex> lock(m_mutex);
    cloned->m_nodeMapping = m_nodeMapping;
    cloned->m_nodeMultiMapping = m_nodeMultiMapping;
    cloned->m_debugInfo = m_debugInfo;
    return cloned;
  }

  std::shared_ptr<SyncCoordinator> coordinator() const { return m_coordinator; }

  /**
   * @brief Get debug information about detected sync groups
   */
  [[nodiscard]] std::vector<std::string> getDebugInfo() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_debugInfo;
  }

  /**
   * @brief Get all sync groups a node belongs to
   */
  [[nodiscard]] std::vector<std::pair<SyncGroupId, BranchId>>
  getNodeMappings(const std::string &node_name) const {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_nodeMultiMapping.find(node_name);
    if (it != m_nodeMultiMapping.end()) {
      return it->second;
    }
    return {};
  }

private:
  /**
   * @brief Result of the Lowest Common Fork search
   */
  struct ForkSearchResult {
    bool valid = false;
    std::string fork_node;
    std::vector<std::vector<std::string>>
        paths; // Paths from fork to join (excluding fork and join)
  };

  /**
   * @brief Find the Lowest Common Fork for a join node
   *
   * Uses reverse BFS to find the nearest common ancestor (fork point)
   * of all incoming branches of the join node.
   *
   * Algorithm:
   * 1. Start from all immediate predecessors of the join node
   * 2. BFS backward, tracking which "source" each visited node came from
   * 3. When a node is visited from multiple sources, it's a common ancestor
   * 4. The first such node found with out_degree > 1 is the Lowest Common Fork
   * 5. Extract all paths from fork to join
   */
  ForkSearchResult findLowestCommonFork(const Graph *graph,
                                        const std::string &join_node_name) {
    ForkSearchResult result;

    auto join_node = graph->getNode(join_node_name);
    if (!join_node)
      return result;

    const auto &predecessors = graph->getIncomingNeighbors(join_node);
    if (predecessors.size() < 2)
      return result;

    // Special case: If all predecessors share a common parent with out_degree >
    // 1, that parent is the fork node

    // First, try the simple case: check if there's a direct common parent
    std::unordered_map<std::string, int> parent_count;
    for (const auto &pred : predecessors) {
      const auto &grandparents = graph->getIncomingNeighbors(pred);
      for (const auto &gp : grandparents) {
        parent_count[gp->getName()]++;
      }
    }

    // Find a common parent that is a fork point
    for (const auto &[parent_name, count] : parent_count) {
      if (count == static_cast<int>(predecessors.size())) {
        auto parent_node = graph->getNode(parent_name);
        if (parent_node && graph->getOutDegree(parent_node) > 1) {
          result.valid = true;
          result.fork_node = parent_name;
          result.paths = findAllPaths(graph, parent_name, join_node_name);
          return result;
        }
      }
    }

    // General case: BFS backward to find the lowest common ancestor
    // Track which sources can reach each node
    std::vector<std::unordered_set<std::string>> reachable_from_source(
        predecessors.size());
    std::vector<std::deque<std::string>> queues(predecessors.size());

    // Initialize with immediate predecessors
    for (size_t i = 0; i < predecessors.size(); ++i) {
      const auto &pred = predecessors[i];
      queues[i].push_back(pred->getName());
      reachable_from_source[i].insert(pred->getName());
    }

    std::string fork_node;
    bool found = false;
    int max_iterations = 1000; // Safety limit
    int iterations = 0;

    while (!found && iterations < max_iterations) {
      bool any_progress = false;
      iterations++;

      // Check for common fork before expanding
      for (const auto &node_name : reachable_from_source[0]) {
        bool reachable_from_all = true;
        for (size_t i = 1; i < reachable_from_source.size(); ++i) {
          if (reachable_from_source[i].count(node_name) == 0) {
            reachable_from_all = false;
            break;
          }
        }
        if (reachable_from_all) {
          auto cand_node = graph->getNode(node_name);
          if (cand_node && graph->getOutDegree(cand_node) > 1) {
            fork_node = node_name;
            found = true;
            break;
          }
        }
      }

      if (found)
        break;

      // Expand BFS for each source
      for (size_t src_idx = 0; src_idx < queues.size(); ++src_idx) {
        if (queues[src_idx].empty())
          continue;

        size_t queue_size = queues[src_idx].size();
        for (size_t q = 0; q < queue_size; ++q) {
          std::string current = queues[src_idx].front();
          queues[src_idx].pop_front();
          any_progress = true;

          auto current_node = graph->getNode(current);
          if (!current_node)
            continue;

          const auto &preds = graph->getIncomingNeighbors(current_node);
          for (const auto &pred : preds) {
            const std::string &pred_name = pred->getName();

            if (reachable_from_source[src_idx].count(pred_name) == 0) {
              reachable_from_source[src_idx].insert(pred_name);
              queues[src_idx].push_back(pred_name);
            }
          }
        }
      }

      if (!any_progress)
        break;
    }

    if (!found)
      return result;

    result.valid = true;
    result.fork_node = fork_node;
    result.paths = findAllPaths(graph, fork_node, join_node_name);

    return result;
  }

  /**
   * @brief Find all paths from source to destination
   *
   * Uses DFS to enumerate all paths. For DAGs, this is guaranteed to terminate.
   */
  std::vector<std::vector<std::string>> findAllPaths(const Graph *graph,
                                                     const std::string &source,
                                                     const std::string &dest) {
    std::vector<std::vector<std::string>> all_paths;
    std::vector<std::string> current_path;
    std::unordered_set<std::string> visited;

    findAllPathsDFS(graph, source, dest, current_path, visited, all_paths);

    return all_paths;
  }

  void findAllPathsDFS(const Graph *graph, const std::string &current,
                       const std::string &dest,
                       std::vector<std::string> &current_path,
                       std::unordered_set<std::string> &visited,
                       std::vector<std::vector<std::string>> &all_paths) {

    visited.insert(current);
    current_path.push_back(current);

    if (current == dest) {
      // Found a path from fork to join
      // Include all nodes EXCEPT the fork node (first element)
      // The join node is handled separately in initialize()
      if (current_path.size() >= 2) {
        // Include all intermediate nodes (excluding fork, excluding join)
        std::vector<std::string> branch_path(current_path.begin() + 1,
                                             current_path.end() - 1);
        all_paths.push_back(branch_path);
      }
    } else {
      auto current_node = graph->getNode(current);
      if (current_node) {
        const auto &neighbors = graph->getOutgoingNeighbors(current_node);
        for (const auto &neighbor : neighbors) {
          if (visited.count(neighbor->getName()) == 0) {
            findAllPathsDFS(graph, neighbor->getName(), dest, current_path,
                            visited, all_paths);
          }
        }
      }
    }

    current_path.pop_back();
    visited.erase(current);
  }

  /**
   * @brief Add a node mapping, supporting multiple group membership
   */
  void addNodeMapping(const std::string &node_name, const SyncGroupId &group_id,
                      const BranchId &branch_id) {
    // Primary mapping (for backward compatibility)
    if (m_nodeMapping.find(node_name) == m_nodeMapping.end()) {
      m_nodeMapping[node_name] = {group_id, branch_id};
    }

    // Multi-mapping for nodes belonging to multiple groups
    m_nodeMultiMapping[node_name].push_back({group_id, branch_id});
  }

  const Graph *m_graph = nullptr;
  std::shared_ptr<SyncCoordinator> m_coordinator;

  // Primary mapping (for simple lookups)
  std::unordered_map<std::string, std::pair<SyncGroupId, BranchId>>
      m_nodeMapping;

  // Multi-mapping for nodes belonging to multiple sync groups
  std::unordered_map<std::string, std::vector<std::pair<SyncGroupId, BranchId>>>
      m_nodeMultiMapping;

  // Debug information
  std::vector<std::string> m_debugInfo;

  mutable std::mutex m_mutex;
};

/**
 * @brief Factory function to create JoinAwareSyncStrategy
 */
inline std::unique_ptr<ISyncStrategy> createJoinAwareSyncStrategy() {
  return std::make_unique<JoinAwareSyncStrategy>();
}

} // namespace ai_pipe

#endif // AI_PIPE_INTERNAL_JOIN_AWARE_SYNC_STRATEGY_HPP
