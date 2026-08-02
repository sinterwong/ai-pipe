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
 * R4.2: topology analysis runs on the engine's CompiledGraph snapshot
 * (dense indices, precomputed adjacency); the strategy retains no
 * reference to any graph after initialize() returns.
 *
 * @version 0.2
 * @date 2026-01-23
 *
 * @copyright Copyright (c) 2026
 *
 */
#ifndef AI_PIPE_INTERNAL_JOIN_AWARE_SYNC_STRATEGY_HPP
#define AI_PIPE_INTERNAL_JOIN_AWARE_SYNC_STRATEGY_HPP

#include "ai_pipe/compiled_graph.hpp"
#include "ai_pipe/i_sync_strategy.hpp"
#include "ai_pipe/strategies.hpp"
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
  using NodeIndex = CompiledGraph::NodeIndex;

  JoinAwareSyncStrategy()
      : m_coordinator(std::make_shared<SyncCoordinator>()) {}

  /**
   * @brief Initialize strategy with the compiled topology snapshot
   *
   * Performs intelligent topology analysis:
   * 1. Identify all join nodes (in-degree > 1)
   * 2. For each join node, find the Lowest Common Fork (LCF)
   * 3. Extract all paths between LCF and join
   * 4. Create sync groups and map all path nodes
   *
   * Only node *names* are retained (in the group mappings); no graph
   * reference survives this call.
   */
  void initialize(const CompiledGraph &graph) override {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_coordinator->reset();
    m_nodeMapping.clear();
    m_nodeMultiMapping.clear();
    m_debugInfo.clear();

    const auto node_count = static_cast<NodeIndex>(graph.nodeCount());

    // Step 1: Find all join points (nodes with in-degree > 1)
    std::vector<NodeIndex> join_points;
    for (NodeIndex i = 0; i < node_count; ++i) {
      if (graph.inDegree(i) > 1) {
        join_points.push_back(i);
      }
    }

    // Step 2: For each join point, find fork-join pairs and create sync groups
    int group_counter = 0;
    for (const auto join_index : join_points) {
      auto result = findLowestCommonFork(graph, join_index);

      if (!result.valid) {
        continue; // No common fork found, skip
      }

      const std::string &fork_name = graph.node(result.fork_node)->getName();
      const std::string &join_name = graph.node(join_index)->getName();

      // Create a sync group for this fork-join pair
      std::string group_id = "sync_group_";
      group_id += std::to_string(group_counter++);
      group_id += "_";
      group_id += fork_name;
      group_id += "_to_";
      group_id += join_name;

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
        for (const auto node_index : path) {
          addNodeMapping(graph.node(node_index)->getName(), group_id,
                         branch_id);
        }
      }

      // Register the sync group with the coordinator
      if (!branch_ids.empty()) {
        m_coordinator->createSyncGroup(group_id, branch_ids);

        // Store debug info
        std::stringstream ss;
        ss << "Group[" << group_id << "]: Fork=" << fork_name
           << ", Join=" << join_name << ", Branches=" << branch_ids.size()
           << " {";
        for (size_t i = 0; i < result.paths.size(); ++i) {
          ss << "[";
          for (size_t j = 0; j < result.paths[i].size(); ++j) {
            if (j > 0)
              ss << "->";
            ss << graph.node(result.paths[i][j])->getName();
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
    NodeIndex fork_node{CompiledGraph::k_invalid_index};
    std::vector<std::vector<NodeIndex>>
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
  static ForkSearchResult findLowestCommonFork(const CompiledGraph &graph,
                                               NodeIndex join_index) {
    ForkSearchResult result;

    const auto &predecessors = graph.predecessors(join_index);
    if (predecessors.size() < 2) {
      return result;
    }

    const auto is_fork = [&graph](NodeIndex index) {
      return graph.successors(index).size() > 1;
    };

    // Simple case first: a direct common parent of all predecessors
    // that fans out is the fork node
    std::unordered_map<NodeIndex, std::size_t> parent_count;
    for (const auto pred : predecessors) {
      for (const auto grandparent : graph.predecessors(pred)) {
        parent_count[grandparent]++;
      }
    }
    for (const auto &[parent, count] : parent_count) {
      if (count == predecessors.size() && is_fork(parent)) {
        result.valid = true;
        result.fork_node = parent;
        result.paths = findAllPaths(graph, parent, join_index);
        return result;
      }
    }

    // General case: BFS backward to find the lowest common ancestor.
    // Track which sources can reach each node.
    std::vector<std::unordered_set<NodeIndex>> reachable_from_source(
        predecessors.size());
    std::vector<std::deque<NodeIndex>> queues(predecessors.size());

    for (std::size_t i = 0; i < predecessors.size(); ++i) {
      queues[i].push_back(predecessors[i]);
      reachable_from_source[i].insert(predecessors[i]);
    }

    NodeIndex fork_node = CompiledGraph::k_invalid_index;
    bool found = false;
    int max_iterations = 1000; // Safety limit
    int iterations = 0;

    while (!found && iterations < max_iterations) {
      bool any_progress = false;
      iterations++;

      // Check for common fork before expanding
      for (const auto candidate : reachable_from_source[0]) {
        bool reachable_from_all = true;
        for (std::size_t i = 1; i < reachable_from_source.size(); ++i) {
          if (reachable_from_source[i].count(candidate) == 0) {
            reachable_from_all = false;
            break;
          }
        }
        if (reachable_from_all && is_fork(candidate)) {
          fork_node = candidate;
          found = true;
          break;
        }
      }

      if (found)
        break;

      // Expand BFS for each source
      for (std::size_t src_idx = 0; src_idx < queues.size(); ++src_idx) {
        auto &queue = queues[src_idx];
        std::size_t queue_size = queue.size();
        for (std::size_t q = 0; q < queue_size; ++q) {
          NodeIndex current = queue.front();
          queue.pop_front();
          any_progress = true;

          for (const auto pred : graph.predecessors(current)) {
            if (reachable_from_source[src_idx].count(pred) == 0) {
              reachable_from_source[src_idx].insert(pred);
              queue.push_back(pred);
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
    result.paths = findAllPaths(graph, fork_node, join_index);

    return result;
  }

  /**
   * @brief Find all paths from source to destination
   *
   * Uses DFS to enumerate all paths. For DAGs, this is guaranteed to terminate.
   */
  static std::vector<std::vector<NodeIndex>>
  findAllPaths(const CompiledGraph &graph, NodeIndex source, NodeIndex dest) {
    std::vector<std::vector<NodeIndex>> all_paths;
    std::vector<NodeIndex> current_path;
    std::unordered_set<NodeIndex> visited;

    findAllPathsDFS(graph, source, dest, current_path, visited, all_paths);

    return all_paths;
  }

  static void findAllPathsDFS(const CompiledGraph &graph, NodeIndex current,
                              NodeIndex dest,
                              std::vector<NodeIndex> &current_path,
                              std::unordered_set<NodeIndex> &visited,
                              std::vector<std::vector<NodeIndex>> &all_paths) {

    visited.insert(current);
    current_path.push_back(current);

    if (current == dest) {
      // Found a path from fork to join
      // Include all nodes EXCEPT the fork node (first element)
      // The join node is handled separately in initialize()
      if (current_path.size() >= 2) {
        // Include all intermediate nodes (excluding fork, excluding join)
        std::vector<NodeIndex> branch_path(current_path.begin() + 1,
                                           current_path.end() - 1);
        all_paths.push_back(branch_path);
      }
    } else {
      for (const auto neighbor : graph.successors(current)) {
        if (visited.count(neighbor) == 0) {
          findAllPathsDFS(graph, neighbor, dest, current_path, visited,
                          all_paths);
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

} // namespace ai_pipe

#endif // AI_PIPE_INTERNAL_JOIN_AWARE_SYNC_STRATEGY_HPP
