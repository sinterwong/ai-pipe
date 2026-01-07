/**
 * @file stream_execution_engine.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Implementation of StreamExecutionEngine
 * @version 0.1
 * @date 2025-12-24
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "stream_execution_engine.hpp"
#include "ai_pipe/logger.hpp"
#include "execution_engine_factory.hpp"
#include "thread_pool.hpp"
#include <algorithm>
#include <queue>

namespace ai_pipe {

// =============================================================================
// Construction & Destruction
// =============================================================================

StreamExecutionEngine::StreamExecutionEngine()
    : m_syncCoordinator(std::make_shared<SyncCoordinator>()) {
  m_statistics.reset();
}

StreamExecutionEngine::StreamExecutionEngine(StreamEngineConfig config)
    : m_config(std::move(config)),
      m_syncCoordinator(std::make_shared<SyncCoordinator>()) {
  m_statistics.reset();
}

StreamExecutionEngine::~StreamExecutionEngine() {
  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }

  // Stop cleanup thread
  m_cleanupRunning.store(false, std::memory_order_release);
  if (m_cleanupThread && m_cleanupThread->joinable()) {
    m_cleanupThread->join();
  }
}

StreamExecutionEngine::StreamExecutionEngine(
    StreamExecutionEngine &&other) noexcept {
  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }

  std::scoped_lock lock(m_engineMutex, other.m_engineMutex);

  m_config = std::move(other.m_config);
  m_graph = std::exchange(other.m_graph, nullptr);
  m_threadPool = std::move(other.m_threadPool);
  m_currentContext = std::move(other.m_currentContext);
  m_nodeStates = std::move(other.m_nodeStates);
  m_nodeNameMap = std::move(other.m_nodeNameMap);
  m_syncCoordinator = std::move(other.m_syncCoordinator);
  m_nodeToSyncGroup = std::move(other.m_nodeToSyncGroup);
  m_nodeToBranch = std::move(other.m_nodeToBranch);

  m_engineState.store(other.m_engineState.exchange(EngineState::STOPPED),
                      std::memory_order_relaxed);
  m_activeTasks.store(other.m_activeTasks.exchange(0),
                      std::memory_order_relaxed);
  m_stopFlag.store(other.m_stopFlag.exchange(true), std::memory_order_relaxed);

  m_onResultCallback = std::move(other.m_onResultCallback);
  m_onErrorCallback = std::move(other.m_onErrorCallback);
  m_dropEventCallback = std::move(other.m_dropEventCallback);
  m_coordinatedDropCallback = std::move(other.m_coordinatedDropCallback);

  m_sinkNodes = std::move(other.m_sinkNodes);
  m_accumulatedResults = std::move(other.m_accumulatedResults);
}

StreamExecutionEngine &
StreamExecutionEngine::operator=(StreamExecutionEngine &&other) noexcept {
  if (this == &other) {
    return *this;
  }

  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    stopExecutionSync();
  }

  std::scoped_lock lock(m_engineMutex, other.m_engineMutex);

  m_config = std::move(other.m_config);
  m_graph = std::exchange(other.m_graph, nullptr);
  m_threadPool = std::move(other.m_threadPool);
  m_currentContext = std::move(other.m_currentContext);
  m_nodeStates = std::move(other.m_nodeStates);
  m_nodeNameMap = std::move(other.m_nodeNameMap);
  m_syncCoordinator = std::move(other.m_syncCoordinator);
  m_nodeToSyncGroup = std::move(other.m_nodeToSyncGroup);
  m_nodeToBranch = std::move(other.m_nodeToBranch);

  m_engineState.store(other.m_engineState.exchange(EngineState::STOPPED),
                      std::memory_order_relaxed);
  m_activeTasks.store(other.m_activeTasks.exchange(0),
                      std::memory_order_relaxed);
  m_stopFlag.store(other.m_stopFlag.exchange(true), std::memory_order_relaxed);

  m_onResultCallback = std::move(other.m_onResultCallback);
  m_onErrorCallback = std::move(other.m_onErrorCallback);
  m_dropEventCallback = std::move(other.m_dropEventCallback);
  m_coordinatedDropCallback = std::move(other.m_coordinatedDropCallback);

  m_sinkNodes = std::move(other.m_sinkNodes);
  m_accumulatedResults = std::move(other.m_accumulatedResults);

  return *this;
}

// =============================================================================
// IExecutionEngine Interface
// =============================================================================

bool StreamExecutionEngine::initialize(Graph *graph, std::uint8_t num_workers) {
  if (!graph) {
    LOG_ERROR_S << "StreamExecutionEngine: Invalid graph pointer.";
    return false;
  }

  std::lock_guard<std::mutex> lock(m_engineMutex);

  m_graph = graph;
  m_config.num_workers = num_workers;
  m_threadPool = std::make_unique<ThreadPool>(num_workers);

  // Initialize node states
  initializeNodeStates();

  // Initialize input queues with backpressure support
  initializeInputQueues();

  // Setup sync groups
  if (m_config.enable_sync_coordination) {
    initializeSyncGroups();
  }

  // Setup drop callbacks
  setupDropCallbacks();

  // Identify sink nodes
  identifySinkNodes();

  // Reset counters
  m_activeTasks.store(0, std::memory_order_relaxed);
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_engineState.store(EngineState::IDLE, std::memory_order_relaxed);
  m_statistics.reset();

  LOG_INFO_S << "StreamExecutionEngine: Initialized with "
             << static_cast<int>(num_workers) << " workers, "
             << m_nodeStates.size() << " nodes, "
             << m_syncCoordinator->syncGroupIds().size() << " sync groups.";

  return true;
}

bool StreamExecutionEngine::execute(const PortDataMap &initial_inputs,
                                    bool wait_for_completion,
                                    std::shared_ptr<PipelineContext> context) {
  m_currentContext = context;

  // Check if we're in streaming mode
  if (m_streamingMode.load(std::memory_order_acquire)) {
    // In streaming mode, use pushInputs instead
    LOG_DEBUG_S << "StreamExecutionEngine: execute() called in streaming mode, "
                << "delegating to pushInputs().";

    auto results = pushInputs(initial_inputs);

    // Check if all inputs were accepted
    bool all_accepted = true;
    for (const auto &[node, result] : results) {
      if (!result.accepted) {
        LOG_ERROR_S << "StreamExecutionEngine: Input rejected for " << node
                    << ": " << result.message;
        all_accepted = false;
      }
    }

    if (!all_accepted) {
      return false;
    }

    if (wait_for_completion) {
      // In streaming mode, wait for current batch to complete
      // by waiting for active tasks to drop to zero
      std::unique_lock<std::mutex> lock(m_engineMutex);
      m_completionCondition.wait(lock, [this] {
        return m_activeTasks.load(std::memory_order_acquire) == 0 ||
               m_stopFlag.load(std::memory_order_acquire);
      });
    }

    return true;
  }

  // Traditional batch execution mode
  {
    std::unique_lock<std::mutex> lock(m_engineMutex);

    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      LOG_ERROR_S << "StreamExecutionEngine: Already running in batch mode.";
      m_currentContext = nullptr;
      return false;
    }

    if (!m_graph || !m_threadPool) {
      LOG_ERROR_S << "StreamExecutionEngine: Not initialized.";
      m_currentContext = nullptr;
      return false;
    }

    LOG_TRACE_S << "StreamExecutionEngine: Starting batch execution.";

    resetInternalState();
    m_engineState.store(EngineState::RUNNING, std::memory_order_release);
  }

  if (!distributeInitialInputs(initial_inputs)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
    LOG_ERROR_S << "StreamExecutionEngine: Failed to distribute inputs.";
    m_currentContext = nullptr;
    return false;
  }

  if (wait_for_completion) {
    return waitForCompletion();
  }

  return true;
}

void StreamExecutionEngine::stopExecutionAsync() {
  LOG_INFO_S << "StreamExecutionEngine: stopExecutionAsync called.";

  bool expected = false;
  if (m_stopFlag.compare_exchange_strong(expected, true,
                                         std::memory_order_acq_rel)) {
    std::lock_guard<std::mutex> lock(m_engineMutex);
    if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
      m_engineState.store(EngineState::STOPPED, std::memory_order_release);
    }
    m_completionCondition.notify_all();
  }
}

void StreamExecutionEngine::stopExecutionSync() {
  LOG_INFO_S << "StreamExecutionEngine: stopExecutionSync called.";

  stopExecutionAsync();

  std::unique_lock<std::mutex> lock(m_engineMutex);
  const auto current_state = m_engineState.load(std::memory_order_acquire);

  if (current_state == EngineState::RUNNING) {
    m_completionCondition.wait(lock, [this] {
      const auto state = m_engineState.load(std::memory_order_acquire);
      return m_activeTasks.load(std::memory_order_acquire) == 0 ||
             state == EngineState::STOPPED || state == EngineState::ERROR;
    });
  }

  if (m_engineState.load(std::memory_order_acquire) == EngineState::RUNNING) {
    m_engineState.store(EngineState::STOPPED, std::memory_order_release);
  }

  LOG_INFO_S << "StreamExecutionEngine: Execution fully stopped.";
}

void StreamExecutionEngine::reset() {
  LOG_INFO_S << "StreamExecutionEngine: Resetting.";

  stopExecutionSync();

  std::lock_guard<std::mutex> lock(m_engineMutex);

  // Reset node states
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (state && state->exec_state) {
      state->exec_state->store(NodeExecutionState::WAITING,
                               std::memory_order_relaxed);
    }
    // Clear input queues
    for (auto &[port_name, queue] : state->input_queues) {
      if (queue) {
        queue->clear();
        queue->resetStatistics();
      }
    }
  }

  // Reset sync coordinator
  if (m_syncCoordinator) {
    m_syncCoordinator->reset();
    // Re-initialize sync groups
    if (m_config.enable_sync_coordination) {
      initializeSyncGroups();
    }
  }

  m_activeTasks.store(0, std::memory_order_relaxed);
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_engineState.store(EngineState::IDLE, std::memory_order_relaxed);
  m_statistics.reset();

  LOG_INFO_S << "StreamExecutionEngine: Reset complete.";
}

EngineState StreamExecutionEngine::getState() const {
  return m_engineState.load(std::memory_order_acquire);
}

std::unordered_map<std::string, NodeExecutionState>
StreamExecutionEngine::getNodeStates() const {
  std::unordered_map<std::string, NodeExecutionState> result;

  for (const auto &[node_ptr, state] : m_nodeStates) {
    if (state && state->exec_state) {
      result[state->name] = state->exec_state->load(std::memory_order_acquire);
    }
  }

  return result;
}

void StreamExecutionEngine::setPipelineResultCallback(
    std::function<void(const PortDataMap &)> callback) {
  m_onResultCallback = std::move(callback);
}

void StreamExecutionEngine::setPipelineErrorCallback(
    std::function<void(const std::string &, const std::string &)> callback) {
  m_onErrorCallback = std::move(callback);
}

// =============================================================================
// Extended Configuration
// =============================================================================

void StreamExecutionEngine::setConfig(StreamEngineConfig config) {
  m_config = std::move(config);
}

const StreamEngineConfig &StreamExecutionEngine::config() const {
  return m_config;
}

void StreamExecutionEngine::setNodeQueueConfig(const std::string &node_name,
                                               const NodeQueueConfig &config) {
  m_config.node_queue_configs[node_name] = config;
}

void StreamExecutionEngine::addSyncGroup(const SyncGroupConfig &group_config) {
  m_config.sync_groups.push_back(group_config);
}

std::shared_ptr<SyncCoordinator>
StreamExecutionEngine::syncCoordinator() const {
  return m_syncCoordinator;
}

// =============================================================================
// Statistics & Monitoring
// =============================================================================

EngineStatisticsSnapshot StreamExecutionEngine::statistics() const {
  return EngineStatisticsSnapshot(m_statistics);
}

const QueueStatistics *
StreamExecutionEngine::queueStatistics(const std::string &node_name,
                                       const std::string &port_name) const {
  auto node_it = m_nodeNameMap.find(node_name);
  if (node_it == m_nodeNameMap.end()) {
    return nullptr;
  }

  auto state_it = m_nodeStates.find(node_it->second);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return nullptr;
  }

  auto queue_it = state_it->second->input_queues.find(port_name);
  if (queue_it == state_it->second->input_queues.end() || !queue_it->second) {
    return nullptr;
  }

  return &queue_it->second->statistics();
}

std::unordered_map<std::string, double>
StreamExecutionEngine::queueFillRatios() const {
  std::unordered_map<std::string, double> result;

  for (const auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    for (const auto &[port_name, queue] : state->input_queues) {
      if (queue) {
        std::string key = state->name + ":" + port_name;
        result[key] = queue->fillRatio();
      }
    }
  }

  return result;
}

void StreamExecutionEngine::setDropEventCallback(DropEventCallback callback) {
  m_dropEventCallback = std::move(callback);
}

void StreamExecutionEngine::setCoordinatedDropCallback(
    CoordinatedDropCallback callback) {
  m_coordinatedDropCallback = std::move(callback);
}

std::string StreamExecutionEngine::info() const {
  std::ostringstream oss;
  oss << "StreamExecutionEngine{\n"
      << "  state: " << static_cast<int>(m_engineState.load()) << "\n"
      << "  nodes: " << m_nodeStates.size() << "\n"
      << "  sync_groups: " << m_syncCoordinator->syncGroupIds().size() << "\n"
      << "  statistics: {\n"
      << "    processed: " << m_statistics.total_frames_processed.load() << "\n"
      << "    dropped: " << m_statistics.total_frames_dropped.load() << "\n"
      << "    sync_drops: " << m_statistics.total_sync_drops.load() << "\n"
      << "    drop_rate: " << m_statistics.dropRate() << "%\n"
      << "    throughput: " << m_statistics.throughput() << " fps\n"
      << "  }\n"
      << "}";
  return oss.str();
}

// =============================================================================
// Initialization Helpers
// =============================================================================

void StreamExecutionEngine::initializeNodeStates() {
  m_nodeStates.clear();
  m_nodeNameMap.clear();

  for (const auto &node : m_graph->getNodes()) {
    auto state = std::make_unique<NodeBackpressureState>(node, node->getName());
    state->queue_config = getNodeQueueConfig(node->getName());

    m_nodeNameMap[node->getName()] = node;
    m_nodeStates[node] = std::move(state);
  }
}

void StreamExecutionEngine::initializeInputQueues() {
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    state->input_queues.clear();

    for (const auto &port_name : node_ptr->getExpectedInputPorts()) {
      auto queue = createQueue(state->name, port_name);
      state->input_queues[port_name] = queue;
    }
  }
}

void StreamExecutionEngine::initializeSyncGroups() {
  m_nodeToSyncGroup.clear();
  m_nodeToBranch.clear();

  // First, add manually configured sync groups
  for (const auto &group_config : m_config.sync_groups) {
    m_syncCoordinator->createSyncGroup(group_config.group_id,
                                       group_config.branch_ids);

    // Update node-to-group mappings
    for (const auto &branch_id : group_config.branch_ids) {
      m_nodeToBranch[branch_id] = branch_id;
      m_nodeToSyncGroup[branch_id] = group_config.group_id;
    }
  }

  // Auto-detect additional sync groups if enabled
  if (m_config.auto_detect_sync_groups) {
    autoDetectSyncGroups();
  }

  // Create sync contexts for nodes in sync groups
  for (auto &[node_ptr, state] : m_nodeStates) {
    if (!state)
      continue;

    auto group_it = m_nodeToSyncGroup.find(state->name);
    auto branch_it = m_nodeToBranch.find(state->name);

    if (group_it != m_nodeToSyncGroup.end() &&
        branch_it != m_nodeToBranch.end()) {
      state->sync_context = std::make_unique<NodeSyncContext>(
          m_syncCoordinator, group_it->second, branch_it->second);
    }
  }
}

void StreamExecutionEngine::autoDetectSyncGroups() {
  // Find branch points (nodes with out-degree > 1)
  // Find join points (nodes with in-degree > 1)
  // Create sync groups for paths between branch and join points

  std::unordered_set<std::string> branch_points;
  std::unordered_set<std::string> join_points;

  for (const auto &node : m_graph->getNodes()) {
    int out_degree = m_graph->getOutDegree(node);
    int in_degree = m_graph->getInDegree(node);

    if (out_degree > 1) {
      branch_points.insert(node->getName());
    }
    if (in_degree > 1) {
      join_points.insert(node->getName());
    }
  }

  // For each branch point, find the corresponding join point and create a sync
  // group
  int group_counter = 0;
  for (const auto &branch_node_name : branch_points) {
    auto branch_node = m_graph->getNode(branch_node_name);
    if (!branch_node)
      continue;

    // Get all downstream paths
    const auto &neighbors = m_graph->getOutgoingNeighbors(branch_node);
    if (neighbors.size() < 2)
      continue;

    // Find common join point using BFS
    std::unordered_map<std::string, int> visit_count;
    std::queue<NodePtr> bfs_queue;

    for (const auto &neighbor : neighbors) {
      std::queue<NodePtr> path_queue;
      std::unordered_set<std::string> visited;

      path_queue.push(neighbor);
      visited.insert(neighbor->getName());

      while (!path_queue.empty()) {
        auto current = path_queue.front();
        path_queue.pop();

        visit_count[current->getName()]++;

        const auto &current_neighbors = m_graph->getOutgoingNeighbors(current);
        for (const auto &next : current_neighbors) {
          if (visited.find(next->getName()) == visited.end()) {
            visited.insert(next->getName());
            path_queue.push(next);
          }
        }
      }
    }

    // Find the first node visited by all paths (join point)
    std::string join_node_name;
    for (const auto &[name, count] : visit_count) {
      if (static_cast<std::size_t>(count) == neighbors.size()) {
        // This is a potential join point
        if (join_points.find(name) != join_points.end()) {
          join_node_name = name;
          break;
        }
      }
    }

    if (!join_node_name.empty()) {
      // Create a sync group for this branch-join structure
      std::string group_id = "auto_sync_" + std::to_string(group_counter++);

      std::vector<BranchId> branch_ids;
      for (const auto &neighbor : neighbors) {
        branch_ids.push_back(neighbor->getName());
        m_nodeToBranch[neighbor->getName()] = neighbor->getName();
        m_nodeToSyncGroup[neighbor->getName()] = group_id;
      }

      m_syncCoordinator->createSyncGroup(group_id, branch_ids);

      LOG_INFO_S << "StreamExecutionEngine: Auto-detected sync group '"
                 << group_id << "' from " << branch_node_name << " to "
                 << join_node_name << " with " << branch_ids.size()
                 << " branches.";
    }
  }
}

void StreamExecutionEngine::setupDropCallbacks() {
  // Setup coordinated drop callback on sync coordinator
  m_syncCoordinator->setDropCallback(
      [this](const SyncGroupId &group_id, const BranchId &branch_id,
             FrameId frame_id, const std::string &reason) {
        handleCoordinatedDrop(group_id, branch_id, frame_id, reason);
      });
}

// =============================================================================
// Queue Management
// =============================================================================

StreamExecutionEngine::QueuePtr
StreamExecutionEngine::createQueue(const std::string &node_name,
                                   const std::string &port_name) {
  NodeQueueConfig config = getNodeQueueConfig(node_name);

  BoundedDropQueueConfig queue_config{
      .capacity = config.capacity,
      .track_statistics = config.track_statistics,
      .node_name = node_name,
      .port_name = port_name,
  };

  auto queue = std::make_shared<BoundedDropQueue<PortDataPtr>>(queue_config);

  // Set drop strategy
  if (config.drop_strategy == "KeepLatest" ||
      config.drop_strategy == "keep_latest") {
    queue->setStrategy(std::make_unique<KeepLatestNStrategy<PortDataPtr>>(
        config.keep_latest_n));
  } else if (config.drop_strategy == "DropTail" ||
             config.drop_strategy == "drop_tail") {
    queue->setStrategy(std::make_unique<DropTailStrategy<PortDataPtr>>());
  } else if (config.drop_strategy == "Adaptive" ||
             config.drop_strategy == "adaptive") {
    queue->setStrategy(std::make_unique<AdaptiveDropStrategy<PortDataPtr>>());
  } else {
    // Default: DropHead
    queue->setStrategy(std::make_unique<DropHeadStrategy<PortDataPtr>>());
  }

  // Set frame ID accessor for sync support
  queue->setFrameIdAccessor(
      [this](const PortDataPtr &data) { return extractFrameId(data); });

  // Set drop callback
  queue->setDropCallback(
      [this](const DropEvent &event) { handleDropEvent(event); });

  return queue;
}

void StreamExecutionEngine::pushToQueue(const NodePtr &node,
                                        const std::string &port_name,
                                        PortDataPtr data) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return;
  }

  auto &state = *state_it->second;
  auto queue_it = state.input_queues.find(port_name);
  if (queue_it == state.input_queues.end() || !queue_it->second) {
    return;
  }

  // Check for sync drops before pushing
  if (state.sync_context) {
    auto frame_id = extractFrameId(data);
    if (frame_id.has_value() &&
        state.sync_context->shouldSkipFrame(frame_id.value())) {
      LOG_TRACE_S << "StreamExecutionEngine: Skipping frame "
                  << frame_id.value() << " for node " << state.name
                  << " due to sync drop.";
      state.sync_context->acknowledgeSyncDrop(frame_id.value());
      m_statistics.total_sync_drops.fetch_add(1, std::memory_order_relaxed);
      return;
    }
  }

  queue_it->second->push(std::move(data));
}

std::optional<PortDataPtr>
StreamExecutionEngine::popFromQueue(const NodePtr &node,
                                    const std::string &port_name) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return std::nullopt;
  }

  auto queue_it = state_it->second->input_queues.find(port_name);
  if (queue_it == state_it->second->input_queues.end() || !queue_it->second) {
    return std::nullopt;
  }

  return queue_it->second->tryPop();
}

NodeQueueConfig
StreamExecutionEngine::getNodeQueueConfig(const std::string &node_name) const {
  auto it = m_config.node_queue_configs.find(node_name);
  if (it != m_config.node_queue_configs.end()) {
    return it->second;
  }
  return m_config.default_queue_config;
}

// =============================================================================
// Sync Helpers
// =============================================================================

std::optional<FrameId>
StreamExecutionEngine::extractFrameId(const PortDataPtr &data) const {
  if (!data) {
    return std::nullopt;
  }

  // Try to get frame metadata from the data packet
  if (data->has<std::shared_ptr<IFrameMetadata>>(m_config.frame_metadata_key)) {
    auto metadata = data->getParam<std::shared_ptr<IFrameMetadata>>(
        m_config.frame_metadata_key);
    if (metadata) {
      return metadata->frameId();
    }
  }

  // Try direct frame ID
  if (data->has<FrameId>("frame_id")) {
    return data->getParam<FrameId>("frame_id");
  }

  return std::nullopt;
}

void StreamExecutionEngine::handleDropEvent(const DropEvent &event) {
  m_statistics.total_frames_dropped.fetch_add(1, std::memory_order_relaxed);
  m_statistics.total_backpressure_events.fetch_add(1,
                                                   std::memory_order_relaxed);

  if (m_config.enable_drop_logging) {
    LOG_WARNING_S << "StreamExecutionEngine: Drop event - " << event.toString();
  }

  // Notify sync coordinator if this node is in a sync group
  auto group_it = m_nodeToSyncGroup.find(event.node_name);
  auto branch_it = m_nodeToBranch.find(event.node_name);

  if (group_it != m_nodeToSyncGroup.end() &&
      branch_it != m_nodeToBranch.end() &&
      event.frame_id != frame_constants::kInvalidFrameId) {
    m_syncCoordinator->reportDrop(group_it->second, branch_it->second,
                                  event.frame_id, event.reason);
  }

  // Call user callback
  if (m_dropEventCallback) {
    m_dropEventCallback(event);
  }
}

void StreamExecutionEngine::handleCoordinatedDrop(const SyncGroupId &group_id,
                                                  const BranchId &branch_id,
                                                  FrameId frame_id,
                                                  const std::string &reason) {
  LOG_INFO_S << "StreamExecutionEngine: Coordinated drop - group=" << group_id
             << ", branch=" << branch_id << ", frame=" << frame_id
             << ", reason=" << reason;

  m_statistics.total_sync_drops.fetch_add(1, std::memory_order_relaxed);

  // Find the node and drop the frame from its queue
  auto node_it = m_nodeNameMap.find(branch_id);
  if (node_it != m_nodeNameMap.end()) {
    auto state_it = m_nodeStates.find(node_it->second);
    if (state_it != m_nodeStates.end() && state_it->second) {
      for (auto &[port_name, queue] : state_it->second->input_queues) {
        if (queue) {
          // Drop the specific frame
          queue->dropIf([frame_id, this](const PortDataPtr &data) {
            auto data_frame_id = extractFrameId(data);
            return data_frame_id.has_value() &&
                   data_frame_id.value() == frame_id;
          });
        }
      }
    }
  }

  // Call user callback
  if (m_coordinatedDropCallback) {
    m_coordinatedDropCallback(group_id, branch_id, frame_id, reason);
  }
}

void StreamExecutionEngine::applySyncDrops(const NodePtr &node) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return;
  }

  auto &state = *state_it->second;
  if (!state.sync_context) {
    return;
  }

  auto pending_drops = m_syncCoordinator->getPendingSyncDrops(
      state.sync_context->groupId(), state.sync_context->branchId());

  for (FrameId frame_id : pending_drops) {
    for (auto &[port_name, queue] : state.input_queues) {
      if (queue) {
        queue->dropIf([frame_id, this](const PortDataPtr &data) {
          auto data_frame_id = extractFrameId(data);
          return data_frame_id.has_value() && data_frame_id.value() == frame_id;
        });
      }
    }
    state.sync_context->acknowledgeSyncDrop(frame_id);
  }
}

// =============================================================================
// Execution Helpers
// =============================================================================

bool StreamExecutionEngine::distributeInitialInputs(
    const PortDataMap &initial_inputs) {
  bool has_scheduled = false;

  for (const auto &node : m_graph->getNodes()) {
    const bool is_source = m_graph->getInDegree(node) == 0;
    if (!is_source) {
      continue;
    }

    const auto &expected_ports = node->getExpectedInputPorts();
    const bool has_input = initial_inputs.count(node->getName()) > 0;

    if (has_input) {
      const auto &data_packet = initial_inputs.at(node->getName());
      if (!expected_ports.empty()) {
        const std::string &target_port = expected_ports[0];
        pushToQueue(node, target_port, data_packet);
        has_scheduled = true;
        tryScheduleNode(node);
      } else {
        has_scheduled = true;
        tryScheduleNode(node);
      }
    } else if (expected_ports.empty()) {
      has_scheduled = true;
      tryScheduleNode(node);
    }
  }

  if (!has_scheduled && !initial_inputs.empty()) {
    LOG_ERROR_S << "StreamExecutionEngine: Initial inputs provided but "
                   "no source nodes consumed them.";
    return false;
  }

  return true;
}

void StreamExecutionEngine::tryScheduleNode(const NodePtr &node) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return;
  }

  auto &state = *state_it->second;
  auto current_state = state.exec_state->load(std::memory_order_acquire);

  if (current_state != NodeExecutionState::WAITING) {
    return;
  }

  std::lock_guard<std::mutex> node_lock(*state.mutex);

  // Re-check after acquiring lock
  current_state = state.exec_state->load(std::memory_order_acquire);
  if (current_state != NodeExecutionState::WAITING) {
    return;
  }

  // Apply any pending sync drops
  applySyncDrops(node);

  if (!isNodeReady(node)) {
    return;
  }

  NodeExecutionState expected = NodeExecutionState::WAITING;
  if (state.exec_state->compare_exchange_strong(
          expected, NodeExecutionState::READY, std::memory_order_acq_rel)) {
    m_activeTasks.fetch_add(1, std::memory_order_acq_rel);
    LOG_TRACE_S << "StreamExecutionEngine: Node " << state.name << " is READY.";

    auto future = m_threadPool->submit(&StreamExecutionEngine::executeNodeTask,
                                       this, node, m_currentContext);
  }
}

void StreamExecutionEngine::executeNodeTask(
    NodePtr node, std::shared_ptr<PipelineContext> context) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    auto state_it = m_nodeStates.find(node);
    if (state_it != m_nodeStates.end() && state_it->second) {
      state_it->second->exec_state->store(NodeExecutionState::WAITING,
                                          std::memory_order_release);
    }
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  auto &state = *state_it->second;

  // Transition to EXECUTING
  NodeExecutionState expected = NodeExecutionState::READY;
  if (!state.exec_state->compare_exchange_strong(
          expected, NodeExecutionState::EXECUTING, std::memory_order_acq_rel)) {
    m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
    checkCompletionAndNotify();
    return;
  }

  LOG_TRACE_S << "StreamExecutionEngine: Node " << state.name
              << " is EXECUTING.";

  PortDataMap inputs;
  PortDataMap outputs;

  // Gather inputs
  bool success = gatherNodeInputs(node, inputs);

  // Report frame received to sync coordinator
  if (success && state.sync_context && !inputs.empty()) {
    for (const auto &[port_name, data] : inputs) {
      auto frame_id = extractFrameId(data);
      if (frame_id.has_value()) {
        state.sync_context->onFrameReceived(frame_id.value());
      }
    }
  }

  // Process node
  if (success) {
    success = processNode(node, inputs, outputs, context);
  }

  // Report frame processed to sync coordinator
  if (success && state.sync_context && !inputs.empty()) {
    for (const auto &[port_name, data] : inputs) {
      auto frame_id = extractFrameId(data);
      if (frame_id.has_value()) {
        state.sync_context->onFrameProcessed(frame_id.value());
      }
    }
  }

  // Handle completion
  if (m_stopFlag.load(std::memory_order_acquire)) {
    state.exec_state->store(NodeExecutionState::WAITING,
                            std::memory_order_release);
  } else if (success) {
    handleNodeSuccess(node, outputs);
  } else {
    handleNodeFailure(node, "Execution failed");
  }

  m_activeTasks.fetch_sub(1, std::memory_order_acq_rel);
  checkCompletionAndNotify();
}

bool StreamExecutionEngine::gatherNodeInputs(const NodePtr &node,
                                             PortDataMap &inputs) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return false;
  }

  auto &state = *state_it->second;
  std::lock_guard<std::mutex> node_lock(*state.mutex);

  for (const auto &port_name : node->getExpectedInputPorts()) {
    auto data = popFromQueue(node, port_name);
    if (data.has_value()) {
      inputs[port_name] = data.value();
    } else {
      LOG_ERROR_S << "StreamExecutionEngine: Input queue empty for "
                  << state.name << ":" << port_name;
      return false;
    }
  }

  return true;
}

bool StreamExecutionEngine::processNode(
    const NodePtr &node, const PortDataMap &inputs, PortDataMap &outputs,
    const std::shared_ptr<PipelineContext> &context) {
  try {
    node->process(inputs, outputs, context);
    m_statistics.total_frames_processed.fetch_add(1, std::memory_order_relaxed);
    return true;
  } catch (const std::exception &e) {
    auto state_it = m_nodeStates.find(node);
    std::string node_name = state_it != m_nodeStates.end() && state_it->second
                                ? state_it->second->name
                                : "Unknown";
    LOG_ERROR_S << "StreamExecutionEngine: Node " << node_name
                << " failed: " << e.what();
    if (m_onErrorCallback) {
      m_onErrorCallback(e.what(), node_name);
    }
    return false;
  } catch (...) {
    auto state_it = m_nodeStates.find(node);
    std::string node_name = state_it != m_nodeStates.end() && state_it->second
                                ? state_it->second->name
                                : "Unknown";
    LOG_ERROR_S << "StreamExecutionEngine: Node " << node_name
                << " failed with unknown exception.";
    if (m_onErrorCallback) {
      m_onErrorCallback("Unknown exception", node_name);
    }
    return false;
  }
}

void StreamExecutionEngine::propagateOutputs(const NodePtr &source_node,
                                             const PortDataMap &outputs) {
  if (m_stopFlag.load(std::memory_order_acquire)) {
    return;
  }

  const auto outgoing_edges = m_graph->getOutgoingEdges(source_node);

  for (const auto &edge : outgoing_edges) {
    auto output_it = outputs.find(edge.sourcePort);
    if (output_it == outputs.end()) {
      continue;
    }

    const auto &data = output_it->second;
    pushToQueue(edge.destNode, edge.destPort, data);
    tryScheduleNode(edge.destNode);
  }
}

void StreamExecutionEngine::handleNodeSuccess(const NodePtr &node,
                                              const PortDataMap &outputs) {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return;
  }

  auto &state = *state_it->second;
  const bool is_streaming = m_streamingMode.load(std::memory_order_acquire);

  if (is_streaming) {
    // In streaming mode: reset to WAITING so node can process more inputs
    state.exec_state->store(NodeExecutionState::WAITING,
                            std::memory_order_release);
    LOG_TRACE_S << "StreamExecutionEngine: Node " << state.name
                << " completed frame, reset to WAITING (streaming mode).";
  } else {
    // In batch mode: mark as COMPLETED
    state.exec_state->store(NodeExecutionState::COMPLETED,
                            std::memory_order_release);
    LOG_TRACE_S << "StreamExecutionEngine: Node " << state.name
                << " COMPLETED (batch mode).";
  }

  if (isSinkNode(node)) {
    collectFinalResults(node, outputs);
  }

  propagateOutputs(node, outputs);

  // In streaming mode: check if this node has more inputs to process
  if (is_streaming && !m_stopFlag.load(std::memory_order_acquire)) {
    // Re-schedule this node if it has more pending inputs
    tryScheduleNode(node);
  }
}

void StreamExecutionEngine::handleNodeFailure(const NodePtr &node,
                                              const std::string &error_msg) {
  auto state_it = m_nodeStates.find(node);
  if (state_it != m_nodeStates.end() && state_it->second) {
    state_it->second->exec_state->store(NodeExecutionState::FAILED,
                                        std::memory_order_release);
    LOG_ERROR_S << "StreamExecutionEngine: Node " << state_it->second->name
                << " FAILED: " << error_msg;
  }
  stopExecutionAsync();
}

void StreamExecutionEngine::checkCompletionAndNotify() {
  const int active_tasks = m_activeTasks.load(std::memory_order_acquire);

  // Always notify the condition variable when tasks change
  // This allows waitForCompletion() and waitForQueueDrain() to check status
  m_completionCondition.notify_all();
  m_queueDrainCondition.notify_all();

  if (active_tasks != 0) {
    return;
  }

  LOG_TRACE_S << "StreamExecutionEngine: All active tasks completed.";

  const auto current_state = m_engineState.load(std::memory_order_acquire);
  const bool was_stopped = m_stopFlag.load(std::memory_order_acquire);
  const bool is_streaming = m_streamingMode.load(std::memory_order_acquire);

  // In streaming mode, don't change engine state when tasks complete
  // The engine stays RUNNING until stop() is called
  if (is_streaming && !was_stopped) {
    LOG_TRACE_S << "StreamExecutionEngine: Streaming mode - staying RUNNING, "
                << "ready for more inputs.";

    // Still invoke result callback for completed frames
    if (current_state == EngineState::RUNNING && m_onResultCallback) {
      PortDataMap results;
      {
        std::lock_guard<std::mutex> lock(m_resultsMutex);
        results = m_accumulatedResults;
        m_accumulatedResults.clear(); // Clear for next batch
      }
      if (!results.empty()) {
        m_onResultCallback(results);
      }
    }
    return;
  }

  // Batch mode: invoke result callback and signal completion
  if (current_state == EngineState::RUNNING && !was_stopped &&
      m_onResultCallback) {
    PortDataMap results;
    {
      std::lock_guard<std::mutex> lock(m_resultsMutex);
      results = m_accumulatedResults;
    }
    m_onResultCallback(results);
  }
}

void StreamExecutionEngine::resetInternalState() {
  m_stopFlag.store(false, std::memory_order_relaxed);
  m_activeTasks.store(0, std::memory_order_relaxed);

  {
    std::lock_guard<std::mutex> lock(m_resultsMutex);
    m_accumulatedResults.clear();
  }

  for (auto &[node_ptr, state] : m_nodeStates) {
    if (state && state->exec_state) {
      state->exec_state->store(NodeExecutionState::WAITING,
                               std::memory_order_relaxed);
    }
  }
}

// =============================================================================
// Utility
// =============================================================================

bool StreamExecutionEngine::isNodeReady(const NodePtr &node) const {
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return false;
  }

  const auto &state = *state_it->second;
  const auto &expected_ports = node->getExpectedInputPorts();

  if (expected_ports.empty()) {
    return m_graph->getInDegree(node) == 0;
  }

  for (const auto &port_name : expected_ports) {
    auto queue_it = state.input_queues.find(port_name);
    if (queue_it == state.input_queues.end() || !queue_it->second ||
        queue_it->second->empty()) {
      return false;
    }
  }

  return true;
}

bool StreamExecutionEngine::isSinkNode(const NodePtr &node) const {
  return std::find(m_sinkNodes.begin(), m_sinkNodes.end(), node) !=
         m_sinkNodes.end();
}

void StreamExecutionEngine::collectFinalResults(const NodePtr &node,
                                                const PortDataMap &outputs) {
  auto state_it = m_nodeStates.find(node);
  std::string node_name = state_it != m_nodeStates.end() && state_it->second
                              ? state_it->second->name
                              : "Unknown";

  std::lock_guard<std::mutex> lock(m_resultsMutex);
  for (const auto &[port_name, data] : outputs) {
    std::string result_key = node_name + ":" + port_name;
    m_accumulatedResults[result_key] = data;
  }
}

bool StreamExecutionEngine::waitForCompletion() {
  std::unique_lock<std::mutex> lock(m_engineMutex);

  m_completionCondition.wait(lock, [this] {
    return m_activeTasks.load(std::memory_order_acquire) == 0 ||
           m_stopFlag.load(std::memory_order_acquire);
  });

  const bool was_stopped = m_stopFlag.load(std::memory_order_acquire);
  const int active_tasks = m_activeTasks.load(std::memory_order_acquire);
  const auto current_state = m_engineState.load(std::memory_order_acquire);

  if (was_stopped && current_state != EngineState::STOPPED) {
    m_engineState.store(EngineState::STOPPED, std::memory_order_release);
  } else if (active_tasks == 0 && current_state == EngineState::RUNNING) {
    m_engineState.store(EngineState::IDLE, std::memory_order_release);
  } else if (current_state != EngineState::ERROR &&
             current_state != EngineState::STOPPED) {
    m_engineState.store(EngineState::ERROR, std::memory_order_release);
  }

  const bool success =
      m_engineState.load(std::memory_order_acquire) == EngineState::IDLE;
  m_currentContext = nullptr;
  return success;
}

void StreamExecutionEngine::identifySinkNodes() {
  m_sinkNodes.clear();
  for (const auto &node : m_graph->getNodes()) {
    if (m_graph->getOutDegree(node) == 0) {
      m_sinkNodes.push_back(node);
    }
  }
}

std::string
StreamExecutionEngine::getSyncGroupForNode(const std::string &node_name) const {
  auto it = m_nodeToSyncGroup.find(node_name);
  return it != m_nodeToSyncGroup.end() ? it->second : "";
}

std::string
StreamExecutionEngine::getBranchIdForNode(const std::string &node_name) const {
  auto it = m_nodeToBranch.find(node_name);
  return it != m_nodeToBranch.end() ? it->second : "";
}

// =============================================================================
// Streaming Input API Implementation
// =============================================================================

bool StreamExecutionEngine::start(std::shared_ptr<PipelineContext> context) {
  LOG_INFO_S << "StreamExecutionEngine: Starting in streaming mode.";

  {
    std::lock_guard<std::mutex> lock(m_engineMutex);

    // Check if already streaming or running
    auto current_state = m_engineState.load(std::memory_order_acquire);
    if (current_state == EngineState::RUNNING) {
      if (m_streamingMode.load(std::memory_order_acquire)) {
        // Already in streaming mode, this is fine
        LOG_DEBUG_S << "StreamExecutionEngine: Already in streaming mode.";
        return true;
      }
      LOG_ERROR_S << "StreamExecutionEngine: Engine running in batch mode.";
      return false;
    }

    if (!m_graph || !m_threadPool) {
      LOG_ERROR_S << "StreamExecutionEngine: Not initialized.";
      return false;
    }

    // Reset internal state but don't clear queues (they may have data)
    m_stopFlag.store(false, std::memory_order_relaxed);
    m_activeTasks.store(0, std::memory_order_relaxed);

    // Set streaming mode flag
    m_streamingMode.store(true, std::memory_order_release);
    m_engineState.store(EngineState::RUNNING, std::memory_order_release);
    m_currentContext = context;

    // Reset statistics for new streaming session
    m_statistics.reset();
    m_statistics.start_time = std::chrono::steady_clock::now();
  }

  LOG_INFO_S << "StreamExecutionEngine: Streaming mode started.";
  return true;
}

void StreamExecutionEngine::stop(bool wait_for_drain) {
  LOG_INFO_S << "StreamExecutionEngine: Stopping streaming mode, "
             << "wait_for_drain=" << std::boolalpha << wait_for_drain;

  // Signal stop
  m_stopFlag.store(true, std::memory_order_release);
  m_streamingMode.store(false, std::memory_order_release);

  if (wait_for_drain) {
    // Wait for all queued data to be processed
    std::unique_lock<std::mutex> lock(m_engineMutex);
    m_completionCondition.wait(lock, [this] {
      return m_activeTasks.load(std::memory_order_acquire) == 0;
    });
  }

  // Transition to IDLE
  m_engineState.store(EngineState::IDLE, std::memory_order_release);
  m_currentContext = nullptr;

  LOG_INFO_S << "StreamExecutionEngine: Streaming mode stopped.";
}

bool StreamExecutionEngine::isStreaming() const {
  return m_streamingMode.load(std::memory_order_acquire);
}

InputSubmitResult
StreamExecutionEngine::pushInput(const std::string &source_node,
                                 const std::string &port_name,
                                 PortDataPtr data) {
  InputSubmitResult result;

  // Check if streaming
  if (!m_streamingMode.load(std::memory_order_acquire)) {
    result.accepted = false;
    result.message = "Engine not in streaming mode. Call start() first.";
    LOG_WARNING_S << "StreamExecutionEngine: " << result.message;
    return result;
  }

  // Check stop flag
  if (m_stopFlag.load(std::memory_order_acquire)) {
    result.accepted = false;
    result.message = "Engine is stopping.";
    return result;
  }

  // Find the node
  auto node_it = m_nodeNameMap.find(source_node);
  if (node_it == m_nodeNameMap.end()) {
    result.accepted = false;
    result.message = "Node not found: " + source_node;
    LOG_ERROR_S << "StreamExecutionEngine: " << result.message;
    return result;
  }

  const auto &node = node_it->second;

  // Check if it's a source node (no incoming edges)
  if (m_graph->getInDegree(node) != 0) {
    result.accepted = false;
    result.message = "Node is not a source node: " + source_node;
    LOG_ERROR_S << "StreamExecutionEngine: " << result.message;
    return result;
  }

  // Find the state
  auto state_it = m_nodeStates.find(node);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    result.accepted = false;
    result.message = "Node state not found: " + source_node;
    return result;
  }

  auto &state = *state_it->second;

  // Determine the port name
  std::string actual_port = port_name;
  if (actual_port.empty()) {
    const auto &ports = node->getExpectedInputPorts();
    if (ports.empty()) {
      // Source node with no input ports - treat as generator
      // Create a synthetic input port or handle specially
      result.accepted = false;
      result.message = "Source node has no input ports: " + source_node;
      return result;
    }
    actual_port = ports[0];
  }

  // Find the queue
  auto queue_it = state.input_queues.find(actual_port);
  if (queue_it == state.input_queues.end() || !queue_it->second) {
    result.accepted = false;
    result.message =
        "Queue not found for port: " + source_node + ":" + actual_port;
    return result;
  }

  auto &queue = queue_it->second;

  // Record queue state before push
  std::size_t size_before = queue->size();
  std::size_t capacity = queue->capacity();
  result.queue_was_full = (size_before >= capacity);

  // Extract frame ID for logging
  auto frame_id = extractFrameId(data);

  // Push to queue - this may trigger drops via the configured strategy
  queue->push(data);

  // Record queue state after push
  std::size_t size_after = queue->size();

  // Check if drops occurred
  auto stats = queue->statisticsSnapshot();
  if (stats.total_dropped > 0) {
    result.frames_dropped = stats.total_dropped;
    // Note: The exact dropped frame ID is available via drop callback
  }

  result.accepted = true;
  result.message = "OK";

  LOG_TRACE_S << "StreamExecutionEngine: Pushed to " << source_node << ":"
              << actual_port << ", frame_id=" << (frame_id ? *frame_id : 0)
              << ", queue: " << size_before << " -> " << size_after << "/"
              << capacity;

  // Trigger node scheduling
  tryScheduleNode(node);

  // Notify any waiters that queue state changed
  m_queueDrainCondition.notify_all();

  return result;
}

InputSubmitResult
StreamExecutionEngine::pushInput(const std::string &source_node,
                                 PortDataPtr data) {
  return pushInput(source_node, "", data);
}

std::unordered_map<std::string, InputSubmitResult>
StreamExecutionEngine::pushInputs(const PortDataMap &inputs) {
  std::unordered_map<std::string, InputSubmitResult> results;

  for (const auto &[node_name, data] : inputs) {
    results[node_name] = pushInput(node_name, data);
  }

  return results;
}

std::size_t
StreamExecutionEngine::queueDepth(const std::string &node_name,
                                  const std::string &port_name) const {
  auto node_it = m_nodeNameMap.find(node_name);
  if (node_it == m_nodeNameMap.end()) {
    return 0;
  }

  auto state_it = m_nodeStates.find(node_it->second);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return 0;
  }

  const auto &state = *state_it->second;

  std::string actual_port = port_name;
  if (actual_port.empty()) {
    const auto &ports = node_it->second->getExpectedInputPorts();
    if (ports.empty()) {
      return 0;
    }
    actual_port = ports[0];
  }

  auto queue_it = state.input_queues.find(actual_port);
  if (queue_it == state.input_queues.end() || !queue_it->second) {
    return 0;
  }

  return queue_it->second->size();
}

bool StreamExecutionEngine::hasQueueCapacity(
    const std::string &node_name, const std::string &port_name) const {
  auto node_it = m_nodeNameMap.find(node_name);
  if (node_it == m_nodeNameMap.end()) {
    return false;
  }

  auto state_it = m_nodeStates.find(node_it->second);
  if (state_it == m_nodeStates.end() || !state_it->second) {
    return false;
  }

  const auto &state = *state_it->second;

  std::string actual_port = port_name;
  if (actual_port.empty()) {
    const auto &ports = node_it->second->getExpectedInputPorts();
    if (ports.empty()) {
      return false;
    }
    actual_port = ports[0];
  }

  auto queue_it = state.input_queues.find(actual_port);
  if (queue_it == state.input_queues.end() || !queue_it->second) {
    return false;
  }

  return queue_it->second->size() < queue_it->second->capacity();
}

bool StreamExecutionEngine::waitForQueueDrain(
    std::size_t max_depth, std::chrono::milliseconds timeout) {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  std::unique_lock<std::mutex> lock(m_queueDrainMutex);

  return m_queueDrainCondition.wait_until(lock, deadline, [this, max_depth] {
    // First check if there are active tasks
    if (m_activeTasks.load(std::memory_order_acquire) > 0) {
      return false; // Still processing
    }

    // Check ALL node queues, not just source nodes
    for (const auto &node : m_graph->getNodes()) {
      auto state_it = m_nodeStates.find(node);
      if (state_it == m_nodeStates.end() || !state_it->second) {
        continue;
      }

      for (const auto &[port_name, queue] : state_it->second->input_queues) {
        if (queue && queue->size() > max_depth) {
          return false; // Still have data above threshold
        }
      }
    }
    return true; // All queues drained and no active tasks
  });
}

} // namespace ai_pipe

// Register StreamExecutionEngine to the factory
AI_PIPE_REGISTER_ENGINE(StreamExecutionEngine, ai_pipe::StreamExecutionEngine)