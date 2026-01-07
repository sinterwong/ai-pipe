/**
 * @file stream_execution_engine.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-12-24
 *
 * This execution engine is designed for real-time processing pipelines with:
 * - High-latency nodes (e.g., deep learning inference)
 * - Producer-consumer speed mismatches
 * - Multi-stream branch/join DAG structures
 *
 * Key features:
 * - Bounded queues with configurable drop strategies
 * - Coordinated frame dropping across parallel branches
 * - Watermark-based progress tracking
 * - Comprehensive statistics and monitoring
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_STREAM_EXECUTION_ENGINE_HPP
#define AI_PIPE_STREAM_EXECUTION_ENGINE_HPP
#include "ai_pipe/context.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/node_base.hpp"
#include "ai_pipe/types.hpp"
#include "bounded_drop_queue.hpp"
#include "execution_engine_base.hpp"
#include "frame_metadata.hpp"
#include "sync_coordinator.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

// Forward declaration for thread pool
namespace ai_pipe {
class ThreadPool;
}

namespace ai_pipe {

// =============================================================================
// Engine Configuration
// =============================================================================

/**
 * @brief Configuration for per-node queue behavior
 */
struct NodeQueueConfig {
  std::size_t capacity = 16;              ///< Maximum queue size
  std::string drop_strategy = "DropHead"; ///< Strategy name
  std::size_t keep_latest_n = 1;          ///< For KeepLatestN strategy
  bool track_statistics = true;           ///< Enable queue statistics
};

/**
 * @brief Configuration for a sync group
 */
struct SyncGroupConfig {
  SyncGroupId group_id;             ///< Unique group identifier
  std::vector<BranchId> branch_ids; ///< Branches in the group
  std::string join_node;            ///< Join node name (optional)
};

/**
 * @brief Input submission result
 */
struct InputSubmitResult {
  bool accepted{false};          ///< Whether input was accepted
  bool queue_was_full{false};    ///< Queue was at capacity
  std::size_t frames_dropped{0}; ///< Frames dropped due to this push
  FrameId dropped_frame_id{0};   ///< ID of dropped frame (if any)
  std::string message;           ///< Status message

  explicit operator bool() const { return accepted; }
};

/**
 * @brief Full configuration for StreamExecutionEngine
 */
struct StreamEngineConfig {
  // Worker threads
  std::uint8_t num_workers = 4;

  // Default queue configuration
  NodeQueueConfig default_queue_config;

  // Per-node queue overrides (node_name -> config)
  std::unordered_map<std::string, NodeQueueConfig> node_queue_configs;

  // Sync groups for multi-stream synchronization
  std::vector<SyncGroupConfig> sync_groups;

  // Global settings
  bool enable_sync_coordination = true; ///< Enable multi-stream sync
  bool auto_detect_sync_groups = true;  ///< Auto-detect branch/join structures
  std::chrono::milliseconds cleanup_interval{5000}; ///< Interval for cleanup

  // Monitoring
  bool enable_statistics = true;
  bool enable_drop_logging = true;

  // Frame metadata key in PortData
  std::string frame_metadata_key = "frame_metadata";
};

// =============================================================================
// Engine Statistics
// =============================================================================

/**
 * @brief Aggregated statistics for the engine
 */
struct EngineStatistics {
  std::atomic<std::uint64_t> total_frames_processed{0};
  std::atomic<std::uint64_t> total_frames_dropped{0};
  std::atomic<std::uint64_t> total_sync_drops{0};
  std::atomic<std::uint64_t> total_backpressure_events{0};
  std::chrono::steady_clock::time_point start_time;

  void reset() {
    total_frames_processed.store(0, std::memory_order_relaxed);
    total_frames_dropped.store(0, std::memory_order_relaxed);
    total_sync_drops.store(0, std::memory_order_relaxed);
    total_backpressure_events.store(0, std::memory_order_relaxed);
    start_time = std::chrono::steady_clock::now();
  }

  [[nodiscard]] double dropRate() const {
    auto processed = total_frames_processed.load(std::memory_order_relaxed);
    auto dropped = total_frames_dropped.load(std::memory_order_relaxed);
    if (processed + dropped == 0)
      return 0.0;
    return static_cast<double>(dropped) /
           static_cast<double>(processed + dropped) * 100.0;
  }

  [[nodiscard]] double throughput() const {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();
    if (elapsed == 0)
      return 0.0;
    return static_cast<double>(
               total_frames_processed.load(std::memory_order_relaxed)) /
           static_cast<double>(elapsed);
  }
};

/**
 * @brief Copyable snapshot of engine statistics
 */
struct EngineStatisticsSnapshot {
  std::uint64_t total_frames_processed{0};
  std::uint64_t total_frames_dropped{0};
  std::uint64_t total_sync_drops{0};
  std::uint64_t total_backpressure_events{0};
  std::chrono::steady_clock::time_point start_time;

  EngineStatisticsSnapshot() = default;

  explicit EngineStatisticsSnapshot(const EngineStatistics &stats)
      : total_frames_processed(
            stats.total_frames_processed.load(std::memory_order_relaxed)),
        total_frames_dropped(
            stats.total_frames_dropped.load(std::memory_order_relaxed)),
        total_sync_drops(
            stats.total_sync_drops.load(std::memory_order_relaxed)),
        total_backpressure_events(
            stats.total_backpressure_events.load(std::memory_order_relaxed)),
        start_time(stats.start_time) {}

  [[nodiscard]] double dropRate() const {
    if (total_frames_processed + total_frames_dropped == 0)
      return 0.0;
    return static_cast<double>(total_frames_dropped) /
           static_cast<double>(total_frames_processed + total_frames_dropped) *
           100.0;
  }

  [[nodiscard]] double throughput() const {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - start_time)
                       .count();
    if (elapsed == 0)
      return 0.0;
    return static_cast<double>(total_frames_processed) /
           static_cast<double>(elapsed);
  }
};

// =============================================================================
// Stream Execution Engine
// =============================================================================

/**
 * @brief Execution engine with backpressure management and multi-stream sync
 *
 * This engine extends the basic execution model with:
 *
 * 1. Bounded Queues: Each node's input ports have bounded queues that
 *    prevent memory exhaustion when producers are faster than consumers.
 *
 * 2. Intelligent Drop Strategies: When queues overflow, configurable
 *    strategies determine which frames to drop (e.g., keep latest N).
 *
 * 3. Synchronized Dropping: For DAG structures with branches that converge,
 *    dropping a frame in one branch triggers coordinated drops in all
 *    parallel branches to maintain frame alignment at join points.
 *
 * Usage:
 * @code
 *   StreamEngineConfig config;
 *   config.num_workers = 8;
 *   config.default_queue_config.capacity = 32;
 *   config.default_queue_config.drop_strategy = "KeepLatest";
 *   config.default_queue_config.keep_latest_n = 2;
 *
 *   // Configure a slow node with smaller queue
 *   config.node_queue_configs["InferenceNode"] = {
 *       .capacity = 4,
 *       .drop_strategy = "KeepLatest",
 *       .keep_latest_n = 1
 *   };
 *
 *   auto engine = std::make_shared<StreamExecutionEngine>(config);
 *   engine->initialize(&graph);
 *   engine->execute(inputs);
 * @endcode
 */
class StreamExecutionEngine : public IExecutionEngine {
public:
  using NodePtr = std::shared_ptr<ILogicNode>;
  using QueuePtr = std::shared_ptr<BoundedDropQueue<PortDataPtr>>;

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  /**
   * @brief Default constructor with default configuration
   */
  StreamExecutionEngine();

  /**
   * @brief Construct with custom configuration
   */
  explicit StreamExecutionEngine(StreamEngineConfig config);

  ~StreamExecutionEngine() override;

  // Non-copyable
  StreamExecutionEngine(const StreamExecutionEngine &) = delete;
  StreamExecutionEngine &operator=(const StreamExecutionEngine &) = delete;

  // Movable
  StreamExecutionEngine(StreamExecutionEngine &&other) noexcept;
  StreamExecutionEngine &operator=(StreamExecutionEngine &&other) noexcept;

  // -------------------------------------------------------------------------
  // IExecutionEngine Interface
  // -------------------------------------------------------------------------

  bool initialize(Graph *graph, std::uint8_t num_workers = 4) override;

  bool execute(const PortDataMap &initial_inputs,
               bool wait_for_completion = true,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  void stopExecutionAsync() override;
  void stopExecutionSync() override;
  void reset() override;

  [[nodiscard]] EngineState getState() const override;

  void setPipelineResultCallback(
      std::function<void(const PortDataMap &)> callback) override;

  void setPipelineErrorCallback(
      std::function<void(const std::string &, const std::string &)> callback)
      override;

  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  getNodeStates() const override;

  // -------------------------------------------------------------------------
  // Extended Configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Update configuration (must be called before initialize)
   */
  void setConfig(StreamEngineConfig config);

  /**
   * @brief Get current configuration
   */
  [[nodiscard]] const StreamEngineConfig &config() const;

  /**
   * @brief Set queue configuration for a specific node
   */
  void setNodeQueueConfig(const std::string &node_name,
                          const NodeQueueConfig &config);

  /**
   * @brief Add a sync group manually
   */
  void addSyncGroup(const SyncGroupConfig &group_config);

  // -------------------------------------------------------------------------
  // Sync Coordinator Access
  // -------------------------------------------------------------------------

  /**
   * @brief Get the sync coordinator for advanced usage
   */
  [[nodiscard]] std::shared_ptr<SyncCoordinator> syncCoordinator() const;

  // -------------------------------------------------------------------------
  // Statistics & Monitoring
  // -------------------------------------------------------------------------

  /**
   * @brief Get engine statistics
   */
  [[nodiscard]] EngineStatisticsSnapshot statistics() const;

  /**
   * @brief Get queue statistics for a specific node/port
   */
  [[nodiscard]] const QueueStatistics *
  queueStatistics(const std::string &node_name,
                  const std::string &port_name) const;

  /**
   * @brief Get all queue fill ratios
   */
  [[nodiscard]] std::unordered_map<std::string, double> queueFillRatios() const;

  /**
   * @brief Set callback for drop events
   */
  void setDropEventCallback(DropEventCallback callback);

  /**
   * @brief Set callback for coordinated drops
   */
  void setCoordinatedDropCallback(CoordinatedDropCallback callback);

  // -------------------------------------------------------------------------
  // Streaming Input API (Concurrent Input Support)
  // -------------------------------------------------------------------------

  /**
   * @brief Start the engine in streaming mode
   *
   * After calling start(), the engine continuously processes data.
   * Use pushInput()/pushInputs() to submit data concurrently.
   *
   * @param context Optional pipeline context
   * @return true if engine started successfully
   */
  bool start(std::shared_ptr<PipelineContext> context = nullptr);

  /**
   * @brief Stop the streaming engine
   * @param wait_for_drain If true, wait for queued data to be processed
   */
  void stop(bool wait_for_drain = true);

  /**
   * @brief Check if engine is in streaming mode
   */
  [[nodiscard]] bool isStreaming() const;

  /**
   * @brief Push input data to a source node (thread-safe, non-blocking)
   *
   * Can be called concurrently from multiple threads.
   * Stream is handled automatically via queue drop strategies.
   *
   * @param source_node Name of the source node
   * @param port_name Input port name (use "" for first port)
   * @param data Data to push
   * @return Submission result with status and drop info
   */
  [[nodiscard]] InputSubmitResult pushInput(const std::string &source_node,
                                            const std::string &port_name,
                                            PortDataPtr data);

  /**
   * @brief Push input to source node's first input port
   */
  [[nodiscard]] InputSubmitResult pushInput(const std::string &source_node,
                                            PortDataPtr data);

  /**
   * @brief Push multiple inputs at once (for source nodes)
   *
   * @param inputs Map of source_node_name -> data
   * @return Map of source_node_name -> submission result
   */
  [[nodiscard]] std::unordered_map<std::string, InputSubmitResult>
  pushInputs(const PortDataMap &inputs);

  /**
   * @brief Get current queue depth for a node's input port
   */
  [[nodiscard]] std::size_t queueDepth(const std::string &node_name,
                                       const std::string &port_name = "") const;

  /**
   * @brief Check if a node's input queue has space
   */
  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name,
                                      const std::string &port_name = "") const;

  /**
   * @brief Wait until all queues drain below threshold
   * @param max_depth Maximum queue depth to wait for
   * @param timeout Maximum wait time
   * @return true if drained, false if timeout
   */
  bool waitForQueueDrain(
      std::size_t max_depth = 0,
      std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});

  // -------------------------------------------------------------------------
  // Debug & Info
  // -------------------------------------------------------------------------

  /**
   * @brief Get engine info string for logging
   */
  [[nodiscard]] std::string info() const;

private:
  // -------------------------------------------------------------------------
  // Internal Types
  // -------------------------------------------------------------------------

  /**
   * @brief Per-node state for backpressure management
   */
  struct NodeBackpressureState {
    NodePtr node;
    std::string name;

    // Input queues per port
    std::unordered_map<std::string, QueuePtr> input_queues;

    // Execution state
    std::unique_ptr<std::atomic<NodeExecutionState>> exec_state;

    // Sync context (if part of a sync group)
    std::unique_ptr<NodeSyncContext> sync_context;

    // Per-node mutex for fine-grained locking
    std::unique_ptr<std::mutex> mutex;

    // Node-specific config
    NodeQueueConfig queue_config;

    NodeBackpressureState() = default;
    explicit NodeBackpressureState(NodePtr n, const std::string &node_name)
        : node(std::move(n)), name(node_name),
          exec_state(std::make_unique<std::atomic<NodeExecutionState>>(
              NodeExecutionState::WAITING)),
          mutex(std::make_unique<std::mutex>()) {}
  };

  // -------------------------------------------------------------------------
  // Initialization Helpers
  // -------------------------------------------------------------------------

  void initializeNodeStates();
  void initializeInputQueues();
  void initializeSyncGroups();
  void autoDetectSyncGroups();
  void setupDropCallbacks();

  // -------------------------------------------------------------------------
  // Execution Helpers
  // -------------------------------------------------------------------------

  bool distributeInitialInputs(const PortDataMap &initial_inputs);
  void tryScheduleNode(const NodePtr &node);
  void executeNodeTask(NodePtr node, std::shared_ptr<PipelineContext> context);

  bool gatherNodeInputs(const NodePtr &node, PortDataMap &inputs);
  bool processNode(const NodePtr &node, const PortDataMap &inputs,
                   PortDataMap &outputs,
                   const std::shared_ptr<PipelineContext> &context);

  void propagateOutputs(const NodePtr &source_node, const PortDataMap &outputs);

  void handleNodeSuccess(const NodePtr &node, const PortDataMap &outputs);
  void handleNodeFailure(const NodePtr &node, const std::string &error_msg);

  void checkCompletionAndNotify();
  void resetInternalState();

  // -------------------------------------------------------------------------
  // Queue Management
  // -------------------------------------------------------------------------

  QueuePtr createQueue(const std::string &node_name,
                       const std::string &port_name);
  void pushToQueue(const NodePtr &node, const std::string &port_name,
                   PortDataPtr data);
  std::optional<PortDataPtr> popFromQueue(const NodePtr &node,
                                          const std::string &port_name);

  // -------------------------------------------------------------------------
  // Sync Helpers
  // -------------------------------------------------------------------------

  std::optional<FrameId> extractFrameId(const PortDataPtr &data) const;
  void handleDropEvent(const DropEvent &event);
  void handleCoordinatedDrop(const SyncGroupId &group_id,
                             const BranchId &branch_id, FrameId frame_id,
                             const std::string &reason);
  void applySyncDrops(const NodePtr &node);

  // -------------------------------------------------------------------------
  // Utility
  // -------------------------------------------------------------------------

  [[nodiscard]] bool isNodeReady(const NodePtr &node) const;
  [[nodiscard]] bool isSinkNode(const NodePtr &node) const;
  void collectFinalResults(const NodePtr &node, const PortDataMap &outputs);
  bool waitForCompletion();
  void identifySinkNodes();

  NodeQueueConfig getNodeQueueConfig(const std::string &node_name) const;
  std::string getSyncGroupForNode(const std::string &node_name) const;
  std::string getBranchIdForNode(const std::string &node_name) const;

private:
  // Configuration
  StreamEngineConfig m_config;

  // Core components
  Graph *m_graph{nullptr};
  std::unique_ptr<ThreadPool> m_threadPool;
  std::shared_ptr<PipelineContext> m_currentContext;

  // Node states
  std::unordered_map<NodePtr, std::unique_ptr<NodeBackpressureState>>
      m_nodeStates;
  std::unordered_map<std::string, NodePtr> m_nodeNameMap;

  // Sync coordination
  std::shared_ptr<SyncCoordinator> m_syncCoordinator;

  // Node-to-sync-group mapping
  std::unordered_map<std::string, SyncGroupId> m_nodeToSyncGroup;
  std::unordered_map<std::string, BranchId> m_nodeToBranch;

  // Engine state
  std::atomic<EngineState> m_engineState{EngineState::IDLE};
  std::atomic<int> m_activeTasks{0};
  std::atomic<bool> m_stopFlag{false};

  // Synchronization
  mutable std::mutex m_engineMutex;
  std::condition_variable m_completionCondition;

  // Callbacks
  std::function<void(const PortDataMap &)> m_onResultCallback;
  std::function<void(const std::string &, const std::string &)>
      m_onErrorCallback;
  DropEventCallback m_dropEventCallback;
  CoordinatedDropCallback m_coordinatedDropCallback;

  // Result collection
  std::vector<NodePtr> m_sinkNodes;
  PortDataMap m_accumulatedResults;
  mutable std::mutex m_resultsMutex;

  // Statistics
  EngineStatistics m_statistics;

  // Streaming mode support
  std::atomic<bool> m_streamingMode{false};
  std::condition_variable m_queueDrainCondition;
  mutable std::mutex m_queueDrainMutex;

  // Cleanup thread
  std::unique_ptr<std::thread> m_cleanupThread;
  std::atomic<bool> m_cleanupRunning{false};
};

} // namespace ai_pipe

#endif // AI_PIPE_BACKPRESSURE_EXECUTION_ENGINE_HPP