/**
 * @file execution_engine_impl.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief ExecutionEngine PIMPL implementation
 * @version 2.0
 * @date 2025-12-24
 *
 * This is an INTERNAL header file. Users should not include this directly.
 *
 * v2.0: Unified error handling with Result<T>. Internal processNode()
 *       now returns Result<void> instead of bool, propagating rich error
 *       context from node exceptions through the entire call chain.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_INTERNAL_EXECUTION_ENGINE_IMPL_HPP
#define AI_PIPE_INTERNAL_EXECUTION_ENGINE_IMPL_HPP

#include "ai_pipe/compiled_graph.hpp"
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/trace.hpp"
#include "lock_free_queue.hpp"
#include "work_stealing_thread_pool.hpp"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <vector>

namespace ai_pipe {

class ExecutionEngine::Impl {
public:
  using NodePtr = std::shared_ptr<ILogicNode>;
  using LockFreeQueueType = LockFreeNodeQueue<PortDataPtr>;

  /// Input ports per node addressable by the EOS bitmask (R6.1).
  static constexpr std::size_t k_max_eos_ports = 64;

  struct NodeState {
    NodePtr node;
    std::string name;

    // This node's index in the CompiledGraph; hot paths address states
    // through m_statesByIndex[index] with zero hashing.
    CompiledGraph::NodeIndex index{CompiledGraph::k_invalid_index};

    // Lock-free queues per input port
    std::unordered_map<std::string, std::shared_ptr<LockFreeQueueType>>
        lock_free_queues;

    // Cached once at initialize: the node's declared input ports and the
    // matching queues in declaration order. The hot scheduling/gather
    // paths iterate these instead of re-calling the virtual
    // getExpectedInputPorts() and hashing port names per execution.
    std::vector<std::string> input_ports;
    std::vector<LockFreeQueueType *> input_queues;

    // True when the sync strategy tracks this node (computed once at
    // initialize): gates per-frame shouldDrop/markProcessed calls.
    bool sync_tracked{false};

    // Per-node execution statistics, snapshotted into
    // EngineStatisticsSnapshot::node_stats.
    AtomicNodeStatistics stats;

    // Single-word scheduling state machine. The WAITING->READY CAS in
    // scheduleNodeExecution() is the only claim point; concurrent
    // schedule attempts may redundantly evaluate the (cheap) strategy
    // but exactly one wins the CAS. NodeState lives behind a
    // unique_ptr and is never moved, so the atomic can be a direct
    // member.
    std::atomic<NodeExecutionState> exec_state{NodeExecutionState::WAITING};

    // Written by the executing worker, read concurrently by scheduling
    // attempts and by completion checks (including stragglers from a
    // previous execution racing a resetInternalState), hence atomic.
    std::atomic<std::chrono::steady_clock::rep> last_execution_ticks{0};
    std::atomic<std::uint64_t> execution_count{0};

    // Join timeout tracking (F6), touched only when
    // EngineConfig::join_wait_timeout > 0. join_wait_since_ticks marks
    // when the watchdog first saw this node partially ready (0 = not
    // waiting); degrade_pending asks the next failed aligned gather to
    // apply the configured degradation instead of waiting.
    std::atomic<std::chrono::steady_clock::rep> join_wait_since_ticks{0};
    std::atomic<bool> degrade_pending{false};

    // True while a deferred reschedule entry for this node is armed in
    // the engine defer timer (R3.2); deduplicates timer entries.
    std::atomic<bool> defer_pending{false};

    // End-of-stream latches (R6.1), one bit per input port, indexed the
    // same as input_ports/input_queues. A bit means "no more data will
    // arrive on this port"; the port is fully drained once the bit is
    // set AND its queue is empty. Kept outside the queue so a drop
    // policy can never evict the marker and a full queue can never
    // block it - see docs/design/eos_flush.md §4.
    //
    // A bitmask (rather than one flag per port) keeps the drained check
    // to a single relaxed load on the gather path. 64 input ports on one
    // node is the cap; signalEndOfStream rejects beyond it, and
    // ready_port_mask in SchedulingContext already sets this precedent.
    std::atomic<std::uint64_t> eos_mask{0};

    // Set once the node's flush hook has run and EOS has been latched
    // on its downstream ports; makes EOS handling exactly-once even
    // though several paths race to detect it.
    std::atomic<bool> eos_propagated{false};

    [[nodiscard]] std::chrono::steady_clock::time_point lastExecution() const {
      return std::chrono::steady_clock::time_point{
          std::chrono::steady_clock::duration{
              last_execution_ticks.load(std::memory_order_relaxed)}};
    }

    QueueConfig queue_config;

    NodeState() = default;

    explicit NodeState(NodePtr n, const std::string &node_name)
        : node(std::move(n)), name(node_name) {}
  };

  Impl();
  explicit Impl(const EngineConfig &config);
  ~Impl();

  // Neither copyable nor movable: worker tasks capture `this`, so the Impl
  // address must stay stable for the engine's lifetime. ExecutionEngine's
  // move semantics come from moving the unique_ptr<Impl>, which never
  // relocates the Impl object itself.
  Impl(const Impl &) = delete;
  Impl &operator=(const Impl &) = delete;
  Impl(Impl &&) = delete;
  Impl &operator=(Impl &&) = delete;

  Result<void>
  setSchedulerStrategy(std::unique_ptr<ISchedulerStrategy> strategy);
  Result<void> setSyncStrategy(std::unique_ptr<ISyncStrategy> strategy);
  Result<void> setTraceSink(std::shared_ptr<ITraceSink> sink);
  void configureForMode(ExecutionMode mode);

  Result<void> initialize(Graph *graph, std::uint8_t num_workers);
  Result<void>
  execute(const PortDataMap &initial_inputs, bool wait_for_completion,
          std::shared_ptr<PipelineContext> context,
          std::optional<std::chrono::milliseconds> timeout = std::nullopt);
  void stopExecutionAsync();
  void stopExecutionSync();
  void reset();

  [[nodiscard]] EngineState getState() const;
  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  getNodeStates() const;

  void
  setPipelineResultCallback(std::function<void(const PortDataMap &)> callback);
  void setPipelineErrorCallback(
      std::function<void(const std::string &, const std::string &)> callback);
  void setDropCallback(std::function<void(const std::string &, std::uint64_t,
                                          const std::string &)>
                           callback);
  void setEndOfStreamCallback(std::function<void()> callback);

  Result<void> startStreaming(std::shared_ptr<PipelineContext> context);
  void stopStreaming(bool wait_for_drain);
  [[nodiscard]] bool isStreaming() const;

  /**
   * @brief Block until the engine leaves RUNNING (R4.4)
   *
   * Rides the completion CV: every final-state transition
   * (completion, stop, error, streaming shutdown) notifies it, so no
   * polling is involved. Returns immediately when not RUNNING.
   */
  void waitForIdle();

  Result<PushStatus> pushInput(const std::string &source_node,
                               const std::string &port_name, PortDataPtr data);

  Result<PushStatus> pushInput(const std::string &source_node,
                               PortDataPtr data);

  Result<void> signalEndOfStream(const std::string &source_node,
                                 const std::string &port_name);
  Result<void> waitForEndOfStream(std::chrono::milliseconds timeout);
  [[nodiscard]] bool isEndOfStreamReached() const;

  [[nodiscard]] EngineStatisticsSnapshot statistics() const;
  [[nodiscard]] std::size_t queueDepth(const std::string &node_name,
                                       const std::string &port_name) const;
  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name,
                                      const std::string &port_name) const;
  Result<void> waitForDrain(std::size_t max_depth,
                            std::chrono::milliseconds timeout);

  void setNodeQueueConfig(const std::string &node_name,
                          const QueueConfig &config);
  [[nodiscard]] const EngineConfig &config() const { return m_config; }

  [[nodiscard]] std::string info() const;
  [[nodiscard]] std::string strategyInfo() const;

private:
  /**
   * @brief Run setup() on every node in topological order
   *
   * On failure, tears down the already-set-up prefix in reverse order
   * and returns the failing node's error.
   */
  Result<void> setupNodes(const std::shared_ptr<PipelineContext> &context);

  /** @brief Run teardown() in reverse setup order (idempotent) */
  void teardownNodes() noexcept;

  void initializeNodeStates();
  void initializeQueues();
  void setupDropCallbacks();

  // Node-state lookup (R4.1): index is the primary key; name and
  // pointer resolve through the CompiledGraph. All return nullptr for
  // unknown keys (indexOf/indexOfPtr yield k_invalid_index, which the
  // range check rejects).
  [[nodiscard]] NodeState *stateByIndex(CompiledGraph::NodeIndex index) const {
    return index < m_statesByIndex.size() ? m_statesByIndex[index].get()
                                          : nullptr;
  }
  [[nodiscard]] NodeState *stateByName(const std::string &name) const {
    return m_compiledGraph ? stateByIndex(m_compiledGraph->indexOf(name))
                           : nullptr;
  }
  [[nodiscard]] NodeState *stateByPtr(const ILogicNode *node) const {
    return m_compiledGraph ? stateByIndex(m_compiledGraph->indexOfPtr(node))
                           : nullptr;
  }

  bool distributeInitialInputs(const PortDataMap &initial_inputs);

  /** @brief Boundary wrapper: resolves the index once, then dispatches */
  void tryScheduleNode(const NodePtr &node);

  /** @brief Hot-path scheduling entry: no hashing, no string compares */
  void tryScheduleNode(CompiledGraph::NodeIndex index);

  void scheduleNodeExecution(NodeState &state);
  void executeNodeTask(CompiledGraph::NodeIndex index,
                       const std::shared_ptr<PipelineContext> &context);

  bool gatherNodeInputs(NodeState &state, PortDataMap &inputs);

  /**
   * @brief Aligned gather for multi-input nodes (R4.3)
   *
   * Binds the engine environment (coordinated-drop-consuming peek,
   * pop-and-deliver, stale-drop accounting) to the unified skeleton in
   * frame_alignment.hpp. The per-policy pairing/discard rules live in
   * the policy structs there (FrameIdPolicy / StreamFrameIdPolicy /
   * TimestampPolicy). Returns false when any port has no poppable data
   * yet (transient; caller reschedules).
   */
  template <typename Policy>
  bool runAlignedGather(NodeState &state, PortDataMap &inputs,
                        const Policy &policy);

  /**
   * @brief Peek a port head, consuming pending coordinated sync drops
   * @return The head packet, or nullopt if the port has no poppable data
   */
  std::optional<PortDataPtr> peekAlignmentHead(NodeState &state,
                                               std::size_t port_index);

  /**
   * @brief Pop an unpairable head, recording the drop and reporting it
   *        to the sync strategy for tracked nodes
   */
  void dropStaleHead(NodeState &state, std::size_t port_index,
                     const PortData &head, const char *reason);

  /**
   * @brief Apply the join timeout degradation to a blocked join (F6)
   *
   * Called when an aligned gather failed and the watchdog flagged the
   * node as timed out. Selects the oldest pairing set among the ports
   * that do have data (per the active alignment policy) and either
   * delivers it as partial inputs (returns true) or discards it as a
   * skipped frame (returns false; reported through the drop callback).
   */
  bool degradeJoinGather(NodeState &state, PortDataMap &inputs);

  /**
   * @brief Watchdog driving join timeouts (streaming mode only)
   *
   * Nothing re-schedules a partially-ready join while no new data
   * arrives, so timeouts need an active wakeup: a lightweight thread
   * (started only when join_wait_timeout > 0 and the graph has
   * multi-input nodes) scans those nodes every timeout/4 and schedules
   * a degraded execution once a node stays partially ready past the
   * timeout. Timeout accuracy is one tick (+-timeout/4).
   */
  void startJoinTimeoutWatchdog();
  void stopJoinTimeoutWatchdog();
  void joinTimeoutWatchdogLoop();

  /**
   * @brief Engine-level deferred reschedule (R3.2)
   *
   * Generalizes the watchdog idea into a timer honoring the
   * scheduler's DeferToNextCycle decisions: a deferred node (e.g.
   * min_interval rate limiting) arms a timer entry, and a lazily
   * started timer thread re-runs tryScheduleNode() when the delay
   * expires. Without this, a rate-limited tail frame was stranded
   * until the next data event happened to re-evaluate the node -
   * possibly forever.
   */
  void scheduleDeferredRetry(NodeState &state, std::chrono::milliseconds delay);
  void stopDeferTimer();
  void deferTimerLoop();

  /**
   * @brief Emit a trace event if a sink is installed (F7)
   *
   * Frame identity is taken from `packet` when non-null. Costs one
   * pointer check when tracing is disabled.
   */
  void emitTrace(TracePhase phase, const std::string &node,
                 std::string_view detail, const PortData *packet,
                 Timestamp start, std::chrono::microseconds duration);

  /** @brief First packet carrying a frame id, or nullptr */
  static const PortData *primaryFrame(const PortDataMap &packets);

  void recordSyncDrop(const NodeState &state, const std::string &port_name,
                      FrameId frame_id, const char *reason);

  /**
   * @brief Invoke the registered user callbacks safely
   *
   * Each helper copies the std::function under m_callbackMutex and invokes
   * the copy: a callback may re-register callbacks from within its own
   * invocation (runAsync restores the resident ones after fulfilling its
   * promise), so the member must not be the object being executed. The
   * copy is invoked under a catch-all guard - a throwing user callback
   * must not skip completion notification or escape into the thread pool.
   */
  void invokeResultCallback(const PortDataMap &results);
  void invokeErrorCallback(const std::string &message,
                           const std::string &node_name);
  void invokeDropCallback(const std::string &node_name, std::uint64_t frame_id,
                          const std::string &reason);
  void invokeEndOfStreamCallback();

  /**
   * @brief Account a successful queue pop
   *
   * total_wait_time_us accumulates the frame's age at dequeue (time
   * from pipeline entry to this pop), so avgWaitTimeUs() reports how
   * long inputs sat queued before being consumed.
   */
  void recordDequeue(const PortDataPtr &data);

  /**
   * @brief Process a single node, converting exceptions to Error
   * @return Result<void> - success, or Error with
   * NodeException/NodeUnknownException
   */
  Result<void> processNode(const NodePtr &node, const PortDataMap &inputs,
                           PortDataMap &outputs,
                           const std::shared_ptr<PipelineContext> &context);

  void propagateOutputs(NodeState &source_state, const PortDataMap &outputs);

  void handleNodeSuccess(NodeState &state, const PortDataMap &outputs);
  void handleNodeFailure(NodeState &state, const Error &error);

  // --- End of stream (R6.1) ---

  /** @brief Is this port's EOS latch set? */
  [[nodiscard]] static bool portEosLatched(const NodeState &state,
                                           std::size_t port_index);

  /** @brief Latch set AND queue empty: no more data will ever arrive */
  [[nodiscard]] static bool portDrained(const NodeState &state,
                                        std::size_t port_index);

  /**
   * @brief Have all of the node's input ports drained?
   *
   * False for a node with no input ports: EOS is a per-port property,
   * so a node with nothing to close can never reach it.
   */
  [[nodiscard]] static bool allInputPortsDrained(const NodeState &state);

  /** @brief Any port drained while at least one is still open */
  [[nodiscard]] static bool hasPartiallyDrainedInputs(const NodeState &state);

  /**
   * @brief Run EOS handling for a node if it is due, exactly once
   *
   * Claims the node through its exec_state (so the flush hook can never
   * run concurrently with process()), invokes onEndOfStream(),
   * propagates whatever it emitted, then latches EOS on every
   * downstream port and cascades the check into those nodes.
   *
   * @return true if this call performed the node's EOS handling; false
   *         when EOS is not due, already done, or the node is busy (a
   *         busy node is re-checked when its execution finishes)
   */
  bool checkNodeEndOfStream(NodeState &state,
                            const std::shared_ptr<PipelineContext> &context);

  /** @brief Latch EOS on a node's input port; cascades the EOS check */
  void latchPortEos(NodeState &state, std::size_t port_index,
                    const std::shared_ptr<PipelineContext> &context);

  /** @brief Account a sink reaching EOS; fires the notification on the last */
  void recordSinkEndOfStream();

  void checkCompletionAndNotify();

  /**
   * @brief Wait for the running batch execution to finish
   *
   * With a timeout (R1.3), the wait is bounded: on expiry the active
   * context's CancellationToken is cancelled (cooperative channel) and
   * the stop protocol triggered, then ExecutionTimeout returns
   * immediately - an in-flight node may still be running until its next
   * cancellation point. The engine stays STOPPED with possible queue
   * residue until reset().
   */
  Result<void> waitForCompletion(
      std::optional<std::chrono::milliseconds> timeout = std::nullopt);

  void resetInternalState();

  /**
   * @brief Unified stop check for scheduling points (R3.1)
   *
   * True when m_stopFlag is set OR the active context's
   * CancellationToken is cancelled. A cancelled token is converted into
   * the engine's stop protocol (stopFlag + waiter wakeup) exactly once
   * via stopExecutionAsync(), so completion waiters and the deeper
   * stopFlag-only checks all observe a plain stop afterwards. The token
   * is reset at execution/streaming start, so a token cancelled during
   * one run cannot poison the next.
   */
  bool isStopRequested();

  /**
   * @brief Wake completion/drain waiters without losing wakeups
   *
   * The empty lock/unlock of m_completionMutex orders state changes
   * (m_activeTasks, m_stopFlag, queue sizes) against a waiter's
   * predicate check, so plain condition waits need no timeout polling.
   */
  void notifyCompletionWaiters();

  [[nodiscard]] bool allQueuesDrained(std::size_t max_depth) const;

  /**
   * @brief Push data to a node's input queue, honoring its drop policy
   * @return true if the data was accepted (possibly evicting an older frame),
   *         false if it was rejected (DropTail policy on a full queue) or the
   *         target queue does not exist
   */
  [[nodiscard]] bool pushToQueue(NodeState &state, const std::string &port_name,
                                 PortDataPtr data);

  void recordQueueRejection(const std::string &node_name,
                            const std::string &port_name);

  /**
   * @brief Assign frame identity to a packet entering the pipeline
   *
   * Stamps a monotonic FrameId (and a capture timestamp) onto packets
   * injected from outside that carry no id yet. Explicit ids are
   * preserved so callers can drive their own frame numbering.
   */
  void stampIncomingFrame(const PortDataPtr &data);

  /**
   * @brief Propagate frame identity from inputs to fresh output packets
   */
  static void inheritFrameIdentity(const PortDataMap &inputs,
                                   PortDataMap &outputs);
  static std::size_t getQueueSize(const NodeState &state,
                                  const std::string &port_name);

  void collectResults(const NodePtr &node, const PortDataMap &outputs);
  [[nodiscard]] QueueConfig
  getNodeQueueConfig(const std::string &node_name) const;
  [[nodiscard]] std::string getFirstInputPort(const NodePtr &node) const;

  static std::string stateToString(EngineState state);

  bool isInputPort(const NodePtr &node, const std::string &port_name) const;
  bool isOutputPort(const NodePtr &node, const std::string &port_name) const;

  std::string getFirstOutputPort(const NodePtr &node) const;

  Result<PushStatus> routeToDownstream(const NodePtr &source_node,
                                       const std::string &output_port,
                                       const PortDataPtr &data);

private:
  EngineConfig m_config;
  std::unordered_map<std::string, QueueConfig> m_nodeQueueConfigs;

  std::unique_ptr<ISchedulerStrategy> m_schedulerStrategy;
  std::unique_ptr<ISyncStrategy> m_syncStrategy;

  // Trace sink (F7). Set only while IDLE, read from worker threads
  // while running; the start/stop transitions provide the ordering.
  std::shared_ptr<ITraceSink> m_traceSink;

  Graph *m_graph{nullptr};

  // Immutable indexed view of m_graph, rebuilt by initialize(). Holds the
  // precomputed routing table and topology sets used on the hot path.
  std::optional<CompiledGraph> m_compiledGraph;

  std::unique_ptr<WorkStealingThreadPool> m_threadPool;

  // Written by execute()/startStreaming() and cleared on completion while
  // worker threads concurrently read it in scheduleNodeExecution(); atomic
  // shared_ptr keeps those accesses race-free.
  std::atomic<std::shared_ptr<PipelineContext>> m_currentContext;

  // Owning per-node states, indexed by CompiledGraph::NodeIndex (R4.1).
  // The single node-state container: name/pointer resolution goes
  // through CompiledGraph (indexOf / indexOfPtr) via the stateBy*
  // helpers, and cold paths (statistics, reset, drain checks) are
  // plain sequential scans.
  std::vector<std::unique_ptr<NodeState>> m_statesByIndex;

  // Multi-input node indices, precomputed at initialize; the join
  // timeout watchdog scans only these.
  std::vector<CompiledGraph::NodeIndex> m_multiInputIndices;

  // Join timeout watchdog (see startJoinTimeoutWatchdog)
  std::thread m_joinWatchdog;
  std::mutex m_watchdogMutex;
  std::condition_variable m_watchdogCV;
  bool m_watchdogStop{false};

  // Deferred reschedule timer (see scheduleDeferredRetry); the thread
  // starts lazily on the first deferred decision.
  struct DeferEntry {
    std::chrono::steady_clock::time_point due;
    CompiledGraph::NodeIndex index{CompiledGraph::k_invalid_index};
    bool operator>(const DeferEntry &other) const { return due > other.due; }
  };
  std::thread m_deferTimer;
  std::mutex m_deferMutex;
  std::condition_variable m_deferCV;
  bool m_deferStop{false};
  bool m_deferRunning{false};
  std::priority_queue<DeferEntry, std::vector<DeferEntry>, std::greater<>>
      m_deferQueue;

  // Nodes that completed setup(), in setup order; drives teardown.
  std::vector<NodePtr> m_setUpNodes;

  // Monotonic FrameId source for packets entering the pipeline without
  // an assigned id (see stampIncomingFrame).
  std::atomic<FrameId> m_nextFrameId{1};

  // Per-stream FrameId sources, used instead of m_nextFrameId when the
  // alignment policy is StreamFrameId (ids must be monotonic within a
  // stream, not globally). Ingress-only, so a plain mutex suffices.
  std::mutex m_streamStampMutex;
  std::unordered_map<StreamId, FrameId> m_nextStreamFrameId;

  std::atomic<EngineState> m_engineState{EngineState::IDLE};
  std::atomic<int> m_activeTasks{0};
  std::atomic<bool> m_stopFlag{false};
  std::atomic<bool> m_streamingMode{false};

  // End-of-stream tracking (R6.1). m_sinkCount is fixed at initialize;
  // m_sinksAtEos counts sinks that have completed EOS handling, and
  // m_endOfStreamReached latches when the two meet. Waiters ride
  // m_completionCV, which every EOS transition notifies.
  std::size_t m_sinkCount{0};
  std::atomic<std::size_t> m_sinksAtEos{0};
  std::atomic<bool> m_endOfStreamReached{false};

  mutable std::mutex m_engineMutex;
  std::mutex m_completionMutex;
  std::condition_variable m_completionCV;
  std::mutex m_resultsMutex;

  PortDataMap m_accumulatedResults;

  // Guards registration and lookup of the three user callbacks below.
  // Never held while a callback runs (see invoke*Callback helpers).
  mutable std::mutex m_callbackMutex;

  std::function<void(const PortDataMap &)> m_resultCallback;
  std::function<void(const std::string &, const std::string &)> m_errorCallback;
  std::function<void(const std::string &, std::uint64_t, const std::string &)>
      m_dropCallback;
  std::function<void()> m_endOfStreamCallback;

  EngineStatistics m_statistics;
};

} // namespace ai_pipe

#endif // AI_PIPE_INTERNAL_EXECUTION_ENGINE_IMPL_HPP
