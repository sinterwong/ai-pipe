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

#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "compiled_graph.hpp"
#include "lock_free_queue.hpp"
#include "work_stealing_thread_pool.hpp"
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <vector>

namespace ai_pipe {

class ExecutionEngine::Impl {
public:
  using NodePtr = std::shared_ptr<ILogicNode>;
  using LockFreeQueueType = LockFreeNodeQueue<PortDataPtr>;

  struct NodeState {
    NodePtr node;
    std::string name;

    // Lock-free queues per input port
    std::unordered_map<std::string, std::shared_ptr<LockFreeQueueType>>
        lock_free_queues;

    // Cached once at initialize: the node's declared input ports and the
    // matching queues in declaration order. The hot scheduling/gather
    // paths iterate these instead of re-calling the virtual
    // getExpectedInputPorts() and hashing port names per execution.
    std::vector<std::string> input_ports;
    std::vector<LockFreeQueueType *> input_queues;

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
  void configureForMode(ExecutionMode mode);

  Result<void> initialize(Graph *graph, std::uint8_t num_workers);
  Result<void> execute(const PortDataMap &initial_inputs,
                       bool wait_for_completion,
                       std::shared_ptr<PipelineContext> context);
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

  Result<void> startStreaming(std::shared_ptr<PipelineContext> context);
  void stopStreaming(bool wait_for_drain);
  [[nodiscard]] bool isStreaming() const;

  Result<PushStatus> pushInput(const std::string &source_node,
                               const std::string &port_name, PortDataPtr data);

  Result<PushStatus> pushInput(const std::string &source_node,
                               PortDataPtr data);

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
  void initializeNodeStates();
  void initializeQueues();
  void identifySinkNodes();
  void setupDropCallbacks();

  bool distributeInitialInputs(const PortDataMap &initial_inputs);
  void scheduleReadyNodes();
  void tryScheduleNode(const NodePtr &node);
  void scheduleNodeExecution(const NodePtr &node);
  void executeNodeTask(NodePtr node, std::shared_ptr<PipelineContext> context);

  bool gatherNodeInputs(const NodePtr &node, PortDataMap &inputs);

  /**
   * @brief Process a single node, converting exceptions to Error
   * @return Result<void> - success, or Error with
   * NodeException/NodeUnknownException
   */
  Result<void> processNode(const NodePtr &node, const PortDataMap &inputs,
                           PortDataMap &outputs,
                           const std::shared_ptr<PipelineContext> &context);

  void propagateOutputs(const NodePtr &source, const PortDataMap &outputs);

  void handleNodeSuccess(const NodePtr &node, const PortDataMap &outputs);
  void handleNodeFailure(const NodePtr &node, const Error &error);

  void checkCompletionAndNotify();
  Result<void> waitForCompletion();
  void resetInternalState();

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
  [[nodiscard]] bool pushToQueue(const NodePtr &node,
                                 const std::string &port_name,
                                 PortDataPtr data);

  void recordQueueRejection(const NodePtr &node, const std::string &port_name);
  std::optional<PortDataPtr> popFromQueue(const NodePtr &node,
                                          const std::string &port_name);
  bool hasDataInQueue(const NodePtr &node, const std::string &port_name) const;
  std::size_t getQueueSize(const NodePtr &node,
                           const std::string &port_name) const;

  [[nodiscard]] bool isSourceNode(const NodePtr &node) const;
  [[nodiscard]] bool isSinkNode(const NodePtr &node) const;
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
                                       PortDataPtr data);

private:
  EngineConfig m_config;
  std::unordered_map<std::string, QueueConfig> m_nodeQueueConfigs;

  std::unique_ptr<ISchedulerStrategy> m_schedulerStrategy;
  std::unique_ptr<ISyncStrategy> m_syncStrategy;

  Graph *m_graph{nullptr};

  // Immutable indexed view of m_graph, rebuilt by initialize(). Holds the
  // precomputed routing table and topology sets used on the hot path.
  std::optional<CompiledGraph> m_compiledGraph;

  std::unique_ptr<WorkStealingThreadPool> m_threadPool;

  // Written by execute()/startStreaming() and cleared on completion while
  // worker threads concurrently read it in scheduleNodeExecution(); atomic
  // shared_ptr keeps those accesses race-free.
  std::atomic<std::shared_ptr<PipelineContext>> m_currentContext;

  std::unordered_map<NodePtr, std::unique_ptr<NodeState>> m_nodeStates;
  std::unordered_map<std::string, NodePtr> m_nodeNameMap;
  std::vector<NodePtr> m_sinkNodes;

  std::atomic<EngineState> m_engineState{EngineState::IDLE};
  std::atomic<int> m_activeTasks{0};
  std::atomic<bool> m_stopFlag{false};
  std::atomic<bool> m_streamingMode{false};

  mutable std::mutex m_engineMutex;
  std::mutex m_completionMutex;
  std::condition_variable m_completionCV;
  std::mutex m_resultsMutex;

  PortDataMap m_accumulatedResults;

  std::function<void(const PortDataMap &)> m_resultCallback;
  std::function<void(const std::string &, const std::string &)> m_errorCallback;
  std::function<void(const std::string &, std::uint64_t, const std::string &)>
      m_dropCallback;

  EngineStatistics m_statistics;
};

} // namespace ai_pipe

#endif // AI_PIPE_INTERNAL_EXECUTION_ENGINE_IMPL_HPP
