/**
 * @file execution_engine_impl.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief ExecutionEngine PIMPL implementation
 * @version 1.0
 * @date 2025-12-24
 *
 * This is an INTERNAL header file. Users should not include this directly.
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_INTERNAL_EXECUTION_ENGINE_IMPL_HPP
#define AI_PIPE_INTERNAL_EXECUTION_ENGINE_IMPL_HPP

#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "bounded_drop_queue.hpp"
#include "drop_strategy.hpp"
#include "thread_pool.hpp"
#include "thread_safe_queue.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <vector>

namespace ai_pipe {

class ExecutionEngine::Impl {
public:
  using NodePtr = std::shared_ptr<ILogicNode>;
  using BoundedQueueType = BoundedDropQueue<PortDataPtr>;
  using UnboundedQueueType = ThreadSafeQueue<PortDataPtr>;

  struct NodeState {
    NodePtr node;
    std::string name;

    std::unordered_map<std::string, std::shared_ptr<BoundedQueueType>>
        bounded_queues;
    std::unordered_map<std::string, std::shared_ptr<UnboundedQueueType>>
        unbounded_queues;

    std::unique_ptr<std::atomic<NodeExecutionState>> exec_state;
    std::chrono::steady_clock::time_point last_execution;
    std::uint64_t execution_count{0};

    std::unique_ptr<std::mutex> mutex;

    QueueConfig queue_config;

    NodeState()
        : exec_state(std::make_unique<std::atomic<NodeExecutionState>>(
              NodeExecutionState::WAITING)),
          mutex(std::make_unique<std::mutex>()) {}

    explicit NodeState(NodePtr n, const std::string &node_name)
        : node(std::move(n)), name(node_name),
          exec_state(std::make_unique<std::atomic<NodeExecutionState>>(
              NodeExecutionState::WAITING)),
          mutex(std::make_unique<std::mutex>()) {}
  };

  Impl();
  explicit Impl(const EngineConfig &config);
  ~Impl();

  Impl(Impl &&other) noexcept;
  Impl &operator=(Impl &&other) noexcept;

  void setSchedulerStrategy(std::unique_ptr<ISchedulerStrategy> strategy);
  void setSyncStrategy(std::unique_ptr<ISyncStrategy> strategy);
  void configureForMode(ExecutionMode mode);

  bool initialize(Graph *graph, std::uint8_t num_workers);
  bool execute(const PortDataMap &initial_inputs, bool wait_for_completion,
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

  bool startStreaming(std::shared_ptr<PipelineContext> context);
  void stopStreaming(bool wait_for_drain);
  [[nodiscard]] bool isStreaming() const;

  QueuePushResult pushInput(const std::string &source_node,
                            const std::string &port_name, PortDataPtr data);

  QueuePushResult pushInput(const std::string &source_node, PortDataPtr data);

  [[nodiscard]] EngineStatisticsSnapshot statistics() const;
  [[nodiscard]] std::size_t queueDepth(const std::string &node_name,
                                       const std::string &port_name) const;
  [[nodiscard]] bool hasQueueCapacity(const std::string &node_name,
                                      const std::string &port_name) const;
  bool waitForDrain(std::size_t max_depth, std::chrono::milliseconds timeout);

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
  bool processNode(const NodePtr &node, const PortDataMap &inputs,
                   PortDataMap &outputs,
                   const std::shared_ptr<PipelineContext> &context);

  void propagateOutputs(const NodePtr &source, const PortDataMap &outputs);

  void handleNodeSuccess(const NodePtr &node, const PortDataMap &outputs);
  void handleNodeFailure(const NodePtr &node, const std::string &error);

  void checkCompletionAndNotify();
  bool waitForCompletion();
  void resetInternalState();

  void pushToQueue(const NodePtr &node, const std::string &port_name,
                   PortDataPtr data);
  std::optional<PortDataPtr> popFromQueue(const NodePtr &node,
                                          const std::string &port_name);
  bool hasDataInQueue(const NodePtr &node, const std::string &port_name) const;
  std::size_t getQueueSize(const NodePtr &node,
                           const std::string &port_name) const;

  [[nodiscard]] bool isSourceNode(const NodePtr &node) const;
  [[nodiscard]] bool isSinkNode(const NodePtr &node) const;
  void collectResults(const NodePtr &node, const PortDataMap &outputs);
  [[nodiscard]] std::vector<std::string>
  getReadyPorts(const NodePtr &node) const;
  [[nodiscard]] QueueConfig
  getNodeQueueConfig(const std::string &node_name) const;
  [[nodiscard]] std::string getFirstInputPort(const NodePtr &node) const;

  static std::string stateToString(EngineState state);

  bool isInputPort(const NodePtr &node, const std::string &port_name) const;
  bool isOutputPort(const NodePtr &node, const std::string &port_name) const;

  // 获取端口
  std::string getFirstOutputPort(const NodePtr &node) const;

  // 数据路由
  QueuePushResult routeToDownstream(const NodePtr &source_node,
                                    const std::string &output_port,
                                    PortDataPtr data);

private:
  EngineConfig m_config;
  std::unordered_map<std::string, QueueConfig> m_nodeQueueConfigs;

  std::unique_ptr<ISchedulerStrategy> m_schedulerStrategy;
  std::unique_ptr<ISyncStrategy> m_syncStrategy;

  Graph *m_graph{nullptr};
  std::unique_ptr<ThreadPool> m_threadPool;
  std::shared_ptr<PipelineContext> m_currentContext;

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
