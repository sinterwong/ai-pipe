/**
 * @file default_execution_engine.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.2
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 */

#ifndef AI_PIPE_DEFAULT_EXECUTION_ENGINE_HPP
#define AI_PIPE_DEFAULT_EXECUTION_ENGINE_HPP

#include "ai_pipe/i_execution_engine.hpp"
#include "thread_pool.hpp"
#include "thread_safe_queue.hpp"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace ai_pipe {

/**
 * @brief Manages per-node execution states with thread-safe access
 */
class NodeStateManager {
public:
  using NodePtr = std::shared_ptr<ILogicNode>;

  void initialize(const std::vector<NodePtr> &nodes);
  void clear();

  [[nodiscard]] NodeExecutionState getState(const NodePtr &node) const;
  void setState(const NodePtr &node, NodeExecutionState state);
  [[nodiscard]] bool compareAndSetState(const NodePtr &node,
                                        NodeExecutionState expected,
                                        NodeExecutionState desired);

  [[nodiscard]] std::unordered_map<std::string, NodeExecutionState>
  getAllStates() const;

  void resetAllToWaiting();

private:
  std::unordered_map<NodePtr, std::unique_ptr<std::atomic<NodeExecutionState>>>
      m_states;
};

/**
 * @brief Manages per-node input queues with thread-safe access
 */
class InputQueueManager {
public:
  using NodePtr = std::shared_ptr<ILogicNode>;
  using QueuePtr = std::shared_ptr<ThreadSafeQueue<PortDataPtr>>;
  using PortQueues = std::unordered_map<std::string, QueuePtr>;

  void initialize(const std::vector<NodePtr> &nodes);
  void clear();

  void pushToQueue(const NodePtr &node, const std::string &port_name,
                   PortDataPtr data);
  [[nodiscard]] std::optional<PortDataPtr>
  tryPopFromQueue(const NodePtr &node, const std::string &port_name);

  [[nodiscard]] bool
  areAllInputsReady(const NodePtr &node,
                    const std::vector<std::string> &expected_ports) const;

  void clearAllQueues();
  void clearNodeQueues(const NodePtr &node);

  [[nodiscard]] bool hasQueue(const NodePtr &node,
                              const std::string &port_name) const;

private:
  std::unordered_map<NodePtr, PortQueues> m_queues;
};

/**
 * @brief Manages per-node mutexes for fine-grained locking
 */
class NodeMutexManager {
public:
  using NodePtr = std::shared_ptr<ILogicNode>;

  void initialize(const std::vector<NodePtr> &nodes);
  void clear();

  [[nodiscard]] std::mutex &getMutex(const NodePtr &node);

private:
  std::unordered_map<NodePtr, std::unique_ptr<std::mutex>> m_mutexes;
};

/**
 * @brief Default execution engine implementing parallel graph execution
 */
class DefaultExecutionEngine : public IExecutionEngine {
public:
  DefaultExecutionEngine();
  ~DefaultExecutionEngine() override;

  // Non-copyable
  DefaultExecutionEngine(const DefaultExecutionEngine &) = delete;
  DefaultExecutionEngine &operator=(const DefaultExecutionEngine &) = delete;

  // Movable
  DefaultExecutionEngine(DefaultExecutionEngine &&other) noexcept;
  DefaultExecutionEngine &operator=(DefaultExecutionEngine &&other) noexcept;

public:
  // IExecutionEngine interface
  bool initialize(Graph *graph, uint8_t num_workers = 4) override;

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

  // Public for potential external use
  void propagateOutputAndScheduleDownstream(
      const std::shared_ptr<ILogicNode> &source_node,
      const PortDataMap &outputs);

  void checkCompletionAndNotify();

private:
  bool distributeInitialInputs(const PortDataMap &initial_inputs);
  void tryScheduleNode(const std::shared_ptr<ILogicNode> &node);
  void executeNodeTask(std::shared_ptr<ILogicNode> node,
                       std::shared_ptr<PipelineContext> context);

  void identifySinkNodes();
  void resetInternalState();
  void collectFinalResults(const std::shared_ptr<ILogicNode> &node,
                           const PortDataMap &outputs);

  [[nodiscard]] bool isNodeReady(const std::shared_ptr<ILogicNode> &node) const;
  [[nodiscard]] bool isSinkNode(const std::shared_ptr<ILogicNode> &node) const;

  bool waitForCompletion();
  void validateSchedulingResult(const PortDataMap &initial_inputs,
                                bool has_scheduled);
  bool gatherNodeInputs(const std::shared_ptr<ILogicNode> &node,
                        PortDataMap &inputs);
  bool processNode(const std::shared_ptr<ILogicNode> &node,
                   const PortDataMap &inputs, PortDataMap &outputs,
                   const std::shared_ptr<PipelineContext> &context);

  void handleNodeSuccess(const std::shared_ptr<ILogicNode> &node,
                         const PortDataMap &outputs);
  void handleNodeFailure(const std::shared_ptr<ILogicNode> &node,
                         const std::string &error_msg);

private:
  // Core components
  Graph *m_graph{nullptr};
  std::unique_ptr<ThreadPool> m_threadPool;
  std::shared_ptr<PipelineContext> m_currentContext;

  // State management (modular components)
  NodeStateManager m_nodeStateManager;
  InputQueueManager m_inputQueueManager;
  NodeMutexManager m_nodeMutexManager;

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

  // Result collection
  std::vector<std::shared_ptr<ILogicNode>> m_sinkNodes;
  PortDataMap m_accumulatedFinalResults;
  mutable std::mutex m_finalResultsMutex;
};

} // namespace ai_pipe

#endif // AI_PIPE_DEFAULT_EXECUTION_ENGINE_HPP