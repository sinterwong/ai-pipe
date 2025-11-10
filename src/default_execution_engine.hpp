/**
 * @file default_execution_engine.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_EXECUTION_ENGINE_HPP
#define AI_PIPE_EXECUTION_ENGINE_HPP

#include "execution_engine_base.hpp"
#include "thread_pool.hpp"
#include "thread_safe_queue.hpp"
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace ai_pipe {
class DefaultExecutionEngine : public IExecutionEngine {
public:
  DefaultExecutionEngine();

  ~DefaultExecutionEngine();

  DefaultExecutionEngine(const DefaultExecutionEngine &) = delete;
  DefaultExecutionEngine &operator=(const DefaultExecutionEngine &) = delete;

  DefaultExecutionEngine(DefaultExecutionEngine &&);
  DefaultExecutionEngine &operator=(DefaultExecutionEngine &&);

public:
  bool initialize(Graph *graph, uint8_t numWorkers = 4) override;

  bool execute(const PortDataMap &initialInputs, bool waitForCompletion = true,
               std::shared_ptr<PipelineContext> context = nullptr) override;

  void stopExecutionAsync() override;

  void stopExecutionSync() override;

  void reset() override;

  EngineState getState() const override;

  void setPipelineResultCallback(
      std::function<void(const PortDataMap &finalResults)> callback) override;

  void setPipelineErrorCallback(std::function<void(const std::string &errorMsg,
                                                   const std::string &nodeName)>
                                    callback) override;

  std::unordered_map<std::string, NodeExecutionState>
  getNodeStates() const override;

  void propagateOutputAndScheduleDownstream(
      const std::shared_ptr<ILogicNode> &sourceNode,
      const PortDataMap &outputs);

  void checkCompletionAndNotify();

private:
  /**
   * @brief Distributes initial inputs to the starting nodes.
   *
   * @param initialInputs The initial data to be fed into the pipeline.
   * @return true if the inputs were distributed successfully, false otherwise.
   */
  bool distributeInitialInputs(const PortDataMap &initialInputs);

  void tryScheduleNode(const std::shared_ptr<ILogicNode> &node);

  void executeNodeTask(std::shared_ptr<ILogicNode> node,
                       std::shared_ptr<PipelineContext> context);

private:
  // Per-node input queues: Node -> PortName -> Queue
  using PortInputQueues =
      std::unordered_map<std::string,
                         std::shared_ptr<ThreadSafeQueue<PortDataPtr>>>;

  Graph *m_graph;
  std::shared_ptr<PipelineContext> m_curContext;
  std::unique_ptr<ThreadPool> m_threadPool;
  std::atomic<EngineState> m_engineState;

  std::unordered_map<std::shared_ptr<ILogicNode>,
                     std::unique_ptr<std::atomic<NodeExecutionState>>>
      m_nodeStates;
  std::unordered_map<std::shared_ptr<ILogicNode>, PortInputQueues>
      m_nodeInputQueues;
  std::unordered_map<std::shared_ptr<ILogicNode>, std::unique_ptr<std::mutex>>
      m_nodeMutexes;

  // number of tasks either executing or ready to be scheduled
  std::atomic<int> m_activeTasks;

  // signal to stop all processing
  std::atomic<bool> m_stopFlag;

  // general mutex for engine state, initialization, and completion condition
  mutable std::mutex m_engineMutex;
  std::condition_variable m_completionCondition;

  std::function<void(const PortDataMap &finalResults)> m_onResultCallback;
  std::function<void(const std::string &errorMsg, const std::string &nodeName)>
      m_onErrorCallback;

  std::vector<std::shared_ptr<ILogicNode>> m_sinkNodes;
  PortDataMap m_accumulatedFinalResults;
  std::mutex m_finalResultsMutex;
};
} // namespace ai_pipe

#endif
