/**
 * @file execution_engine.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-04-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __PIPE_EXECUTION_ENGINE_HPP__
#define __PIPE_EXECUTION_ENGINE_HPP__
#include "ai_pipe/types.hpp"
#include "graph.hpp"
#include "thread_pool.hpp"
#include "thread_safe_queue.hpp"
#include <functional>
#include <memory>
#include <string>

namespace ai_pipe {
class ExecutionEngine {
public:
  ExecutionEngine();

  ~ExecutionEngine();

  ExecutionEngine(const ExecutionEngine &) = delete;
  ExecutionEngine &operator=(const ExecutionEngine &) = delete;

  ExecutionEngine(ExecutionEngine &&);
  ExecutionEngine &operator=(ExecutionEngine &&);

public:
  bool initialize(Graph *graph, uint8_t numWorkers = 4);

  bool execute(const PortDataMap &initialInputs, bool waitForCompletion = true,
               std::shared_ptr<PipelineContext> context = nullptr);

  void stopExecutionAsync();

  void stopExecutionSync();

  void reset();

  PipelineState getState() const;

  void setPipelineResultCallback(
      std::function<void(const PortDataMap &finalResults)> callback);

  void setPipelineErrorCallback(std::function<void(const std::string &errorMsg,
                                                   const std::string &nodeName)>
                                    callback);

  std::unordered_map<std::string, NodeExecutionState> getNodeStates() const;

  void propagateOutputAndScheduleDownstream(
      const std::shared_ptr<NodeBase> &sourceNode, const PortDataMap &outputs);

  void checkCompletionAndNotify();

private:
  // 分发输入数据到起始节点
  bool distributeInitialInputs(const PortDataMap &initialInputs);

  void tryScheduleNode(const std::shared_ptr<NodeBase> &node);

  void executeNodeTask(std::shared_ptr<NodeBase> node,
                       std::shared_ptr<PipelineContext> context);

private:
  // Per-node input queues: Node -> PortName -> Queue
  using PortInputQueues =
      std::unordered_map<std::string,
                         std::shared_ptr<ThreadSafeQueue<PortDataPtr>>>;

  Graph *graph_;
  std::shared_ptr<PipelineContext> curContext_;
  std::unique_ptr<ThreadPool> threadPool_;
  std::atomic<PipelineState> pipelineState_;

  std::unordered_map<std::shared_ptr<NodeBase>,
                     std::unique_ptr<std::atomic<NodeExecutionState>>>
      nodeStates_;
  std::unordered_map<std::shared_ptr<NodeBase>, PortInputQueues>
      nodeInputQueues_;
  std::unordered_map<std::shared_ptr<NodeBase>, std::unique_ptr<std::mutex>>
      nodeMutexes_;

  // number of tasks either executing or ready to be scheduled
  std::atomic<int> activeTasks_;

  // signal to stop all processing
  std::atomic<bool> stopFlag_;

  // general mutex for engine state, initialization, and completion condition
  mutable std::mutex engineMutex_;
  std::condition_variable completionCondition_;

  std::function<void(const PortDataMap &finalResults)> onResultCallback_;
  std::function<void(const std::string &errorMsg, const std::string &nodeName)>
      onErrorCallback_;

  std::vector<std::shared_ptr<NodeBase>> sinkNodes_;
  PortDataMap accumulatedFinalResults_;
  std::mutex finalResultsMutex_;
};
} // namespace ai_pipe

#endif
