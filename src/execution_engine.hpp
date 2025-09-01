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

  EngineState getState() const;

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

  Graph *mGraph;
  std::shared_ptr<PipelineContext> mCurContext;
  std::unique_ptr<ThreadPool> mThreadPool;
  std::atomic<EngineState> mEngineState;

  std::unordered_map<std::shared_ptr<NodeBase>,
                     std::unique_ptr<std::atomic<NodeExecutionState>>>
      mNodeStates;
  std::unordered_map<std::shared_ptr<NodeBase>, PortInputQueues>
      mNodeInputQueues;
  std::unordered_map<std::shared_ptr<NodeBase>, std::unique_ptr<std::mutex>>
      mNodeMutexes;

  // number of tasks either executing or ready to be scheduled
  std::atomic<int> mActiveTasks;

  // signal to stop all processing
  std::atomic<bool> mStopFlag;

  // general mutex for engine state, initialization, and completion condition
  mutable std::mutex mEngineMutex;
  std::condition_variable mCompletionCondition;

  std::function<void(const PortDataMap &finalResults)> mOnResultCallback;
  std::function<void(const std::string &errorMsg, const std::string &nodeName)>
      mOnErrorCallback;

  std::vector<std::shared_ptr<NodeBase>> mSinkNodes;
  PortDataMap mAccumulatedFinalResults;
  std::mutex mFinalResultsMutex;
};
} // namespace ai_pipe

#endif
