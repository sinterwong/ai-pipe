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
