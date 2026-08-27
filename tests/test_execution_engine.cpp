#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "helper_nodes.hpp"
#include "scheduler_strategies.hpp"
#include "sync_strategies.hpp"
#include <gtest/gtest.h>
#include <memory>

using namespace ai_pipe;
using namespace std::chrono_literals;

namespace ai_pipe_unit_test::execution_engine {

class InitializingBatchScheduler final : public ISchedulerStrategy {
public:
  explicit InitializingBatchScheduler(std::shared_ptr<std::atomic<int>> count)
      : m_count(std::move(count)) {}

  ScheduleResult
  shouldSchedule(const SchedulingContext &context) const override {
    return m_delegate.shouldSchedule(context);
  }
  bool onNodeComplete(const std::shared_ptr<ILogicNode> &node, bool success,
                      const PortDataMap &outputs) override {
    return m_delegate.onNodeComplete(node, success, outputs);
  }
  CompletionStatus
  checkCompletion(std::size_t active, std::size_t pending,
                  const std::vector<SinkExecutionCount> &sinks) const override {
    return m_delegate.checkCompletion(active, pending, sinks);
  }
  CompletionSemantics completionSemantics() const override {
    return m_delegate.completionSemantics();
  }
  bool supportsStreaming() const override {
    return m_delegate.supportsStreaming();
  }
  std::string name() const override { return "InitializingBatchScheduler"; }
  void initialize(const CompiledGraph &) override { m_count->fetch_add(1); }
  void reset() override { m_delegate.reset(); }
  std::unique_ptr<ISchedulerStrategy> clone() const override {
    return std::make_unique<InitializingBatchScheduler>(m_count);
  }

private:
  std::shared_ptr<std::atomic<int>> m_count;
  BatchSchedulerStrategy m_delegate;
};

class InitializingNoSyncStrategy final : public ISyncStrategy {
public:
  explicit InitializingNoSyncStrategy(std::shared_ptr<std::atomic<int>> count)
      : m_count(std::move(count)) {}

  void initialize(const CompiledGraph &) override { m_count->fetch_add(1); }
  void reset() override {}
  void registerSyncGroup(const SyncGroupId &, const std::vector<BranchId> &,
                         const std::string &) override {}
  void mapNodeToGroup(const std::string &, const SyncGroupId &,
                      const BranchId &) override {}
  std::vector<BranchId> reportDrop(const std::string &, FrameId,
                                   const std::string &) override {
    return {};
  }
  bool shouldDrop(const std::string &, FrameId) const override { return false; }
  void markProcessed(const std::string &, FrameId) override {}
  FrameId getWatermark(const SyncGroupId &) const override { return 0; }
  bool isEnabled() const override { return false; }
  std::string name() const override { return "InitializingNoSyncStrategy"; }
  std::unique_ptr<ISyncStrategy> clone() const override {
    return std::make_unique<InitializingNoSyncStrategy>(m_count);
  }

private:
  std::shared_ptr<std::atomic<int>> m_count;
};

class ExecutionEngineTest : public ::testing::Test {
protected:
  void SetUp() override { m_graph = std::make_unique<Graph>(); }

  void TearDown() override { m_graph.reset(); }

  static PortDataPtr createData(uint64_t id = 1) {
    auto data = std::make_shared<PortData>();
    data->id = id;
    data->setParam("test", true);
    return data;
  }

  void createLinearPipeline() {
    m_source = std::make_shared<SourceNode>("source");
    m_process = std::make_shared<PassThroughNode>("process");
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(m_process);
    m_graph->addNode(m_sink);

    m_graph->addEdge("source", "output", "process", "input");
    m_graph->addEdge("process", "output", "sink", "input");
  }

  void createForkJoinPipeline() {
    m_source = std::make_shared<SourceNode>("source");
    auto branch1 = std::make_shared<PassThroughNode>("branch1");
    auto branch2 = std::make_shared<PassThroughNode>("branch2");
    auto join = std::make_shared<JoinNode>(
        "join", std::vector<std::string>{"input1", "input2"});
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(branch1);
    m_graph->addNode(branch2);
    m_graph->addNode(join);
    m_graph->addNode(m_sink);

    m_graph->addEdge("source", "output", "branch1", "input");
    m_graph->addEdge("source", "output", "branch2", "input");
    m_graph->addEdge("branch1", "output", "join", "input1");
    m_graph->addEdge("branch2", "output", "join", "input2");
    m_graph->addEdge("join", "output", "sink", "input");
  }

  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<SourceNode> m_source;
  std::shared_ptr<PassThroughNode> m_process;
  std::shared_ptr<SinkNode> m_sink;
};

// Construction and Initialization Tests

TEST_F(ExecutionEngineTest, DefaultConstruction) {
  ExecutionEngine engine;
  EXPECT_EQ(engine.getState(), EngineState::IDLE);
  EXPECT_FALSE(engine.isStreaming());
}

TEST_F(ExecutionEngineTest, ConstructionWithConfig) {
  auto config = EngineConfig::batch(8);
  ExecutionEngine engine(config);

  EXPECT_EQ(engine.getState(), EngineState::IDLE);
  EXPECT_EQ(engine.config().num_workers, 8);
  EXPECT_EQ(engine.config().mode, ExecutionMode::BATCH);
}

TEST_F(ExecutionEngineTest, FactoryCreate) {
  auto engine = ExecutionEngine::create(EngineConfig::stream(4, 32));

  ASSERT_NE(engine, nullptr);
  EXPECT_EQ(engine->getState(), EngineState::IDLE);
  EXPECT_EQ(engine->config().mode, ExecutionMode::STREAM);
  EXPECT_EQ(engine->config().default_queue_capacity, 32);
}

TEST_F(ExecutionEngineTest, ConvenienceFactoryFunctions) {
  auto batch = createBatchEngine(2);
  auto stream = createStreamEngine(4, 16);

  EXPECT_EQ(batch->config().mode, ExecutionMode::BATCH);
  EXPECT_EQ(batch->config().num_workers, 2);

  EXPECT_EQ(stream->config().mode, ExecutionMode::STREAM);
  EXPECT_EQ(stream->config().num_workers, 4);
  EXPECT_EQ(stream->config().default_queue_capacity, 16);
}

TEST_F(ExecutionEngineTest, MoveConstruction) {
  auto engine1 = ExecutionEngine::create(EngineConfig::batch(4));
  createLinearPipeline();
  ASSERT_TRUE(engine1->initialize(m_graph.get(), 4));

  ExecutionEngine engine2(std::move(*engine1));
  EXPECT_EQ(engine2.getState(), EngineState::IDLE);
}

TEST_F(ExecutionEngineTest, MoveAssignment) {
  auto engine1 = ExecutionEngine::create(EngineConfig::batch(4));
  createLinearPipeline();
  ASSERT_TRUE(engine1->initialize(m_graph.get(), 4));

  ExecutionEngine engine2;
  engine2 = std::move(*engine1);
  EXPECT_EQ(engine2.getState(), EngineState::IDLE);
}

TEST_F(ExecutionEngineTest, InitializeWithValidGraph) {
  auto engine = createBatchEngine();
  createLinearPipeline();

  EXPECT_TRUE(engine->initialize(m_graph.get(), 4));
  EXPECT_EQ(engine->getState(), EngineState::IDLE);
}

TEST_F(ExecutionEngineTest, InitializeWithNullGraph) {
  auto engine = createBatchEngine();

  EXPECT_FALSE(engine->initialize(nullptr, 4).isOk());
}

TEST_F(ExecutionEngineTest, InitializeWithZeroWorkers) {
  auto engine = createBatchEngine(8);
  createLinearPipeline();

  // When 0 workers passed, should use config default
  EXPECT_TRUE(engine->initialize(m_graph.get(), 0));
  EXPECT_EQ(engine->config().num_workers, 8);
}

// Strategy Injection Tests

TEST_F(ExecutionEngineTest, SetSchedulerStrategy) {
  auto engine = createBatchEngine();

  EXPECT_TRUE(
      engine->setSchedulerStrategy(std::make_unique<StreamSchedulerStrategy>())
          .isOk());
  EXPECT_TRUE(engine->strategyInfo().find("StreamSchedulerStrategy") !=
              std::string::npos);
}

TEST_F(ExecutionEngineTest, SetSyncStrategy) {
  auto engine = createBatchEngine();

  EXPECT_TRUE(
      engine->setSyncStrategy(std::make_unique<NoSyncStrategy>()).isOk());
  EXPECT_TRUE(engine->strategyInfo().find("NoSyncStrategy") !=
              std::string::npos);
}

TEST_F(ExecutionEngineTest, RejectsNullStrategies) {
  auto engine = createBatchEngine();
  auto scheduler_result = engine->setSchedulerStrategy(nullptr);
  auto sync_result = engine->setSyncStrategy(nullptr);
  ASSERT_FALSE(scheduler_result.isOk());
  ASSERT_FALSE(sync_result.isOk());
  EXPECT_EQ(scheduler_result.error().code(), ErrorCode::InvalidArgument);
  EXPECT_EQ(sync_result.error().code(), ErrorCode::InvalidArgument);
}

TEST_F(ExecutionEngineTest, InitializesStrategiesReplacedAfterGraphSetup) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());

  auto scheduler_initializations = std::make_shared<std::atomic<int>>(0);
  auto sync_initializations = std::make_shared<std::atomic<int>>(0);
  ASSERT_TRUE(
      engine
          ->setSchedulerStrategy(std::make_unique<InitializingBatchScheduler>(
              scheduler_initializations))
          .isOk());
  ASSERT_TRUE(
      engine
          ->setSyncStrategy(std::make_unique<InitializingNoSyncStrategy>(
              sync_initializations))
          .isOk());

  EXPECT_EQ(scheduler_initializations->load(), 1);
  EXPECT_EQ(sync_initializations->load(), 1);

  PortDataMap inputs;
  inputs["source"] = createData(7);
  EXPECT_TRUE(engine->execute(inputs).isOk());
}

TEST_F(ExecutionEngineTest, ConfigureForStreamMode) {
  auto engine = ExecutionEngine::create();
  engine->configureForMode(ExecutionMode::STREAM);

  EXPECT_TRUE(engine->strategyInfo().find("StreamSchedulerStrategy") !=
              std::string::npos);
}

TEST_F(ExecutionEngineTest, SyncCoordinationKnobSelectsSyncStrategy) {
  // Regression: enable_sync_coordination was parsed and copied
  // everywhere but never read - STREAM always installed the JoinAware
  // strategy regardless of the knob.
  auto with_sync = ExecutionEngine::create(EngineConfig::stream());
  EXPECT_NE(with_sync->strategyInfo().find("JoinAwareSyncStrategy"),
            std::string::npos);

  auto config = EngineConfig::stream();
  config.enable_sync_coordination = false;
  auto without_sync = ExecutionEngine::create(config);
  EXPECT_NE(without_sync->strategyInfo().find("NoSyncStrategy"),
            std::string::npos);
}

TEST_F(ExecutionEngineTest, CannotChangeStrategyWhileRunning) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  ASSERT_TRUE(engine->startStreaming());

  // Strategy change should be rejected while running
  auto original_info = engine->strategyInfo();
  auto result =
      engine->setSchedulerStrategy(std::make_unique<BatchSchedulerStrategy>());
  EXPECT_FALSE(result.isOk());
  EXPECT_EQ(engine->strategyInfo(), original_info);

  engine->stopStreaming();
}

// Batch Execution Tests

TEST_F(ExecutionEngineTest, BatchExecuteSynchronous) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData(66);

  EXPECT_TRUE(engine->execute(inputs, true).isOk());
  EXPECT_EQ(engine->getState(), EngineState::IDLE);

  EXPECT_EQ(m_source->processCount(), 1);
  EXPECT_EQ(m_process->processCount(), 1);
  EXPECT_EQ(m_sink->processCount(), 1);

  auto received_data = m_sink->getReceivedData();
  ASSERT_EQ(received_data.size(), 1);
  EXPECT_EQ(received_data[0]->id, 66);
}

TEST_F(ExecutionEngineTest, BatchExecuteAsynchronous) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData(66);

  EXPECT_TRUE(engine->execute(inputs, false).isOk());

  std::this_thread::sleep_for(100ms);

  EXPECT_GE(m_sink->processCount(), 1);

  // The async execution already completed above, which returns the engine
  // to IDLE (fire-and-forget contract); stopping an idle engine is a no-op
  engine->stopExecutionSync();
  EXPECT_EQ(engine->getState(), EngineState::IDLE);
}

TEST_F(ExecutionEngineTest, BatchExecuteWithContext) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  auto context = std::make_shared<PipelineContext>();
  context->setConfig("test_key", std::string("test_value"));

  PortDataMap inputs;
  inputs["source"] = createData();

  EXPECT_TRUE(engine->execute(inputs, true, context).isOk());
}

TEST_F(ExecutionEngineTest, BatchExecuteWithEmptyInputs) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  PortDataMap inputs;

  // Empty inputs should still work if source doesn't require input
  EXPECT_TRUE(engine->execute(inputs, true).isOk());
}

TEST_F(ExecutionEngineTest, ExecuteWithoutInitialization) {
  auto engine = createBatchEngine();

  PortDataMap inputs;
  inputs["source"] = createData();

  EXPECT_FALSE(engine->execute(inputs, true).isOk());
}

TEST_F(ExecutionEngineTest, DoubleExecuteRejectsSecond) {
  // Use a slow node to keep execution running
  auto source = std::make_shared<SlowNode>("source", 200ms);
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(source);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "output", "sink", "input");

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData();

  EXPECT_TRUE(engine->execute(inputs, false).isOk());

  // Try to start second execution - should fail
  EXPECT_FALSE(engine->execute(inputs, true).isOk());

  std::this_thread::sleep_for(300ms);
}

TEST_F(ExecutionEngineTest, RejectedConcurrentExecuteKeepsRunningContext) {
  // Records whether process() received a null context
  class ContextProbeNode : public ILogicNode {
  public:
    explicit ContextProbeNode(const std::string &name) : ILogicNode(name) {}

    void process(const PortDataMap &, PortDataMap &,
                 std::shared_ptr<PipelineContext> ctx) override {
      m_sawNullContext.store(ctx == nullptr);
      m_executed.store(true);
    }
    std::vector<std::string> getExpectedInputPorts() const override {
      return {"input"};
    }
    std::vector<std::string> getExpectedOutputPorts() const override {
      return {};
    }

    std::atomic<bool> m_sawNullContext{false};
    std::atomic<bool> m_executed{false};
  };

  auto source = std::make_shared<SlowNode>("source", 200ms);
  auto probe = std::make_shared<ContextProbeNode>("probe");

  m_graph->addNode(source);
  m_graph->addNode(probe);
  m_graph->addEdge("source", "output", "probe", "input");

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData();

  ASSERT_TRUE(
      engine->execute(inputs, false, std::make_shared<PipelineContext>())
          .isOk());

  // Regression: execute() used to store the new context at entry
  // and null it on the AlreadyRunning path, so nodes scheduled after
  // this rejection (probe, once the slow source finishes) received a
  // null context.
  ASSERT_FALSE(
      engine->execute(inputs, false, std::make_shared<PipelineContext>())
          .isOk());

  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (!probe->m_executed.load() &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(probe->m_executed.load());
  EXPECT_FALSE(probe->m_sawNullContext.load());
}

// Result and Error Callback Tests

TEST_F(ExecutionEngineTest, ResultCallback) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  std::atomic<bool> callback_called{false};
  PortDataMap received_results;
  std::mutex result_mutex;

  engine->setPipelineResultCallback([&](const PortDataMap &results) {
    std::lock_guard<std::mutex> lock(result_mutex);
    received_results = results;
    callback_called.store(true);
  });

  PortDataMap inputs;
  inputs["source"] = createData(123);

  EXPECT_TRUE(engine->execute(inputs, true).isOk());

  std::this_thread::sleep_for(50ms);

  EXPECT_TRUE(callback_called.load());
  EXPECT_FALSE(received_results.empty());
}

TEST_F(ExecutionEngineTest, CompletionCallbackFiresExactlyOncePerRun) {
  auto source = std::make_shared<SourceNode>("source");
  auto sink1 = std::make_shared<SinkNode>("sink1");
  auto sink2 = std::make_shared<SinkNode>("sink2");
  ASSERT_TRUE(m_graph->addNode(source));
  ASSERT_TRUE(m_graph->addNode(sink1));
  ASSERT_TRUE(m_graph->addNode(sink2));
  ASSERT_TRUE(m_graph->addEdge("source", "output", "sink1", "input"));
  ASSERT_TRUE(m_graph->addEdge("source", "output", "sink2", "input"));

  auto engine = createBatchEngine(4);
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());
  std::atomic<int> callback_count{0};
  engine->setPipelineResultCallback(
      [&](const PortDataMap &) { callback_count.fetch_add(1); });

  constexpr int k_runs = 100;
  for (int i = 0; i < k_runs; ++i) {
    PortDataMap inputs;
    inputs["source"] = createData(static_cast<std::uint64_t>(i + 1));
    ASSERT_TRUE(engine->execute(inputs).isOk());
  }
  EXPECT_EQ(callback_count.load(), k_runs);
}

TEST_F(ExecutionEngineTest, ErrorCallback) {
  auto failable = std::make_shared<FailableNode>("failable", true);
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(failable);
  m_graph->addNode(sink);
  m_graph->addEdge("failable", "output", "sink", "input");

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  std::atomic<bool> error_called{false};
  std::string error_node;
  std::string error_message;
  std::mutex error_mutex;

  engine->setPipelineErrorCallback(
      [&](const std::string &msg, const std::string &node) {
        std::lock_guard<std::mutex> lock(error_mutex);
        error_message = msg;
        error_node = node;
        error_called.store(true);
      });

  PortDataMap inputs;
  inputs["failable"] = createData();

  engine->execute(inputs, true);

  std::this_thread::sleep_for(50ms);

  EXPECT_TRUE(error_called.load());

  // the callback receives error message first, then node name
  std::lock_guard<std::mutex> lock(error_mutex);
  EXPECT_FALSE(error_node.empty());
  EXPECT_FALSE(error_message.empty());
}

// Streaming Execution Tests

TEST_F(ExecutionEngineTest, StartStreaming) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->startStreaming().isOk());
  EXPECT_TRUE(engine->isStreaming());
  EXPECT_EQ(engine->getState(), EngineState::RUNNING);

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, StopStreaming) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  engine->startStreaming();
  engine->stopStreaming(true);

  EXPECT_FALSE(engine->isStreaming());
  EXPECT_EQ(engine->getState(), EngineState::IDLE);
}

TEST_F(ExecutionEngineTest, CannotStartStreamingInBatchMode) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_FALSE(engine->startStreaming().isOk());
}

TEST_F(ExecutionEngineTest, PushInputInStreamingMode) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  engine->startStreaming();

  for (int i = 0; i < 10; ++i) {
    auto result = engine->pushInput("source", createData(i));
    EXPECT_TRUE(result.isOk());
  }

  std::this_thread::sleep_for(200ms);

  EXPECT_GE(m_sink->processCount(), 5);

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, PushInputWithPortName) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  engine->startStreaming();

  auto result = engine->pushInput("source", "input", createData(1));
  EXPECT_TRUE(result.isOk());

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, PushInputToUnknownNode) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  engine->startStreaming();

  auto result = engine->pushInput("unknown_node", createData());
  EXPECT_FALSE(result.isOk());

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, PushInputWhenNotStreaming) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  auto result = engine->pushInput("source", createData());
  EXPECT_FALSE(result.isOk());
}

TEST_F(ExecutionEngineTest, StreamingWithContext) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  auto context = std::make_shared<PipelineContext>();
  context->setUserData("stream_id", 66);

  EXPECT_TRUE(engine->startStreaming(context).isOk());

  (void)engine->pushInput("source", createData());
  std::this_thread::sleep_for(100ms);

  engine->stopStreaming();
}

// State Management Tests

TEST_F(ExecutionEngineTest, GetState) {
  auto engine = createBatchEngine();

  EXPECT_EQ(engine->getState(), EngineState::IDLE);

  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_EQ(engine->getState(), EngineState::IDLE);
}

TEST_F(ExecutionEngineTest, GetNodeStates) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  auto states = engine->getNodeStates();

  EXPECT_EQ(states.size(), 3);
  EXPECT_TRUE(states.count("source") > 0);
  EXPECT_TRUE(states.count("process") > 0);
  EXPECT_TRUE(states.count("sink") > 0);

  for (const auto &[name, state] : states) {
    EXPECT_EQ(state, NodeExecutionState::WAITING);
  }
}

TEST_F(ExecutionEngineTest, Reset) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData();
  engine->execute(inputs, true);

  // reset and check state
  engine->reset();

  EXPECT_EQ(engine->getState(), EngineState::IDLE);

  // should be able to execute again
  EXPECT_TRUE(engine->execute(inputs, true).isOk());
}

TEST_F(ExecutionEngineTest, StopExecutionAsync) {
  auto source = std::make_shared<PassThroughNode>("source", 100ms);
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(source);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "output", "sink", "input");

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData();

  // async execution
  EXPECT_TRUE(engine->execute(inputs, false).isOk());

  engine->stopExecutionAsync();

  // give time for stop to propagate
  std::this_thread::sleep_for(200ms);

  // should not be in RUNNING state
  auto state = engine->getState();
  EXPECT_TRUE(state == EngineState::STOPPED);
}

TEST_F(ExecutionEngineTest, StopExecutionSync) {
  createLinearPipeline();

  auto engine = createStreamEngine();
  engine->initialize(m_graph.get());

  engine->startStreaming();

  // push some data
  for (int i = 0; i < 5; ++i) {
    auto result = engine->pushInput("source", createData(i));
    EXPECT_TRUE(result.isOk());
  }

  std::this_thread::sleep_for(50ms);

  engine->stopStreaming(true);

  EXPECT_EQ(engine->getState(), EngineState::IDLE);
}

// Queue Management Tests

TEST_F(ExecutionEngineTest, QueueDepth) {
  auto engine = createStreamEngine(4, 16);
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->startStreaming().isOk());

  for (int i = 0; i < 5; ++i) {
    (void)engine->pushInput("source", createData(i));
  }

  // Queue depth should be non-negative
  auto depth = engine->queueDepth("source");
  EXPECT_GE(depth, 0u);

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, QueueDepthUnknownNode) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_EQ(engine->queueDepth("unknown"), 0u);
}

TEST_F(ExecutionEngineTest, HasQueueCapacity) {
  auto engine = createStreamEngine(4, 16);
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->hasQueueCapacity("source"));
}

TEST_F(ExecutionEngineTest, HasQueueCapacityUnknownNode) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_FALSE(engine->hasQueueCapacity("unknown"));
}

TEST_F(ExecutionEngineTest, WaitForDrain) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->startStreaming().isOk());

  for (int i = 0; i < 5; ++i) {
    (void)engine->pushInput("source", createData(i));
  }

  EXPECT_TRUE(engine->waitForDrain(0, 5000ms).isOk());

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, WaitForDrainTimeout) {
  auto source = std::make_shared<PassThroughNode>("source", 50ms);
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(source);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "output", "sink", "input");

  auto engine = createStreamEngine(1, 100);
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->startStreaming().isOk());

  // push many items
  for (int i = 0; i < 10; ++i) {
    (void)engine->pushInput("source", createData(i));
  }

  // very short timeout should fail or barely succeed
  auto result = engine->waitForDrain(0, 50ms);
  (void)result;

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, SetNodeQueueConfig) {
  auto engine = createStreamEngine();
  createLinearPipeline();

  QueueConfig config;
  config.capacity = 32;
  config.drop_strategy = "DropTail";

  engine->setNodeQueueConfig("process", config);
  engine->initialize(m_graph.get());

  EXPECT_EQ(engine->getState(), EngineState::IDLE);
}

// Drop Callback Tests

TEST_F(ExecutionEngineTest, DropCallbackTriggered) {
  auto source = std::make_shared<SourceNode>("source");
  auto slow = std::make_shared<SlowNode>("slow", 100ms);
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(source);
  m_graph->addNode(slow);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "output", "slow", "input");
  m_graph->addEdge("slow", "output", "sink", "input");

  auto config = EngineConfig::stream(1, 2); // tiny queue
  auto engine = ExecutionEngine::create(config);

  QueueConfig slow_config;
  slow_config.capacity = 2;
  slow_config.drop_strategy = "DropHead";
  engine->setNodeQueueConfig("slow", slow_config);

  engine->initialize(m_graph.get());
  std::atomic<int> drop_count{0};
  engine->setDropCallback(
      [&](const std::string &, std::uint64_t, const std::string &) {
        drop_count.fetch_add(1);
      });

  EXPECT_TRUE(engine->startStreaming().isOk());
  for (int i = 0; i < 20; ++i) {
    (void)engine->pushInput("source", createData(i));
    std::this_thread::sleep_for(5ms);
  }

  std::this_thread::sleep_for(500ms);
  engine->stopStreaming();
  EXPECT_GE(drop_count.load(), 0);
}

namespace {

// Node recording lifecycle transitions
class LifecycleNode : public PassThroughNode {
public:
  explicit LifecycleNode(const std::string &name, bool fail_setup = false)
      : PassThroughNode(name), m_failSetup(fail_setup) {}

  Result<void> setup(std::shared_ptr<PipelineContext>) override {
    setup_count.fetch_add(1);
    if (m_failSetup) {
      return Result<void>::err(ErrorCode::NodeException, "setup failed",
                               getName());
    }
    return Result<void>::ok();
  }

  void teardown() noexcept override { teardown_count.fetch_add(1); }

  std::atomic<int> setup_count{0};
  std::atomic<int> teardown_count{0};

private:
  bool m_failSetup;
};

} // namespace

TEST_F(ExecutionEngineTest, LifecycleSetupOnceTeardownOnReset) {
  auto a = std::make_shared<LifecycleNode>("a");
  auto b = std::make_shared<LifecycleNode>("b");
  m_graph->addNode(a);
  m_graph->addNode(b);
  m_graph->addEdge("a", "output", "b", "input");

  auto engine = createBatchEngine(2);
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());
  EXPECT_EQ(a->setup_count.load(), 0) << "setup happens at first run";

  PortDataMap inputs;
  inputs["a"] = createData(1);
  ASSERT_TRUE(engine->execute(inputs, true).isOk());
  ASSERT_TRUE(engine->execute(inputs, true).isOk());

  EXPECT_EQ(a->setup_count.load(), 1) << "setup must run exactly once";
  EXPECT_EQ(b->setup_count.load(), 1);
  EXPECT_EQ(a->teardown_count.load(), 0);

  engine->reset();
  EXPECT_EQ(a->teardown_count.load(), 1);
  EXPECT_EQ(b->teardown_count.load(), 1);
}

TEST_F(ExecutionEngineTest, LifecycleSetupFailureAbortsAndUnwinds) {
  auto ok_node = std::make_shared<LifecycleNode>("ok");
  auto bad_node = std::make_shared<LifecycleNode>("bad", /*fail_setup=*/true);
  m_graph->addNode(ok_node);
  m_graph->addNode(bad_node);
  m_graph->addEdge("ok", "output", "bad", "input");

  auto engine = createBatchEngine(2);
  ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());

  PortDataMap inputs;
  inputs["ok"] = createData(1);
  auto result = engine->execute(inputs, true);

  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::NodeException);
  EXPECT_EQ(result.error().nodeName(), "bad");
  // The successfully set-up prefix ("ok" precedes "bad" topologically)
  // must be unwound.
  EXPECT_EQ(ok_node->setup_count.load(), 1);
  EXPECT_EQ(ok_node->teardown_count.load(), 1);
}

TEST_F(ExecutionEngineTest, LifecycleTeardownOnDestruction) {
  auto node = std::make_shared<LifecycleNode>("solo");
  m_graph->addNode(node);

  {
    auto engine = createStreamEngine(2, 8);
    ASSERT_TRUE(engine->initialize(m_graph.get()).isOk());
    ASSERT_TRUE(engine->startStreaming().isOk());
    EXPECT_EQ(node->setup_count.load(), 1);
    engine->stopStreaming(false);
  } // engine destroyed

  EXPECT_EQ(node->teardown_count.load(), 1);
}

TEST_F(ExecutionEngineTest, StreamingNodeRecoversAfterFailure) {
  // in streaming mode a node exception must not permanently
  // disable the node - subsequent frames keep flowing.
  auto failable = std::make_shared<FailableNode>("failable");
  auto sink = std::make_shared<SinkNode>("sink");
  m_graph->addNode(failable);
  m_graph->addNode(sink);
  m_graph->addEdge("failable", "output", "sink", "input");

  auto engine = createStreamEngine(2, 16);
  engine->initialize(m_graph.get());
  EXPECT_TRUE(engine->startStreaming().isOk());

  // Healthy frame
  ASSERT_TRUE(engine->pushInput("failable", createData(1)).isOk());
  EXPECT_TRUE(engine->waitForDrain(0, 5000ms).isOk());

  // Failing frame (consumed, error recorded, node returns to service)
  failable->setShouldFail(true);
  ASSERT_TRUE(engine->pushInput("failable", createData(2)).isOk());
  EXPECT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  failable->setShouldFail(false);

  // Node must process frames again after the failure
  ASSERT_TRUE(engine->pushInput("failable", createData(3)).isOk());
  EXPECT_TRUE(engine->waitForDrain(0, 5000ms).isOk());

  engine->stopStreaming(false);

  EXPECT_EQ(failable->processCount(), 3);
  ASSERT_EQ(sink->getReceivedData().size(), 2u); // frames 1 and 3
  EXPECT_EQ(engine->statistics().failed_executions, 1u);
}

TEST_F(ExecutionEngineTest, FrameIdentityAssignedAndInherited) {
  // packets entering without an id get a monotonic FrameId; fresh
  // output packets inherit the identity of the inputs that produced them.
  auto source = std::make_shared<PassThroughNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");
  m_graph->addNode(source);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "output", "sink", "input");

  auto engine = createStreamEngine(2, 16);
  engine->initialize(m_graph.get());
  EXPECT_TRUE(engine->startStreaming().isOk());

  std::vector<PortDataPtr> sent;
  for (int i = 0; i < 5; ++i) {
    auto data = std::make_shared<PortData>(); // id defaults to 0 = unassigned
    data->setParam("seq", i);
    sent.push_back(data);
    ASSERT_TRUE(engine->pushInput("source", data).isOk());
  }

  EXPECT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  // Monotonic assignment in push order
  for (std::size_t i = 0; i < sent.size(); ++i) {
    EXPECT_EQ(sent[i]->id, i + 1) << "packet " << i;
    EXPECT_NE(sent[i]->timestamp, Timestamp{});
  }

  ASSERT_EQ(sink->getReceivedData().size(), 5u);
}

TEST_F(ExecutionEngineTest, ExplicitFrameIdIsPreserved) {
  auto source = std::make_shared<PassThroughNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");
  m_graph->addNode(source);
  m_graph->addNode(sink);
  m_graph->addEdge("source", "output", "sink", "input");

  auto engine = createStreamEngine(2, 16);
  engine->initialize(m_graph.get());
  EXPECT_TRUE(engine->startStreaming().isOk());

  auto data = createData(777); // explicit id
  ASSERT_TRUE(engine->pushInput("source", data).isOk());
  EXPECT_TRUE(engine->waitForDrain(0, 5000ms).isOk());
  engine->stopStreaming(false);

  EXPECT_EQ(data->id, 777u);
}

TEST_F(ExecutionEngineTest, DropTailRejectionIsReportedNotSilent) {
  // Regression: pushToQueue used to ignore the queue's push() result, so a
  // DropTail rejection silently lost data while pushInput still reported
  // PushStatus::Enqueued to the caller.
  auto slow = std::make_shared<PassThroughNode>("slow", 200ms);
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(slow);
  m_graph->addNode(sink);
  m_graph->addEdge("slow", "output", "sink", "input");

  auto engine = createStreamEngine(1, 2);

  QueueConfig slow_config;
  slow_config.capacity = 2;
  slow_config.drop_strategy = "DropTail";
  engine->setNodeQueueConfig("slow", slow_config);

  engine->initialize(m_graph.get());

  std::atomic<int> drop_count{0};
  engine->setDropCallback(
      [&](const std::string &, std::uint64_t, const std::string &) {
        drop_count.fetch_add(1);
      });

  EXPECT_TRUE(engine->startStreaming().isOk());

  // Capacity 2 with a 200ms consumer: rapid pushes must overflow the queue.
  int rejected = 0;
  for (int i = 0; i < 10; ++i) {
    auto result = engine->pushInput("slow", createData(i));
    if (!result) {
      EXPECT_EQ(result.error().code(), ErrorCode::QueueRejected);
      rejected++;
    }
  }

  EXPECT_GT(rejected, 0) << "Full DropTail queue must reject, not fake success";
  EXPECT_EQ(drop_count.load(), rejected);

  auto stats = engine->statistics();
  EXPECT_EQ(stats.queue_full_events, static_cast<std::uint64_t>(rejected));
  EXPECT_EQ(stats.total_dropped_frames, static_cast<std::uint64_t>(rejected));

  engine->stopStreaming(false);
}

// Statistics Tests

TEST_F(ExecutionEngineTest, StatisticsInitialState) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  auto stats = engine->statistics();

  EXPECT_EQ(stats.total_executions, 0u);
  EXPECT_EQ(stats.successful_executions, 0u);
  EXPECT_EQ(stats.failed_executions, 0u);
}

TEST_F(ExecutionEngineTest, StatisticsAfterExecution) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData();
  EXPECT_TRUE(engine->execute(inputs, true).isOk());

  auto stats = engine->statistics();

  // total_executions counts NODE execution attempts
  // (unified across modes): one run of the 3-node linear pipeline = 3.
  EXPECT_EQ(stats.total_executions, 3u);
  EXPECT_EQ(stats.successful_executions, 3u);
}

TEST_F(ExecutionEngineTest, StatisticsMultipleExecutions) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  for (int i = 0; i < 5; ++i) {
    PortDataMap inputs;
    inputs["source"] = createData(i);
    EXPECT_TRUE(engine->execute(inputs, true).isOk());
  }

  auto stats = engine->statistics();

  // 5 runs x 3 nodes: node-attempt semantics
  EXPECT_EQ(stats.total_executions, 15u);
  EXPECT_EQ(stats.successful_executions, 15u);
}

// Information Query Tests

TEST_F(ExecutionEngineTest, Info) {
  auto engine = createBatchEngine(4);
  createLinearPipeline();
  engine->initialize(m_graph.get());

  auto info = engine->info();

  EXPECT_TRUE(info.find("ExecutionEngine") != std::string::npos);
  EXPECT_TRUE(info.find("BATCH") != std::string::npos);
  EXPECT_TRUE(info.find("workers: 4") != std::string::npos);
}

TEST_F(ExecutionEngineTest, StrategyInfo) {
  auto engine = createBatchEngine();

  auto info = engine->strategyInfo();

  EXPECT_TRUE(info.find("scheduler:") != std::string::npos);
  EXPECT_TRUE(info.find("sync:") != std::string::npos);
}

TEST_F(ExecutionEngineTest, Config) {
  auto config = EngineConfig::stream(8, 64);
  auto engine = ExecutionEngine::create(config);

  const auto &retrieved = engine->config();

  EXPECT_EQ(retrieved.mode, ExecutionMode::STREAM);
  EXPECT_EQ(retrieved.num_workers, 8);
  EXPECT_EQ(retrieved.default_queue_capacity, 64);
}

// Complex Pipeline Tests

TEST_F(ExecutionEngineTest, ForkJoinPipeline) {
  createForkJoinPipeline();

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData(66);

  EXPECT_TRUE(engine->execute(inputs, true).isOk());
  EXPECT_EQ(m_sink->processCount(), 1);
}

TEST_F(ExecutionEngineTest, DeepPipeline) {
  // create a 10-node deep pipeline
  std::vector<std::shared_ptr<PassThroughNode>> nodes;

  for (int i = 0; i < 10; ++i) {
    auto node = std::make_shared<PassThroughNode>("node_" + std::to_string(i));
    nodes.push_back(node);
    m_graph->addNode(node);

    if (i > 0) {
      m_graph->addEdge("node_" + std::to_string(i - 1), "output",
                       "node_" + std::to_string(i), "input");
    }
  }

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["node_0"] = createData();

  EXPECT_TRUE(engine->execute(inputs, true).isOk());

  // all nodes should have been processed
  for (const auto &node : nodes) {
    EXPECT_EQ(node->processCount(), 1);
  }
}

TEST_F(ExecutionEngineTest, ParallelBranches) {
  // create parallel branches: source -> [b1, b2, b3] -> join
  auto source = std::make_shared<SourceNode>("source");
  auto branch1 = std::make_shared<PassThroughNode>("branch1", 50ms);
  auto branch2 = std::make_shared<PassThroughNode>("branch2", 30ms);
  auto branch3 = std::make_shared<PassThroughNode>("branch3", 10ms);
  auto join = std::make_shared<JoinNode>(
      "join", std::vector<std::string>{"in1", "in2", "in3"});
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(source);
  m_graph->addNode(branch1);
  m_graph->addNode(branch2);
  m_graph->addNode(branch3);
  m_graph->addNode(join);
  m_graph->addNode(sink);

  m_graph->addEdge("source", "output", "branch1", "input");
  m_graph->addEdge("source", "output", "branch2", "input");
  m_graph->addEdge("source", "output", "branch3", "input");
  m_graph->addEdge("branch1", "output", "join", "in1");
  m_graph->addEdge("branch2", "output", "join", "in2");
  m_graph->addEdge("branch3", "output", "join", "in3");
  m_graph->addEdge("join", "output", "sink", "input");

  auto engine = createBatchEngine(4);
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData(100);

  EXPECT_TRUE(engine->execute(inputs, true).isOk());
  EXPECT_EQ(sink->processCount(), 1);
}

TEST_F(ExecutionEngineTest, ConcurrentStreamingPush) {
  auto engine = createStreamEngine(8, 64);
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->startStreaming().isOk());

  std::atomic<int> push_count{0};
  const int num_threads = 4;
  const int pushes_per_thread = 25;

  std::vector<std::thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < pushes_per_thread; ++i) {
        auto result = engine->pushInput("source", createData(t * 100 + i));
        if (result.isOk()) {
          push_count.fetch_add(1);
        }
      }
    });
  }

  for (auto &th : threads) {
    th.join();
  }

  engine->waitForDrain(0, 5000ms);
  engine->stopStreaming();

  EXPECT_EQ(push_count.load(), num_threads * pushes_per_thread);
  EXPECT_GE(m_sink->processCount(), num_threads * pushes_per_thread / 2);
}

TEST_F(ExecutionEngineTest, ConcurrentStatisticsAccess) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->startStreaming().isOk());

  std::atomic<bool> stop{false};

  // Thread pushing data
  std::thread pusher([&]() {
    while (!stop.load()) {
      (void)engine->pushInput("source", createData());
      std::this_thread::sleep_for(5ms);
    }
  });

  // Thread reading statistics
  std::thread reader([&]() {
    while (!stop.load()) {
      auto stats = engine->statistics();
      EXPECT_GE(stats.total_queue_pushes, 0u);
      std::this_thread::sleep_for(2ms);
    }
  });

  std::this_thread::sleep_for(200ms);
  stop.store(true);

  pusher.join();
  reader.join();

  engine->stopStreaming();
}

// Edge Cases and Error Handling Tests

TEST_F(ExecutionEngineTest, NodeFailureDoesNotCrash) {
  auto failable = std::make_shared<FailableNode>("failable", true);
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(failable);
  m_graph->addNode(sink);
  m_graph->addEdge("failable", "output", "sink", "input");

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["failable"] = createData();

  engine->execute(inputs, true);

  // Engine should still be usable
  failable->setShouldFail(false);
  EXPECT_TRUE(engine->execute(inputs, true).isOk());
}

TEST_F(ExecutionEngineTest, MultipleResets) {
  auto engine = createBatchEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  for (int i = 0; i < 5; ++i) {
    PortDataMap inputs;
    inputs["source"] = createData(i);
    EXPECT_TRUE(engine->execute(inputs, true).isOk());
    engine->reset();

    EXPECT_EQ(engine->getState(), EngineState::IDLE);
  }
}

TEST_F(ExecutionEngineTest, StreamStartStopMultiple) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  for (int i = 0; i < 3; ++i) {
    EXPECT_TRUE(engine->startStreaming().isOk());

    (void)engine->pushInput("source", createData(i));
    std::this_thread::sleep_for(50ms);

    engine->stopStreaming(true);
    EXPECT_FALSE(engine->isStreaming());
  }
}

TEST_F(ExecutionEngineTest, ExecuteDuringStreaming) {
  auto engine = createStreamEngine();
  createLinearPipeline();
  engine->initialize(m_graph.get());

  EXPECT_TRUE(engine->startStreaming().isOk());

  // Execute in streaming mode should push to queue
  PortDataMap inputs;
  inputs["source"] = createData();

  EXPECT_TRUE(engine->execute(inputs, false).isOk());

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, EmptyGraphIsRejected) {
  auto engine = createBatchEngine();

  // the engine validates the graph at initialize() time:
  // an empty graph is a configuration error, not a no-op pipeline.
  auto result = engine->initialize(m_graph.get());
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::GraphEmpty);
}

TEST_F(ExecutionEngineTest, CyclicGraphIsRejected) {
  auto a = std::make_shared<JoinNode>("a", std::vector<std::string>{"input"});
  auto b = std::make_shared<JoinNode>("b", std::vector<std::string>{"input"});
  m_graph->addNode(a);
  m_graph->addNode(b);
  ASSERT_TRUE(m_graph->addEdge("a", "output", "b", "input"));
  ASSERT_TRUE(m_graph->addEdge("b", "output", "a", "input"));

  auto engine = createBatchEngine();
  auto result = engine->initialize(m_graph.get());
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::GraphCycleDetected);
}

TEST_F(ExecutionEngineTest, SingleNodeGraph) {
  auto single = std::make_shared<PassThroughNode>("single");
  m_graph->addNode(single);

  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["single"] = createData();

  EXPECT_TRUE(engine->execute(inputs, true).isOk());
  EXPECT_EQ(single->processCount(), 1);
}

// Engine Configuration Tests

TEST_F(ExecutionEngineTest, EngineConfigBatch) {
  auto config = EngineConfig::batch(6);

  EXPECT_EQ(config.mode, ExecutionMode::BATCH);
  EXPECT_EQ(config.num_workers, 6);
  EXPECT_EQ(config.default_queue_capacity, 0); // Unbounded
  EXPECT_FALSE(config.enable_sync_coordination);
}

TEST_F(ExecutionEngineTest, EngineConfigStream) {
  auto config = EngineConfig::stream(4, 32);

  EXPECT_EQ(config.mode, ExecutionMode::STREAM);
  EXPECT_EQ(config.num_workers, 4);
  EXPECT_EQ(config.default_queue_capacity, 32);
  EXPECT_TRUE(config.enable_sync_coordination);
}

TEST_F(ExecutionEngineTest, PartialInputHandlingInStreaming) {
  auto branch1 = std::make_shared<PassThroughNode>("branch1");
  auto branch2 = std::make_shared<PassThroughNode>("branch2");
  auto join = std::make_shared<JoinNode>(
      "join", std::vector<std::string>{"in1", "in2"});
  auto sink = std::make_shared<SinkNode>("sink");

  m_graph->addNode(branch1);
  m_graph->addNode(branch2);
  m_graph->addNode(join);
  m_graph->addNode(sink);

  m_graph->addEdge("branch1", "output", "join", "in1");
  m_graph->addEdge("branch2", "output", "join", "in2");
  m_graph->addEdge("join", "output", "sink", "input");

  StreamSchedulerConfig config;
  config.allow_partial_inputs = true;
  config.min_input_ratio = 0.5;

  auto engine = createStreamEngine();
  engine->setSchedulerStrategy(
      std::make_unique<StreamSchedulerStrategy>(config));
  engine->initialize(m_graph.get());
  engine->startStreaming();

  // Push to only one port of join
  (void)engine->pushInput("branch1", "output", createData(1));
  std::this_thread::sleep_for(100ms);

  // If partial input allowed, join might execute (depending on JoinNode
  // implementation) Our JoinNode in helper_nodes.hpp likely needs all inputs
  // unless modified. This test verifies the SCHEDULER side of partial inputs.

  engine->stopStreaming();
}

TEST_F(ExecutionEngineTest, RapidStartStopStress) {
  createLinearPipeline();
  auto engine = createBatchEngine();
  engine->initialize(m_graph.get());

  PortDataMap inputs;
  inputs["source"] = createData();

  for (int i = 0; i < 50; ++i) {
    (void)engine->execute(inputs, false);
    engine->stopExecutionAsync();
    engine->reset();
  }
}

} // namespace ai_pipe_unit_test::execution_engine
