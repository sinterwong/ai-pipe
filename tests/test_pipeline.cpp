#include "ai_pipe/data_packet.hpp"
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/error.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/pipeline.hpp"
#include "scheduler_strategies.hpp"
#include "sync_strategies.hpp"
#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace ai_pipe;
using namespace std::chrono_literals;

// =============================================================================
// Helper Functions
// =============================================================================

inline PortDataPtr makeDataPacket(uint64_t id = 0) {
  auto packet = std::make_shared<common_utils::DataPacket>();
  packet->id = id;
  return packet;
}

inline PortDataPtr makeDataPacketWithValue(uint64_t id, const std::string &key,
                                           int value) {
  auto packet = std::make_shared<common_utils::DataPacket>();
  packet->id = id;
  packet->setParam(key, value);
  return packet;
}

// =============================================================================
// Mock Node for Testing
// =============================================================================

class TestNode : public ILogicNode {
public:
  explicit TestNode(const std::string &name,
                    std::chrono::milliseconds delay = 0ms)
      : ILogicNode(name), m_delay(delay) {
    // Default ports
    m_inputPorts = {"input"};
    m_outputPorts = {"output"};
  }

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    m_processCount.fetch_add(1);

    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
    }

    // Copy input to output
    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return m_inputPorts;
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return m_outputPorts;
  }

  void setInputPorts(const std::vector<std::string> &ports) {
    m_inputPorts = ports;
  }

  void setOutputPorts(const std::vector<std::string> &ports) {
    m_outputPorts = ports;
  }

  int processCount() const { return m_processCount.load(); }

private:
  std::vector<std::string> m_inputPorts;
  std::vector<std::string> m_outputPorts;
  std::chrono::milliseconds m_delay;
  std::atomic<int> m_processCount{0};
};

// Source node that generates data
class SourceNode : public ILogicNode {
public:
  explicit SourceNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    outputs["output"] = makeDataPacketWithValue(m_nextId++, "value", 42);
    m_processCount.fetch_add(1);
  }

  std::vector<std::string> getExpectedInputPorts() const override { return {}; }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int processCount() const { return m_processCount.load(); }

private:
  std::atomic<int> m_processCount{0};
  std::atomic<uint64_t> m_nextId{1};
};

// Sink node that collects data
class SinkNode : public ILogicNode {
public:
  explicit SinkNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {
    m_processCount.fetch_add(1);

    for (const auto &[port, data] : inputs) {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_receivedData.push_back(data);
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  int processCount() const { return m_processCount.load(); }

  std::vector<PortDataPtr> receivedData() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_receivedData;
  }

private:
  std::atomic<int> m_processCount{0};
  mutable std::mutex m_mutex;
  std::vector<PortDataPtr> m_receivedData;
};

// =============================================================================
// PipelineOptions Tests (unchanged — struct itself is the same)
// =============================================================================

TEST(PipelineOptionsTest, DefaultValues) {
  PipelineOptions opts;

  EXPECT_EQ(opts.mode, ExecutionMode::BATCH);
  EXPECT_EQ(opts.num_workers, 4);
  EXPECT_EQ(opts.execution_timeout.count(), 0);
  EXPECT_EQ(opts.queue_capacity, 0);
  EXPECT_EQ(opts.drop_strategy, "DropHead");
  EXPECT_FALSE(opts.enable_sync_coordination);
  EXPECT_TRUE(opts.enable_statistics);
}

TEST(PipelineOptionsTest, BatchFactory) {
  auto opts = PipelineOptions::batch(8);

  EXPECT_EQ(opts.mode, ExecutionMode::BATCH);
  EXPECT_EQ(opts.num_workers, 8);
  EXPECT_EQ(opts.queue_capacity, 0);
  EXPECT_FALSE(opts.enable_sync_coordination);
}

TEST(PipelineOptionsTest, BatchFactoryDefaultWorkers) {
  auto opts = PipelineOptions::batch();

  EXPECT_EQ(opts.num_workers, 4);
}

TEST(PipelineOptionsTest, StreamFactory) {
  auto opts = PipelineOptions::stream(6, 32);

  EXPECT_EQ(opts.mode, ExecutionMode::STREAM);
  EXPECT_EQ(opts.num_workers, 6);
  EXPECT_EQ(opts.queue_capacity, 32);
  EXPECT_TRUE(opts.enable_sync_coordination);
}

TEST(PipelineOptionsTest, StreamFactoryDefaultValues) {
  auto opts = PipelineOptions::stream();

  EXPECT_EQ(opts.num_workers, 4);
  EXPECT_EQ(opts.queue_capacity, 16);
}

// =============================================================================
// ExecutionOutput Tests (replaces ExecutionResult)
// =============================================================================

TEST(ExecutionOutputTest, DefaultValues) {
  // v2.0: ExecutionResult removed → ExecutionOutput is the success payload
  ExecutionOutput output;

  EXPECT_TRUE(output.outputs.empty());
  EXPECT_EQ(output.elapsed.count(), 0);
}

TEST(ExecutionOutputTest, InsideResultSuccess) {
  // v2.0: Pipeline::run() returns Result<ExecutionOutput>
  ExecutionOutput output;
  output.outputs["test"] = makeDataPacket(1);
  output.elapsed = std::chrono::milliseconds(150);

  Result<ExecutionOutput> result = std::move(output);

  EXPECT_TRUE(result.isOk());
  EXPECT_TRUE(static_cast<bool>(result));
  EXPECT_EQ(result.value().outputs.size(), 1);
  EXPECT_EQ(result.value().elapsed.count(), 150);
}

TEST(ExecutionOutputTest, InsideResultError) {
  // v2.0: error branch carries Error instead of string
  auto result = Result<ExecutionOutput>::err(ErrorCode::ExecutionFailed,
                                             "node crashed", "detector");

  EXPECT_FALSE(result.isOk());
  EXPECT_FALSE(static_cast<bool>(result));
  EXPECT_EQ(result.error().code(), ErrorCode::ExecutionFailed);
  EXPECT_EQ(result.error().message(), "node crashed");
  EXPECT_EQ(result.error().nodeName(), "detector");
}

// =============================================================================
// IPipelineObserver Tests
// =============================================================================

TEST(IPipelineObserverTest, DefaultImplementationsDoNotCrash) {
  class TestObserver : public IPipelineObserver {};

  TestObserver observer;

  EXPECT_NO_THROW(observer.onExecutionStarted());
  EXPECT_NO_THROW(observer.onExecutionCompleted({}));
  // v2.0: onExecutionFailed now takes const Error& instead of two strings
  EXPECT_NO_THROW(observer.onExecutionFailed(
      Error(ErrorCode::ExecutionFailed, "error", "node")));
  EXPECT_NO_THROW(observer.onFrameDropped("node", 100, "reason"));
}

// =============================================================================
// CallbackObserver Tests
// =============================================================================

TEST(CallbackObserverTest, OnStart) {
  bool started = false;

  CallbackObserver observer;
  observer.onStart([&started]() { started = true; });

  observer.onExecutionStarted();

  EXPECT_TRUE(started);
}

TEST(CallbackObserverTest, OnResult) {
  PortDataMap received;

  CallbackObserver observer;
  observer.onResult([&received](const PortDataMap &r) { received = r; });

  PortDataMap outputs;
  outputs["test"] = makeDataPacket(1);
  observer.onExecutionCompleted(outputs);

  EXPECT_EQ(received.size(), 1);
}

TEST(CallbackObserverTest, OnError) {
  // v2.0: onError callback now receives const Error& instead of two strings
  ErrorCode received_code = ErrorCode::Ok;
  std::string received_msg;
  std::string received_node;

  CallbackObserver observer;
  observer.onError([&](const Error &e) {
    received_code = e.code();
    received_msg = e.message();
    received_node = e.nodeName();
  });

  observer.onExecutionFailed(
      Error(ErrorCode::NodeException, "test error", "test_node"));

  EXPECT_EQ(received_code, ErrorCode::NodeException);
  EXPECT_EQ(received_msg, "test error");
  EXPECT_EQ(received_node, "test_node");
}

TEST(CallbackObserverTest, OnDrop) {
  std::string received_node;
  std::uint64_t received_frame;
  std::string received_reason;

  CallbackObserver observer;
  observer.onDrop(
      [&](const std::string &n, std::uint64_t f, const std::string &r) {
        received_node = n;
        received_frame = f;
        received_reason = r;
      });

  observer.onFrameDropped("drop_node", 100, "backpressure");

  EXPECT_EQ(received_node, "drop_node");
  EXPECT_EQ(received_frame, 100);
  EXPECT_EQ(received_reason, "backpressure");
}

TEST(CallbackObserverTest, Chaining) {
  bool started = false;
  bool completed = false;

  CallbackObserver observer;
  observer.onStart([&started]() { started = true; })
      .onResult([&completed](const PortDataMap &) { completed = true; });

  observer.onExecutionStarted();
  observer.onExecutionCompleted({});

  EXPECT_TRUE(started);
  EXPECT_TRUE(completed);
}

TEST(CallbackObserverTest, NullCallbacksDoNotCrash) {
  CallbackObserver observer;

  EXPECT_NO_THROW(observer.onExecutionStarted());
  EXPECT_NO_THROW(observer.onExecutionCompleted({}));
  // v2.0: pass Error object instead of two strings
  EXPECT_NO_THROW(observer.onExecutionFailed(Error()));
  EXPECT_NO_THROW(observer.onFrameDropped("", 0, ""));
}

// =============================================================================
// PipelineBuilder Tests (build() now returns Result<Pipeline>)
// =============================================================================

class PipelineBuilderTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_graph = std::make_unique<Graph>();

    m_source = std::make_shared<SourceNode>("source");
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(m_sink);
    m_graph->addEdge("source", "output", "sink", "input");
  }

  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<SourceNode> m_source;
  std::shared_ptr<SinkNode> m_sink;
};

TEST_F(PipelineBuilderTest, BasicBuild) {
  // v2.0: build() returns Result<Pipeline>
  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withMode(ExecutionMode::BATCH)
                    .withWorkers(2)
                    .build();

  ASSERT_TRUE(result.isOk()) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
  EXPECT_EQ(result.value().mode(), ExecutionMode::BATCH);
}

TEST_F(PipelineBuilderTest, WithOptions) {
  auto opts = PipelineOptions::batch(8);

  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withOptions(opts)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, WithContext) {
  auto context = std::make_shared<PipelineContext>();
  context->setConfig("test_key", 42);

  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withContext(context)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().context().hasConfig("test_key"));
}

TEST_F(PipelineBuilderTest, WithTimeout) {
  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withTimeout(5000ms)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, WithQueueCapacity) {
  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withMode(ExecutionMode::STREAM)
                    .withQueueCapacity(32)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, WithDropStrategy) {
  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withMode(ExecutionMode::STREAM)
                    .withQueueCapacity(16)
                    .withDropStrategy("DropTail")
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, WithSyncCoordination) {
  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withMode(ExecutionMode::STREAM)
                    .withQueueCapacity(16)
                    .withSyncCoordination(true)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, WithSchedulerStrategy) {
  auto strategy = std::make_unique<BatchSchedulerStrategy>();

  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withSchedulerStrategy(std::move(strategy))
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, WithSyncStrategy) {
  auto strategy = createNoSyncStrategy();

  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withSyncStrategy(std::move(strategy))
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, WithObserver) {
  auto observer = std::make_shared<CallbackObserver>();
  bool started = false;
  observer->onStart([&started]() { started = true; });

  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withObserver(observer)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, OnResultCallback) {
  bool received = false;

  auto result =
      Pipeline::create()
          .withGraph(std::move(*m_graph))
          .onResult([&received](const PortDataMap &) { received = true; })
          .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, OnErrorCallback) {
  // v2.0: onError callback now receives const Error& instead of two strings
  ErrorCode captured_code = ErrorCode::Ok;

  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .onError([&captured_code](const Error &e) {
                      captured_code = e.code();
                    })
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST_F(PipelineBuilderTest, MoveConstruction) {
  auto builder1 = Pipeline::create();
  builder1.withGraph(std::move(*m_graph));
  PipelineBuilder builder2(std::move(builder1));

  // v2.0: build() returns Result<Pipeline>
  auto result = builder2.build();
  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

// =============================================================================
// Pipeline Basic Tests
// =============================================================================

class PipelineTest : public ::testing::Test {
protected:
  void SetUp() override {
    m_graph = std::make_unique<Graph>();

    m_source = std::make_shared<SourceNode>("source");
    m_sink = std::make_shared<SinkNode>("sink");

    m_graph->addNode(m_source);
    m_graph->addNode(m_sink);
    m_graph->addEdge("source", "output", "sink", "input");
  }

  // Helper: build and unwrap, fail the test on error
  Pipeline buildPipeline(Graph &&graph) {
    auto result = Pipeline::create().withGraph(std::move(graph)).build();
    EXPECT_TRUE(result) << result.errorMessage();
    return std::move(result).value();
  }

  std::unique_ptr<Graph> m_graph;
  std::shared_ptr<SourceNode> m_source;
  std::shared_ptr<SinkNode> m_sink;
};

TEST_F(PipelineTest, DefaultConstruction) {
  Pipeline pipeline;
  EXPECT_FALSE(pipeline.isReady());
}

TEST_F(PipelineTest, MoveConstruction) {
  // v2.0: build() returns Result<Pipeline>, unwrap via helper
  auto pipeline1 = buildPipeline(std::move(*m_graph));

  EXPECT_TRUE(pipeline1.isReady());

  Pipeline pipeline2(std::move(pipeline1));
  EXPECT_TRUE(pipeline2.isReady());
}

TEST_F(PipelineTest, MoveAssignment) {
  auto pipeline1 = buildPipeline(std::move(*m_graph));

  Pipeline pipeline2;
  pipeline2 = std::move(pipeline1);

  EXPECT_TRUE(pipeline2.isReady());
}

TEST_F(PipelineTest, GraphAccessor) {
  auto pipeline = buildPipeline(std::move(*m_graph));

  const Graph &g = pipeline.graph();
  EXPECT_EQ(g.getNodes().size(), 2);
  EXPECT_EQ(g.getEdges().size(), 1);
}

TEST_F(PipelineTest, ContextAccessor) {
  auto ctx = std::make_shared<PipelineContext>();
  ctx->setConfig("test", 100);

  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withContext(ctx)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().context().hasConfig("test"));
}

TEST_F(PipelineTest, Info) {
  auto pipeline = buildPipeline(std::move(*m_graph));

  std::string info = pipeline.info();
  EXPECT_FALSE(info.empty());
}

TEST_F(PipelineTest, ModeAccessor) {
  auto result = Pipeline::create()
                    .withGraph(std::move(*m_graph))
                    .withMode(ExecutionMode::BATCH)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_EQ(result.value().mode(), ExecutionMode::BATCH);
}

// =============================================================================
// Pipeline Observer Management Tests
// =============================================================================

TEST_F(PipelineTest, AddObserver) {
  auto pipeline = buildPipeline(std::move(*m_graph));

  auto observer = std::make_shared<CallbackObserver>();

  EXPECT_NO_THROW(pipeline.addObserver(observer));
}

TEST_F(PipelineTest, RemoveObserver) {
  auto pipeline = buildPipeline(std::move(*m_graph));

  auto observer = std::make_shared<CallbackObserver>();
  pipeline.addObserver(observer);

  EXPECT_NO_THROW(pipeline.removeObserver(observer));
}

// =============================================================================
// Pipeline State Tests
// =============================================================================

TEST_F(PipelineTest, InitialState) {
  auto pipeline = buildPipeline(std::move(*m_graph));

  EXPECT_TRUE(pipeline.isReady());
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_FALSE(pipeline.hasError());
}

// =============================================================================
// Convenience Factory Functions Tests (now return Result<Pipeline>)
// =============================================================================

TEST(PipelineFactoryTest, MakeBatchPipeline) {
  Graph graph;
  auto source = std::make_shared<SourceNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  // v2.0: returns Result<Pipeline>
  auto result = makeBatchPipeline(std::move(graph), 4);

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
  EXPECT_EQ(result.value().mode(), ExecutionMode::BATCH);
}

TEST(PipelineFactoryTest, MakeBatchPipelineDefaultWorkers) {
  Graph graph;
  auto source = std::make_shared<SourceNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  auto result = makeBatchPipeline(std::move(graph));

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

TEST(PipelineFactoryTest, MakeStreamPipeline) {
  Graph graph;
  auto source = std::make_shared<SourceNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  // v2.0: returns Result<Pipeline>
  auto result = makeStreamPipeline(std::move(graph), 4, 32);

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
  EXPECT_EQ(result.value().mode(), ExecutionMode::STREAM);
}

TEST(PipelineFactoryTest, MakeStreamPipelineDefaultParams) {
  Graph graph;
  auto source = std::make_shared<SourceNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");
  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  auto result = makeStreamPipeline(std::move(graph));

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
}

// =============================================================================
// Complex Graph Tests
// =============================================================================

TEST(PipelineComplexGraphTest, DiamondTopology) {
  Graph graph;

  auto source = std::make_shared<SourceNode>("source");
  auto left = std::make_shared<TestNode>("left");
  auto right = std::make_shared<TestNode>("right");
  auto sink = std::make_shared<SinkNode>("sink");

  left->setInputPorts({"input"});
  left->setOutputPorts({"output"});
  right->setInputPorts({"input"});
  right->setOutputPorts({"output"});

  graph.addNode(source);
  graph.addNode(left);
  graph.addNode(right);
  graph.addNode(sink);

  graph.addEdge("source", "output", "left", "input");
  graph.addEdge("source", "output", "right", "input");
  graph.addEdge("left", "output", "sink", "input");
  graph.addEdge("right", "output", "sink", "input");

  // v2.0: build() returns Result<Pipeline>
  auto result = Pipeline::create()
                    .withGraph(std::move(graph))
                    .withMode(ExecutionMode::BATCH)
                    .build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
  EXPECT_EQ(result.value().graph().getNodes().size(), 4);
  EXPECT_EQ(result.value().graph().getEdges().size(), 4);
}

TEST(PipelineComplexGraphTest, LinearChain) {
  Graph graph;

  std::vector<std::shared_ptr<TestNode>> nodes;
  for (int i = 0; i < 5; ++i) {
    auto node = std::make_shared<TestNode>("node_" + std::to_string(i));
    if (i == 0) {
      node->setInputPorts({});
    } else {
      node->setInputPorts({"input"});
    }
    if (i == 4) {
      node->setOutputPorts({});
    } else {
      node->setOutputPorts({"output"});
    }
    nodes.push_back(node);
    graph.addNode(node);
  }

  for (int i = 0; i < 4; ++i) {
    graph.addEdge("node_" + std::to_string(i), "output",
                  "node_" + std::to_string(i + 1), "input");
  }

  // v2.0: build() returns Result<Pipeline>
  auto result = Pipeline::create().withGraph(std::move(graph)).build();

  ASSERT_TRUE(result) << result.errorMessage();
  EXPECT_TRUE(result.value().isReady());
  EXPECT_EQ(result.value().graph().getNodes().size(), 5);
}

TEST_F(PipelineTest, ExecutionTimeout) {
  auto source = std::make_shared<TestNode>("slow_node", 500ms);
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("slow_node", "output", "sink", "input");

  auto pipeline = Pipeline::create()
      .withGraph(std::move(graph))
      .withTimeout(50ms) // shorter than node delay
      .build().value();

  PortDataMap inputs;
  inputs["slow_node"] = makeDataPacket(1);

  auto result = pipeline.run(inputs);
  EXPECT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::ExecutionTimeout);
}

TEST_F(PipelineTest, CancellationMidExecution) {
  auto source = std::make_shared<TestNode>("slow_node", 500ms);
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("slow_node", "output", "sink", "input");

  auto pipeline = Pipeline::create()
      .withGraph(std::move(graph))
      .build().value();

  PortDataMap inputs;
  inputs["slow_node"] = makeDataPacket(1);

  std::thread t([&]() {
    std::this_thread::sleep_for(100ms);
    pipeline.cancel();
  });

  auto result = pipeline.run(inputs);
  t.join();

  // If cancellation works, it might return ExecutionStopped or ExecutionFailed
  EXPECT_FALSE(result.isOk());
  EXPECT_TRUE(result.errorCode() == ErrorCode::ExecutionStopped ||
              result.errorCode() == ErrorCode::ExecutionFailed);
}
