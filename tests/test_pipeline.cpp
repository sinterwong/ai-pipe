#include "ai_pipe/data_packet.hpp"
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/error.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/pipeline.hpp"
#include "ai_pipe/trace.hpp"
#include "scheduler_strategies.hpp"
#include "sync_strategies.hpp"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <gtest/gtest.h>
#include <memory>
#include <mutex>
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
                      .build()
                      .value();

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

  auto pipeline =
      Pipeline::create().withGraph(std::move(graph)).build().value();

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

// =============================================================================
// Pipeline Facade Coverage: build failures, streaming, submit/runAsync,
// observers wired end-to-end, trace sink, uninitialized safety
// =============================================================================

namespace {

// Sink whose process() blocks until released, for deterministic queue-depth
// and drop-path tests
class GatedSinkNode : public ILogicNode {
public:
  explicit GatedSinkNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {
    m_entered.fetch_add(1);
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this] { return m_open; });
    m_processed.fetch_add(1);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  void open() {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_open = true;
    }
    m_cv.notify_all();
  }

  // Wait until at least `count` process() calls have started
  bool waitEntered(int count,
                   std::chrono::milliseconds timeout = 2000ms) const {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (m_entered.load() < count) {
      if (std::chrono::steady_clock::now() > deadline) {
        return false;
      }
      std::this_thread::sleep_for(1ms);
    }
    return true;
  }

  int processed() const { return m_processed.load(); }

private:
  mutable std::mutex m_mutex;
  std::condition_variable m_cv;
  bool m_open = false;
  std::atomic<int> m_entered{0};
  std::atomic<int> m_processed{0};
};

// Node that always throws, for end-to-end error propagation
class ThrowingNode : public ILogicNode {
public:
  explicit ThrowingNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {
    if (m_armed.load()) {
      throw std::runtime_error("ThrowingNode failure");
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  void disarm() { m_armed.store(false); }

private:
  std::atomic<bool> m_armed{true};
};

class CountingTraceSink : public ITraceSink {
public:
  void onEvent(const TraceEvent &) override { m_events.fetch_add(1); }
  int events() const { return m_events.load(); }

private:
  std::atomic<int> m_events{0};
};

// Poll a predicate until it holds or the timeout elapses
template <typename Pred>
bool waitFor(Pred pred, std::chrono::milliseconds timeout = 2000ms) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() > deadline) {
      return false;
    }
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

} // namespace

// =============================================================================
// PipelineBuilder Failure Paths
// =============================================================================

TEST(PipelineBuildFailureTest, BuildWithoutGraphFails) {
  auto result = Pipeline::create().build();

  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::InvalidArgument);
  EXPECT_NE(result.errorMessage().find("No graph"), std::string::npos);
}

TEST(PipelineBuildFailureTest, BuildWithCyclicGraphFails) {
  auto node_a = std::make_shared<TestNode>("a");
  auto node_b = std::make_shared<TestNode>("b");

  Graph graph;
  graph.addNode(node_a);
  graph.addNode(node_b);
  graph.addEdge("a", "output", "b", "input");
  graph.addEdge("b", "output", "a", "input");

  auto result = Pipeline::create().withGraph(std::move(graph)).build();

  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::GraphCycleDetected);
}

// =============================================================================
// Streaming via the Pipeline Facade
// =============================================================================

class PipelineStreamingTest : public ::testing::Test {
protected:
  // source (no input ports) -> sink; pushes to "source" route downstream
  Pipeline buildSourceSinkStream(std::size_t queue_capacity = 8) {
    m_source = std::make_shared<SourceNode>("source");
    m_sink = std::make_shared<SinkNode>("sink");

    Graph graph;
    graph.addNode(m_source);
    graph.addNode(m_sink);
    graph.addEdge("source", "output", "sink", "input");

    auto result = Pipeline::create()
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::STREAM)
                      .withWorkers(2)
                      .withQueueCapacity(queue_capacity)
                      .build();
    EXPECT_TRUE(result) << result.errorMessage();
    return std::move(result).value();
  }

  // Single gated sink; pushes go straight into its input queue
  Pipeline buildGatedStream(std::size_t queue_capacity,
                            PipelineBuilder &&builder) {
    m_gate = std::make_shared<GatedSinkNode>("gate");

    Graph graph;
    graph.addNode(m_gate);

    // Single worker: the gate blocks it, so queue depths and drop points
    // are deterministic (a second worker would run the node concurrently)
    auto result = std::move(builder)
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::STREAM)
                      .withWorkers(1)
                      .withQueueCapacity(queue_capacity)
                      .build();
    EXPECT_TRUE(result) << result.errorMessage();
    return std::move(result).value();
  }

  std::shared_ptr<SourceNode> m_source;
  std::shared_ptr<SinkNode> m_sink;
  std::shared_ptr<GatedSinkNode> m_gate;
};

TEST_F(PipelineStreamingTest, StartPushDrainStopFullChain) {
  auto pipeline = buildSourceSinkStream();

  EXPECT_FALSE(pipeline.isStreaming());
  ASSERT_TRUE(pipeline.start().isOk());
  EXPECT_TRUE(pipeline.isStreaming());
  EXPECT_EQ(pipeline.state(), PipelineState::RUNNING);

  constexpr int k_frames = 5;
  for (int i = 1; i <= k_frames; ++i) {
    auto push = pipeline.pushInput("source", makeDataPacket(i));
    ASSERT_TRUE(push.isOk()) << push.errorMessage();
  }

  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());
  EXPECT_EQ(m_sink->processCount(), k_frames);

  auto stats = pipeline.statistics();
  EXPECT_GE(stats.total_input_frames, static_cast<std::uint64_t>(k_frames));

  pipeline.stop(true);
  EXPECT_FALSE(pipeline.isStreaming());
  EXPECT_EQ(pipeline.state(), PipelineState::IDLE);
}

TEST_F(PipelineStreamingTest, NodeExceptionIsPerFrameNotPipelineFatal) {
  auto thrower = std::make_shared<ThrowingNode>("thrower");

  Graph graph;
  graph.addNode(thrower);

  std::atomic<int> error_count{0};
  auto pipeline = Pipeline::create()
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::STREAM)
                      .withWorkers(1)
                      .onError([&](const Error &) { error_count.fetch_add(1); })
                      .build()
                      .value();

  ASSERT_TRUE(pipeline.start().isOk());
  ASSERT_TRUE(pipeline.pushInput("thrower", makeDataPacket(1)).isOk());
  ASSERT_TRUE(waitFor([&] { return error_count.load() >= 1; }));

  // Regression (R1.2): a per-frame node exception in streaming mode used
  // to latch the facade into ERROR while the engine kept running -
  // isRunning() went false and validateState() refused everything until
  // reset(). The facade must stay RUNNING and keep accepting frames.
  EXPECT_TRUE(pipeline.isStreaming());
  EXPECT_TRUE(pipeline.isRunning());
  EXPECT_EQ(pipeline.state(), PipelineState::RUNNING);
  EXPECT_FALSE(pipeline.hasError());

  thrower->disarm();
  auto push = pipeline.pushInput("thrower", makeDataPacket(2));
  ASSERT_TRUE(push.isOk()) << push.errorMessage();
  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());

  pipeline.stop(true);
  EXPECT_EQ(pipeline.state(), PipelineState::IDLE);
  EXPECT_EQ(error_count.load(), 1);
}

TEST_F(PipelineStreamingTest, StartFailsInBatchMode) {
  auto source = std::make_shared<SourceNode>("source");
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  auto pipeline = Pipeline::create()
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::BATCH)
                      .build()
                      .value();

  auto result = pipeline.start();
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::InvalidState);
  EXPECT_FALSE(pipeline.isStreaming());
}

TEST_F(PipelineStreamingTest, StartTwiceFails) {
  auto pipeline = buildSourceSinkStream();

  ASSERT_TRUE(pipeline.start().isOk());
  auto second = pipeline.start();
  ASSERT_FALSE(second.isOk());
  EXPECT_EQ(second.errorCode(), ErrorCode::AlreadyRunning);

  pipeline.stop(false);
}

TEST_F(PipelineStreamingTest, PushBeforeStartIsRejected) {
  auto pipeline = buildSourceSinkStream();

  auto result = pipeline.pushInput("source", makeDataPacket(1));
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::NotStreaming);
}

TEST_F(PipelineStreamingTest, PushToUnknownNodeFails) {
  auto pipeline = buildSourceSinkStream();
  ASSERT_TRUE(pipeline.start().isOk());

  auto result = pipeline.pushInput("no_such_node", makeDataPacket(1));
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::NodeNotFound);

  pipeline.stop(false);
}

TEST_F(PipelineStreamingTest, NamedPortPushReachesSink) {
  auto pipeline = buildSourceSinkStream();
  ASSERT_TRUE(pipeline.start().isOk());

  auto push = pipeline.pushInput("sink", "input", makeDataPacket(7));
  ASSERT_TRUE(push.isOk()) << push.errorMessage();

  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());
  ASSERT_EQ(m_sink->receivedData().size(), 1u);
  EXPECT_EQ(m_sink->receivedData()[0]->id, 7u);

  pipeline.stop(true);
}

TEST_F(PipelineStreamingTest, QueueDepthAndCapacityDuringStream) {
  auto pipeline = buildGatedStream(4, Pipeline::create());
  ASSERT_TRUE(pipeline.start().isOk());

  // First frame occupies the worker; the gate blocks it inside process()
  ASSERT_TRUE(pipeline.pushInput("gate", makeDataPacket(1)).isOk());
  ASSERT_TRUE(m_gate->waitEntered(1));

  ASSERT_TRUE(pipeline.pushInput("gate", makeDataPacket(2)).isOk());
  ASSERT_TRUE(pipeline.pushInput("gate", makeDataPacket(3)).isOk());

  EXPECT_EQ(pipeline.queueDepth("gate"), 2u);
  EXPECT_TRUE(pipeline.hasQueueCapacity("gate"));
  EXPECT_EQ(pipeline.queueDepth("no_such_node"), 0u);

  m_gate->open();
  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());
  EXPECT_EQ(pipeline.queueDepth("gate"), 0u);
  EXPECT_EQ(m_gate->processed(), 3);

  pipeline.stop(true);
}

TEST_F(PipelineStreamingTest, WaitForDrainTimesOutWhileBlocked) {
  auto pipeline = buildGatedStream(4, Pipeline::create());
  ASSERT_TRUE(pipeline.start().isOk());

  ASSERT_TRUE(pipeline.pushInput("gate", makeDataPacket(1)).isOk());
  ASSERT_TRUE(m_gate->waitEntered(1));

  auto result = pipeline.waitForDrain(0, 100ms);
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::ExecutionTimeout);

  m_gate->open();
  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());
  pipeline.stop(true);
}

TEST_F(PipelineStreamingTest, DropNotificationReachesBuilderOnDrop) {
  std::atomic<int> drop_count{0};
  std::string dropped_node;
  std::string drop_reason;
  std::mutex drop_mutex;

  auto builder = Pipeline::create();
  builder.onDrop(
      [&](const std::string &node, std::uint64_t, const std::string &reason) {
        std::lock_guard<std::mutex> lock(drop_mutex);
        dropped_node = node;
        drop_reason = reason;
        drop_count.fetch_add(1);
      });

  auto pipeline = buildGatedStream(1, std::move(builder));
  ASSERT_TRUE(pipeline.start().isOk());

  // Frame 1 blocks inside the gate. The queue rounds the requested
  // capacity 1 up to the minimum of 2, so frames 2+3 fill it and frame 4
  // evicts frame 2 (DropHead default)
  ASSERT_TRUE(pipeline.pushInput("gate", makeDataPacket(1)).isOk());
  ASSERT_TRUE(m_gate->waitEntered(1));
  ASSERT_TRUE(pipeline.pushInput("gate", makeDataPacket(2)).isOk());
  ASSERT_TRUE(pipeline.pushInput("gate", makeDataPacket(3)).isOk());
  // Direct input-port pushes report the eviction via the drop callback
  // (the push itself is accepted)
  auto fourth = pipeline.pushInput("gate", makeDataPacket(4));
  ASSERT_TRUE(fourth.isOk()) << fourth.errorMessage();

  m_gate->open();
  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());
  pipeline.stop(true);

  EXPECT_EQ(drop_count.load(), 1);
  EXPECT_EQ(m_gate->processed(), 3);
  {
    std::lock_guard<std::mutex> lock(drop_mutex);
    EXPECT_EQ(dropped_node, "gate");
    EXPECT_FALSE(drop_reason.empty());
  }
  EXPECT_GE(pipeline.statistics().total_dropped_frames, 1u);
}

// =============================================================================
// submit() / runAsync() via the Facade
// =============================================================================

class PipelineAsyncTest : public ::testing::Test {
protected:
  Pipeline buildWithResultFlag() {
    m_source = std::make_shared<SourceNode>("source");
    m_sink = std::make_shared<SinkNode>("sink");

    Graph graph;
    graph.addNode(m_source);
    graph.addNode(m_sink);
    graph.addEdge("source", "output", "sink", "input");

    auto result =
        Pipeline::create()
            .withGraph(std::move(graph))
            .onResult([this](const PortDataMap &) { m_results.fetch_add(1); })
            .build();
    EXPECT_TRUE(result) << result.errorMessage();
    return std::move(result).value();
  }

  std::shared_ptr<SourceNode> m_source;
  std::shared_ptr<SinkNode> m_sink;
  std::atomic<int> m_results{0};
};

TEST_F(PipelineAsyncTest, SubmitCompletesAndReturnsToIdle) {
  auto pipeline = buildWithResultFlag();

  PortDataMap inputs;
  inputs["source"] = makeDataPacket(1);

  ASSERT_TRUE(pipeline.submit(inputs).isOk());
  ASSERT_TRUE(waitFor([&] { return m_results.load() >= 1; }));

  // Fire-and-forget completion must return the pipeline to IDLE so that
  // subsequent executions are possible. The engine flips its own state
  // shortly after the result callback, so wait for both.
  ASSERT_TRUE(waitFor([&] {
    return pipeline.state() == PipelineState::IDLE &&
           pipeline.engineState() == EngineState::IDLE;
  }));

  ASSERT_TRUE(pipeline.submit(inputs).isOk());
  ASSERT_TRUE(waitFor([&] { return m_results.load() >= 2; }));
  ASSERT_TRUE(waitFor([&] {
    return pipeline.state() == PipelineState::IDLE &&
           pipeline.engineState() == EngineState::IDLE;
  }));
  EXPECT_EQ(m_sink->processCount(), 2);
}

TEST_F(PipelineAsyncTest, RunAsyncDeliversResult) {
  auto pipeline = buildWithResultFlag();

  PortDataMap inputs;
  inputs["source"] = makeDataPacket(1);

  auto future = pipeline.runAsync(inputs);
  ASSERT_EQ(future.wait_for(5s), std::future_status::ready);

  auto result = future.get();
  ASSERT_TRUE(result.isOk()) << result.errorMessage();
  EXPECT_GE(result.value().elapsed.count(), 0);
  EXPECT_EQ(pipeline.state(), PipelineState::IDLE);
}

TEST_F(PipelineAsyncTest, RunAsyncPropagatesNodeFailure) {
  auto thrower = std::make_shared<ThrowingNode>("thrower");

  Graph graph;
  graph.addNode(thrower);

  auto pipeline =
      Pipeline::create().withGraph(std::move(graph)).build().value();

  PortDataMap inputs;
  inputs["thrower"] = makeDataPacket(1);

  auto future = pipeline.runAsync(inputs);
  ASSERT_EQ(future.wait_for(5s), std::future_status::ready);

  auto result = future.get();
  ASSERT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::NodeException);
  EXPECT_TRUE(pipeline.hasError());
}

TEST_F(PipelineAsyncTest, RunAfterRunAsyncCompletesWithoutRefire) {
  auto pipeline = buildWithResultFlag();

  PortDataMap inputs;
  inputs["source"] = makeDataPacket(1);

  auto future = pipeline.runAsync(inputs);
  ASSERT_EQ(future.wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(future.get().isOk());
  ASSERT_TRUE(waitFor([&] { return m_results.load() >= 1; }));
  const int results_after_async = m_results.load();

  // Regression (R1.1): runAsync used to leave its one-shot promise
  // callbacks registered on the engine. The next run() then re-fired
  // them; the second set_value threw std::future_error out of
  // checkCompletionAndNotify, skipping notifyCompletionWaiters, and
  // the synchronous run() hung forever. Run it on a side thread so a
  // regression fails the assertion instead of hanging the suite.
  auto second =
      std::async(std::launch::async, [&] { return pipeline.run(inputs); });
  ASSERT_EQ(second.wait_for(5s), std::future_status::ready)
      << "run() after runAsync() hung";
  auto run_result = second.get();
  ASSERT_TRUE(run_result.isOk()) << run_result.errorMessage();

  // Exactly one additional observer notification: the resident result
  // callback fired for run(), and the stale runAsync handler did not.
  ASSERT_TRUE(
      waitFor([&] { return m_results.load() >= results_after_async + 1; }));
  EXPECT_EQ(m_results.load(), results_after_async + 1);
  EXPECT_EQ(pipeline.state(), PipelineState::IDLE);
}

// =============================================================================
// CancellationToken wiring (R3.1)
// =============================================================================

namespace {

// Pass-through node that blocks until released, recording entry
class GatedPassNode : public ILogicNode {
public:
  explicit GatedPassNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    m_entered.store(true);
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this] { return m_open; });
    for (const auto &[port, data] : inputs) {
      outputs["output"] = data;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  void open() {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_open = true;
    }
    m_cv.notify_all();
  }

  bool waitEntered(std::chrono::milliseconds timeout = 2000ms) const {
    return waitFor([this] { return m_entered.load(); }, timeout);
  }

private:
  mutable std::mutex m_mutex;
  std::condition_variable m_cv;
  bool m_open = false;
  std::atomic<bool> m_entered{false};
};

// Node whose process() spins until the cooperative token fires
class CancellationAwareNode : public ILogicNode {
public:
  explicit CancellationAwareNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &,
               std::shared_ptr<PipelineContext> ctx) override {
    m_entered.store(true);
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline) {
      if (ctx && ctx->isCancellationRequested()) {
        m_sawCancellation.store(true);
        return;
      }
      std::this_thread::sleep_for(1ms);
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  bool waitEntered(std::chrono::milliseconds timeout = 2000ms) const {
    return waitFor([this] { return m_entered.load(); }, timeout);
  }
  bool sawCancellation() const { return m_sawCancellation.load(); }

private:
  std::atomic<bool> m_entered{false};
  std::atomic<bool> m_sawCancellation{false};
};

} // namespace

TEST(PipelineCancellationTest, CancelReachesNodeThroughToken) {
  auto node = std::make_shared<CancellationAwareNode>("aware");

  Graph graph;
  graph.addNode(node);

  auto pipeline =
      Pipeline::create().withGraph(std::move(graph)).build().value();

  PortDataMap inputs;
  inputs["aware"] = makeDataPacket(1);

  auto future =
      std::async(std::launch::async, [&] { return pipeline.run(inputs); });
  ASSERT_TRUE(node->waitEntered());

  // cancel() must reach the node mid-process via the cooperative token,
  // not only stop future scheduling: the node exits its loop well before
  // its 5s deadline.
  pipeline.cancel();

  ASSERT_EQ(future.wait_for(3s), std::future_status::ready);
  EXPECT_FALSE(future.get().isOk());
  // run() returns via the stop protocol as soon as cancel() lands; the
  // node observes the token at its next loop iteration. Its deadline is
  // 5s, so seeing the flag within 3s proves the token (not the deadline)
  // ended the loop.
  EXPECT_TRUE(waitFor([&] { return node->sawCancellation(); }, 3000ms));
}

TEST(PipelineCancellationTest, DirectTokenCancelStopsScheduling) {
  auto gate = std::make_shared<GatedPassNode>("gate");
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(gate);
  graph.addNode(sink);
  graph.addEdge("gate", "output", "sink", "input");

  auto pipeline =
      Pipeline::create().withGraph(std::move(graph)).build().value();

  PortDataMap inputs;
  inputs["gate"] = makeDataPacket(1);

  auto future =
      std::async(std::launch::async, [&] { return pipeline.run(inputs); });
  ASSERT_TRUE(gate->waitEntered());

  // Cancel through the shared context token alone - no facade cancel().
  // The engine's scheduling points must treat the cancelled token as a
  // stop request: the sink downstream of the gate never executes.
  pipeline.context().requestCancellation();
  gate->open();

  ASSERT_EQ(future.wait_for(3s), std::future_status::ready);
  EXPECT_FALSE(future.get().isOk());
  EXPECT_EQ(sink->processCount(), 0);
}

// =============================================================================
// Real run(timeout) semantics (R1.3)
// =============================================================================

TEST(PipelineTimeoutTest, TimeoutFiresWhileNodeHangs) {
  auto gate = std::make_shared<GatedPassNode>("gate");

  Graph graph;
  graph.addNode(gate);

  auto pipeline =
      Pipeline::create().withGraph(std::move(graph)).build().value();

  PortDataMap inputs;
  inputs["gate"] = makeDataPacket(1);

  // Regression (R1.3): the timeout used to be checked only after the
  // engine finished - with a hung node, run(timeout) never returned.
  const auto start = std::chrono::steady_clock::now();
  auto result = pipeline.run(inputs, 100ms);
  const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::steady_clock::now() - start);

  EXPECT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::ExecutionTimeout);
  EXPECT_LT(elapsed.count(), 2000) << "timeout did not bound the wait";
  EXPECT_TRUE(pipeline.hasError());

  // Contract: reset() releases the residue and restores IDLE
  gate->open();
  pipeline.reset();
  EXPECT_TRUE(pipeline.isReady());
}

TEST(PipelineTimeoutTest, TimeoutCancelsCooperativeNode) {
  auto node = std::make_shared<CancellationAwareNode>("aware");

  Graph graph;
  graph.addNode(node);

  auto pipeline =
      Pipeline::create().withGraph(std::move(graph)).build().value();

  PortDataMap inputs;
  inputs["aware"] = makeDataPacket(1);

  auto result = pipeline.run(inputs, 100ms);
  EXPECT_FALSE(result.isOk());
  EXPECT_EQ(result.errorCode(), ErrorCode::ExecutionTimeout);

  // The expiry must reach the node through the cooperative token (its
  // own deadline is 5s, far beyond this wait)
  EXPECT_TRUE(waitFor([&] { return node->sawCancellation(); }, 3000ms));
}

// =============================================================================
// End-to-End Observer Error Notification + reset()
// =============================================================================

TEST(PipelineErrorRecoveryTest, ObserverSeesErrorAndResetRecovers) {
  auto thrower = std::make_shared<ThrowingNode>("thrower");

  Graph graph;
  graph.addNode(thrower);

  std::atomic<int> error_count{0};
  std::atomic<ErrorCode> seen_code{ErrorCode::Ok};

  auto pipeline = Pipeline::create()
                      .withGraph(std::move(graph))
                      .onError([&](const Error &error) {
                        seen_code.store(error.code());
                        error_count.fetch_add(1);
                      })
                      .build()
                      .value();

  PortDataMap inputs;
  inputs["thrower"] = makeDataPacket(1);

  auto result = pipeline.run(inputs);
  ASSERT_FALSE(result.isOk());
  EXPECT_TRUE(pipeline.hasError());
  EXPECT_GE(error_count.load(), 1);
  EXPECT_EQ(seen_code.load(), ErrorCode::NodeException);

  // While in ERROR state further runs are rejected until reset()
  auto rejected = pipeline.run(inputs);
  ASSERT_FALSE(rejected.isOk());
  EXPECT_EQ(rejected.errorCode(), ErrorCode::InvalidState);

  pipeline.reset();
  EXPECT_EQ(pipeline.state(), PipelineState::IDLE);
  EXPECT_TRUE(pipeline.isReady());

  thrower->disarm();
  auto retry = pipeline.run(inputs);
  ASSERT_TRUE(retry.isOk()) << retry.errorMessage();
}

// =============================================================================
// Trace Sink via the Facade
// =============================================================================

TEST(PipelineTraceSinkTest, BatchRunEmitsEventsToInstalledSink) {
  auto source = std::make_shared<SourceNode>("source");
  auto sink_node = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(sink_node);
  graph.addEdge("source", "output", "sink", "input");

  auto pipeline =
      Pipeline::create().withGraph(std::move(graph)).build().value();

  auto trace_sink = std::make_shared<CountingTraceSink>();
  ASSERT_TRUE(pipeline.setTraceSink(trace_sink).isOk());

  PortDataMap inputs;
  inputs["source"] = makeDataPacket(1);
  ASSERT_TRUE(pipeline.run(inputs).isOk());

  EXPECT_GT(trace_sink->events(), 0);
}

TEST(PipelineTraceSinkTest, InstallWhileStreamingIsRejected) {
  auto source = std::make_shared<SourceNode>("source");
  auto sink_node = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(sink_node);
  graph.addEdge("source", "output", "sink", "input");

  auto pipeline = Pipeline::create()
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::STREAM)
                      .withQueueCapacity(4)
                      .build()
                      .value();

  ASSERT_TRUE(pipeline.start().isOk());
  auto result = pipeline.setTraceSink(std::make_shared<CountingTraceSink>());
  EXPECT_FALSE(result.isOk());
  pipeline.stop(false);
}

// =============================================================================
// Uninitialized Pipeline: accessors must be safe
// =============================================================================

TEST(PipelineUninitializedTest, AccessorsAreSafeWithoutEngine) {
  Pipeline pipeline;

  EXPECT_EQ(pipeline.engineState(), EngineState::ERROR);
  EXPECT_TRUE(pipeline.nodeStates().empty());
  EXPECT_EQ(pipeline.statistics().total_executions, 0u);
  EXPECT_EQ(pipeline.queueDepth("any"), 0u);
  EXPECT_FALSE(pipeline.hasQueueCapacity("any"));
  EXPECT_FALSE(pipeline.isStreaming());
  EXPECT_TRUE(pipeline.waitForDrain(0, 10ms).isOk());
  EXPECT_NE(pipeline.info().find("Not initialized"), std::string::npos);

  auto push = pipeline.pushInput("any", makeDataPacket(1));
  ASSERT_FALSE(push.isOk());
  EXPECT_EQ(push.errorCode(), ErrorCode::NotInitialized);

  auto sink_result = pipeline.setTraceSink(nullptr);
  ASSERT_FALSE(sink_result.isOk());
  EXPECT_EQ(sink_result.errorCode(), ErrorCode::NotInitialized);

  auto start_result = pipeline.start();
  ASSERT_FALSE(start_result.isOk());
  EXPECT_EQ(start_result.errorCode(), ErrorCode::NotInitialized);

  // No-ops rather than crashes
  pipeline.stop(true);
  pipeline.cancel();
  pipeline.wait();
}

// =============================================================================
// HYBRID Execution Mode (end-to-end through the facade)
//
// Existing HYBRID coverage stopped at config/strategy-creation assertions;
// these tests actually move data through a HYBRID pipeline in both its
// streaming and batch invocations.
// =============================================================================

namespace {

// Two-input join emitting one packet per aligned pair
class JoinPairNode : public ILogicNode {
public:
  explicit JoinPairNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    m_pairs.fetch_add(1);
    auto it = inputs.find("input1");
    outputs["output"] =
        (it != inputs.end() && it->second) ? it->second : makeDataPacket(0);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input1", "input2"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int pairs() const { return m_pairs.load(); }

private:
  std::atomic<int> m_pairs{0};
};

} // namespace

class PipelineHybridTest : public ::testing::Test {
protected:
  // source (no input ports) -> sink
  Pipeline buildLinearHybrid() {
    m_source = std::make_shared<SourceNode>("source");
    m_sink = std::make_shared<SinkNode>("sink");

    Graph graph;
    graph.addNode(m_source);
    graph.addNode(m_sink);
    graph.addEdge("source", "output", "sink", "input");

    auto result = Pipeline::create()
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::HYBRID)
                      .withWorkers(2)
                      .withQueueCapacity(8)
                      .build();
    EXPECT_TRUE(result) << result.errorMessage();
    return std::move(result).value();
  }

  std::shared_ptr<SourceNode> m_source;
  std::shared_ptr<SinkNode> m_sink;
};

TEST_F(PipelineHybridTest, ModeAccessorReportsHybrid) {
  auto pipeline = buildLinearHybrid();
  EXPECT_EQ(pipeline.mode(), ExecutionMode::HYBRID);
  EXPECT_TRUE(pipeline.isReady());
}

TEST_F(PipelineHybridTest, StreamingFullChainProcessesEveryFrame) {
  auto pipeline = buildLinearHybrid();

  // start() accepts HYBRID alongside STREAM
  ASSERT_TRUE(pipeline.start().isOk());
  EXPECT_TRUE(pipeline.isStreaming());

  constexpr int k_frames = 5;
  for (int i = 1; i <= k_frames; ++i) {
    auto push = pipeline.pushInput("source", makeDataPacket(i));
    ASSERT_TRUE(push.isOk()) << push.errorMessage();
  }

  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());
  EXPECT_EQ(m_sink->processCount(), k_frames);
  EXPECT_GE(pipeline.statistics().total_input_frames,
            static_cast<std::uint64_t>(k_frames));

  pipeline.stop(true);
  EXPECT_FALSE(pipeline.isStreaming());
  EXPECT_EQ(pipeline.state(), PipelineState::IDLE);
}

TEST_F(PipelineHybridTest, StreamingForkJoinAlignsFrames) {
  // source fans out to two branches that rejoin: exercises the
  // JoinAwareSyncStrategy + frame alignment path under HYBRID
  auto source = std::make_shared<SourceNode>("source");
  auto branch1 = std::make_shared<TestNode>("branch1");
  auto branch2 = std::make_shared<TestNode>("branch2");
  auto join = std::make_shared<JoinPairNode>("join");
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(source);
  graph.addNode(branch1);
  graph.addNode(branch2);
  graph.addNode(join);
  graph.addNode(sink);
  graph.addEdge("source", "output", "branch1", "input");
  graph.addEdge("source", "output", "branch2", "input");
  graph.addEdge("branch1", "output", "join", "input1");
  graph.addEdge("branch2", "output", "join", "input2");
  graph.addEdge("join", "output", "sink", "input");

  auto pipeline = Pipeline::create()
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::HYBRID)
                      .withWorkers(2)
                      .withQueueCapacity(8)
                      .build()
                      .value();

  ASSERT_TRUE(pipeline.start().isOk());

  constexpr int k_frames = 4;
  for (int i = 1; i <= k_frames; ++i) {
    ASSERT_TRUE(pipeline.pushInput("source", makeDataPacket(i)).isOk());
  }

  ASSERT_TRUE(pipeline.waitForDrain(0, 5000ms).isOk());
  pipeline.stop(true);

  EXPECT_EQ(join->pairs(), k_frames);
  EXPECT_EQ(sink->processCount(), k_frames);
}

TEST_F(PipelineHybridTest, BatchRunOverInputDrivenNodesCompletes) {
  // HYBRID reschedules nodes on success (continuous semantics), so a
  // batch run only terminates when every node is input-driven: with no
  // fresh input the reschedule stalls and the execution drains.
  // (A self-generating source node under a HYBRID batch run keeps
  // regenerating until queues overflow - that combination belongs to
  // streaming, where pushInput drives the pace.)
  auto entry = std::make_shared<TestNode>("entry");
  auto sink = std::make_shared<SinkNode>("sink");

  Graph graph;
  graph.addNode(entry);
  graph.addNode(sink);
  graph.addEdge("entry", "output", "sink", "input");

  auto pipeline = Pipeline::create()
                      .withGraph(std::move(graph))
                      .withMode(ExecutionMode::HYBRID)
                      .withWorkers(2)
                      .withQueueCapacity(8)
                      .build()
                      .value();

  PortDataMap inputs;
  inputs["entry"] = makeDataPacket(1);

  auto result = pipeline.run(inputs);
  ASSERT_TRUE(result.isOk()) << result.errorMessage();
  EXPECT_EQ(pipeline.state(), PipelineState::IDLE);
  EXPECT_EQ(sink->processCount(), 1);

  // A second run must work as well
  auto again = pipeline.run(inputs);
  ASSERT_TRUE(again.isOk()) << again.errorMessage();
  EXPECT_EQ(sink->processCount(), 2);
}
