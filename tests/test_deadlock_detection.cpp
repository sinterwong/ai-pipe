/**
 * @file test_deadlock_detection.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2026-01-29
 *
 * @copyright Copyright (c) 2026
 *
 */
#include "ai_pipe/context.hpp"
#include "ai_pipe/data_types.hpp"
#include "ai_pipe/execution_engine.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "join_aware_sync_strategy.hpp"
#include <atomic>
#include <chrono>
#include <future>
#include <gtest/gtest.h>
#include <thread>
#include <vector>

using namespace ai_pipe;
using namespace std::chrono_literals;

// =============================================================================
// Test Configuration
// =============================================================================
constexpr auto g_deadlock_timeout = 5s;
constexpr auto g_short_timeout = 2s;

// =============================================================================
// Test Node Implementations
// =============================================================================

class TestSourceNode : public ILogicNode {
public:
  explicit TestSourceNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto packet = std::make_shared<PortData>();
    packet->id = m_counter.fetch_add(1, std::memory_order_relaxed);
    outputs["output"] = packet;
  }

  std::vector<std::string> getExpectedInputPorts() const override { return {}; }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

private:
  std::atomic<std::uint64_t> m_counter{0};
};

class TestSinkNode : public ILogicNode {
public:
  explicit TestSinkNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &,
               std::shared_ptr<PipelineContext>) override {
    if (!inputs.empty()) {
      m_receiveCount.fetch_add(1, std::memory_order_relaxed);
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {};
  }

  std::uint64_t receiveCount() const { return m_receiveCount.load(); }

private:
  std::atomic<std::uint64_t> m_receiveCount{0};
};

class TestPassthroughNode : public ILogicNode {
public:
  explicit TestPassthroughNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto it = inputs.find("input");
    if (it != inputs.end()) {
      outputs["output"] = it->second;
    }
    m_execCount.fetch_add(1, std::memory_order_relaxed);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  std::uint64_t executionCount() const { return m_execCount.load(); }

private:
  std::atomic<std::uint64_t> m_execCount{0};
};

class TestDelayNode : public ILogicNode {
public:
  TestDelayNode(const std::string &name, std::chrono::microseconds delay)
      : ILogicNode(name), m_delay(delay) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    if (m_delay.count() > 0) {
      std::this_thread::sleep_for(m_delay);
    }
    auto it = inputs.find("input");
    if (it != inputs.end()) {
      outputs["output"] = it->second;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }
  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

private:
  std::chrono::microseconds m_delay;
};

class TestFanOutNode : public ILogicNode {
public:
  TestFanOutNode(const std::string &name, std::size_t output_count)
      : ILogicNode(name), m_outputCount(output_count) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    auto it = inputs.find("input");
    if (it != inputs.end()) {
      for (std::size_t i = 0; i < m_outputCount; ++i) {
        outputs["output_" + std::to_string(i)] = it->second;
      }
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    std::vector<std::string> ports;
    for (std::size_t i = 0; i < m_outputCount; ++i) {
      ports.push_back("output_" + std::to_string(i));
    }
    return ports;
  }

private:
  std::size_t m_outputCount;
};

class TestAggregatorNode : public ILogicNode {
public:
  TestAggregatorNode(const std::string &name, std::size_t input_count)
      : ILogicNode(name), m_inputCount(input_count) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext>) override {
    for (std::size_t i = 0; i < m_inputCount; ++i) {
      auto it = inputs.find("input_" + std::to_string(i));
      if (it != inputs.end() && it->second) {
        outputs["output"] = it->second;
        break;
      }
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    std::vector<std::string> ports;
    for (std::size_t i = 0; i < m_inputCount; ++i) {
      ports.push_back("input_" + std::to_string(i));
    }
    return ports;
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

private:
  std::size_t m_inputCount;
};

// =============================================================================
// Test Fixture
// =============================================================================

class DeadlockDetectionTest : public ::testing::Test {
protected:
  static PortDataMap createInput(std::uint64_t id = 0) {
    auto packet = std::make_shared<PortData>();
    packet->id = id;
    PortDataMap inputs;
    inputs["output"] = packet;
    return inputs;
  }

  template <typename Func>
  static bool runWithTimeout(Func &&func, std::chrono::milliseconds timeout) {
    auto future = std::async(std::launch::async, std::forward<Func>(func));
    return future.wait_for(timeout) != std::future_status::timeout;
  }

  template <typename Func>
  static void assertNoDeadlock(Func &&func, std::chrono::seconds timeout,
                               const std::string &context) {
    auto future = std::async(std::launch::async, std::forward<Func>(func));
    auto status = future.wait_for(timeout);
    ASSERT_NE(status, std::future_status::timeout)
        << "DEADLOCK DETECTED in " << context << " after " << timeout.count()
        << "s";
    future.get();
  }
};

// =============================================================================
// TEST: Basic Linear Pipeline (Sanity Check)
// =============================================================================

TEST_F(DeadlockDetectionTest, LinearPipeline_Basic) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "initialize");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   g_deadlock_timeout, "execute");
}

TEST_F(DeadlockDetectionTest, LinearPipeline_DeepChain) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  graph.addNode(source);

  std::string prev = "source";
  constexpr int depth = 20;

  for (int i = 0; i < depth; ++i) {
    std::string name = "node_" + std::to_string(i);
    auto node = std::make_shared<TestPassthroughNode>(name);
    graph.addNode(node);
    graph.addEdge(prev, "output", name, "input");
    prev = name;
  }

  auto sink = std::make_shared<TestSinkNode>("sink");
  graph.addNode(sink);
  graph.addEdge(prev, "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "initialize deep chain");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   g_deadlock_timeout, "execute deep chain");
}

// =============================================================================
// TEST: Fork-Join Topologies (High Deadlock Risk)
// =============================================================================

TEST_F(DeadlockDetectionTest, Diamond_SimpleForkJoin) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto fork = std::make_shared<TestFanOutNode>("fork", 2);
  auto branch1 = std::make_shared<TestPassthroughNode>("branch1");
  auto branch2 = std::make_shared<TestPassthroughNode>("branch2");
  auto join = std::make_shared<TestAggregatorNode>("join", 2);
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(fork);
  graph.addNode(branch1);
  graph.addNode(branch2);
  graph.addNode(join);
  graph.addNode(sink);

  graph.addEdge("source", "output", "fork", "input");
  graph.addEdge("fork", "output_0", "branch1", "input");
  graph.addEdge("fork", "output_1", "branch2", "input");
  graph.addEdge("branch1", "output", "join", "input_0");
  graph.addEdge("branch2", "output", "join", "input_1");
  graph.addEdge("join", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "diamond initialize");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   g_deadlock_timeout, "diamond execute");
}

TEST_F(DeadlockDetectionTest, Diamond_ManyBranches) {
  constexpr int branch_count = 8;
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto fork = std::make_shared<TestFanOutNode>("fork", branch_count);
  auto join = std::make_shared<TestAggregatorNode>("join", branch_count);
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(fork);
  graph.addNode(join);
  graph.addNode(sink);

  graph.addEdge("source", "output", "fork", "input");

  for (int i = 0; i < branch_count; ++i) {
    std::string name = "branch_" + std::to_string(i);
    auto branch = std::make_shared<TestPassthroughNode>(name);
    graph.addNode(branch);
    graph.addEdge("fork", "output_" + std::to_string(i), name, "input");
    graph.addEdge(name, "output", "join", "input_" + std::to_string(i));
  }

  graph.addEdge("join", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "many branches initialize");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   g_deadlock_timeout, "many branches execute");
}

TEST_F(DeadlockDetectionTest, Diamond_UnbalancedDelays) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto fork = std::make_shared<TestFanOutNode>("fork", 4);

  // Very different delays - potential race condition trigger
  auto fast1 = std::make_shared<TestDelayNode>("fast1", 1ms);
  auto fast2 = std::make_shared<TestDelayNode>("fast2", 1ms);
  auto slow1 = std::make_shared<TestDelayNode>("slow1", 100ms);
  auto slow2 = std::make_shared<TestDelayNode>("slow2", 100ms);

  auto join = std::make_shared<TestAggregatorNode>("join", 4);
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(fork);
  graph.addNode(fast1);
  graph.addNode(fast2);
  graph.addNode(slow1);
  graph.addNode(slow2);
  graph.addNode(join);
  graph.addNode(sink);

  graph.addEdge("source", "output", "fork", "input");
  graph.addEdge("fork", "output_0", "fast1", "input");
  graph.addEdge("fork", "output_1", "fast2", "input");
  graph.addEdge("fork", "output_2", "slow1", "input");
  graph.addEdge("fork", "output_3", "slow2", "input");
  graph.addEdge("fast1", "output", "join", "input_0");
  graph.addEdge("fast2", "output", "join", "input_1");
  graph.addEdge("slow1", "output", "join", "input_2");
  graph.addEdge("slow2", "output", "join", "input_3");
  graph.addEdge("join", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "unbalanced delays initialize");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   g_deadlock_timeout, "unbalanced delays execute");
}

// =============================================================================
// TEST: Thread Pool Exhaustion (Critical Deadlock Scenario)
// =============================================================================

TEST_F(DeadlockDetectionTest, ThreadPool_SingleWorkerForkJoin) {
  // CRITICAL: Single worker with fork-join can deadlock if not handled properly
  constexpr int branches = 4;

  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto fork = std::make_shared<TestFanOutNode>("fork", branches);
  auto join = std::make_shared<TestAggregatorNode>("join", branches);
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(fork);
  graph.addNode(join);
  graph.addNode(sink);

  graph.addEdge("source", "output", "fork", "input");

  for (int i = 0; i < branches; ++i) {
    std::string name = "branch_" + std::to_string(i);
    auto branch = std::make_shared<TestPassthroughNode>(name);
    graph.addNode(branch);
    graph.addEdge("fork", "output_" + std::to_string(i), name, "input");
    graph.addEdge(name, "output", "join", "input_" + std::to_string(i));
  }

  graph.addEdge("join", "output", "sink", "input");

  // Single worker - high deadlock risk
  auto engine = ExecutionEngine::create(EngineConfig::batch(1));
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());

  assertNoDeadlock([&]() { return engine->initialize(&graph, 1); },
                   g_deadlock_timeout, "single worker fork-join initialize");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   g_deadlock_timeout, "single worker fork-join execute");
}

TEST_F(DeadlockDetectionTest, ThreadPool_MoreNodesThanWorkers) {
  constexpr int node_count = 32;
  constexpr int worker_count = 4;

  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  graph.addNode(source);

  std::string prev = "source";
  for (int i = 0; i < node_count; ++i) {
    std::string name = "node_" + std::to_string(i);
    auto node =
        std::make_shared<TestDelayNode>(name, std::chrono::microseconds(100));
    graph.addNode(node);
    graph.addEdge(prev, "output", name, "input");
    prev = name;
  }

  auto sink = std::make_shared<TestSinkNode>("sink");
  graph.addNode(sink);
  graph.addEdge(prev, "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(worker_count));

  assertNoDeadlock([&]() { return engine->initialize(&graph, worker_count); },
                   g_deadlock_timeout, "more nodes than workers initialize");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   std::chrono::seconds(10), "more nodes than workers execute");
}

// =============================================================================
// TEST: Repeated Executions
// =============================================================================

TEST_F(DeadlockDetectionTest, RepeatedExecution_Basic) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto pass = std::make_shared<TestPassthroughNode>("pass");
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(pass);
  graph.addNode(sink);

  graph.addEdge("source", "output", "pass", "input");
  graph.addEdge("pass", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "repeated init");

  constexpr int iterations = 100;
  for (int i = 0; i < iterations; ++i) {
    bool success =
        runWithTimeout([&]() { return engine->execute(createInput(i)); },
                       std::chrono::milliseconds(500));

    ASSERT_TRUE(success) << "DEADLOCK on iteration " << i;
  }
}

TEST_F(DeadlockDetectionTest, RepeatedExecution_ForkJoin) {
  constexpr int branches = 4;
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto fork = std::make_shared<TestFanOutNode>("fork", branches);
  auto join = std::make_shared<TestAggregatorNode>("join", branches);
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(fork);
  graph.addNode(join);
  graph.addNode(sink);

  graph.addEdge("source", "output", "fork", "input");

  for (int i = 0; i < branches; ++i) {
    std::string name = "branch_" + std::to_string(i);
    auto branch = std::make_shared<TestPassthroughNode>(name);
    graph.addNode(branch);
    graph.addEdge("fork", "output_" + std::to_string(i), name, "input");
    graph.addEdge(name, "output", "join", "input_" + std::to_string(i));
  }

  graph.addEdge("join", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "repeated fork-join init");

  constexpr int iterations = 50;
  for (int i = 0; i < iterations; ++i) {
    bool success =
        runWithTimeout([&]() { return engine->execute(createInput(i)); },
                       std::chrono::milliseconds(1000));

    ASSERT_TRUE(success) << "DEADLOCK on fork-join iteration " << i;
  }
}

// =============================================================================
// TEST: Streaming Mode
// =============================================================================

TEST_F(DeadlockDetectionTest, Streaming_BasicFlow) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::stream(4, 16));

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "streaming init");
  assertNoDeadlock([&]() { return engine->startStreaming(); },
                   g_deadlock_timeout, "start streaming");

  constexpr int frames = 50;
  for (int i = 0; i < frames; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    auto result = engine->pushInput("source", "output", packet);
    EXPECT_TRUE(result.isOk()) << "Frame " << i << " was rejected";
  }

  assertNoDeadlock(
      [&]() {
        engine->stopStreaming(true);
        return true;
      },
      g_deadlock_timeout, "stop streaming");
}

TEST_F(DeadlockDetectionTest, Streaming_RapidStartStop) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto pass = std::make_shared<TestPassthroughNode>("pass");
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(pass);
  graph.addNode(sink);

  graph.addEdge("source", "output", "pass", "input");
  graph.addEdge("pass", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::stream(4, 16));

  assertNoDeadlock([&]() { return engine->initialize(&graph, 4); },
                   g_deadlock_timeout, "rapid start/stop init");

  constexpr int cycles = 20;
  for (int c = 0; c < cycles; ++c) {
    bool start_ok = runWithTimeout([&]() { return engine->startStreaming(); },
                                   std::chrono::milliseconds(500));
    ASSERT_TRUE(start_ok) << "DEADLOCK on start, cycle " << c;

    // Push a few frames
    for (int i = 0; i < 5; ++i) {
      auto packet = std::make_shared<PortData>();
      packet->id = c * 5 + i;
      (void)engine->pushInput("source", "output", packet);
    }

    bool stop_ok = runWithTimeout(
        [&]() {
          engine->stopStreaming(true);
          return true;
        },
        std::chrono::milliseconds(2000));
    ASSERT_TRUE(stop_ok) << "DEADLOCK on stop, cycle " << c;
  }
}

TEST_F(DeadlockDetectionTest, Streaming_ConcurrentPushAndStop) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto delay = std::make_shared<TestDelayNode>("delay", 10ms);
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(delay);
  graph.addNode(sink);

  graph.addEdge("source", "output", "delay", "input");
  graph.addEdge("delay", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::stream(4, 32));

  ASSERT_TRUE(engine->initialize(&graph, 4));
  ASSERT_TRUE(engine->startStreaming());

  std::atomic<bool> stop_flag{false};
  std::atomic<int> pushed_count{0};

  // Producer thread
  std::thread producer([&]() {
    while (!stop_flag.load(std::memory_order_acquire)) {
      auto packet = std::make_shared<PortData>();
      packet->id = pushed_count.load();
      auto result = engine->pushInput("source", "output", packet);
      if (result.isOk()) {
        pushed_count.fetch_add(1, std::memory_order_relaxed);
      }
      std::this_thread::sleep_for(1ms);
    }
  });

  // Let it run for a bit
  std::this_thread::sleep_for(100ms);

  // Stop from main thread while producer is still running
  stop_flag.store(true, std::memory_order_release);

  bool stop_ok = runWithTimeout(
      [&]() {
        engine->stopStreaming(true);
        return true;
      },
      g_deadlock_timeout);

  producer.join();

  ASSERT_TRUE(stop_ok)
      << "DEADLOCK: stopStreaming() blocked while producer was active";
  EXPECT_GT(pushed_count.load(), 0) << "No frames were pushed";
}

// =============================================================================
// TEST: Error Recovery
// =============================================================================

TEST_F(DeadlockDetectionTest, ErrorRecovery_StopDuringExecution) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto slow = std::make_shared<TestDelayNode>("slow", 500ms);
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(slow);
  graph.addNode(sink);

  graph.addEdge("source", "output", "slow", "input");
  graph.addEdge("slow", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));
  ASSERT_TRUE(engine->initialize(&graph, 4));

  // Start execution without waiting
  auto exec_future = std::async(std::launch::async, [&]() {
    return engine->execute(createInput(), true);
  });

  // Wait a bit then stop
  std::this_thread::sleep_for(50ms);

  assertNoDeadlock(
      [&]() {
        engine->stopExecutionSync();
        return true;
      },
      g_deadlock_timeout, "stop during execution");

  // The execute() should complete
  ASSERT_EQ(exec_future.wait_for(g_deadlock_timeout), std::future_status::ready)
      << "DEADLOCK: execute() did not return after stopExecutionSync()";
}

TEST_F(DeadlockDetectionTest, ErrorRecovery_ResetDuringStreaming) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::stream(4, 16));

  ASSERT_TRUE(engine->initialize(&graph, 4));
  ASSERT_TRUE(engine->startStreaming());

  // Push some frames
  for (int i = 0; i < 10; ++i) {
    auto packet = std::make_shared<PortData>();
    packet->id = i;
    (void)engine->pushInput("source", "output", packet);
  }

  // Reset while streaming
  assertNoDeadlock(
      [&]() {
        engine->reset();
        return true;
      },
      g_deadlock_timeout, "reset during streaming");

  // Should be able to start again
  assertNoDeadlock([&]() { return engine->startStreaming(); },
                   g_deadlock_timeout, "restart after reset");
  assertNoDeadlock(
      [&]() {
        engine->stopStreaming(true);
        return true;
      },
      g_deadlock_timeout, "stop after restart");
}

// =============================================================================
// TEST: Nested Fork-Join
// =============================================================================

TEST_F(DeadlockDetectionTest, NestedForkJoin_TwoLevels) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  graph.addNode(source);

  // Level 1: fork into 2 branches
  auto fork1 = std::make_shared<TestFanOutNode>("fork1", 2);
  graph.addNode(fork1);
  graph.addEdge("source", "output", "fork1", "input");

  // Each level 1 branch has another fork-join
  for (int b = 0; b < 2; ++b) {
    std::string prefix = "l1_b" + std::to_string(b) + "_";

    auto inner_fork = std::make_shared<TestFanOutNode>(prefix + "fork", 2);
    graph.addNode(inner_fork);
    graph.addEdge("fork1", "output_" + std::to_string(b), prefix + "fork",
                  "input");

    auto inner_b0 = std::make_shared<TestPassthroughNode>(prefix + "inner0");
    auto inner_b1 = std::make_shared<TestPassthroughNode>(prefix + "inner1");
    graph.addNode(inner_b0);
    graph.addNode(inner_b1);
    graph.addEdge(prefix + "fork", "output_0", prefix + "inner0", "input");
    graph.addEdge(prefix + "fork", "output_1", prefix + "inner1", "input");

    auto inner_join = std::make_shared<TestAggregatorNode>(prefix + "join", 2);
    graph.addNode(inner_join);
    graph.addEdge(prefix + "inner0", "output", prefix + "join", "input_0");
    graph.addEdge(prefix + "inner1", "output", prefix + "join", "input_1");
  }

  // Level 1 join
  auto join1 = std::make_shared<TestAggregatorNode>("join1", 2);
  graph.addNode(join1);
  graph.addEdge("l1_b0_join", "output", "join1", "input_0");
  graph.addEdge("l1_b1_join", "output", "join1", "input_1");

  auto sink = std::make_shared<TestSinkNode>("sink");
  graph.addNode(sink);
  graph.addEdge("join1", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(8));
  engine->setSyncStrategy(std::make_unique<JoinAwareSyncStrategy>());

  assertNoDeadlock([&]() { return engine->initialize(&graph, 8); },
                   g_deadlock_timeout, "nested fork-join initialize");
  assertNoDeadlock([&]() { return engine->execute(createInput()); },
                   g_deadlock_timeout, "nested fork-join execute");
}

// =============================================================================
// TEST: Stress Tests
// =============================================================================

TEST_F(DeadlockDetectionTest, Stress_HighFrequencyExecution) {
  Graph graph;

  auto source = std::make_shared<TestSourceNode>("source");
  auto sink = std::make_shared<TestSinkNode>("sink");

  graph.addNode(source);
  graph.addNode(sink);
  graph.addEdge("source", "output", "sink", "input");

  auto engine = ExecutionEngine::create(EngineConfig::batch(4));
  ASSERT_TRUE(engine->initialize(&graph, 4));

  auto start = std::chrono::steady_clock::now();
  constexpr int max_iterations = 500;
  constexpr auto max_duration = 5s;

  int iterations = 0;
  while (iterations < max_iterations) {
    auto now = std::chrono::steady_clock::now();
    if (now - start > max_duration)
      break;

    bool success = runWithTimeout(
        [&]() { return engine->execute(createInput(iterations)); },
        std::chrono::milliseconds(100));

    ASSERT_TRUE(success) << "DEADLOCK on stress iteration " << iterations;
    ++iterations;
  }

  EXPECT_GT(iterations, 100) << "Too few iterations completed";
}

TEST_F(DeadlockDetectionTest, Stress_ConcurrentEngines) {
  constexpr int engine_count = 4;
  std::atomic<int> successful{0};
  std::atomic<bool> deadlock_detected{false};

  auto run_engine = [&](int id) {
    Graph graph;

    auto source = std::make_shared<TestSourceNode>("source");
    auto pass = std::make_shared<TestPassthroughNode>("pass");
    auto sink = std::make_shared<TestSinkNode>("sink");

    graph.addNode(source);
    graph.addNode(pass);
    graph.addNode(sink);

    graph.addEdge("source", "output", "pass", "input");
    graph.addEdge("pass", "output", "sink", "input");

    auto engine = ExecutionEngine::create(EngineConfig::batch(2));

    if (!runWithTimeout([&]() { return engine->initialize(&graph, 2); },
                        g_short_timeout)) {
      deadlock_detected.store(true);
      return;
    }

    for (int i = 0; i < 50 && !deadlock_detected.load(); ++i) {
      if (!runWithTimeout(
              [&]() { return engine->execute(createInput(id * 50 + i)); },
              g_short_timeout)) {
        deadlock_detected.store(true);
        return;
      }
    }

    successful.fetch_add(1);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < engine_count; ++i) {
    threads.emplace_back(run_engine, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  EXPECT_FALSE(deadlock_detected.load()) << "DEADLOCK in concurrent engines";
  EXPECT_EQ(successful.load(), engine_count) << "Not all engines completed";
}
