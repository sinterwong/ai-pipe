/**
 * @file test_stream_execution_engine.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-12-24
 *
 * @copyright Copyright (c) 2025
 *
 */

#include "ai_pipe/graph.hpp"
#include "ai_pipe/i_logic_node.hpp"
#include "ai_pipe/logger.hpp"
#include "stream_execution_engine.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <gtest/gtest.h>
#include <iostream>
#include <thread>

using namespace ai_pipe;

class StreamExecutionEngineTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Initialize logger for tests
    ai_pipe::logging::LoggerConfig cfg;
    cfg.async_enabled = false;
    cfg.json_output = false;
    cfg.console_enabled = true;
    cfg.file_enabled = true;
    cfg.show_thread_id = true;
    cfg.file_path = "logs/ut_stream_execution_engine.log";
    cfg.min_level = ai_pipe::logging::LogLevel::Trace;
    ai_pipe::logging::Logger::instance().configure(cfg);
  }
};

/**
 * @brief Simple source node that receives input
 */
class TestSourceNode : public ILogicNode {
public:
  explicit TestSourceNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context) override {
    if (inputs.count("input")) {
      auto input = inputs.at("input");
      auto frame_id = input->getOptionalParam<FrameId>("frame_id");
      LOG_DEBUG_S << m_name << ": Received frame "
                  << (frame_id ? *frame_id : 0);
      outputs["output"] = input;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }
};

/**
 * @brief Slow processing node that simulates inference
 */
class SlowProcessNode : public ILogicNode {
public:
  SlowProcessNode(const std::string &name, int delay_ms)
      : ILogicNode(name), m_delayMs(delay_ms) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context) override {
    if (!inputs.count("input")) {
      return;
    }

    auto input = inputs.at("input");
    auto frame_id = input->getOptionalParam<FrameId>("frame_id");

    LOG_INFO_S << m_name << ": Processing frame " << (frame_id ? *frame_id : 0)
               << " (delay=" << m_delayMs << "ms)";

    // Simulate slow processing
    std::this_thread::sleep_for(std::chrono::milliseconds(m_delayMs));

    m_processedCount++;
    outputs["output"] = input;

    LOG_INFO_S << m_name << ": Completed frame " << (frame_id ? *frame_id : 0);
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int processedCount() const { return m_processedCount.load(); }

private:
  int m_delayMs;
  std::atomic<int> m_processedCount{0};
};

/**
 * @brief Output sink node
 */
class TestSinkNode : public ILogicNode {
public:
  explicit TestSinkNode(const std::string &name) : ILogicNode(name) {}

  void process(const PortDataMap &inputs, PortDataMap &outputs,
               std::shared_ptr<PipelineContext> context) override {
    if (inputs.count("input")) {
      auto input = inputs.at("input");
      auto frame_id = input->getOptionalParam<FrameId>("frame_id");

      m_receivedCount++;
      LOG_INFO_S << m_name << ": Output frame " << (frame_id ? *frame_id : 0)
                 << " (total: " << m_receivedCount << ")";

      outputs["output"] = input;
    }
  }

  std::vector<std::string> getExpectedInputPorts() const override {
    return {"input"};
  }

  std::vector<std::string> getExpectedOutputPorts() const override {
    return {"output"};
  }

  int receivedCount() const { return m_receivedCount.load(); }

private:
  std::atomic<int> m_receivedCount{0};
};

TEST_F(StreamExecutionEngineTest, BasicBatchExecution) {
  // Build graph
  Graph graph;
  auto source = std::make_shared<TestSourceNode>("Source");
  auto process = std::make_shared<SlowProcessNode>("Process", 50);
  auto sink = std::make_shared<TestSinkNode>("Sink");

  graph.addNode(source);
  graph.addNode(process);
  graph.addNode(sink);

  graph.addEdge("Source", "output", "Process", "input");
  graph.addEdge("Process", "output", "Sink", "input");

  // Configure engine
  StreamEngineConfig config;
  config.num_workers = 4;
  config.default_queue_config = {.capacity = 8, .drop_strategy = "DropHead"};

  auto engine = std::make_shared<StreamExecutionEngine>(config);
  ASSERT_TRUE(engine->initialize(&graph));

  // Prepare input
  PortDataMap inputs;
  auto frame = std::make_shared<PortData>();
  frame->setParam("frame_id", FrameId(1));
  inputs["Source"] = frame;

  // Execute
  bool success = engine->execute(inputs, true);
  ASSERT_TRUE(success);
  auto stats = engine->statistics();
  ASSERT_EQ(stats.total_frames_processed, 3); // Process and Sink nodes
  ASSERT_EQ(stats.total_frames_dropped, 0);
  ASSERT_EQ(sink->receivedCount(), 1);

  LOG_INFO_S << "Engine Stats: Processed=" << stats.total_frames_processed
             << ", Dropped=" << stats.total_frames_dropped
             << ", Throughput=" << stats.throughput();
}

TEST_F(StreamExecutionEngineTest, StreamingModeBasic) {
  // Build graph
  Graph graph;
  auto source = std::make_shared<TestSourceNode>("Source");
  auto process = std::make_shared<SlowProcessNode>("Process", 30);
  auto sink = std::make_shared<TestSinkNode>("Sink");

  graph.addNode(source);
  graph.addNode(process);
  graph.addNode(sink);

  graph.addEdge("Source", "output", "Process", "input");
  graph.addEdge("Process", "output", "Sink", "input");

  // Configure engine
  StreamEngineConfig config;
  config.num_workers = 4;
  config.default_queue_config = {
      .capacity = 8, .drop_strategy = "KeepLatest", .keep_latest_n = 2};

  auto engine = std::make_shared<StreamExecutionEngine>(config);
  ASSERT_TRUE(engine->initialize(&graph));
  ASSERT_TRUE(engine->start());
  ASSERT_TRUE(engine->isStreaming());

  // Push multiple inputs
  int pushed = 0;
  for (int i = 1; i <= 10; ++i) {
    auto frame = std::make_shared<PortData>();
    frame->setParam("frame_id", FrameId(i));
    auto result = engine->pushInput("Source", frame);

    if (result.accepted) {
      pushed++;
      LOG_TRACE_S << "Pushed frame " << i;
    } else {
      LOG_TRACE_S << "Frame " << i << " rejected: " << result.message;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  // Wait for drain
  LOG_TRACE_S << "Waiting for queue drain...";
  engine->waitForQueueDrain(0, std::chrono::seconds(10));

  // Stop
  engine->stop();
  auto stats = engine->statistics();
  LOG_INFO_S << "Engine Stats: Processed=" << stats.total_frames_processed
             << ", Dropped=" << stats.total_frames_dropped
             << ", Throughput=" << stats.throughput();
  ASSERT_TRUE(sink->receivedCount() != 0);
}

TEST_F(StreamExecutionEngineTest, ConcurrentMultiProducer) {
  // Build graph
  Graph graph;
  auto source = std::make_shared<TestSourceNode>("Source");
  auto process = std::make_shared<SlowProcessNode>("Process", 20);
  auto sink = std::make_shared<TestSinkNode>("Sink");

  graph.addNode(source);
  graph.addNode(process);
  graph.addNode(sink);

  graph.addEdge("Source", "output", "Process", "input");
  graph.addEdge("Process", "output", "Sink", "input");

  // Configure engine
  StreamEngineConfig config;
  config.num_workers = 4;
  config.default_queue_config = {
      .capacity = 16, .drop_strategy = "KeepLatest", .keep_latest_n = 4};

  auto engine = std::make_shared<StreamExecutionEngine>(config);
  ASSERT_TRUE(engine->initialize(&graph));
  ASSERT_TRUE(engine->start());

  // Spawn multiple producers
  constexpr int num_producers = 3;
  constexpr int frames_per_producer = 10;

  std::vector<std::thread> producers;
  std::atomic<int> total_pushed{0};
  std::atomic<int> total_rejected{0};

  for (int p = 0; p < num_producers; ++p) {
    producers.emplace_back([&, producer_id = p]() {
      for (int i = 0; i < frames_per_producer; ++i) {
        FrameId frame_id = producer_id * 1000 + i;

        auto frame = std::make_shared<PortData>();
        frame->setParam("frame_id", frame_id);
        frame->setParam("producer_id", producer_id);

        auto result = engine->pushInput("Source", frame);

        if (result.accepted) {
          total_pushed++;
        } else {
          total_rejected++;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(15));
      }
    });
  }

  // Wait for producers
  for (auto &t : producers) {
    t.join();
  }

  // Wait for drain
  LOG_TRACE_S << "All producers finished";
  LOG_TRACE_S << "Total pushed: " << total_pushed.load()
              << ", Total rejected: " << total_rejected.load();

  engine->waitForQueueDrain(0, std::chrono::seconds(15));
  engine->stop(true);

  auto stats = engine->statistics();
  LOG_INFO_S << "Engine Stats: Processed=" << stats.total_frames_processed
             << ", Dropped=" << stats.total_frames_dropped
             << ", Sync Drops=" << stats.total_sync_drops
             << ", Throughput=" << stats.throughput();

  ASSERT_GT(sink->receivedCount(), 0);
}

TEST_F(StreamExecutionEngineTest, BackpressureDrops) {
  // Build graph with very slow processor
  Graph graph;
  auto source = std::make_shared<TestSourceNode>("Source");
  auto process = std::make_shared<SlowProcessNode>("Process", 100); // Very slow
  auto sink = std::make_shared<TestSinkNode>("Sink");

  graph.addNode(source);
  graph.addNode(process);
  graph.addNode(sink);

  graph.addEdge("Source", "output", "Process", "input");
  graph.addEdge("Process", "output", "Sink", "input");

  // Small queue to force drops
  StreamEngineConfig config;
  config.num_workers = 2;
  config.default_queue_config = {
      .capacity = 4, .drop_strategy = "KeepLatest", .keep_latest_n = 1};

  auto engine = std::make_shared<StreamExecutionEngine>(config);

  // Track drops
  std::atomic<int> drop_count{0};
  engine->setDropEventCallback([&](const DropEvent &event) {
    drop_count++;
    LOG_TRACE_S << "Drop event: frame=" << event.frame_id
                << ", reason=" << event.reason;
  });

  ASSERT_TRUE(engine->initialize(&graph));
  ASSERT_TRUE(engine->start());

  // Push frames faster than processor can handle
  int pushed = 0;
  for (int i = 1; i <= 20; ++i) {
    auto frame = std::make_shared<PortData>();
    frame->setParam("frame_id", FrameId(i));

    auto result = engine->pushInput("Source", frame);
    if (result.accepted) {
      pushed++;
    }

    // Fast input (10ms) vs slow processing (100ms)
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  LOG_TRACE_S << "Pushed: " << pushed;
  LOG_TRACE_S << "Drop callbacks: " << drop_count;

  // Wait for processing
  engine->waitForQueueDrain(0, std::chrono::seconds(30));
  engine->stop(true);

  auto stats = engine->statistics();
  LOG_INFO_S << "Engine Stats: Processed=" << stats.total_frames_processed
             << ", Dropped=" << stats.total_frames_dropped
             << ", Sync Drops=" << stats.total_sync_drops
             << ", Throughput=" << stats.throughput();

  // Expect some drops due to backpressure
  ASSERT_GT(stats.total_frames_dropped, 0);
  ASSERT_EQ(drop_count.load(), stats.total_frames_dropped);
  ASSERT_GT(sink->receivedCount(), 0);      // Some frames should still pass
  ASSERT_LT(sink->receivedCount(), pushed); // Not all frames should pass
}

// bool test_execute_in_streaming() {
//   std::cout << "\n========== Test 5: Execute in Streaming Mode ==========\n";

//   Graph graph;
//   auto source = std::make_shared<TestSourceNode>("Source");
//   auto sink = std::make_shared<TestSinkNode>("Sink");

//   graph.addNode(source);
//   graph.addNode(sink);

//   graph.addEdge("Source", "output", "Sink", "input");

//   BackpressureEngineConfig config;
//   config.num_workers = 2;

//   auto engine = std::make_shared<BackpressureExecutionEngine>(config);
//   engine->initialize(&graph);

//   // Start streaming
//   engine->start();

//   // Use execute() in streaming mode - should delegate to pushInputs()
//   for (int i = 1; i <= 5; ++i) {
//     PortDataMap inputs;
//     auto frame = std::make_shared<PortData>();
//     frame->setParam("frame_id", FrameId(i));
//     inputs["Source"] = frame;

//     bool success = engine->execute(inputs, false);
//     std::cout << "execute() for frame " << i << ": "
//               << (success ? "OK" : "FAILED") << std::endl;

//     if (!success) {
//       std::cerr << "execute() should work in streaming mode" << std::endl;
//       return false;
//     }

//     std::this_thread::sleep_for(std::chrono::milliseconds(30));
//   }

//   engine->waitForQueueDrain();
//   engine->stop(true);

//   std::cout << "Sink received: " << sink->receivedCount() << std::endl;

//   if (sink->receivedCount() != 5) {
//     std::cerr << "Expected 5 frames, got " << sink->receivedCount()
//               << std::endl;
//     return false;
//   }

//   std::cout << "Test 5 PASSED\n";
//   return true;
// }

// //
// =============================================================================
// // Test 6: Queue Depth Monitoring
// //
// =============================================================================

// bool test_queue_monitoring() {
//   std::cout << "\n========== Test 6: Queue Depth Monitoring ==========\n";

//   Graph graph;
//   auto source = std::make_shared<TestSourceNode>("Source");
//   auto process = std::make_shared<SlowProcessNode>("Process", 50);
//   auto sink = std::make_shared<TestSinkNode>("Sink");

//   graph.addNode(source);
//   graph.addNode(process);
//   graph.addNode(sink);

//   graph.addEdge("Source", "output", "Process", "input");
//   graph.addEdge("Process", "output", "Sink", "input");

//   BackpressureEngineConfig config;
//   config.num_workers = 2;
//   config.default_queue_config.capacity = 8;

//   auto engine = std::make_shared<BackpressureExecutionEngine>(config);
//   engine->initialize(&graph);
//   engine->start();

//   // Push some frames
//   for (int i = 1; i <= 5; ++i) {
//     auto frame = std::make_shared<PortData>();
//     frame->setParam("frame_id", FrameId(i));
//     (void)engine->pushInput("Source", frame);
//   }

//   // Check queue depth
//   std::this_thread::sleep_for(std::chrono::milliseconds(10));

//   auto depth = engine->queueDepth("Process", "input");
//   std::cout << "Process input queue depth: " << depth << std::endl;

//   bool has_capacity = engine->hasQueueCapacity("Process", "input");
//   std::cout << "Has capacity: " << (has_capacity ? "yes" : "no") <<
//   std::endl;

//   // Wait and verify drain
//   engine->waitForQueueDrain(0, std::chrono::seconds(10));

//   depth = engine->queueDepth("Process", "input");
//   std::cout << "After drain, queue depth: " << depth << std::endl;

//   engine->stop(true);

//   if (depth != 0) {
//     std::cerr << "Queue should be empty after drain" << std::endl;
//     return false;
//   }

//   std::cout << "Test 6 PASSED\n";
//   return true;
// }
TEST_F(StreamExecutionEngineTest, ExecuteInStreamingMode) {
  // Build graph
  Graph graph;
  auto source = std::make_shared<TestSourceNode>("Source");
  auto sink = std::make_shared<TestSinkNode>("Sink");

  graph.addNode(source);
  graph.addNode(sink);

  graph.addEdge("Source", "output", "Sink", "input");

  // Configure engine
  StreamEngineConfig config;
  config.num_workers = 2;

  auto engine = std::make_shared<StreamExecutionEngine>(config);
  ASSERT_TRUE(engine->initialize(&graph));

  // Start streaming
  ASSERT_TRUE(engine->start());
  ASSERT_TRUE(engine->isStreaming());

  // Use execute() in streaming mode - should delegate to pushInputs()
  for (int i = 1; i <= 5; ++i) {
    PortDataMap inputs;
    auto frame = std::make_shared<PortData>();
    frame->setParam("frame_id", FrameId(i));
    inputs["Source"] = frame;

    bool success = engine->execute(inputs, false);
    LOG_INFO_S << "execute() for frame " << i << ": "
               << (success ? "OK" : "FAILED");

    ASSERT_TRUE(success); // execute() should return true if inputs are accepted

    std::this_thread::sleep_for(std::chrono::milliseconds(30));
  }

  // Wait for drain
  LOG_TRACE_S << "Waiting for queue drain...";
  engine->waitForQueueDrain(0, std::chrono::seconds(10));

  // Stop
  engine->stop(true);

  LOG_INFO_S << "Sink received: " << sink->receivedCount();

  ASSERT_EQ(sink->receivedCount(), 5);
}

TEST_F(StreamExecutionEngineTest, QueueMonitoring) {
  // Build graph
  Graph graph;
  auto source = std::make_shared<TestSourceNode>("Source");
  auto process = std::make_shared<SlowProcessNode>("Process", 50);
  auto sink = std::make_shared<TestSinkNode>("Sink");

  graph.addNode(source);
  graph.addNode(process);
  graph.addNode(sink);

  graph.addEdge("Source", "output", "Process", "input");
  graph.addEdge("Process", "output", "Sink", "input");

  // Configure engine
  StreamEngineConfig config;
  config.num_workers = 2;
  config.default_queue_config.capacity = 8;

  auto engine = std::make_shared<StreamExecutionEngine>(config);
  ASSERT_TRUE(engine->initialize(&graph));
  ASSERT_TRUE(engine->start());

  // Push some frames
  for (int i = 1; i <= 5; ++i) {
    auto frame = std::make_shared<PortData>();
    frame->setParam("frame_id", FrameId(i));
    (void)engine->pushInput("Source", frame);
  }

  // Check queue depth
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  auto depth = engine->queueDepth("Process", "input");
  LOG_INFO_S << "Process input queue depth: " << depth;
  ASSERT_GT(depth, 0); // Should have some frames in queue

  bool has_capacity = engine->hasQueueCapacity("Process", "input");
  LOG_INFO_S << "Has capacity: " << (has_capacity ? "yes" : "no");
  ASSERT_TRUE(has_capacity); // Queue capacity is 8, 5 frames pushed, so still
                             // has capacity

  // Wait and verify drain
  LOG_TRACE_S << "Waiting for queue drain...";
  engine->waitForQueueDrain(0, std::chrono::seconds(10));

  depth = engine->queueDepth("Process", "input");
  LOG_INFO_S << "After drain, queue depth: " << depth;
  ASSERT_EQ(depth, 0); // Queue should be empty after drain

  engine->stop(true);

  ASSERT_EQ(sink->receivedCount(), 5);
}
