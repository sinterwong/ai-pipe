/**
 * @file through_pass_pipe_test.cpp
 * @brief Unit tests for Pipeline with new fluent API
 */

#include "ai_pipe/context.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/pipeline.hpp"
#include "nodes/through_pass_node.hpp"
#include "pipeline_config_builder.hpp"
#include <atomic>
#include <filesystem>
#include <gtest/gtest.h>
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace testing_algo_infer_pipeline {

namespace fs = std::filesystem;

class ThroughPassPipeTest : public ::testing::Test {
protected:
  void SetUp() override {
    Logger::LogConfig log_config;
    log_config.appName = "Test-Unit";
    log_config.logPath = "./logs";
    log_config.logLevel = LogLevel::INFO;
    log_config.enableConsole = true;
    log_config.enableColor = true;
    Logger::instance()->initialize(log_config);
  }

  void TearDown() override {}

  fs::path resourceDir = fs::path("assets");
  fs::path confDir = resourceDir / "conf";

  // Helper to create a standard test graph
  ai_pipe::Graph createTestGraph() {
    ai_pipe::Graph graph;

    auto node1 = std::make_shared<ai_pipe::examples::ThroughPassNode>(
        "ThroughPass01", ai_pipe::examples::ThroughPassNodeParams{});
    auto node2 = std::make_shared<ai_pipe::examples::ThroughPassNode>(
        "ThroughPass02", ai_pipe::examples::ThroughPassNodeParams{});
    auto node3 = std::make_shared<ai_pipe::examples::ThroughPassNode>(
        "ThroughPass03", ai_pipe::examples::ThroughPassNodeParams{});
    auto node4 = std::make_shared<ai_pipe::examples::ThroughPassNode>(
        "ThroughPass04", ai_pipe::examples::ThroughPassNodeParams{});
    auto node5 = std::make_shared<ai_pipe::examples::ThroughPassNode>(
        "ThroughPass05", ai_pipe::examples::ThroughPassNodeParams{});

    graph.addNode(node1);
    graph.addNode(node2);
    graph.addNode(node3);
    graph.addNode(node4);
    graph.addNode(node5);

    graph.addEdge("ThroughPass01", "output", "ThroughPass02", "input");
    graph.addEdge("ThroughPass02", "output", "ThroughPass03", "input");
    graph.addEdge("ThroughPass03", "output", "ThroughPass04", "input");
    graph.addEdge("ThroughPass03", "output", "ThroughPass05", "input");

    return graph;
  }

  // Helper to create standard test inputs
  ai_pipe::PortDataMap createTestInputs() {
    ai_pipe::PortDataMap inputs;
    auto image_path_data = std::make_shared<ai_pipe::PortData>();
    image_path_data->setParam<std::string>("input", "");
    inputs["ThroughPass01"] = image_path_data;
    return inputs;
  }
};

// =============================================================================
// Test: Synchronous Execution with run()
// =============================================================================

TEST_F(ThroughPassPipeTest, SynchronousExecution) {
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  std::atomic<bool> result_received{false};
  std::atomic<bool> error_occurred{false};
  std::string last_error_msg;

  // Build pipeline with fluent API
  auto pipeline =
      ai_pipe::Pipeline::create()
          .withGraph(createTestGraph())
          .withContext(context)
          .withWorkers(4)
          .onResult([&](const ai_pipe::PortDataMap &final_results) {
            result_received = true;
            EXPECT_TRUE(final_results.count("ThroughPass04:output"));
            EXPECT_TRUE(final_results.count("ThroughPass05:output"));
          })
          .onError(
              [&](const std::string &error_msg, const std::string &node_name) {
                error_occurred = true;
                last_error_msg =
                    "Pipeline error in node '" + node_name + "': " + error_msg;
                std::cerr << last_error_msg << std::endl;
              })
          .build();

  // Pipeline should be ready immediately after build
  ASSERT_TRUE(pipeline.isReady());
  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);

  // Execute synchronously
  auto result = pipeline.run(createTestInputs());

  // Verify execution result
  ASSERT_TRUE(result) << "Pipeline execution failed: " << result.error_message;
  EXPECT_TRUE(result.success);
  EXPECT_TRUE(result.outputs.count("ThroughPass04:output"));
  EXPECT_TRUE(result.outputs.count("ThroughPass05:output"));
  EXPECT_GT(result.elapsed.count(), 0);

  // Verify callbacks were invoked correctly
  EXPECT_TRUE(result_received) << "Pipeline result callback was not invoked.";
  EXPECT_FALSE(error_occurred)
      << "Pipeline error callback was invoked: " << last_error_msg;

  // Pipeline should return to IDLE after completion
  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);

  // Reset and verify
  pipeline.reset();
  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);
}

// =============================================================================
// Test: Asynchronous Execution with runAsync()
// =============================================================================

TEST_F(ThroughPassPipeTest, AsynchronousExecution) {
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  std::atomic<bool> result_received{false};
  std::atomic<bool> error_occurred{false};
  std::string last_error_msg;

  auto pipeline =
      ai_pipe::Pipeline::create()
          .withGraph(createTestGraph())
          .withContext(context)
          .withWorkers(4)
          .onResult([&](const ai_pipe::PortDataMap &final_results) {
            result_received = true;
            EXPECT_TRUE(final_results.count("ThroughPass04:output"));
            EXPECT_TRUE(final_results.count("ThroughPass05:output"));
          })
          .onError(
              [&](const std::string &error_msg, const std::string &node_name) {
                error_occurred = true;
                last_error_msg =
                    "Pipeline error in node '" + node_name + "': " + error_msg;
                std::cerr << last_error_msg << std::endl;
              })
          .build();

  ASSERT_TRUE(pipeline.isReady());

  // Execute asynchronously
  auto future = pipeline.runAsync(createTestInputs());

  // Wait with timeout
  std::future_status status = future.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready)
      << "Pipeline execution timed out.";

  // Get result
  auto result = future.get();
  ASSERT_TRUE(result) << "Pipeline execution failed: " << result.error_message;
  EXPECT_TRUE(result.success);
  EXPECT_TRUE(result.outputs.count("ThroughPass04:output"));
  EXPECT_TRUE(result.outputs.count("ThroughPass05:output"));

  EXPECT_TRUE(result_received) << "Pipeline result callback was not invoked.";
  EXPECT_FALSE(error_occurred)
      << "Pipeline error callback was invoked: " << last_error_msg;

  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);
}

// =============================================================================
// Test: Fire-and-Forget with submit()
// =============================================================================

TEST_F(ThroughPassPipeTest, FireAndForgetExecution) {
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  std::atomic<bool> result_received{false};
  std::condition_variable cv;
  std::mutex cv_mutex;

  auto pipeline =
      ai_pipe::Pipeline::create()
          .withGraph(createTestGraph())
          .withContext(context)
          .withWorkers(4)
          .onResult([&](const ai_pipe::PortDataMap &final_results) {
            result_received = true;
            EXPECT_TRUE(final_results.count("ThroughPass04:output"));
            EXPECT_TRUE(final_results.count("ThroughPass05:output"));
            cv.notify_one();
          })
          .build();

  ASSERT_TRUE(pipeline.isReady());

  // Submit and forget
  bool submitted = pipeline.submit(createTestInputs());
  ASSERT_TRUE(submitted);

  // Wait for completion via callback
  {
    std::unique_lock<std::mutex> lock(cv_mutex);
    bool completed = cv.wait_for(lock, std::chrono::seconds(10),
                                 [&] { return result_received.load(); });
    ASSERT_TRUE(completed) << "Pipeline execution timed out.";
  }

  // Or just use wait()
  pipeline.wait();

  EXPECT_TRUE(result_received);
  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);
}

// =============================================================================
// Test: Execution with Timeout
// =============================================================================

TEST_F(ThroughPassPipeTest, ExecutionWithTimeout) {
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(createTestGraph())
                      .withContext(context)
                      .withWorkers(4)
                      .build();

  // Execute with generous timeout (should succeed)
  auto result = pipeline.run(createTestInputs(), std::chrono::seconds(30));

  ASSERT_TRUE(result) << "Pipeline execution failed: " << result.error_message;
  EXPECT_TRUE(result.success);
}

// =============================================================================
// Test: Build from JSON Configuration using PipelineConfigBuilder
// =============================================================================

TEST_F(ThroughPassPipeTest, BuildFromJsonConfig) {
  const std::string graph_config_path = confDir / "through_pass_pipe.json";
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  // Build pipeline from JSON config using PipelineConfigBuilder
  auto pipeline = ai_pipe::examples::PipelineConfigBuilder::buildPipeline(
      graph_config_path, context, 4);

  ASSERT_TRUE(pipeline.isReady());
  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);

  // Add observers after construction
  std::atomic<bool> result_received{false};
  std::atomic<bool> error_occurred{false};
  std::string last_error_msg;

  auto observer = std::make_shared<ai_pipe::CallbackObserver>();
  observer->onResult([&](const ai_pipe::PortDataMap &final_results) {
    result_received = true;
    EXPECT_TRUE(final_results.count("ThroughPass04:output"));
    EXPECT_TRUE(final_results.count("ThroughPass05:output"));
  });
  observer->onError([&](const std::string &error_msg,
                        const std::string &node_name) {
    error_occurred = true;
    last_error_msg = "Pipeline error in node '" + node_name + "': " + error_msg;
    std::cerr << last_error_msg << std::endl;
  });
  pipeline.addObserver(observer);

  // Execute synchronously
  auto result = pipeline.run(createTestInputs());

  ASSERT_TRUE(result) << "Pipeline execution failed: " << result.error_message;

  EXPECT_TRUE(result_received) << "Pipeline result callback was not invoked.";
  EXPECT_FALSE(error_occurred)
      << "Pipeline error callback was invoked: " << last_error_msg;

  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);

  pipeline.reset();
  ASSERT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);
}

// =============================================================================
// Test: Build from JSON with Custom Options
// =============================================================================

TEST_F(ThroughPassPipeTest, BuildFromJsonConfigWithOptions) {
  const std::string graph_config_path = confDir / "through_pass_pipe.json";
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  // Custom options override config file
  ai_pipe::PipelineOptions options{
      .engine_type = "DefaultExecutionEngine",
      .num_workers = 8,
      .execution_timeout = std::chrono::seconds{60},
      .auto_reset_on_error = false,
  };

  auto pipeline = ai_pipe::examples::PipelineConfigBuilder::buildPipeline(
      graph_config_path, context, options);

  ASSERT_TRUE(pipeline.isReady());

  auto result = pipeline.run(createTestInputs());
  ASSERT_TRUE(result) << result.error_message;
}

// =============================================================================
// Test: tryBuildPipeline returns nullopt on invalid config
// =============================================================================

TEST_F(ThroughPassPipeTest, TryBuildPipelineInvalidConfig) {
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  // Try with non-existent file
  auto result = ai_pipe::examples::PipelineConfigBuilder::tryBuildPipeline(
      "non_existent_config.json", context, 4);

  EXPECT_FALSE(result.has_value()) << "Should fail with non-existent config";
}

// =============================================================================
// Test: Using Custom Observer
// =============================================================================

class TestPipelineObserver : public ai_pipe::IPipelineObserver {
public:
  std::atomic<bool> started{false};
  std::atomic<bool> completed{false};
  std::atomic<bool> failed{false};
  ai_pipe::PortDataMap last_results;
  std::string last_error;
  std::string last_error_node;
  std::mutex results_mutex;

  void onExecutionStarted() override { started = true; }

  void onExecutionCompleted(const ai_pipe::PortDataMap &results) override {
    completed = true;
    std::lock_guard<std::mutex> lock(results_mutex);
    last_results = results;
  }

  void onExecutionFailed(const std::string &error,
                         const std::string &node_name) override {
    failed = true;
    last_error = error;
    last_error_node = node_name;
  }
};

TEST_F(ThroughPassPipeTest, CustomObserver) {
  auto context = std::make_shared<ai_pipe::PipelineContext>();
  auto observer = std::make_shared<TestPipelineObserver>();

  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(createTestGraph())
                      .withContext(context)
                      .withWorkers(4)
                      .withObserver(observer)
                      .build();

  auto result = pipeline.run(createTestInputs());

  ASSERT_TRUE(result);
  EXPECT_TRUE(observer->started);
  EXPECT_TRUE(observer->completed);
  EXPECT_FALSE(observer->failed);

  {
    std::lock_guard<std::mutex> lock(observer->results_mutex);
    EXPECT_TRUE(observer->last_results.count("ThroughPass04:output"));
    EXPECT_TRUE(observer->last_results.count("ThroughPass05:output"));
  }
}

// =============================================================================
// Test: Using makePipeline() Factory Function
// =============================================================================

TEST_F(ThroughPassPipeTest, FactoryFunction) {
  auto pipeline = ai_pipe::makePipeline(createTestGraph(), 4);

  ASSERT_TRUE(pipeline.isReady());

  auto result = pipeline.run(createTestInputs());
  ASSERT_TRUE(result) << result.error_message;
}

// =============================================================================
// Test: Multiple Executions
// =============================================================================

TEST_F(ThroughPassPipeTest, MultipleExecutions) {
  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(createTestGraph())
                      .withWorkers(4)
                      .build();

  // Run multiple times
  for (int i = 0; i < 3; ++i) {
    auto result = pipeline.run(createTestInputs());
    ASSERT_TRUE(result) << "Execution " << i
                        << " failed: " << result.error_message;
    EXPECT_TRUE(result.outputs.count("ThroughPass04:output"));
    EXPECT_TRUE(result.outputs.count("ThroughPass05:output"));
  }
}

// =============================================================================
// Test: Cancel Execution
// =============================================================================

TEST_F(ThroughPassPipeTest, CancelExecution) {
  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(createTestGraph())
                      .withWorkers(4)
                      .build();

  // Submit async
  bool submitted = pipeline.submit(createTestInputs());
  ASSERT_TRUE(submitted);

  // Cancel immediately
  pipeline.cancel();
  pipeline.wait();

  // Pipeline should be back to usable state
  EXPECT_FALSE(pipeline.isRunning());
}

// =============================================================================
// Test: State Queries
// =============================================================================

TEST_F(ThroughPassPipeTest, StateQueries) {
  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(createTestGraph())
                      .withWorkers(4)
                      .build();

  // After build: ready, not running, no error
  EXPECT_TRUE(pipeline.isReady());
  EXPECT_FALSE(pipeline.isRunning());
  EXPECT_FALSE(pipeline.hasError());
  EXPECT_EQ(pipeline.state(), ai_pipe::PipelineState::IDLE);

  // Engine state should also be IDLE
  EXPECT_EQ(pipeline.engineState(), ai_pipe::EngineState::IDLE);

  // Node states should exist
  auto node_states = pipeline.nodeStates();
  EXPECT_FALSE(node_states.empty());
}

// =============================================================================
// Test: Pipeline::create().tryBuild() without Graph
// =============================================================================

TEST_F(ThroughPassPipeTest, TryBuildWithoutGraph) {
  // Try to build without providing a graph
  auto result = ai_pipe::Pipeline::create().withWorkers(4).tryBuild();

  EXPECT_FALSE(result.has_value()) << "Should fail without graph";
}

// =============================================================================
// Test: Access Graph and Context
// =============================================================================

TEST_F(ThroughPassPipeTest, AccessGraphAndContext) {
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(createTestGraph())
                      .withContext(context)
                      .withWorkers(4)
                      .build();

  // Access graph
  const auto &graph = pipeline.graph();
  EXPECT_FALSE(graph.getNodes().empty());

  // Access context
  auto &ctx = pipeline.context();
  // Context should be the same instance we provided
  EXPECT_EQ(&ctx, context.get());
}

// =============================================================================
// Test: Build Graph Only using PipelineConfigBuilder
// =============================================================================

TEST_F(ThroughPassPipeTest, BuildGraphFromConfig) {
  const std::string graph_config_path = confDir / "through_pass_pipe.json";

  auto graph =
      ai_pipe::examples::PipelineConfigBuilder::buildGraph(graph_config_path);

  EXPECT_FALSE(graph.getNodes().empty());
  EXPECT_FALSE(graph.hasCycle());

  // Can use the graph with Pipeline builder
  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(std::move(graph))
                      .withWorkers(4)
                      .build();

  ASSERT_TRUE(pipeline.isReady());
}

// =============================================================================
// Test: Build Graph and Options using PipelineConfigBuilder
// =============================================================================

TEST_F(ThroughPassPipeTest, BuildGraphAndOptionsFromConfig) {
  const std::string graph_config_path = confDir / "through_pass_pipe.json";

  auto [graph, options] =
      ai_pipe::examples::PipelineConfigBuilder::buildGraphAndOptions(
          graph_config_path);

  EXPECT_FALSE(graph.getNodes().empty());

  // Use extracted graph and options
  auto pipeline = ai_pipe::Pipeline::create()
                      .withGraph(std::move(graph))
                      .withOptions(options)
                      .build();

  ASSERT_TRUE(pipeline.isReady());

  auto result = pipeline.run(createTestInputs());
  ASSERT_TRUE(result) << result.error_message;
}

} // namespace testing_algo_infer_pipeline