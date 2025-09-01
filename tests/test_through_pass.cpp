#include "ai_pipe/context.hpp"
#include "ai_pipe/graph.hpp"
#include "ai_pipe/pipeline.hpp"
#include "nodes/through_pass_node.hpp"
#include "pipeline_builder.hpp"
#include <filesystem>
#include <gtest/gtest.h>
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace testing_algo_infer_pipeline {

namespace fs = std::filesystem;

class ThroughPassPipeTest : public ::testing::Test {
protected:
  void SetUp() override {
    Logger::LogConfig logConfig;
    logConfig.appName = "Test-Unit";
    logConfig.logPath = "./logs";
    logConfig.logLevel = LogLevel::INFO;
    logConfig.enableConsole = true;
    logConfig.enableColor = true;
    Logger::instance()->initialize(logConfig);
  }

  void TearDown() override {}
  fs::path resourceDir = fs::path("assets");
  fs::path confDir = resourceDir / "conf";
};

TEST_F(ThroughPassPipeTest, Normal) {
  ai_pipe::Pipeline pipeline;
  auto context = std::make_shared<ai_pipe::PipelineContext>();
  int numWorkers = 4;
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

  ASSERT_TRUE(pipeline.initialize(std::move(graph), context, numWorkers));
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::IDLE);
  ASSERT_TRUE(pipeline.start());
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::RUNNING);

  ai_pipe::PortDataMap inputs;
  auto imagePathData = std::make_shared<ai_pipe::PortData>();
  imagePathData->setParam<std::string>("input", "");
  inputs["ThroughPass01"] = imagePathData;

  bool resultReceived = false;
  bool errorOccurred = false;
  std::string lastErrorMsg;

  pipeline.setPipelineResultCallback(
      [&](const ai_pipe::PortDataMap &finalResults) {
        resultReceived = true;
        ASSERT_TRUE(finalResults.count("ThroughPass04:output"));
        ASSERT_TRUE(finalResults.count("ThroughPass05:output"));
      });

  pipeline.setPipelineErrorCallback(
      [&](const std::string &errorMsg, const std::string &nodeName) {
        errorOccurred = true;
        lastErrorMsg = "Pipeline error in node '" + nodeName + "': " + errorMsg;
        std::cerr << lastErrorMsg << std::endl;
      });

  auto retFuture = pipeline.feedDataAndGetResultFuture(inputs);

  std::future_status status = retFuture.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready)
      << "Pipeline execution timed out.";

  bool success = retFuture.get();
  ASSERT_TRUE(success);

  ASSERT_TRUE(resultReceived) << "Pipeline result callback was not invoked.";
  ASSERT_FALSE(errorOccurred)
      << "Pipeline error callback was invoked: " << lastErrorMsg;

  ASSERT_TRUE(pipeline.stop());
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::IDLE);
  pipeline.reset();
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::UNINITIALIZED);
}

TEST_F(ThroughPassPipeTest, NormalWithJson) {
  const std::string graphConfigPath = confDir / "through_pass_pipe.json";
  auto context = std::make_shared<ai_pipe::PipelineContext>();
  int numWorkers = 4;
  auto pipeline = ai_pipe::examples::PipelineBuilder::buildPipelineFromConfig(
      graphConfigPath, context, numWorkers);

  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::IDLE);

  ai_pipe::PortDataMap inputs;
  auto imagePathData = std::make_shared<ai_pipe::PortData>();
  imagePathData->setParam<std::string>("input", "");
  // Use the top-level input port name defined in the graph's "inputs" section
  inputs["ThroughPass01"] = imagePathData;

  bool resultReceived = false;
  bool errorOccurred = false;
  std::string lastErrorMsg;

  pipeline.setPipelineResultCallback(
      [&](const ai_pipe::PortDataMap &finalResults) {
        resultReceived = true;
        ASSERT_TRUE(finalResults.count("ThroughPass04:output"));
        ASSERT_TRUE(finalResults.count("ThroughPass05:output"));
      });

  pipeline.setPipelineErrorCallback(
      [&](const std::string &errorMsg, const std::string &nodeName) {
        errorOccurred = true;
        lastErrorMsg = "Pipeline error in node '" + nodeName + "': " + errorMsg;
        std::cerr << lastErrorMsg << std::endl;
      });

  ASSERT_TRUE(pipeline.start());
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::RUNNING);

  // Use feedDataAndGetResultFuture for synchronous-like testing
  auto retFuture = pipeline.feedDataAndGetResultFuture(inputs);

  // 10-second timeout
  std::future_status status = retFuture.wait_for(std::chrono::seconds(10));
  ASSERT_EQ(status, std::future_status::ready)
      << "Pipeline execution timed out.";

  // Will rethrow exceptions from pipeline if any
  bool success = retFuture.get();
  ASSERT_TRUE(success);

  ASSERT_TRUE(resultReceived) << "Pipeline result callback was not invoked.";
  ASSERT_FALSE(errorOccurred)
      << "Pipeline error callback was invoked: " << lastErrorMsg;

  ASSERT_TRUE(pipeline.stop());
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::IDLE);
  pipeline.reset();
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::UNINITIALIZED);
}
} // namespace testing_algo_infer_pipeline
