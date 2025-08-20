#include "ai_pipe/context.hpp"
#include "ai_pipe/pipeline.hpp"
#include "dummy_module.hpp"
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
  const std::string graphConfigPath = confDir / "through_pass_pipe.json";
  ai_pipe::Pipeline pipeline;
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  ai_pipe::PipelineConfig pipelineConfig;
  pipelineConfig.graphConfigPath = graphConfigPath;
  pipelineConfig.numWorkers = 4;

  ASSERT_TRUE(pipeline.initialize(pipelineConfig, context));
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
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::STOPPED);
  pipeline.reset();
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::IDLE);
}
} // namespace testing_algo_infer_pipeline
