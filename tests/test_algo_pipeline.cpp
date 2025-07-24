#include <filesystem>

#include "ai_pipe/pipe_types.hpp"
#include "ai_pipe/pipeline.hpp"
#include "ai_pipe/pipeline_builder.hpp"
#include "ai_pipe/pipeline_context.hpp"

#include <ai_core/algo_manager.hpp>
#include <gtest/gtest.h>
#include <logger.hpp>
#include <nlohmann/json.hpp>
#include <opencv2/opencv.hpp>

namespace testing_algo_infer_pipeline {

namespace fs = std::filesystem;

class AlgoInferPipeTest : public ::testing::Test {
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
  fs::path dataDir = resourceDir / "data";
};

TEST_F(AlgoInferPipeTest, YoloPipelineSingleImage) {
  const std::string imagePathStr = dataDir / "yolov11" / "image.png";

  std::filesystem::path inputImagePath(imagePathStr);
  std::string expectedOutputFileName = inputImagePath.stem().string() +
                                       "_0_visualized" +
                                       inputImagePath.extension().string();
  std::string expectedOutputPathStr =
      (std::filesystem::path("output_visualizations") / expectedOutputFileName)
          .string();

  const std::string graphConfigPath =
      confDir / "yolo_infer_pipe_single_frame.json";
  ai_pipe::Pipeline pipeline;
  auto context = std::make_shared<ai_pipe::PipelineContext>();

  ai_pipe::PipelineConfig pipelineConfig;
  pipelineConfig.graphConfigPath = graphConfigPath;
  pipelineConfig.numWorkers = 4;

  ASSERT_TRUE(pipeline.initialize(pipelineConfig, context));
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::IDLE);

  ai_pipe::PortDataMap inputs;
  auto imagePathData = std::make_shared<ai_pipe::PortData>();
  imagePathData->setParam<std::string>("image_path", imagePathStr);
  // Use the top-level input port name defined in the graph's "inputs" section
  inputs["ImageReader"] = imagePathData;

  bool resultReceived = false;
  bool errorOccurred = false;
  std::string lastErrorMsg;

  pipeline.setPipelineResultCallback(
      [&](const ai_pipe::PortDataMap &finalResults) {
        resultReceived = true;
        const std::string outputPortKey =
            "Visualization:visualized_image_output_data";
        ASSERT_TRUE(finalResults.count(outputPortKey));
        const auto &viz_data = finalResults.at(outputPortKey)
                                   ->getParam<cv::Mat>("visualized_image");
        ASSERT_FALSE(viz_data.empty());
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

  ASSERT_TRUE(std::filesystem::exists(expectedOutputPathStr))
      << "Expected output image file was not created: "
      << expectedOutputPathStr;

  ASSERT_TRUE(pipeline.stop());
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::STOPPED);
  pipeline.reset();
  ASSERT_EQ(pipeline.getState(), ai_pipe::PipelineState::IDLE);

  if (std::filesystem::exists(expectedOutputPathStr)) {
    std::filesystem::remove(expectedOutputPathStr);
  }
}
} // namespace testing_algo_infer_pipeline