#include <filesystem>
#include <fstream>

#include "ai_pipe/pipe_types.hpp"
#include "ai_pipe/pipeline.hpp"
#include "ai_pipe/pipeline_builder.hpp"
#include "ai_pipe/pipeline_context.hpp"

#include <ai_core/algo_manager.hpp>
#include <gtest/gtest.h>
#include <logger.hpp>
#include <nlohmann/json.hpp>

namespace testing_demo_pipeline {

namespace fs = std::filesystem;

using namespace ai_core;
using namespace ai_core::dnn;

ai_core::AlgoConstructParams
loadAlgoParamFromJson(const std::string &configPath) {
  ai_core::AlgoConstructParams params;

  std::ifstream file(configPath);
  if (!file.is_open()) {
    LOG_ERRORS << "Failed to open config file: " << configPath;
    throw std::runtime_error("Failed to open config file: " + configPath);
  }

  nlohmann::json j;
  try {
    file >> j;
  } catch (const nlohmann::json::parse_error &e) {
    LOG_ERRORS << "Failed to parse config JSON: " << e.what();
    throw std::runtime_error("Failed to parse config JSON: " +
                             std::string(e.what()));
  }

  try {
    if (!j.contains("algorithms") || !j["algorithms"].is_array()) {
      LOG_ERRORS << "Config missing 'algorithms' array or it's not an array.";
      throw std::runtime_error(
          "Config missing 'algorithms' array or not an array.");
    }

    if (j["algorithms"].empty()) {
      LOG_ERRORS << "Config 'algorithms' array is empty.";
      throw std::runtime_error("Config 'algorithms' array is empty.");
    }

    const auto &algoConfig = j["algorithms"][0];

    // model name and types
    params.setParam("moduleName", algoConfig["name"].get<std::string>());
    const auto &types = algoConfig["types"];
    params.setParam("preprocType", types["preproc"].get<std::string>());
    params.setParam("inferType", types["infer"].get<std::string>());
    params.setParam("postprocType", types["postproc"].get<std::string>());

    // parse preprocessing args
    const auto &preprocJson = algoConfig["preprocParams"];
    if (params.getParam<std::string>("preprocType") == "FramePreprocess") {
      ai_core::FramePreprocessArg framePreprocessArg;
      const auto &preprocJson = algoConfig["preprocParams"];
      if (preprocJson.contains("inputShape")) {
        framePreprocessArg.modelInputShape.w =
            preprocJson["inputShape"].at("w").get<int>();
        framePreprocessArg.modelInputShape.h =
            preprocJson["inputShape"].at("h").get<int>();
        framePreprocessArg.modelInputShape.c =
            preprocJson["inputShape"].at("c").get<int>();
      }

      if (preprocJson.contains("mean")) {
        framePreprocessArg.meanVals =
            preprocJson["mean"].get<std::vector<float>>();
      }
      if (preprocJson.contains("std")) {
        framePreprocessArg.normVals =
            preprocJson["std"].get<std::vector<float>>();
      }
      if (preprocJson.contains("pad")) {
        framePreprocessArg.pad = preprocJson["pad"].get<std::vector<int>>()[0];
      }

      if (preprocJson.contains("hwc2chw")) {
        framePreprocessArg.hwc2chw = preprocJson["hwc2chw"].get<bool>();
      } else {
        framePreprocessArg.hwc2chw = false;
      }
      if (preprocJson.contains("needResize")) {
        framePreprocessArg.needResize = preprocJson["needResize"].get<bool>();
      } else {
        framePreprocessArg.needResize = false;
      }

      if (preprocJson.contains("isEqualScale")) {
        framePreprocessArg.isEqualScale =
            preprocJson["isEqualScale"].get<bool>();
      } else {
        framePreprocessArg.isEqualScale = false;
      }
      framePreprocessArg.dataType =
          static_cast<ai_core::DataType>(preprocJson["dataType"].get<int>());
      framePreprocessArg.inputName =
          preprocJson["inputNames"].get<std::vector<std::string>>()[0];
      params.setParam("preprocParams", framePreprocessArg);
    } else {
      LOG_ERRORS << "Unsupported preprocType: "
                 << params.getParam<std::string>("preprocType");
      throw std::runtime_error("Unsupported preprocType");
    }

    // parse infer args
    ai_core::AlgoInferParams inferParams;
    const auto &inferJson = algoConfig["inferParams"];
    std::string modelRelPath = inferJson.at("modelPath").get<std::string>();
    // FIXME: 这里可能是个坑
    inferParams.modelPath =
        (std::filesystem::path(configPath).parent_path().parent_path() /
         modelRelPath)
            .string();
    inferParams.deviceType =
        static_cast<ai_core::DeviceType>(inferJson.at("deviceType").get<int>());
    inferParams.dataType =
        static_cast<ai_core::DataType>(inferJson.at("dataType").get<int>());
    inferParams.needDecrypt = inferJson.at("needDecrypt").get<bool>();
    params.setParam("inferParams", inferParams);

    // parse postprocessing args
    const auto &postProcJson = algoConfig["postprocParams"];

    const auto outputNames =
        postProcJson["outputNames"].get<std::vector<std::string>>();
    // FIXME: 就这么先瞎写写吧，后面再完善
    if (params.getParam<std::string>("postprocType") == "RTMDet" ||
        params.getParam<std::string>("postprocType") == "Yolov11Det" ||
        params.getParam<std::string>("postprocType") == "NanoDet") {
      ai_core::AnchorDetParams anchorDetParams;
      if (postProcJson.contains("condThre")) {
        anchorDetParams.condThre = postProcJson.at("condThre").get<float>();
      }
      if (postProcJson.contains("nmsThre")) {
        anchorDetParams.nmsThre = postProcJson.at("nmsThre").get<float>();
      }
      anchorDetParams.outputNames = outputNames;
      params.setParam("postprocParams", anchorDetParams);
    } else {
      ai_core::GenericPostParams genericPostParams;
      genericPostParams.outputNames = outputNames;
      params.setParam("postprocParams", genericPostParams);
    }
  } catch (const nlohmann::json::exception &e) {
    LOG_ERRORS << "JSON parsing error: " << e.what();
    throw std::runtime_error("JSON parsing error: " + std::string(e.what()));
  } catch (const std::exception &e) {
    LOG_ERRORS << "Standard exception: " << e.what();
    throw std::runtime_error("Standard exception: " + std::string(e.what()));
  }
  return params;
}

TEST(DemoPipelineTest, RunPipeline) {
  fs::path resourceDir = fs::path("resources");
  fs::path confDir = resourceDir / "conf";
  fs::path dataDir = resourceDir / "data";

  const std::string imagePathStr = dataDir / "yolov11" / "image.png";

  std::filesystem::path inputImagePath(imagePathStr);
  std::string expectedOutputFileName = inputImagePath.stem().string() +
                                       "_0_visualized" +
                                       inputImagePath.extension().string();
  std::string expectedOutputPathStr =
      (std::filesystem::path("output_visualizations") / expectedOutputFileName)
          .string();

  const std::string graphConfigPath =
      confDir / "test_demo_pipeline_config.json";
  ai_pipe::Pipeline pipeline;
  auto context = std::make_shared<ai_pipe::PipelineContext>();
  // create algoManager
  AlgoConstructParams params =
      loadAlgoParamFromJson(confDir / "test_algo_manager_ort.json");
  std::string name = params.getParam<std::string>("moduleName");

  AlgoModuleTypes moduleTypes;
  moduleTypes.preprocModule = params.getParam<std::string>("preprocType");
  moduleTypes.inferModule = params.getParam<std::string>("inferType");
  moduleTypes.postprocModule = params.getParam<std::string>("postprocType");

  AlgoInferParams inferParams = params.getParam<AlgoInferParams>("inferParams");

  std::shared_ptr<AlgoInference> engine =
      std::make_shared<AlgoInference>(moduleTypes, inferParams);

  ASSERT_NE(engine, nullptr);
  ASSERT_EQ(engine->initialize(), InferErrorCode::SUCCESS);
  std::shared_ptr<AlgoManager> algoManager = std::make_shared<AlgoManager>();
  ASSERT_NE(algoManager, nullptr);

  ASSERT_EQ(algoManager->registerAlgo(name, engine), InferErrorCode::SUCCESS);
  ASSERT_TRUE(algoManager->hasAlgo(name));
  ASSERT_NE(algoManager->getAlgo(name), nullptr);
  context->setAlgoManager(algoManager);
  ASSERT_TRUE(context->isValid());

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

  //   if (std::filesystem::exists(expectedOutputPathStr)) {
  //     std::filesystem::remove(expectedOutputPathStr);
  //   }
}
} // namespace testing_demo_pipeline