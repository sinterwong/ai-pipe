#include <filesystem>

#include "ai_sdk/ai_sdk.hpp"

#include <gtest/gtest.h>

namespace fs = std::filesystem;

using namespace ai_workflow;

class AIWorkflowSDKTest : public ::testing::Test {
protected:
  void SetUp() override {}
  void TearDown() override {}

  fs::path resourceDir = fs::path("resources");
  fs::path confDir = resourceDir / "conf";
  fs::path modelDir = resourceDir / "models";
  fs::path dataDir = resourceDir / "data";
};

TEST_F(AIWorkflowSDKTest, CppAPI) {
  ASSERT_EQ(AIWorkflowSDK::getVersion(), "v0.2.1");
}
