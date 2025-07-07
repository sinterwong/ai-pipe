/**
 * @file ai_sdk.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-01-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef __AI_WORKFLOW_SDK_HPP__
#define __AI_WORKFLOW_SDK_HPP__
#include "api/ai_export.h"
#include "api/ai_types.h"
#include <memory>
#include <string>

namespace ai_workflow {

class AIWorkflowSDKImpl;

class AI_WORKFLOW_SDK_API AIWorkflowSDK {
public:
  AIWorkflowSDK();
  ~AIWorkflowSDK();

  ErrorCode initialize(const SDKConfig &config);

  ErrorCode terminate();

  static std::string getVersion();

private:
  std::unique_ptr<AIWorkflowSDKImpl> impl_;

  AIWorkflowSDK(const AIWorkflowSDK &) = delete;
  AIWorkflowSDK &operator=(const AIWorkflowSDK &) = delete;
};

} // namespace ai_workflow

#endif
