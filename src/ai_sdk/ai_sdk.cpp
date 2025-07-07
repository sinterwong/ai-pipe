/**
 * @file ai_sdk.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2024-11-30
 *
 * @copyright Copyright (c) 2024
 *
 */

#include "ai_sdk.hpp"
#include "ai_sdk_impl.hpp"
#include "ai_version.h"

#include <string>

namespace ai_workflow {

AIWorkflowSDK::AIWorkflowSDK() : impl_(std::make_unique<AIWorkflowSDKImpl>()) {}

AIWorkflowSDK::~AIWorkflowSDK() {}

ErrorCode AIWorkflowSDK::initialize(const SDKConfig &config) {
  if (!impl_) {
    return ErrorCode::INITIALIZATION_FAILED;
  }
  return impl_->initialize(config);
}

ErrorCode AIWorkflowSDK::terminate() {
  if (!impl_) {
    return ErrorCode::INVALID_STATE;
  }
  return impl_->terminate();
}

std::string AIWorkflowSDK::getVersion() { return AI_WORKFLOW_VERSION_STR; }

} // namespace ai_workflow