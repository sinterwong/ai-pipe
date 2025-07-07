/**
 * @file ai_sdk_impl.cpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-01-20
 *
 * @copyright Copyright (c) 2025
 *
 */
#include "ai_sdk_impl.hpp"
#include "api/ai_types.h"

#include <logger.hpp>
#include <opencv2/core/types.hpp>
#include <opencv2/imgcodecs.hpp>

namespace ai_workflow {

AIWorkflowSDKImpl::AIWorkflowSDKImpl() {}

AIWorkflowSDKImpl::~AIWorkflowSDKImpl() {}

ErrorCode AIWorkflowSDKImpl::initialize(const SDKConfig &config) {
  return ErrorCode::SUCCESS;
}

ErrorCode AIWorkflowSDKImpl::terminate() { return ErrorCode::SUCCESS; }

void AIWorkflowSDKImpl::processLoop() {}

} // namespace ai_workflow
