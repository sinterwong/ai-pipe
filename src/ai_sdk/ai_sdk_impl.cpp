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
#include <chrono>
#include <thread>

#include "ai_sdk_impl.hpp"
#include "api/ai_types.h"

#include <logger.hpp>
#include <opencv2/core/types.hpp>
#include <opencv2/imgcodecs.hpp>

namespace ai_workflow {

AIWorkflowSDKImpl::AIWorkflowSDKImpl() {}

AIWorkflowSDKImpl::~AIWorkflowSDKImpl() {
  if (isRunning) {
    terminate();
  }
}

ErrorCode AIWorkflowSDKImpl::initialize(const SDKConfig &config) {
  isRunning.store(true);
  processThread = std::thread(&AIWorkflowSDKImpl::processLoop, this);
  return ErrorCode::SUCCESS;
}

ErrorCode AIWorkflowSDKImpl::calcCurrentROI(const ImageData &input, Rect &roi) {
  return ErrorCode::SUCCESS;
}

ErrorCode AIWorkflowSDKImpl::pushInput(const InputPacket &input) {
  inputQueue.push(input);
  return ErrorCode::SUCCESS;
}

ErrorCode AIWorkflowSDKImpl::tryGetNextOutput(OutputPacket &output) {
  auto output_ = outputQueue.wait_pop_for(std::chrono::milliseconds(1000));
  if (!output_) {
    return ErrorCode::TRY_GET_NEXT_OVERTIME;
  }
  output = *output_;

  return ErrorCode::SUCCESS;
}

ErrorCode AIWorkflowSDKImpl::terminate() {

  isRunning.store(false);
  processThread.join();

  // clean queue
  inputQueue.clear();
  outputQueue.clear();

  LOG_INFOS << "AIWorkflowSDKImpl::terminate success";
  return ErrorCode::SUCCESS;
}

void AIWorkflowSDKImpl::processLoop() {
  while (isRunning) {
    auto input = inputQueue.wait_pop_for(std::chrono::milliseconds(100));
    if (!input) {
      std::this_thread::yield();
      continue;
    }
    OutputPacket output;
    // TODO: 处理完成output后推入输出队列
    outputQueue.push(output);
  }
}

} // namespace ai_workflow
