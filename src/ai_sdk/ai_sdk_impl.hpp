/**
 * @file ai_sdk_impl.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-01-20
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef __AI_WORKFLOW_SDK_IMPL_HPP__
#define __AI_WORKFLOW_SDK_IMPL_HPP__

#include "api/ai_types.h"
#include <atomic>
#include <thread>
#include <thread_safe_queue.hpp>

namespace ai_workflow {

class AIWorkflowSDKImpl {

public:
  AIWorkflowSDKImpl();

  ~AIWorkflowSDKImpl();

  ErrorCode initialize(const SDKConfig &config);

  ErrorCode calcCurrentROI(const ImageData &input, Rect &roi);

  ErrorCode pushInput(const InputPacket &input);

  ErrorCode tryGetNextOutput(OutputPacket &output);

  ErrorCode terminate();

private:
  common_utils::ThreadSafeQueue<InputPacket> inputQueue;

  common_utils::ThreadSafeQueue<OutputPacket> outputQueue;

  std::thread processThread;

  std::atomic<bool> isRunning;

private:
  void processLoop();
};

} // namespace ai_workflow

#endif