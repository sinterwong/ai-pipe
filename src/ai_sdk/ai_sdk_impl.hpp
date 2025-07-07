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
#include <thread_safe_queue.hpp>

namespace ai_workflow {

class AIWorkflowSDKImpl {

public:
  AIWorkflowSDKImpl();

  ~AIWorkflowSDKImpl();

  ErrorCode initialize(const SDKConfig &config);

  ErrorCode terminate();

private:
  void processLoop();
};

} // namespace ai_workflow

#endif