#include "ai_sdk.hpp"
#include "api/ai_sdk.h"
#include "api/ai_types.h"
#include <cstring>

AI_WORKFLOW_SDK_API AIWorkflowSDKHandle AIWorkflowSDK_Create() {
  ai_workflow::AIWorkflowSDK *sdk = new ai_workflow::AIWorkflowSDK();
  return (AIWorkflowSDKHandle)sdk;
}

AI_WORKFLOW_SDK_API void AIWorkflowSDK_Destroy(AIWorkflowSDKHandle handle) {
  if (handle) {
    ai_workflow::AIWorkflowSDK *sdk = (ai_workflow::AIWorkflowSDK *)handle;
    delete sdk;
  }
}

AI_WORKFLOW_SDK_API ai_workflow::ErrorCode
AIWorkflowSDK_Initialize(AIWorkflowSDKHandle handle,
                         const ai_workflow::SDKConfig *config) {
  if (!handle || !config) {
    return ai_workflow::ErrorCode::INITIALIZATION_FAILED;
  }
  ai_workflow::AIWorkflowSDK *sdk = (ai_workflow::AIWorkflowSDK *)handle;
  return sdk->initialize(*config);
}

AI_WORKFLOW_SDK_API ai_workflow::ErrorCode
AIWorkflowSDK_Terminate(AIWorkflowSDKHandle handle) {
  if (!handle) {
    return ai_workflow::ErrorCode::INVALID_STATE;
  }
  ai_workflow::AIWorkflowSDK *sdk = (ai_workflow::AIWorkflowSDK *)handle;
  return sdk->terminate();
}

AI_WORKFLOW_SDK_API const char *AIWorkflowSDK_GetVersion() {
  std::string version = ai_workflow::AIWorkflowSDK::getVersion();
  char *c_version = (char *)malloc(version.length() + 1);
  if (c_version) {
    strcpy(c_version, version.c_str());
  }
  return c_version;
}
