#ifndef __AI_WORKFLOW_SDK_SDK_H__
#define __AI_WORKFLOW_SDK_SDK_H__
#include "ai_export.h"
#include "ai_types.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *AIWorkflowSDKHandle;

AI_WORKFLOW_SDK_API AIWorkflowSDKHandle AIWorkflowSDK_Create();

AI_WORKFLOW_SDK_API void AIWorkflowSDK_Destroy(AIWorkflowSDKHandle handle);

AI_WORKFLOW_SDK_API ai_workflow::ErrorCode
AIWorkflowSDK_Initialize(AIWorkflowSDKHandle handle,
                      const ai_workflow::SDKConfig *config);

AI_WORKFLOW_SDK_API ai_workflow::ErrorCode
AIWorkflowSDK_PushInput(AIWorkflowSDKHandle handle,
                     const ai_workflow::InputPacket *input);

AI_WORKFLOW_SDK_API ai_workflow::ErrorCode
AIWorkflowSDK_CalcCurrentROI(AIWorkflowSDKHandle handle,
                          const ai_workflow::ImageData *input,
                          ai_workflow::Rect *roi);

AI_WORKFLOW_SDK_API ai_workflow::ErrorCode
AIWorkflowSDK_TryGetNextOutput(AIWorkflowSDKHandle handle,
                            ai_workflow::OutputPacket *result);

AI_WORKFLOW_SDK_API ai_workflow::ErrorCode
AIWorkflowSDK_Terminate(AIWorkflowSDKHandle handle);

AI_WORKFLOW_SDK_API const char *AIWorkflowSDK_GetVersion();
#ifdef __cplusplus
}
#endif

#endif