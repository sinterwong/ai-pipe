#ifndef __AI_WORKFLOW_SDK_EXPORT_H_
#define __AI_WORKFLOW_SDK_EXPORT_H_

#ifdef _WIN32
#ifdef AI_WORKFLOW_SDK_BUILD_DLL
#define AI_WORKFLOW_SDK_API __declspec(dllexport)
#else
#define AI_WORKFLOW_SDK_API __declspec(dllimport)
#endif
#else
#define AI_WORKFLOW_SDK_API __attribute__((visibility("default")))
#endif

#endif // __AI_WORKFLOW_SDK_EXPORT_H_
