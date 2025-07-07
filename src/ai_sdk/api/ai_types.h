#ifndef __AI_WORKFLOW_TYPES_H__
#define __AI_WORKFLOW_TYPES_H__
#include "ai_export.h"
#include <cstdint>
#include <string>

namespace ai_workflow {

struct AI_WORKFLOW_SDK_API SDKConfig {
  uint32_t numWorkers{1};
  std::string logPath;
  uint logLevel = 2; // 0-4 [Trace, Debug, Info, Warning, Error]

  // TODO: Parameters used for building the pipeline and algorithms.
};

enum class ErrorCode {
  SUCCESS = 0,
  INVALID_INPUT = -1,
  FILE_NOT_FOUND = -2,
  INVALID_FILE_FORMAT = -3,
  INITIALIZATION_FAILED = -4,
  PROCESSING_ERROR = -5,
  INVALID_STATE = -6,
  TRY_GET_NEXT_OVERTIME = -7
};
} // namespace ai_workflow
#endif
