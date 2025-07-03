#ifndef __AI_WORKFLOW_TYPES_H__
#define __AI_WORKFLOW_TYPES_H__
#include "ai_export.h"
#include <cstdint>
#include <string>
#include <vector>

namespace ai_workflow {

struct Rect {
  int32_t x;
  int32_t y;
  int32_t w;
  int32_t h;
};

struct AI_WORKFLOW_SDK_API RetBBox {
  Rect rect;     // 检测框坐标
  float score;   // 目标得分
  int32_t label; // 目标类别
};

struct AI_WORKFLOW_SDK_API ImageData {
  std::vector<uint8_t> frameData; // 图像数据（encode）
  int64_t frameIndex;             // 帧号
};

struct AI_WORKFLOW_SDK_API SDKConfig {
  uint32_t numWorkers{1};   // 工作线程数量
  std::string modelRoot;    // 模型路径（所有模型所在的文件夹）
  std::string algoConfPath; // 算法配置路径
  std::string logPath;      // 日志路径
  uint logLevel = 2;        // 0-4 [Trace, Debug, Info, Warning, Error]
};

struct AI_WORKFLOW_SDK_API InputPacket {
  std::string uuid;  // 数据标识
  ImageData frame;   // 图像帧
  int64_t timestamp; // 时间戳(微秒)
};

struct AI_WORKFLOW_SDK_API OutputPacket {};

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
