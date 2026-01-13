/**
 * @file error_code.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2026-01-13
 *
 * @copyright Copyright (c) 2026
 *
 */
#ifndef AI_PIPE_ENUM_HPP
#define AI_PIPE_ENUM_HPP

namespace ai_pipe {
// 执行状态枚举
enum class NodeExecutionState {
  WAITING,   // 等待输入数据
  READY,     // 输入数据就绪，可以执行
  EXECUTING, // 正在执行中
  COMPLETED, // 执行完成
  FAILED     // 执行失败
};

enum class EngineState {
  IDLE, // 空闲
  RUNNING,
  STOPPED,
  ERROR
};

enum class PipelineState { UNINITIALIZED, IDLE, RUNNING, STOPPING, ERROR };
} // namespace ai_pipe
#endif