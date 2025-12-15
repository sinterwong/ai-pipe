#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>

namespace ai_pipe::logging {

// 日志级别枚举
enum class LogLevel { Debug = 0, Info = 1, Warning = 2, Error = 3 };

// 控制台颜色代码
namespace ConsoleColor {
constexpr const char *RESET = "\033[0m";
constexpr const char *RED = "\033[31m";
constexpr const char *GREEN = "\033[32m";
constexpr const char *YELLOW = "\033[33m";
constexpr const char *BLUE = "\033[34m";
constexpr const char *MAGENTA = "\033[35m";
constexpr const char *CYAN = "\033[36m";
constexpr const char *WHITE = "\033[37m";
constexpr const char *BOLD_RED = "\033[1;31m";
} // namespace ConsoleColor

// 日志条目结构
struct LogEntry {
  LogLevel level;
  std::string message;
  std::string timestamp;
  std::string file_name;
  int line_number;
  std::string function_name;
  std::thread::id thread_id;
};

// Logger配置
struct LoggerConfig {
  LogLevel min_level = LogLevel::Debug;
  bool enable_console = true;
  bool enable_file = false;
  bool enable_color = true;
  bool enable_async = false;
  bool show_thread_id = true;
  bool show_file_info = true;
  std::string log_file_path = "app.log";
  size_t max_file_size = 10 * 1024 * 1024; // 10MB
  int max_backup_files = 5;
};

class Logger {
public:
  // 获取单例实例
  static Logger &getInstance();

  // 禁止拷贝和移动
  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  Logger(Logger &&) = delete;
  Logger &operator=(Logger &&) = delete;

  // 配置方法
  void configure(const LoggerConfig &config);
  LoggerConfig getConfig() const;

  // 设置日志级别
  void setLogLevel(LogLevel level);
  LogLevel getLogLevel() const;

  // 启用/禁用功能
  void enableConsole(bool enable);
  void enableFile(bool enable);
  void enableColor(bool enable);
  void enableAsync(bool enable);

  // 设置日志文件路径
  void setLogFilePath(const std::string &path);

  // 核心日志方法
  void log(LogLevel level, const std::string &message,
           const char *file = nullptr, int line = 0,
           const char *func = nullptr);

  // 格式化日志方法
  template <typename... Args>
  void logFormat(LogLevel level, const char *file, int line, const char *func,
                 const char *fmt, Args &&...args);

  // 便捷日志方法
  void debug(const std::string &message, const char *file = nullptr,
             int line = 0, const char *func = nullptr);
  void info(const std::string &message, const char *file = nullptr,
            int line = 0, const char *func = nullptr);
  void warning(const std::string &message, const char *file = nullptr,
               int line = 0, const char *func = nullptr);
  void error(const std::string &message, const char *file = nullptr,
             int line = 0, const char *func = nullptr);

  // 刷新日志
  void flush();

  // 关闭日志系统
  void shutdown();

private:
  Logger();
  ~Logger();

  // 内部方法
  void writeToConsole(const LogEntry &entry);
  void writeToFile(const LogEntry &entry);
  void processEntry(const LogEntry &entry);
  void asyncWorker();
  void rotateLogFile();

  std::string formatLogEntry(const LogEntry &entry, bool with_color) const;
  std::string getCurrentTimestamp() const;
  std::string levelToString(LogLevel level) const;
  const char *levelToColor(LogLevel level) const;
  std::string extractFileName(const std::string &path) const;

  // 成员变量
  LoggerConfig m_config;
  std::ofstream m_fileStream;
  mutable std::mutex m_mutex;
  mutable std::mutex m_fileMutex;

  // 异步日志相关
  std::queue<LogEntry> m_logQueue;
  std::mutex m_queueMutex;
  std::condition_variable m_condition;
  std::unique_ptr<std::thread> m_workerThread;
  std::atomic<bool> m_isRunning{false};
  std::atomic<bool> m_isInitialized{false};
};

// 流式日志辅助类
class LogStream {
public:
  LogStream(Logger &logger, LogLevel level, const char *file, int line,
            const char *func);
  ~LogStream();

  template <typename T> LogStream &operator<<(const T &value) {
    m_stream << value;
    return *this;
  }

private:
  Logger &m_logger;
  LogLevel m_level;
  const char *m_file;
  int m_line;
  const char *m_func;
  std::ostringstream m_stream;
};

// 格式化实现
template <typename... Args>
void Logger::logFormat(LogLevel level, const char *file, int line,
                       const char *func, const char *fmt, Args &&...args) {
  if (level < m_config.min_level) {
    return;
  }

  // 使用snprintf进行格式化
  int size = std::snprintf(nullptr, 0, fmt, std::forward<Args>(args)...);
  if (size <= 0) {
    return;
  }

  std::string buffer(size + 1, '\0');
  std::snprintf(buffer.data(), buffer.size(), fmt, std::forward<Args>(args)...);
  buffer.resize(size);

  log(level, buffer, file, line, func);
}

} // namespace ai_pipe::logging

// ============================================================================
// 函数调用式宏 - LOG_DEBUG("message"), LOG_INFO("message"), etc.
// ============================================================================
#define LOG_DEBUG(msg)                                                         \
  logging::Logger::getInstance().debug(msg, __FILE__, __LINE__, __func__)

#define LOG_INFO(msg)                                                          \
  logging::Logger::getInstance().info(msg, __FILE__, __LINE__, __func__)

#define LOG_WARNING(msg)                                                       \
  logging::Logger::getInstance().warning(msg, __FILE__, __LINE__, __func__)

#define LOG_ERROR(msg)                                                         \
  logging::Logger::getInstance().error(msg, __FILE__, __LINE__, __func__)

// ============================================================================
// 格式化宏 - LOG_DEBUG_FMT("format %d", value), etc.
// ============================================================================
#define LOG_DEBUG_FMT(fmt, ...)                                                \
  logging::Logger::getInstance().logFormat(logging::LogLevel::Debug, __FILE__, \
                                           __LINE__, __func__, fmt,            \
                                           ##__VA_ARGS__)

#define LOG_INFO_FMT(fmt, ...)                                                 \
  logging::Logger::getInstance().logFormat(logging::LogLevel::Info, __FILE__,  \
                                           __LINE__, __func__, fmt,            \
                                           ##__VA_ARGS__)

#define LOG_WARNING_FMT(fmt, ...)                                              \
  logging::Logger::getInstance().logFormat(logging::LogLevel::Warning,         \
                                           __FILE__, __LINE__, __func__, fmt,  \
                                           ##__VA_ARGS__)

#define LOG_ERROR_FMT(fmt, ...)                                                \
  logging::Logger::getInstance().logFormat(logging::LogLevel::Error, __FILE__, \
                                           __LINE__, __func__, fmt,            \
                                           ##__VA_ARGS__)

// ============================================================================
// 流式宏 - LOG_DEBUGS << "message" << value, etc.
// ============================================================================
#define LOG_DEBUGS                                                             \
  logging::LogStream(logging::Logger::getInstance(), logging::LogLevel::Debug, \
                     __FILE__, __LINE__, __func__)

#define LOG_INFOS                                                              \
  logging::LogStream(logging::Logger::getInstance(), logging::LogLevel::Info,  \
                     __FILE__, __LINE__, __func__)

#define LOG_WARNINGS                                                           \
  logging::LogStream(logging::Logger::getInstance(),                           \
                     logging::LogLevel::Warning, __FILE__, __LINE__, __func__)

#define LOG_ERRORS                                                             \
  logging::LogStream(logging::Logger::getInstance(), logging::LogLevel::Error, \
                     __FILE__, __LINE__, __func__)
