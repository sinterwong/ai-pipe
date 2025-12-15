#include "ai_pipe/logger.hpp"
#include <algorithm>
#include <cstring>
#include <filesystem>

#ifdef _WIN32
#include <windows.h>
#endif

namespace ai_pipe::logging {

// ============================================================================
// Logger 实现
// ============================================================================

Logger &Logger::getInstance() {
  static Logger instance;
  return instance;
}

Logger::Logger() {
  m_isInitialized.store(true);

#ifdef _WIN32
  // Windows下启用ANSI颜色支持
  HANDLE h_console = GetStdHandle(STD_OUTPUT_HANDLE);
  DWORD console_mode;
  if (GetConsoleMode(h_console, &console_mode)) {
    SetConsoleMode(h_console,
                   console_mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
  }
#endif
}

Logger::~Logger() { shutdown(); }

void Logger::configure(const LoggerConfig &config) {
  std::lock_guard<std::mutex> lock(m_mutex);

  bool need_reopen_file = (m_config.log_file_path != config.log_file_path) ||
                          (m_config.enable_file != config.enable_file);
  bool need_async_change = (m_config.enable_async != config.enable_async);

  m_config = config;

  // 创建父目录
  std::filesystem::path log_dir =
      std::filesystem::path(m_config.log_file_path).parent_path();
  if (!log_dir.empty() && !std::filesystem::exists(log_dir)) {
    std::filesystem::create_directories(log_dir);
  }

  // 处理文件变更
  if (need_reopen_file) {
    std::lock_guard<std::mutex> file_lock(m_fileMutex);
    if (m_fileStream.is_open()) {
      m_fileStream.close();
    }
    if (m_config.enable_file) {
      m_fileStream.open(m_config.log_file_path, std::ios::out | std::ios::app);
    }
  }

  // 处理异步模式变更
  if (need_async_change) {
    if (m_config.enable_async && !m_isRunning.load()) {
      m_isRunning.store(true);
      m_workerThread =
          std::make_unique<std::thread>(&Logger::asyncWorker, this);
    } else if (!m_config.enable_async && m_isRunning.load()) {
      m_isRunning.store(false);
      m_condition.notify_all();
      if (m_workerThread && m_workerThread->joinable()) {
        m_workerThread->join();
      }
      m_workerThread.reset();
    }
  }
}

LoggerConfig Logger::getConfig() const {
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_config;
}

void Logger::setLogLevel(LogLevel level) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_config.min_level = level;
}

LogLevel Logger::getLogLevel() const {
  std::lock_guard<std::mutex> lock(m_mutex);
  return m_config.min_level;
}

void Logger::enableConsole(bool enable) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_config.enable_console = enable;
}

void Logger::enableFile(bool enable) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_config.enable_file = enable;

  std::lock_guard<std::mutex> file_lock(m_fileMutex);
  if (enable && !m_fileStream.is_open()) {
    m_fileStream.open(m_config.log_file_path, std::ios::out | std::ios::app);
  } else if (!enable && m_fileStream.is_open()) {
    m_fileStream.close();
  }
}

void Logger::enableColor(bool enable) {
  std::lock_guard<std::mutex> lock(m_mutex);
  m_config.enable_color = enable;
}

void Logger::enableAsync(bool enable) {
  LoggerConfig new_config;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    new_config = m_config;
    new_config.enable_async = enable;
  }
  configure(new_config);
}

void Logger::setLogFilePath(const std::string &path) {
  std::lock_guard<std::mutex> lock(m_mutex);

  if (m_config.log_file_path == path) {
    return;
  }

  m_config.log_file_path = path;

  std::lock_guard<std::mutex> file_lock(m_fileMutex);
  if (m_fileStream.is_open()) {
    m_fileStream.close();
  }
  if (m_config.enable_file) {
    m_fileStream.open(m_config.log_file_path, std::ios::out | std::ios::app);
  }
}

void Logger::log(LogLevel level, const std::string &message, const char *file,
                 int line, const char *func) {
  // 快速检查日志级别
  if (level < m_config.min_level) {
    return;
  }

  LogEntry entry;
  entry.level = level;
  entry.message = message;
  entry.timestamp = getCurrentTimestamp();
  entry.file_name = file ? extractFileName(file) : "";
  entry.line_number = line;
  entry.function_name = func ? func : "";
  entry.thread_id = std::this_thread::get_id();

  if (m_config.enable_async && m_isRunning.load()) {
    std::lock_guard<std::mutex> lock(m_queueMutex);
    m_logQueue.push(std::move(entry));
    m_condition.notify_one();
  } else {
    processEntry(entry);
  }
}

void Logger::debug(const std::string &message, const char *file, int line,
                   const char *func) {
  log(LogLevel::Debug, message, file, line, func);
}

void Logger::info(const std::string &message, const char *file, int line,
                  const char *func) {
  log(LogLevel::Info, message, file, line, func);
}

void Logger::warning(const std::string &message, const char *file, int line,
                     const char *func) {
  log(LogLevel::Warning, message, file, line, func);
}

void Logger::error(const std::string &message, const char *file, int line,
                   const char *func) {
  log(LogLevel::Error, message, file, line, func);
}

void Logger::flush() {
  std::lock_guard<std::mutex> file_lock(m_fileMutex);
  if (m_fileStream.is_open()) {
    m_fileStream.flush();
  }
  std::cout.flush();
  std::cerr.flush();
}

void Logger::shutdown() {
  if (!m_isInitialized.exchange(false)) {
    return;
  }

  // 停止异步线程
  m_isRunning.store(false);
  m_condition.notify_all();

  if (m_workerThread && m_workerThread->joinable()) {
    m_workerThread->join();
  }
  m_workerThread.reset();

  // 处理剩余队列
  {
    std::lock_guard<std::mutex> lock(m_queueMutex);
    while (!m_logQueue.empty()) {
      processEntry(m_logQueue.front());
      m_logQueue.pop();
    }
  }

  // 关闭文件
  std::lock_guard<std::mutex> file_lock(m_fileMutex);
  if (m_fileStream.is_open()) {
    m_fileStream.flush();
    m_fileStream.close();
  }
}

void Logger::writeToConsole(const LogEntry &entry) {
  std::string formatted = formatLogEntry(entry, m_config.enable_color);

  // ERROR级别输出到stderr，其他输出到stdout
  if (entry.level == LogLevel::Error) {
    std::cerr << formatted << std::endl;
  } else {
    std::cout << formatted << std::endl;
  }
}

void Logger::writeToFile(const LogEntry &entry) {
  std::lock_guard<std::mutex> file_lock(m_fileMutex);

  if (!m_fileStream.is_open()) {
    return;
  }

  // 检查是否需要轮转
  rotateLogFile();

  std::string formatted = formatLogEntry(entry, false);
  m_fileStream << formatted << "\n";
  m_fileStream.flush();
}

void Logger::processEntry(const LogEntry &entry) {
  if (m_config.enable_console) {
    writeToConsole(entry);
  }
  if (m_config.enable_file) {
    writeToFile(entry);
  }
}

void Logger::asyncWorker() {
  while (m_isRunning.load()) {
    std::unique_lock<std::mutex> lock(m_queueMutex);

    m_condition.wait(
        lock, [this] { return !m_logQueue.empty() || !m_isRunning.load(); });

    while (!m_logQueue.empty()) {
      LogEntry entry = std::move(m_logQueue.front());
      m_logQueue.pop();
      lock.unlock();

      processEntry(entry);

      lock.lock();
    }
  }
}

void Logger::rotateLogFile() {
  // 检查文件大小
  if (!m_fileStream.is_open()) {
    return;
  }

  auto current_pos = m_fileStream.tellp();
  if (current_pos < 0 ||
      static_cast<size_t>(current_pos) < m_config.max_file_size) {
    return;
  }

  m_fileStream.close();

  // 轮转备份文件
  namespace fs = std::filesystem;

  for (int i = m_config.max_backup_files - 1; i >= 0; --i) {
    std::string old_name =
        m_config.log_file_path + (i > 0 ? "." + std::to_string(i) : "");
    std::string new_name = m_config.log_file_path + "." + std::to_string(i + 1);

    if (fs::exists(old_name)) {
      if (i == m_config.max_backup_files - 1) {
        fs::remove(old_name);
      } else {
        fs::rename(old_name, new_name);
      }
    }
  }

  // 重新打开新文件
  m_fileStream.open(m_config.log_file_path, std::ios::out | std::ios::app);
}

std::string Logger::formatLogEntry(const LogEntry &entry,
                                   bool with_color) const {
  std::ostringstream oss;

  // 时间戳
  oss << "[" << entry.timestamp << "] ";

  // 日志级别（带颜色）
  if (with_color) {
    oss << levelToColor(entry.level);
  }
  oss << "[" << std::setw(7) << levelToString(entry.level) << "]";
  if (with_color) {
    oss << ConsoleColor::RESET;
  }

  // 线程ID
  if (m_config.show_thread_id) {
    oss << " [T:" << entry.thread_id << "]";
  }

  // 文件信息
  if (m_config.show_file_info && !entry.file_name.empty()) {
    oss << " [" << entry.file_name << ":" << entry.line_number;
    if (!entry.function_name.empty()) {
      oss << " " << entry.function_name << "()";
    }
    oss << "]";
  }

  // 消息
  oss << " " << entry.message;

  return oss.str();
}

std::string Logger::getCurrentTimestamp() const {
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) %
            1000;

  std::tm tm_now;
#ifdef _WIN32
  localtime_s(&tm_now, &time_t_now);
#else
  localtime_r(&time_t_now, &tm_now);
#endif

  std::ostringstream oss;
  oss << std::put_time(&tm_now, "%Y-%m-%d %H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(3) << ms.count();

  return oss.str();
}

std::string Logger::levelToString(LogLevel level) const {
  switch (level) {
  case LogLevel::Debug:
    return "DEBUG";
  case LogLevel::Info:
    return "INFO";
  case LogLevel::Warning:
    return "WARNING";
  case LogLevel::Error:
    return "ERROR";
  default:
    return "UNKNOWN";
  }
}

const char *Logger::levelToColor(LogLevel level) const {
  switch (level) {
  case LogLevel::Debug:
    return ConsoleColor::CYAN;
  case LogLevel::Info:
    return ConsoleColor::GREEN;
  case LogLevel::Warning:
    return ConsoleColor::YELLOW;
  case LogLevel::Error:
    return ConsoleColor::BOLD_RED;
  default:
    return ConsoleColor::WHITE;
  }
}

std::string Logger::extractFileName(const std::string &path) const {
  size_t last_sep = path.find_last_of("/\\");
  if (last_sep != std::string::npos) {
    return path.substr(last_sep + 1);
  }
  return path;
}

// ============================================================================
// LogStream 实现
// ============================================================================

LogStream::LogStream(Logger &logger, LogLevel level, const char *file, int line,
                     const char *func)
    : m_logger(logger), m_level(level), m_file(file), m_line(line),
      m_func(func) {}

LogStream::~LogStream() {
  m_logger.log(m_level, m_stream.str(), m_file, m_line, m_func);
}

} // namespace ai_pipe::logging
