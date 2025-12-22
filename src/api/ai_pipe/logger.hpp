#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

// Source location support (C++20 or fallback)
#if __cplusplus >= 202002L && __has_include(<source_location>)
#include <source_location>
#define AI_PIPE_HAS_SOURCE_LOCATION 1
#else
#define AI_PIPE_HAS_SOURCE_LOCATION 0
#endif

namespace ai_pipe::logging {

// ============================================================================
// Log Level
// ============================================================================

enum class LogLevel : uint8_t {
  Trace = 0,
  Debug = 1,
  Info = 2,
  Warning = 3,
  Error = 4,
  Fatal = 5,
  Off = 6
};

// Compile-time log level threshold (define before including header to override)
#ifndef AI_PIPE_LOG_LEVEL
#define AI_PIPE_LOG_LEVEL 0 // Default: all levels enabled
#endif

constexpr LogLevel kCompileTimeLogLevel =
    static_cast<LogLevel>(AI_PIPE_LOG_LEVEL);

[[nodiscard]] constexpr bool isLevelEnabled(LogLevel level) noexcept {
  return level >= kCompileTimeLogLevel;
}

// ============================================================================
// Console Colors (ANSI escape codes)
// ============================================================================

namespace color {
inline constexpr std::string_view kReset = "\033[0m";
inline constexpr std::string_view kRed = "\033[31m";
inline constexpr std::string_view kGreen = "\033[32m";
inline constexpr std::string_view kYellow = "\033[33m";
inline constexpr std::string_view kBlue = "\033[34m";
inline constexpr std::string_view kMagenta = "\033[35m";
inline constexpr std::string_view kCyan = "\033[36m";
inline constexpr std::string_view kWhite = "\033[37m";
inline constexpr std::string_view kBoldRed = "\033[1;31m";
inline constexpr std::string_view kGray = "\033[90m";
} // namespace color

// ============================================================================
// Source Location (C++20 compatible fallback)
// ============================================================================

struct SourceLocation {
  const char *file = "";
  const char *function = "";
  int line = 0;

  constexpr SourceLocation() noexcept = default;
  constexpr SourceLocation(const char *f, const char *fn, int l) noexcept
      : file(f), function(fn), line(l) {}

#if AI_PIPE_HAS_SOURCE_LOCATION
  static constexpr SourceLocation
  current(const std::source_location &loc =
              std::source_location::current()) noexcept {
    return {loc.file_name(), loc.function_name(), static_cast<int>(loc.line())};
  }
#endif
};

// ============================================================================
// Log Entry - Optimized for minimal allocations
// ============================================================================

struct LogEntry {
  LogLevel level = LogLevel::Info;
  std::string message;
  std::chrono::system_clock::time_point timestamp;
  SourceLocation location;
  std::thread::id thread_id;
  std::string_view category; // Non-owning reference, must outlive entry

  LogEntry() noexcept = default;

  LogEntry(LogLevel lvl, std::string msg, SourceLocation loc,
           std::string_view cat = {}) noexcept
      : level(lvl), message(std::move(msg)),
        timestamp(std::chrono::system_clock::now()), location(loc),
        thread_id(std::this_thread::get_id()), category(cat) {}
};

// ============================================================================
// Logger Configuration
// ============================================================================

struct LoggerConfig {
  LogLevel min_level = LogLevel::Debug;
  bool console_enabled = true;
  bool file_enabled = false;
  bool color_enabled = true;
  bool async_enabled = false;
  bool show_thread_id = true;
  bool show_source_location = true;
  bool show_category = true;
  bool json_output = false; // Structured logging mode

  std::string file_path = "app.log";
  size_t max_file_size = 10 * 1024 * 1024; // 10MB
  int max_backup_count = 5;
  size_t async_queue_size = 8192;  // Ring buffer size for async mode
  size_t flush_interval_ms = 1000; // Periodic flush interval

  // Log pattern: %T=timestamp, %L=level, %t=thread, %s=source, %c=category,
  // %m=message
  std::string pattern = "[%T] [%L] [%t] [%s] %m";
};

// ============================================================================
// Lock-Free SPSC Ring Buffer for Async Logging
// ============================================================================

template <typename T> class SPSCRingBuffer {
public:
  explicit SPSCRingBuffer(size_t capacity)
      : capacity_(nextPowerOf2(capacity)), mask_(capacity_ - 1),
        buffer_(std::make_unique<Slot[]>(capacity_)) {}

  // Producer: try to push, returns false if full
  bool tryPush(T &&item) noexcept {
    const size_t head = head_.load(std::memory_order_relaxed);
    const size_t next_head = (head + 1) & mask_;

    if (next_head == tail_.load(std::memory_order_acquire)) {
      return false; // Full
    }

    buffer_[head].data = std::move(item);
    head_.store(next_head, std::memory_order_release);
    return true;
  }

  // Consumer: try to pop, returns false if empty
  bool tryPop(T &item) noexcept {
    const size_t tail = tail_.load(std::memory_order_relaxed);

    if (tail == head_.load(std::memory_order_acquire)) {
      return false; // Empty
    }

    item = std::move(buffer_[tail].data);
    tail_.store((tail + 1) & mask_, std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool empty() const noexcept {
    return head_.load(std::memory_order_acquire) ==
           tail_.load(std::memory_order_acquire);
  }

  [[nodiscard]] size_t capacity() const noexcept { return capacity_; }

private:
  static size_t nextPowerOf2(size_t n) noexcept {
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    return n + 1;
  }

  struct Slot {
    T data;
  };

  const size_t capacity_;
  const size_t mask_;
  std::unique_ptr<Slot[]> buffer_;

  // Separate cache lines to avoid false sharing
  alignas(64) std::atomic<size_t> head_{0};
  alignas(64) std::atomic<size_t> tail_{0};
};

// ============================================================================
// Thread-Local Formatting Buffer
// ============================================================================

class FormatBuffer {
public:
  static constexpr size_t kInitialCapacity = 1024;

  FormatBuffer() { buffer_.reserve(kInitialCapacity); }

  void clear() noexcept { buffer_.clear(); }

  void append(std::string_view sv) { buffer_.append(sv); }

  void append(char c) { buffer_.push_back(c); }

  template <typename T> void appendNumber(T value) {
    // Fast integer to string conversion
    char temp[32];
    char *end = temp + sizeof(temp);
    char *ptr = end;

    if constexpr (std::is_signed_v<T>) {
      bool negative = value < 0;
      if (negative)
        value = -value;

      do {
        *--ptr = '0' + (value % 10);
        value /= 10;
      } while (value > 0);

      if (negative)
        *--ptr = '-';
    } else {
      do {
        *--ptr = '0' + (value % 10);
        value /= 10;
      } while (value > 0);
    }

    buffer_.append(ptr, end - ptr);
  }

  void appendPadded(int value, int width, char pad = '0') {
    char temp[16];
    int len = 0;
    int v = value;

    do {
      temp[len++] = '0' + (v % 10);
      v /= 10;
    } while (v > 0);

    for (int i = len; i < width; ++i) {
      buffer_.push_back(pad);
    }

    for (int i = len - 1; i >= 0; --i) {
      buffer_.push_back(temp[i]);
    }
  }

  [[nodiscard]] std::string_view view() const noexcept {
    return {buffer_.data(), buffer_.size()};
  }

  [[nodiscard]] const std::string &str() const noexcept { return buffer_; }

private:
  std::string buffer_;
};

// Thread-local buffer accessor
inline FormatBuffer &getThreadLocalBuffer() {
  thread_local FormatBuffer buffer;
  buffer.clear();
  return buffer;
}

// ============================================================================
// Logger Core
// ============================================================================

class Logger {
public:
  using LogCallback = std::function<void(const LogEntry &)>;

  static Logger &instance() noexcept {
    static Logger logger;
    return logger;
  }

  // Non-copyable, non-movable
  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  Logger(Logger &&) = delete;
  Logger &operator=(Logger &&) = delete;

  // Configuration
  void configure(const LoggerConfig &config);
  [[nodiscard]] LoggerConfig config() const;

  void setLevel(LogLevel level) noexcept;
  [[nodiscard]] LogLevel level() const noexcept;

  void enableConsole(bool enable) noexcept;
  void enableFile(bool enable);
  void enableColor(bool enable) noexcept;
  void enableAsync(bool enable);
  void enableJson(bool enable) noexcept;

  void setFilePath(const std::string &path);
  void setPattern(const std::string &pattern);

  // Register custom log handler (e.g., for remote logging)
  void addCallback(LogCallback callback);
  void clearCallbacks() noexcept;

  // Core logging - use macros for source location capture
  void log(LogLevel level, std::string_view message, const SourceLocation &loc,
           std::string_view category = {});

  // Printf-style formatting
  template <typename... Args>
  void logf(LogLevel level, const SourceLocation &loc,
            std::string_view category, const char *fmt, Args &&...args);

  // Convenience methods
  void trace(std::string_view msg, const SourceLocation &loc = {},
             std::string_view cat = {});
  void debug(std::string_view msg, const SourceLocation &loc = {},
             std::string_view cat = {});
  void info(std::string_view msg, const SourceLocation &loc = {},
            std::string_view cat = {});
  void warning(std::string_view msg, const SourceLocation &loc = {},
               std::string_view cat = {});
  void error(std::string_view msg, const SourceLocation &loc = {},
             std::string_view cat = {});
  void fatal(std::string_view msg, const SourceLocation &loc = {},
             std::string_view cat = {});

  // Flush all pending logs
  void flush();

  // Graceful shutdown
  void shutdown();

  // Check if level is enabled (for conditional logging)
  [[nodiscard]] bool isEnabled(LogLevel level) const noexcept {
    return level >= level_.load(std::memory_order_relaxed);
  }

private:
  Logger();
  ~Logger();

  void processEntry(const LogEntry &entry);
  void writeConsole(const LogEntry &entry);
  void writeFile(const LogEntry &entry);
  void asyncWorker();
  void rotateFile();

  void formatEntry(FormatBuffer &buf, const LogEntry &entry,
                   bool with_color) const;
  void formatJson(FormatBuffer &buf, const LogEntry &entry) const;
  void formatTimestamp(FormatBuffer &buf,
                       std::chrono::system_clock::time_point tp) const;

  [[nodiscard]] static std::string_view levelName(LogLevel level) noexcept;
  [[nodiscard]] static std::string_view levelColor(LogLevel level) noexcept;
  [[nodiscard]] static std::string_view
  extractFilename(std::string_view path) noexcept;

  // Configuration (atomic where possible for lock-free reads)
  std::atomic<LogLevel> level_{LogLevel::Debug};
  std::atomic<bool> console_enabled_{true};
  std::atomic<bool> file_enabled_{false};
  std::atomic<bool> color_enabled_{true};
  std::atomic<bool> async_enabled_{false};
  std::atomic<bool> json_enabled_{false};
  std::atomic<bool> show_thread_id_{true};
  std::atomic<bool> show_source_{true};
  std::atomic<bool> show_category_{true};

  LoggerConfig config_;
  mutable std::mutex config_mutex_;

  // File output
  std::ofstream file_stream_;
  std::mutex file_mutex_;

  // Async logging
  std::unique_ptr<SPSCRingBuffer<LogEntry>> ring_buffer_;
  std::unique_ptr<std::thread> worker_thread_;
  std::atomic<bool> running_{false};
  std::atomic<bool> initialized_{false};
  std::condition_variable worker_cv_;
  std::mutex worker_mutex_;

  // Custom callbacks
  std::vector<LogCallback> callbacks_;
  std::mutex callback_mutex_;
};

// ============================================================================
// Stream-Style Logger
// ============================================================================

class LogStream {
public:
  LogStream(Logger &logger, LogLevel level, SourceLocation loc,
            std::string_view category = {}) noexcept
      : logger_(logger), level_(level), location_(loc), category_(category),
        enabled_(logger.isEnabled(level)) {}

  ~LogStream() {
    if (enabled_) {
      logger_.log(level_, stream_.str(), location_, category_);
    }
  }

  // Movable but not copyable
  LogStream(LogStream &&) = default;
  LogStream &operator=(LogStream &&) = delete;
  LogStream(const LogStream &) = delete;
  LogStream &operator=(const LogStream &) = delete;

  template <typename T> LogStream &operator<<(const T &value) {
    if (enabled_) {
      stream_ << value;
    }
    return *this;
  }

  // Manipulator support
  LogStream &operator<<(std::ostream &(*manip)(std::ostream &)) {
    if (enabled_) {
      manip(stream_);
    }
    return *this;
  }

private:
  Logger &logger_;
  LogLevel level_;
  SourceLocation location_;
  std::string_view category_;
  bool enabled_;
  std::ostringstream stream_;
};

// ============================================================================
// Printf-style Implementation
// ============================================================================

template <typename... Args>
void Logger::logf(LogLevel level, const SourceLocation &loc,
                  std::string_view category, const char *fmt, Args &&...args) {
  if (!isEnabled(level))
    return;

  // Calculate required size
  int size = std::snprintf(nullptr, 0, fmt, std::forward<Args>(args)...);
  if (size <= 0)
    return;

  std::string buffer(static_cast<size_t>(size) + 1, '\0');
  std::snprintf(buffer.data(), buffer.size(), fmt, std::forward<Args>(args)...);
  buffer.resize(static_cast<size_t>(size));

  log(level, buffer, loc, category);
}

// ============================================================================
// Hex Dump Utility
// ============================================================================

std::string hexDump(const void *data, size_t size, size_t bytes_per_line = 16);

} // namespace ai_pipe::logging

// ============================================================================
// Logging Macros
// ============================================================================

// Internal macro for source location capture
#define AI_PIPE_LOG_LOCATION                                                   \
  ::ai_pipe::logging::SourceLocation { __FILE__, __func__, __LINE__ }

// Check if log level is enabled at compile time
#define AI_PIPE_LOG_ENABLED(level)                                             \
  (::ai_pipe::logging::isLevelEnabled(::ai_pipe::logging::LogLevel::level))

// Basic logging macros - message only
#define LOG_TRACE(msg)                                                         \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Trace)) {                                \
      ::ai_pipe::logging::Logger::instance().trace(msg, AI_PIPE_LOG_LOCATION); \
    }                                                                          \
  } while (0)

#define LOG_DEBUG(msg)                                                         \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Debug)) {                                \
      ::ai_pipe::logging::Logger::instance().debug(msg, AI_PIPE_LOG_LOCATION); \
    }                                                                          \
  } while (0)

#define LOG_INFO(msg)                                                          \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Info)) {                                 \
      ::ai_pipe::logging::Logger::instance().info(msg, AI_PIPE_LOG_LOCATION);  \
    }                                                                          \
  } while (0)

#define LOG_WARNING(msg)                                                       \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Warning)) {                              \
      ::ai_pipe::logging::Logger::instance().warning(msg,                      \
                                                     AI_PIPE_LOG_LOCATION);    \
    }                                                                          \
  } while (0)

#define LOG_ERROR(msg)                                                         \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Error)) {                                \
      ::ai_pipe::logging::Logger::instance().error(msg, AI_PIPE_LOG_LOCATION); \
    }                                                                          \
  } while (0)

#define LOG_FATAL(msg)                                                         \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Fatal)) {                                \
      ::ai_pipe::logging::Logger::instance().fatal(msg, AI_PIPE_LOG_LOCATION); \
    }                                                                          \
  } while (0)

// Printf-style formatting macros
#define LOG_TRACE_FMT(fmt, ...)                                                \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Trace)) {                                \
      ::ai_pipe::logging::Logger::instance().logf(                             \
          ::ai_pipe::logging::LogLevel::Trace, AI_PIPE_LOG_LOCATION, {}, fmt,  \
          ##__VA_ARGS__);                                                      \
    }                                                                          \
  } while (0)

#define LOG_DEBUG_FMT(fmt, ...)                                                \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Debug)) {                                \
      ::ai_pipe::logging::Logger::instance().logf(                             \
          ::ai_pipe::logging::LogLevel::Debug, AI_PIPE_LOG_LOCATION, {}, fmt,  \
          ##__VA_ARGS__);                                                      \
    }                                                                          \
  } while (0)

#define LOG_INFO_FMT(fmt, ...)                                                 \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Info)) {                                 \
      ::ai_pipe::logging::Logger::instance().logf(                             \
          ::ai_pipe::logging::LogLevel::Info, AI_PIPE_LOG_LOCATION, {}, fmt,   \
          ##__VA_ARGS__);                                                      \
    }                                                                          \
  } while (0)

#define LOG_WARNING_FMT(fmt, ...)                                              \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Warning)) {                              \
      ::ai_pipe::logging::Logger::instance().logf(                             \
          ::ai_pipe::logging::LogLevel::Warning, AI_PIPE_LOG_LOCATION, {},     \
          fmt, ##__VA_ARGS__);                                                 \
    }                                                                          \
  } while (0)

#define LOG_ERROR_FMT(fmt, ...)                                                \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Error)) {                                \
      ::ai_pipe::logging::Logger::instance().logf(                             \
          ::ai_pipe::logging::LogLevel::Error, AI_PIPE_LOG_LOCATION, {}, fmt,  \
          ##__VA_ARGS__);                                                      \
    }                                                                          \
  } while (0)

#define LOG_FATAL_FMT(fmt, ...)                                                \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(Fatal)) {                                \
      ::ai_pipe::logging::Logger::instance().logf(                             \
          ::ai_pipe::logging::LogLevel::Fatal, AI_PIPE_LOG_LOCATION, {}, fmt,  \
          ##__VA_ARGS__);                                                      \
    }                                                                          \
  } while (0)

// Stream-style macros
#define LOG_TRACE_S                                                            \
  if constexpr (AI_PIPE_LOG_ENABLED(Trace))                                    \
  ::ai_pipe::logging::LogStream(::ai_pipe::logging::Logger::instance(),        \
                                ::ai_pipe::logging::LogLevel::Trace,           \
                                AI_PIPE_LOG_LOCATION)

#define LOG_DEBUG_S                                                            \
  if constexpr (AI_PIPE_LOG_ENABLED(Debug))                                    \
  ::ai_pipe::logging::LogStream(::ai_pipe::logging::Logger::instance(),        \
                                ::ai_pipe::logging::LogLevel::Debug,           \
                                AI_PIPE_LOG_LOCATION)

#define LOG_INFO_S                                                             \
  if constexpr (AI_PIPE_LOG_ENABLED(Info))                                     \
  ::ai_pipe::logging::LogStream(::ai_pipe::logging::Logger::instance(),        \
                                ::ai_pipe::logging::LogLevel::Info,            \
                                AI_PIPE_LOG_LOCATION)

#define LOG_WARNING_S                                                          \
  if constexpr (AI_PIPE_LOG_ENABLED(Warning))                                  \
  ::ai_pipe::logging::LogStream(::ai_pipe::logging::Logger::instance(),        \
                                ::ai_pipe::logging::LogLevel::Warning,         \
                                AI_PIPE_LOG_LOCATION)

#define LOG_ERROR_S                                                            \
  if constexpr (AI_PIPE_LOG_ENABLED(Error))                                    \
  ::ai_pipe::logging::LogStream(::ai_pipe::logging::Logger::instance(),        \
                                ::ai_pipe::logging::LogLevel::Error,           \
                                AI_PIPE_LOG_LOCATION)

#define LOG_FATAL_S                                                            \
  if constexpr (AI_PIPE_LOG_ENABLED(Fatal))                                    \
  ::ai_pipe::logging::LogStream(::ai_pipe::logging::Logger::instance(),        \
                                ::ai_pipe::logging::LogLevel::Fatal,           \
                                AI_PIPE_LOG_LOCATION)

// Category-based logging macros
#define LOG_CAT(level, cat, msg)                                               \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(level)) {                                \
      ::ai_pipe::logging::Logger::instance().log(                              \
          ::ai_pipe::logging::LogLevel::level, msg, AI_PIPE_LOG_LOCATION,      \
          cat);                                                                \
    }                                                                          \
  } while (0)

// Conditional logging (only evaluates expression if level is enabled)
#define LOG_IF(level, condition, msg)                                          \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(level)) {                                \
      if (condition) {                                                         \
        ::ai_pipe::logging::Logger::instance().log(                            \
            ::ai_pipe::logging::LogLevel::level, msg, AI_PIPE_LOG_LOCATION);   \
      }                                                                        \
    }                                                                          \
  } while (0)

// Log once (useful for warnings that shouldn't repeat)
#define LOG_ONCE(level, msg)                                                   \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(level)) {                                \
      static std::atomic<bool> logged{false};                                  \
      if (!logged.exchange(true, std::memory_order_relaxed)) {                 \
        ::ai_pipe::logging::Logger::instance().log(                            \
            ::ai_pipe::logging::LogLevel::level, msg, AI_PIPE_LOG_LOCATION);   \
      }                                                                        \
    }                                                                          \
  } while (0)

// Rate-limited logging (logs at most once per N milliseconds)
#define LOG_EVERY_MS(level, ms, msg)                                           \
  do {                                                                         \
    if constexpr (AI_PIPE_LOG_ENABLED(level)) {                                \
      static std::atomic<int64_t> last_log_time{0};                            \
      auto now = std::chrono::steady_clock::now().time_since_epoch().count();  \
      auto last = last_log_time.load(std::memory_order_relaxed);               \
      if (now - last >= static_cast<int64_t>(ms) * 1000000) {                  \
        if (last_log_time.compare_exchange_strong(                             \
                last, now, std::memory_order_relaxed)) {                       \
          ::ai_pipe::logging::Logger::instance().log(                          \
              ::ai_pipe::logging::LogLevel::level, msg, AI_PIPE_LOG_LOCATION); \
        }                                                                      \
      }                                                                        \
    }                                                                          \
  } while (0)

// ============================================================================
// Backward-Compatible Stream Macros (legacy naming: LOG_INFOS, LOG_DEBUG_S,
// etc.)
// ============================================================================

#define LOG_TRACES LOG_TRACE_S
#define LOG_DEBUGS LOG_DEBUG_S
#define LOG_INFOS LOG_INFO_S
#define LOG_WARNINGS LOG_WARNING_S
#define LOG_ERRORS LOG_ERROR_S
#define LOG_FATALS LOG_FATAL_S