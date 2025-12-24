#include "logger.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>

#ifdef _WIN32
#include <windows.h>
#endif

namespace ai_pipe::logging {

// ============================================================================
// Logger Implementation
// ============================================================================

Logger::Logger() {
  initialized_.store(true, std::memory_order_release);

#ifdef _WIN32
  // Enable ANSI color support on Windows 10+
  HANDLE console = GetStdHandle(STD_OUTPUT_HANDLE);
  DWORD mode;
  if (GetConsoleMode(console, &mode)) {
    SetConsoleMode(console, mode | ENABLE_VIRTUAL_TERMINAL_PROCESSING);
  }
#endif
}

Logger::~Logger() { shutdown(); }

void Logger::configure(const LoggerConfig &config) {
  // Atomics can be set lock-free
  level_.store(config.min_level, std::memory_order_relaxed);
  console_enabled_.store(config.console_enabled, std::memory_order_relaxed);
  color_enabled_.store(config.color_enabled, std::memory_order_relaxed);
  json_enabled_.store(config.json_output, std::memory_order_relaxed);
  show_thread_id_.store(config.show_thread_id, std::memory_order_relaxed);
  show_source_.store(config.show_source_location, std::memory_order_relaxed);
  show_category_.store(config.show_category, std::memory_order_relaxed);

  bool need_file_reopen = false;
  bool need_async_change = false;

  {
    std::lock_guard lock(config_mutex_);
    need_file_reopen = (config_.file_path != config.file_path) ||
                       (config_.file_enabled != config.file_enabled);
    need_async_change = (config_.async_enabled != config.async_enabled);
    config_ = config;
  }

  // Handle file changes
  if (need_file_reopen) {
    std::lock_guard lock(file_mutex_);

    if (file_stream_.is_open()) {
      file_stream_.flush();
      file_stream_.close();
    }

    if (config.file_enabled) {
      // Create parent directories if needed
      auto parent = std::filesystem::path(config.file_path).parent_path();
      if (!parent.empty() && !std::filesystem::exists(parent)) {
        std::filesystem::create_directories(parent);
      }
      file_stream_.open(config.file_path, std::ios::out | std::ios::app);
    }

    file_enabled_.store(config.file_enabled, std::memory_order_relaxed);
  }

  // Handle async mode changes
  if (need_async_change) {
    if (config.async_enabled && !running_.load(std::memory_order_acquire)) {
      // Start async worker
      ring_buffer_ =
          std::make_unique<SPSCRingBuffer<LogEntry>>(config.async_queue_size);
      running_.store(true, std::memory_order_release);
      worker_thread_ =
          std::make_unique<std::thread>(&Logger::asyncWorker, this);
    } else if (!config.async_enabled &&
               running_.load(std::memory_order_acquire)) {
      // Stop async worker
      running_.store(false, std::memory_order_release);
      {
        std::lock_guard lock(worker_mutex_);
        worker_cv_.notify_one();
      }

      if (worker_thread_ && worker_thread_->joinable()) {
        worker_thread_->join();
      }
      worker_thread_.reset();

      // Drain remaining entries
      if (ring_buffer_) {
        LogEntry entry;
        while (ring_buffer_->tryPop(entry)) {
          processEntry(entry);
        }
        ring_buffer_.reset();
      }
    }

    async_enabled_.store(config.async_enabled, std::memory_order_release);
  }
}

LoggerConfig Logger::config() const {
  std::lock_guard lock(config_mutex_);
  return config_;
}

void Logger::setLevel(LogLevel level) noexcept {
  level_.store(level, std::memory_order_relaxed);
}

LogLevel Logger::level() const noexcept {
  return level_.load(std::memory_order_relaxed);
}

void Logger::enableConsole(bool enable) noexcept {
  console_enabled_.store(enable, std::memory_order_relaxed);
}

void Logger::enableFile(bool enable) {
  if (enable == file_enabled_.load(std::memory_order_relaxed)) {
    return;
  }

  std::lock_guard lock(file_mutex_);

  if (enable && !file_stream_.is_open()) {
    std::string path;
    {
      std::lock_guard config_lock(config_mutex_);
      path = config_.file_path;
    }
    file_stream_.open(path, std::ios::out | std::ios::app);
  } else if (!enable && file_stream_.is_open()) {
    file_stream_.flush();
    file_stream_.close();
  }

  file_enabled_.store(enable, std::memory_order_relaxed);
}

void Logger::enableColor(bool enable) noexcept {
  color_enabled_.store(enable, std::memory_order_relaxed);
}

void Logger::enableAsync(bool enable) {
  LoggerConfig cfg = config();
  cfg.async_enabled = enable;
  configure(cfg);
}

void Logger::enableJson(bool enable) noexcept {
  json_enabled_.store(enable, std::memory_order_relaxed);
}

void Logger::setFilePath(const std::string &path) {
  std::lock_guard config_lock(config_mutex_);
  if (config_.file_path == path)
    return;

  config_.file_path = path;

  std::lock_guard file_lock(file_mutex_);
  if (file_stream_.is_open()) {
    file_stream_.flush();
    file_stream_.close();
  }

  if (file_enabled_.load(std::memory_order_relaxed)) {
    auto parent = std::filesystem::path(path).parent_path();
    if (!parent.empty() && !std::filesystem::exists(parent)) {
      std::filesystem::create_directories(parent);
    }
    file_stream_.open(path, std::ios::out | std::ios::app);
  }
}

void Logger::setPattern(const std::string &pattern) {
  std::lock_guard lock(config_mutex_);
  config_.pattern = pattern;
}

void Logger::addCallback(LogCallback callback) {
  std::lock_guard lock(callback_mutex_);
  callbacks_.push_back(std::move(callback));
}

void Logger::clearCallbacks() noexcept {
  std::lock_guard lock(callback_mutex_);
  callbacks_.clear();
}

void Logger::log(LogLevel level, std::string_view message,
                 const SourceLocation &loc, std::string_view category) {
  if (!isEnabled(level))
    return;

  LogEntry entry(level, std::string(message), loc, category);

  if (async_enabled_.load(std::memory_order_acquire) && ring_buffer_) {
    // Try lock-free push; fall back to sync if queue full
    if (!ring_buffer_->tryPush(std::move(entry))) {
      // Queue full - process synchronously to avoid log loss
      processEntry(entry);
    } else {
      // Notify worker thread
      worker_cv_.notify_one();
    }
  } else {
    processEntry(entry);
  }
}

void Logger::trace(std::string_view msg, const SourceLocation &loc,
                   std::string_view cat) {
  log(LogLevel::Trace, msg, loc, cat);
}

void Logger::debug(std::string_view msg, const SourceLocation &loc,
                   std::string_view cat) {
  log(LogLevel::Debug, msg, loc, cat);
}

void Logger::info(std::string_view msg, const SourceLocation &loc,
                  std::string_view cat) {
  log(LogLevel::Info, msg, loc, cat);
}

void Logger::warning(std::string_view msg, const SourceLocation &loc,
                     std::string_view cat) {
  log(LogLevel::Warning, msg, loc, cat);
}

void Logger::error(std::string_view msg, const SourceLocation &loc,
                   std::string_view cat) {
  log(LogLevel::Error, msg, loc, cat);
}

void Logger::fatal(std::string_view msg, const SourceLocation &loc,
                   std::string_view cat) {
  log(LogLevel::Fatal, msg, loc, cat);
}

void Logger::flush() {
  // Flush async queue first
  if (async_enabled_.load(std::memory_order_acquire) && ring_buffer_) {
    // Signal worker and wait briefly for drain
    worker_cv_.notify_one();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Flush file
  {
    std::lock_guard lock(file_mutex_);
    if (file_stream_.is_open()) {
      file_stream_.flush();
    }
  }

  std::cout.flush();
  std::cerr.flush();
}

void Logger::shutdown() {
  if (!initialized_.exchange(false, std::memory_order_acq_rel)) {
    return;
  }

  // Stop async worker
  if (running_.exchange(false, std::memory_order_acq_rel)) {
    worker_cv_.notify_all();

    if (worker_thread_ && worker_thread_->joinable()) {
      worker_thread_->join();
    }
    worker_thread_.reset();

    // Drain remaining entries
    if (ring_buffer_) {
      LogEntry entry;
      while (ring_buffer_->tryPop(entry)) {
        processEntry(entry);
      }
      ring_buffer_.reset();
    }
  }

  // Clear callbacks
  {
    std::lock_guard lock(callback_mutex_);
    callbacks_.clear();
  }

  // Close file
  {
    std::lock_guard lock(file_mutex_);
    if (file_stream_.is_open()) {
      file_stream_.flush();
      file_stream_.close();
    }
  }
}

void Logger::processEntry(const LogEntry &entry) {
  if (console_enabled_.load(std::memory_order_relaxed)) {
    writeConsole(entry);
  }

  if (file_enabled_.load(std::memory_order_relaxed)) {
    writeFile(entry);
  }

  // Invoke callbacks
  {
    std::lock_guard lock(callback_mutex_);
    for (const auto &cb : callbacks_) {
      try {
        cb(entry);
      } catch (...) {
        // Ignore callback exceptions
      }
    }
  }
}

void Logger::writeConsole(const LogEntry &entry) {
  auto &buf = getThreadLocalBuffer();

  bool use_color = color_enabled_.load(std::memory_order_relaxed);

  if (json_enabled_.load(std::memory_order_relaxed)) {
    formatJson(buf, entry);
  } else {
    formatEntry(buf, entry, use_color);
  }

  // Error/Fatal to stderr, others to stdout
  if (entry.level >= LogLevel::Error) {
    std::cerr << buf.view() << '\n';
  } else {
    std::cout << buf.view() << '\n';
  }
}

void Logger::writeFile(const LogEntry &entry) {
  std::lock_guard lock(file_mutex_);

  if (!file_stream_.is_open())
    return;

  // Check rotation
  rotateFile();

  auto &buf = getThreadLocalBuffer();

  if (json_enabled_.load(std::memory_order_relaxed)) {
    formatJson(buf, entry);
  } else {
    formatEntry(buf, entry, false); // No color for files
  }

  file_stream_ << buf.view() << '\n';

  // Flush for Error/Fatal levels immediately
  if (entry.level >= LogLevel::Error) {
    file_stream_.flush();
  }
}

void Logger::asyncWorker() {
  LogEntry entry;
  std::vector<LogEntry> batch;
  batch.reserve(64);

  while (running_.load(std::memory_order_acquire)) {
    // Wait for entries or shutdown signal
    {
      std::unique_lock lock(worker_mutex_);
      worker_cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return !ring_buffer_->empty() ||
               !running_.load(std::memory_order_acquire);
      });
    }

    // Batch process entries for efficiency
    batch.clear();
    while (ring_buffer_->tryPop(entry) && batch.size() < 64) {
      batch.push_back(std::move(entry));
    }

    for (const auto &e : batch) {
      processEntry(e);
    }
  }

  // Final drain on shutdown
  while (ring_buffer_->tryPop(entry)) {
    processEntry(entry);
  }
}

void Logger::rotateFile() {
  if (!file_stream_.is_open())
    return;

  size_t max_size;
  int max_backups;
  std::string path;
  {
    std::lock_guard lock(config_mutex_);
    max_size = config_.max_file_size;
    max_backups = config_.max_backup_count;
    path = config_.file_path;
  }

  auto pos = file_stream_.tellp();
  if (pos < 0 || static_cast<size_t>(pos) < max_size) {
    return;
  }

  file_stream_.close();

  namespace fs = std::filesystem;

  // Rotate: app.log.5 -> delete, app.log.4 -> app.log.5, ..., app.log ->
  // app.log.1
  for (int i = max_backups - 1; i >= 0; --i) {
    std::string old_name = path + (i > 0 ? "." + std::to_string(i) : "");
    std::string new_name = path + "." + std::to_string(i + 1);

    if (fs::exists(old_name)) {
      if (i == max_backups - 1) {
        fs::remove(old_name);
      } else {
        fs::rename(old_name, new_name);
      }
    }
  }

  file_stream_.open(path, std::ios::out | std::ios::app);
}

void Logger::formatEntry(FormatBuffer &buf, const LogEntry &entry,
                         bool with_color) const {
  // Timestamp
  buf.append('[');
  formatTimestamp(buf, entry.timestamp);
  buf.append(']');

  // Level with optional color
  buf.append(" [");
  if (with_color) {
    buf.append(levelColor(entry.level));
  }

  auto level_str = levelName(entry.level);
  // Pad to 7 chars for alignment
  for (size_t i = level_str.size(); i < 7; ++i) {
    buf.append(' ');
  }
  buf.append(level_str);

  if (with_color) {
    buf.append(color::kReset);
  }
  buf.append(']');

  // Thread ID
  if (show_thread_id_.load(std::memory_order_relaxed)) {
    buf.append(" [T:");
    std::ostringstream oss;
    oss << entry.thread_id;
    buf.append(oss.str());
    buf.append(']');
  }

  // Category
  if (show_category_.load(std::memory_order_relaxed) &&
      !entry.category.empty()) {
    buf.append(" [");
    buf.append(entry.category);
    buf.append(']');
  }

  // Source location
  if (show_source_.load(std::memory_order_relaxed) &&
      entry.location.file != nullptr && entry.location.file[0] != '\0') {
    buf.append(" [");
    buf.append(extractFilename(entry.location.file));
    buf.append(':');
    buf.appendNumber(entry.location.line);

    if (entry.location.function != nullptr &&
        entry.location.function[0] != '\0') {
      buf.append(' ');
      buf.append(entry.location.function);
      buf.append("()");
    }
    buf.append(']');
  }

  // Message
  buf.append(' ');
  buf.append(entry.message);
}

void Logger::formatJson(FormatBuffer &buf, const LogEntry &entry) const {
  buf.append("{\"ts\":\"");
  formatTimestamp(buf, entry.timestamp);
  buf.append("\",\"level\":\"");
  buf.append(levelName(entry.level));
  buf.append("\"");

  if (show_thread_id_.load(std::memory_order_relaxed)) {
    buf.append(",\"thread\":\"");
    std::ostringstream oss;
    oss << entry.thread_id;
    buf.append(oss.str());
    buf.append("\"");
  }

  if (show_category_.load(std::memory_order_relaxed) &&
      !entry.category.empty()) {
    buf.append(",\"cat\":\"");
    buf.append(entry.category);
    buf.append("\"");
  }

  if (show_source_.load(std::memory_order_relaxed) &&
      entry.location.file != nullptr && entry.location.file[0] != '\0') {
    buf.append(",\"file\":\"");
    buf.append(extractFilename(entry.location.file));
    buf.append("\",\"line\":");
    buf.appendNumber(entry.location.line);

    if (entry.location.function != nullptr &&
        entry.location.function[0] != '\0') {
      buf.append(",\"func\":\"");
      buf.append(entry.location.function);
      buf.append("\"");
    }
  }

  // Escape message for JSON
  buf.append(",\"msg\":\"");
  for (char c : entry.message) {
    switch (c) {
    case '"':
      buf.append("\\\"");
      break;
    case '\\':
      buf.append("\\\\");
      break;
    case '\n':
      buf.append("\\n");
      break;
    case '\r':
      buf.append("\\r");
      break;
    case '\t':
      buf.append("\\t");
      break;
    default:
      buf.append(c);
      break;
    }
  }
  buf.append("\"}");
}

void Logger::formatTimestamp(FormatBuffer &buf,
                             std::chrono::system_clock::time_point tp) const {
  auto time_t_val = std::chrono::system_clock::to_time_t(tp);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                tp.time_since_epoch()) %
            1000;

  std::tm tm_val;
#ifdef _WIN32
  localtime_s(&tm_val, &time_t_val);
#else
  localtime_r(&time_t_val, &tm_val);
#endif

  // YYYY-MM-DD HH:MM:SS.mmm
  buf.appendNumber(tm_val.tm_year + 1900);
  buf.append('-');
  buf.appendPadded(tm_val.tm_mon + 1, 2);
  buf.append('-');
  buf.appendPadded(tm_val.tm_mday, 2);
  buf.append(' ');
  buf.appendPadded(tm_val.tm_hour, 2);
  buf.append(':');
  buf.appendPadded(tm_val.tm_min, 2);
  buf.append(':');
  buf.appendPadded(tm_val.tm_sec, 2);
  buf.append('.');
  buf.appendPadded(static_cast<int>(ms.count()), 3);
}

std::string_view Logger::levelName(LogLevel level) noexcept {
  switch (level) {
  case LogLevel::Trace:
    return "TRACE";
  case LogLevel::Debug:
    return "DEBUG";
  case LogLevel::Info:
    return "INFO";
  case LogLevel::Warning:
    return "WARNING";
  case LogLevel::Error:
    return "ERROR";
  case LogLevel::Fatal:
    return "FATAL";
  default:
    return "UNKNOWN";
  }
}

std::string_view Logger::levelColor(LogLevel level) noexcept {
  switch (level) {
  case LogLevel::Trace:
    return color::kGray;
  case LogLevel::Debug:
    return color::kCyan;
  case LogLevel::Info:
    return color::kGreen;
  case LogLevel::Warning:
    return color::kYellow;
  case LogLevel::Error:
    return color::kBoldRed;
  case LogLevel::Fatal:
    return color::kMagenta;
  default:
    return color::kWhite;
  }
}

std::string_view Logger::extractFilename(std::string_view path) noexcept {
  auto pos = path.find_last_of("/\\");
  if (pos != std::string_view::npos) {
    return path.substr(pos + 1);
  }
  return path;
}

// ============================================================================
// Hex Dump Utility
// ============================================================================

std::string hexDump(const void *data, size_t size, size_t bytes_per_line) {
  static constexpr char hex_chars[] = "0123456789ABCDEF";

  const auto *bytes = static_cast<const uint8_t *>(data);
  std::string result;
  result.reserve(size * 4); // Approximate

  for (size_t i = 0; i < size; i += bytes_per_line) {
    // Offset
    char offset_buf[16];
    std::snprintf(offset_buf, sizeof(offset_buf), "%08zx ", i);
    result += offset_buf;

    // Hex bytes
    for (size_t j = 0; j < bytes_per_line; ++j) {
      if (i + j < size) {
        result += hex_chars[bytes[i + j] >> 4];
        result += hex_chars[bytes[i + j] & 0x0F];
        result += ' ';
      } else {
        result += "   ";
      }

      if (j == 7)
        result += ' '; // Extra space in middle
    }

    result += " |";

    // ASCII representation
    for (size_t j = 0; j < bytes_per_line && i + j < size; ++j) {
      char c = static_cast<char>(bytes[i + j]);
      result += (c >= 32 && c < 127) ? c : '.';
    }

    result += "|\n";
  }

  return result;
}

} // namespace ai_pipe::logging