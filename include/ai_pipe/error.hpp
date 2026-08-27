#ifndef AI_PIPE_ERROR_HPP
#define AI_PIPE_ERROR_HPP

#include <cassert>
#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>

namespace ai_pipe {

// Error Code Enumeration

/**
 * @brief Categorized error codes for the AI Pipe framework.
 *
 * Error codes are grouped by domain:
 *   0xx - General
 *   1xx - Configuration / initialization
 *   2xx - Execution
 *   3xx - Queue / streaming
 *   4xx - Node-level
 *   5xx - Plugin loading
 */
enum class ErrorCode : std::uint16_t {
  // General
  Ok = 0,
  InternalError = 1,

  // Configuration / Initialization
  InvalidArgument = 100,
  InvalidState = 101,
  NotInitialized = 102,
  AlreadyInitialized = 103,
  GraphCycleDetected = 104,
  GraphEmpty = 105,
  InvalidConfiguration = 106,

  // Execution
  AlreadyRunning = 200,
  ExecutionFailed = 201,
  ExecutionTimeout = 202,
  ExecutionStopped = 203,
  ExecutionAborted = 204,

  // Queue / Streaming
  StreamingNotSupported = 300,
  NotStreaming = 301,
  QueueRejected = 302,
  QueueFull = 303,
  NodeNotFound = 304,
  PortNotFound = 305,
  NoDownstreamConnection = 306,
  /// Port already carries an end-of-stream latch; later data is rejected.
  EndOfStreamSignaled = 307,

  // Node-level
  NodeException = 400,
  NodeUnknownException = 401,
  InputUnavailable = 402,

  // Plugin loading
  PluginLoadFailed = 500,
  PluginSymbolMissing = 501,
  PluginVersionMismatch = 502,
  /// Reserved for source compatibility; resident plugins are never unmapped.
  PluginInUse = 503,
};

/**
 * @brief Convert error code to human-readable string.
 */
inline const char *errorCodeToString(ErrorCode code) {
  switch (code) {
  case ErrorCode::Ok:
    return "Ok";
  case ErrorCode::InternalError:
    return "InternalError";
  case ErrorCode::InvalidArgument:
    return "InvalidArgument";
  case ErrorCode::InvalidState:
    return "InvalidState";
  case ErrorCode::NotInitialized:
    return "NotInitialized";
  case ErrorCode::AlreadyInitialized:
    return "AlreadyInitialized";
  case ErrorCode::GraphCycleDetected:
    return "GraphCycleDetected";
  case ErrorCode::GraphEmpty:
    return "GraphEmpty";
  case ErrorCode::InvalidConfiguration:
    return "InvalidConfiguration";
  case ErrorCode::AlreadyRunning:
    return "AlreadyRunning";
  case ErrorCode::ExecutionFailed:
    return "ExecutionFailed";
  case ErrorCode::ExecutionTimeout:
    return "ExecutionTimeout";
  case ErrorCode::ExecutionStopped:
    return "ExecutionStopped";
  case ErrorCode::ExecutionAborted:
    return "ExecutionAborted";
  case ErrorCode::StreamingNotSupported:
    return "StreamingNotSupported";
  case ErrorCode::NotStreaming:
    return "NotStreaming";
  case ErrorCode::QueueRejected:
    return "QueueRejected";
  case ErrorCode::QueueFull:
    return "QueueFull";
  case ErrorCode::NodeNotFound:
    return "NodeNotFound";
  case ErrorCode::PortNotFound:
    return "PortNotFound";
  case ErrorCode::NoDownstreamConnection:
    return "NoDownstreamConnection";
  case ErrorCode::EndOfStreamSignaled:
    return "EndOfStreamSignaled";
  case ErrorCode::NodeException:
    return "NodeException";
  case ErrorCode::NodeUnknownException:
    return "NodeUnknownException";
  case ErrorCode::InputUnavailable:
    return "InputUnavailable";
  case ErrorCode::PluginLoadFailed:
    return "PluginLoadFailed";
  case ErrorCode::PluginSymbolMissing:
    return "PluginSymbolMissing";
  case ErrorCode::PluginVersionMismatch:
    return "PluginVersionMismatch";
  case ErrorCode::PluginInUse:
    return "PluginInUse";
  }
  return "Unknown";
}

// Error Type

/**
 * @brief Rich error type carrying code, message, and optional context.
 *
 * Error is designed to be lightweight and copyable. The message string
 * is only allocated when there is actually an error, so the success
 * path has zero string allocation overhead.
 */
class Error {
public:
  Error() : m_code(ErrorCode::Ok) {}

  explicit Error(ErrorCode code) : m_code(code) {}

  Error(ErrorCode code, std::string message)
      : m_code(code), m_message(std::move(message)) {}

  Error(ErrorCode code, std::string message, std::string node_name)
      : m_code(code), m_message(std::move(message)),
        m_nodeName(std::move(node_name)) {}

  // Accessors

  [[nodiscard]] ErrorCode code() const noexcept { return m_code; }

  [[nodiscard]] const std::string &message() const noexcept {
    return m_message;
  }

  [[nodiscard]] const std::string &nodeName() const noexcept {
    return m_nodeName;
  }

  [[nodiscard]] bool isOk() const noexcept { return m_code == ErrorCode::Ok; }

  explicit operator bool() const noexcept { return !isOk(); }

  // Formatted output

  [[nodiscard]] std::string toString() const {
    std::string result = errorCodeToString(m_code);
    if (!m_message.empty()) {
      result += ": " + m_message;
    }
    if (!m_nodeName.empty()) {
      result += " [node: " + m_nodeName + "]";
    }
    return result;
  }

  // Comparison

  [[nodiscard]] bool operator==(ErrorCode code) const noexcept {
    return m_code == code;
  }

  [[nodiscard]] bool operator!=(ErrorCode code) const noexcept {
    return m_code != code;
  }

  // Convenience factories

  static Error ok() { return Error{}; }

  static Error invalidArgument(const std::string &msg) {
    return {ErrorCode::InvalidArgument, msg};
  }

  static Error invalidState(const std::string &msg) {
    return {ErrorCode::InvalidState, msg};
  }

  static Error notInitialized(const std::string &msg = "Not initialized") {
    return {ErrorCode::NotInitialized, msg};
  }

  static Error alreadyRunning(const std::string &msg = "Already running") {
    return {ErrorCode::AlreadyRunning, msg};
  }

  static Error executionFailed(const std::string &msg,
                               const std::string &node = "") {
    return {ErrorCode::ExecutionFailed, msg, node};
  }

  static Error nodeException(const std::string &msg, const std::string &node) {
    return {ErrorCode::NodeException, msg, node};
  }

  static Error nodeNotFound(const std::string &node_name) {
    return {ErrorCode::NodeNotFound, "Unknown node: " + node_name, node_name};
  }

  static Error portNotFound(const std::string &port_name,
                            const std::string &node_name) {
    return {ErrorCode::PortNotFound,
            "Port '" + port_name + "' not found on node: " + node_name,
            node_name};
  }

private:
  ErrorCode m_code;
  std::string m_message;
  std::string m_nodeName;
};

// Result<T> - Primary (non-void specialization)

/**
 * Holds either a value of type `T` or an `Error`.
 *
 * Usage:
 * @code
 *   auto result = engine->execute(inputs);
 *   if (result) {
 *     // use result.value()
 *   } else {
 *     LOG_ERROR_S << result.error().toString();
 *   }
 *
 *   // Or with value_or:
 *   auto output = pipeline.run(inputs).value_or(defaultOutput);
 * @endcode
 *
 * @tparam T The success value type. Must be move-constructible.
 *           Must not be ai_pipe::Error (use Result<void> instead).
 */
template <typename T> class Result {
  static_assert(std::is_move_constructible_v<T>,
                "Result<T> requires T to be move-constructible");
  static_assert(!std::is_same_v<std::decay_t<T>, Error>,
                "Result<Error> is not supported; use Result<void>");

public:
  // Success constructors

  Result(T value) : m_hasValue(true) { new (&m_value) T(std::move(value)); }

  static Result ok(T value) { return Result(std::move(value)); }

  // Error constructors

  Result(Error error) : m_hasValue(false) {
    new (&m_error) Error(std::move(error));
  }

  static Result err(Error error) { return Result(std::move(error)); }

  static Result err(ErrorCode code, std::string message = {}) {
    return Result(Error(code, std::move(message)));
  }

  static Result err(ErrorCode code, std::string message,
                    std::string node_name) {
    return Result(Error(code, std::move(message), std::move(node_name)));
  }

  // Destructor

  ~Result() { destroy(); }

  // Copy

  Result(const Result &other) : m_hasValue(other.m_hasValue) {
    if (m_hasValue) {
      new (&m_value) T(other.m_value);
    } else {
      new (&m_error) Error(other.m_error);
    }
  }

  Result &operator=(const Result &other) {
    if (this != &other) {
      destroy();
      m_hasValue = other.m_hasValue;
      if (m_hasValue) {
        new (&m_value) T(other.m_value);
      } else {
        new (&m_error) Error(other.m_error);
      }
    }
    return *this;
  }

  // Move

  Result(Result &&other) noexcept(std::is_nothrow_move_constructible_v<T>)
      : m_hasValue(other.m_hasValue) {
    if (m_hasValue) {
      new (&m_value) T(std::move(other.m_value));
    } else {
      new (&m_error) Error(std::move(other.m_error));
    }
  }

  Result &
  operator=(Result &&other) noexcept(std::is_nothrow_move_assignable_v<T>) {
    if (this != &other) {
      destroy();
      m_hasValue = other.m_hasValue;
      if (m_hasValue) {
        new (&m_value) T(std::move(other.m_value));
      } else {
        new (&m_error) Error(std::move(other.m_error));
      }
    }
    return *this;
  }

  // Query

  [[nodiscard]] bool isOk() const noexcept { return m_hasValue; }

  [[nodiscard]] bool isErr() const noexcept { return !m_hasValue; }

  explicit operator bool() const noexcept { return m_hasValue; }

  // Value access

  [[nodiscard]] T &value() & {
    assert(m_hasValue && "Result::value() called on error");
    return m_value;
  }

  [[nodiscard]] const T &value() const & {
    assert(m_hasValue && "Result::value() called on error");
    return m_value;
  }

  [[nodiscard]] T &&value() && {
    assert(m_hasValue && "Result::value() called on error");
    return std::move(m_value);
  }

  [[nodiscard]] T valueOr(T default_value) const & {
    if (m_hasValue) {
      return m_value;
    }
    return default_value;
  }

  [[nodiscard]] T valueOr(T default_value) && {
    if (m_hasValue) {
      return std::move(m_value);
    }
    return default_value;
  }

  // Error access

  [[nodiscard]] const Error &error() const & {
    assert(!m_hasValue && "Result::error() called on success");
    return m_error;
  }

  [[nodiscard]] Error &error() & {
    assert(!m_hasValue && "Result::error() called on success");
    return m_error;
  }

  // Convenience

  [[nodiscard]] ErrorCode errorCode() const noexcept {
    if (!m_hasValue) {
      return m_error.code();
    }
    return ErrorCode::Ok;
  }

  [[nodiscard]] std::string errorMessage() const {
    if (!m_hasValue) {
      return m_error.toString();
    }
    return {};
  }

private:
  void destroy() {
    if (m_hasValue) {
      m_value.~T();
    } else {
      m_error.~Error();
    }
  }

  bool m_hasValue;
  union {
    T m_value;
    Error m_error;
  };
};

// Result<void> - Specialization for operations with no return value

/**
 * @brief Specialization of Result for void operations (init, start, stop...).
 *
 * Usage:
 * @code
 *   Result<void> result = engine->initialize(graph, 4);
 *   if (!result) {
 *     LOG_ERROR_S << result.error().toString();
 *   }
 * @endcode
 */
template <> class Result<void> {
public:
  // Success

  Result() : m_error(ErrorCode::Ok) {}

  static Result ok() { return Result(); }

  // Error

  Result(Error error) : m_error(std::move(error)) {}

  static Result err(Error error) { return Result(std::move(error)); }

  static Result err(ErrorCode code, std::string message = {}) {
    return Result(Error(code, std::move(message)));
  }

  static Result err(ErrorCode code, std::string message,
                    std::string node_name) {
    return Result(Error(code, std::move(message), std::move(node_name)));
  }

  // Query

  [[nodiscard]] bool isOk() const noexcept { return m_error.isOk(); }

  [[nodiscard]] bool isErr() const noexcept { return !isOk(); }

  explicit operator bool() const noexcept { return isOk(); }

  // Error access

  [[nodiscard]] const Error &error() const & { return m_error; }

  [[nodiscard]] Error &error() & { return m_error; }

  [[nodiscard]] ErrorCode errorCode() const noexcept { return m_error.code(); }

  [[nodiscard]] std::string errorMessage() const {
    if (isErr()) {
      return m_error.toString();
    }
    return {};
  }

private:
  Error m_error;
};

// Convenience alias

/** @brief Result type for void-returning operations. */
using VoidResult = Result<void>;

} // namespace ai_pipe

#endif // AI_PIPE_ERROR_HPP
