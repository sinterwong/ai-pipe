#include "ai_pipe/engine_log.hpp"
#include "logger.hpp"

namespace ai_pipe {

namespace {

logging::LogLevel toInternalLevel(PipeLogLevel level) {
  switch (level) {
  case PipeLogLevel::KTrace:
    return logging::LogLevel::Trace;
  case PipeLogLevel::KDebug:
    return logging::LogLevel::Debug;
  case PipeLogLevel::KInfo:
    return logging::LogLevel::Info;
  case PipeLogLevel::KWarning:
    return logging::LogLevel::Warning;
  case PipeLogLevel::KError:
    break;
  }
  return logging::LogLevel::Error;
}

PipeLogLevel toPublicLevel(logging::LogLevel level) {
  switch (level) {
  case logging::LogLevel::Trace:
    return PipeLogLevel::KTrace;
  case logging::LogLevel::Debug:
    return PipeLogLevel::KDebug;
  case logging::LogLevel::Info:
    return PipeLogLevel::KInfo;
  case logging::LogLevel::Warning:
    return PipeLogLevel::KWarning;
  case logging::LogLevel::Error:
  case logging::LogLevel::Fatal:
  case logging::LogLevel::Off:
    break;
  }
  return PipeLogLevel::KError;
}

} // namespace

void setEngineLogLevel(PipeLogLevel level) {
  logging::Logger::instance().setLevel(toInternalLevel(level));
}

PipeLogLevel engineLogLevel() {
  return toPublicLevel(logging::Logger::instance().level());
}

void setEngineConsoleLogging(bool enabled) {
  logging::Logger::instance().enableConsole(enabled);
}

std::uint64_t addEngineLogSink(EngineLogSink sink) {
  return logging::Logger::instance().addCallback(
      [sink = std::move(sink)](const logging::LogEntry &entry) {
        if (sink) {
          sink(toPublicLevel(entry.level), entry.message);
        }
      });
}

void removeEngineLogSink(std::uint64_t id) {
  logging::Logger::instance().removeCallback(id);
}

} // namespace ai_pipe
