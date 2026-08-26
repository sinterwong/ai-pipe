#ifndef AI_PIPE_ENGINE_LOG_HPP
#define AI_PIPE_ENGINE_LOG_HPP

#include "ai_pipe/context.hpp"
#include <cstdint>
#include <functional>
#include <string>

namespace ai_pipe {

/**
 * @brief Sink observing the framework's own log records
 *
 * Invoked synchronously on the logging thread (or the async logger's
 * worker). Must not throw; exceptions are swallowed by the logger.
 */
using EngineLogSink =
    std::function<void(PipeLogLevel level, const std::string &message)>;

/** @brief Set the minimum severity of framework log output (global) */
void setEngineLogLevel(PipeLogLevel level);

/** @brief Current minimum severity of framework log output */
[[nodiscard]] PipeLogLevel engineLogLevel();

/**
 * @brief Enable/disable the framework logger's built-in console output
 *
 * Injected sinks keep receiving records either way; this only controls
 * the internal stdout/stderr writer.
 */
void setEngineConsoleLogging(bool enabled);

/**
 * @brief Register a process-wide sink for framework log records
 * @return Registration id for removeEngineLogSink()
 *
 * Records below the global level (setEngineLogLevel) are filtered
 * before reaching sinks.
 */
std::uint64_t addEngineLogSink(EngineLogSink sink);

/** @brief Remove a sink registered via addEngineLogSink() */
void removeEngineLogSink(std::uint64_t id);

} // namespace ai_pipe

#endif // AI_PIPE_ENGINE_LOG_HPP
