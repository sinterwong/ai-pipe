/**
 * @file engine_log.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Public control surface for the framework's own logging (R2.3)
 * @version 0.1
 * @date 2026-07-18
 *
 * The engine and facade log through a process-wide internal logger
 * (logger.hpp, a private header). This file is the public control
 * surface over that channel: linkers can set the global severity
 * threshold, silence the built-in console output, and inject sinks
 * that observe every framework log record - without reaching into
 * private headers.
 *
 * Relation to PipelineContext::ILoggerAdapter: the adapter is the
 * per-context channel for *node* logging (ctx->logInfo(...)), and
 * PipelineContext::attachEngineLogs() bridges framework logs into that
 * adapter per context. The functions here are process-wide and
 * context-free - use them when there is no context (or before one
 * exists), e.g. to silence the framework in a library consumer.
 *
 * Follow-up direction (recorded, not implemented): unify the two
 * channels by routing the framework's internal logging through the
 * adapter abstraction itself, making the internal logger an
 * implementation detail of one adapter. Until then this surface and
 * attachEngineLogs() are thin bridges over Logger::addCallback.
 *
 * @copyright Copyright (c) 2026
 */
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
