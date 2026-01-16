/**
 * @file logger_adapter.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Logger adapter for glog-based logging systems
 * @version 0.1
 * @date 2025-12-15
 *
 * @copyright Copyright (c) 2025
 *
 * Usage:
 * @code
 *   #include "logger_adapter.hpp"
 *
 *   auto ctx = std::make_shared<PipelineContext>();
 *   ctx->setLoggerAdapter(std::make_shared<LoggerAdapter>());
 *
 *   // Now ctx->logInfo("node", "message") will use your logger wrapper
 * @endcode
 */

#ifndef AI_PIPE_GLOG_LOGGER_ADAPTER_HPP
#define AI_PIPE_GLOG_LOGGER_ADAPTER_HPP

#include "ai_pipe/context.hpp"
#include "logger.hpp"

namespace ai_pipe {

/**
 * @brief Logger adapter that bridges to glog wrapper
 *
 * Converts PipelineContext log calls to your existing glog-based
 * LOG_INFO_S, LOG_ERROR_S, etc. macros.
 */
class LoggerAdapter : public ILoggerAdapter {
public:
  /**
   * @brief Configuration for log formatting
   */
  struct Config {
    bool include_node_name = true;     // Include [NodeName] prefix
    bool include_execution_id = false; // Include execution ID
    std::string prefix = "";           // Optional custom prefix
  };

  LoggerAdapter() = default;
  explicit LoggerAdapter(Config config) : m_config(std::move(config)) {}

  void log(PipeLogLevel level, const std::string &node_name,
           const std::string &message) override {
    // Format message
    std::string formatted = formatMessage(node_name, message);

    // Route to appropriate glog level
    switch (level) {
    case PipeLogLevel::KTrace:
      LOG_TRACE_S << formatted;
      break;

    case PipeLogLevel::KDebug:
      LOG_DEBUG_S << formatted;
      break;

    case PipeLogLevel::KInfo:
      LOG_INFO_S << formatted;
      break;

    case PipeLogLevel::KWarning:
      LOG_WARNING_S << formatted;
      break;

    case PipeLogLevel::KError:
      LOG_ERROR_S << formatted;
      break;
    }
  }

private:
  std::string formatMessage(const std::string &node_name,
                            const std::string &message) const {
    std::string result;

    if (!m_config.prefix.empty()) {
      result += m_config.prefix + " ";
    }

    if (m_config.include_node_name && !node_name.empty()) {
      result += "[" + node_name + "] ";
    }

    result += message;
    return result;
  }

  Config m_config;
};

/**
 * @brief Convenience function to create and set glog adapter
 */
inline void setupLoggerAdapter(std::shared_ptr<PipelineContext> ctx,
                               LoggerAdapter::Config config = {}) {
  ctx->setLoggerAdapter(std::make_shared<LoggerAdapter>(std::move(config)));
}

} // namespace ai_pipe

#endif // AI_PIPE_GLOG_LOGGER_ADAPTER_HPP