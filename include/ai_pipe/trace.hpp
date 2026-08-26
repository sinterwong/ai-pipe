#ifndef AI_PIPE_TRACE_HPP
#define AI_PIPE_TRACE_HPP

#include "ai_pipe/error.hpp"
#include "ai_pipe/frame_metadata.hpp"
#include <chrono>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace ai_pipe {

// Trace Event

/**
 * @brief Frame lifecycle phase of a trace event
 */
enum class TracePhase : std::uint8_t {
  Enqueue,   ///< Packet accepted into a node's input queue (instant)
  Schedule,  ///< Node READY -> worker pickup (span = scheduling delay)
  Execute,   ///< Node process() call (span)
  Propagate, ///< Outputs routed to downstream queues (span)
};

/**
 * @brief Convert trace phase to string
 */
inline std::string_view tracePhaseToString(TracePhase phase) {
  switch (phase) {
  case TracePhase::Enqueue:
    return "enqueue";
  case TracePhase::Schedule:
    return "schedule";
  case TracePhase::Execute:
    return "execute";
  case TracePhase::Propagate:
    return "propagate";
  }
  return "unknown";
}

/**
 * @brief A single per-frame span event
 *
 * The string_view fields alias engine-owned storage that is only
 * guaranteed to live for the duration of the onEvent() call: sinks
 * that retain events must copy them.
 */
struct TraceEvent {
  TracePhase phase{TracePhase::Execute};
  std::string_view node;   ///< Node name
  std::string_view detail; ///< Phase-specific info (e.g. port name)
  FrameId frame_id{0};     ///< 0 when unknown at emit time
  StreamId stream_id{0};
  Timestamp start{};                     ///< Span start (steady clock)
  std::chrono::microseconds duration{0}; ///< 0 for instant events
  std::uint64_t thread_id{0};            ///< Hashed executing thread id
};

// Trace Sink Interface

/**
 * @brief Consumer of engine trace events
 *
 * onEvent() is called from engine worker threads (and the ingress
 * thread for Enqueue events) concurrently, on the hot path:
 * implementations must be thread-safe and cheap. Blocking or heavy
 * work belongs in a downstream consumer, not in the sink.
 */
class ITraceSink {
public:
  virtual ~ITraceSink() = default;

  virtual void onEvent(const TraceEvent &event) = 0;
};

// Chrome Trace Event Sink

/**
 * @brief Built-in sink buffering events for Chrome Trace Event export
 *
 * Buffers every event in memory (mutex-protected; suitable for test
 * runs and bounded capture sessions, not for unbounded 7x24 tracing)
 * and serializes the Chrome Trace Event JSON format:
 * open chrome://tracing or https://ui.perfetto.dev and load the file.
 *
 * Span events map to "ph":"X" complete events; zero-duration events
 * (Enqueue) map to "ph":"i" instants. Frame and stream ids are
 * attached as args, so Perfetto queries can slice by frame.
 */
class ChromeTraceSink final : public ITraceSink {
public:
  void onEvent(const TraceEvent &event) override {
    Record record;
    record.phase = event.phase;
    record.node.assign(event.node);
    record.detail.assign(event.detail);
    record.frame_id = event.frame_id;
    record.stream_id = event.stream_id;
    record.start_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          event.start.time_since_epoch())
                          .count();
    record.duration_us = event.duration.count();
    record.thread_id = event.thread_id;

    std::lock_guard<std::mutex> lock(m_mutex);
    m_records.push_back(std::move(record));
  }

  /** @brief Number of buffered events */
  [[nodiscard]] std::size_t eventCount() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_records.size();
  }

  /** @brief Discard all buffered events */
  void clear() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_records.clear();
  }

  /** @brief Serialize buffered events as Chrome Trace Event JSON */
  [[nodiscard]] std::string toJson() const {
    std::vector<Record> records;
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      records = m_records;
    }

    std::ostringstream out;
    out << "{\"traceEvents\":[";
    bool first = true;
    for (const auto &record : records) {
      if (!first) {
        out << ",";
      }
      first = false;
      out << "{\"name\":\"" << escape(record.node) << "\",\"cat\":\""
          << tracePhaseToString(record.phase) << "\",\"ph\":\""
          << (record.duration_us > 0 ? "X" : "i")
          << "\",\"ts\":" << record.start_us
          << ",\"pid\":1,\"tid\":" << record.thread_id;
      if (record.duration_us > 0) {
        out << ",\"dur\":" << record.duration_us;
      } else {
        out << ",\"s\":\"t\"";
      }
      out << ",\"args\":{\"frame_id\":" << record.frame_id
          << ",\"stream_id\":" << record.stream_id << ",\"detail\":\""
          << escape(record.detail) << "\"}}";
    }
    out << "]}";
    return out.str();
  }

  /** @brief Write toJson() to a file */
  Result<void> writeFile(const std::string &path) const {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    if (!file) {
      return Result<void>::err(ErrorCode::InvalidArgument,
                               "Cannot open trace file for writing: " + path);
    }
    file << toJson();
    if (!file.good()) {
      return Result<void>::err(ErrorCode::InvalidArgument,
                               "Failed writing trace file: " + path);
    }
    return Result<void>::ok();
  }

private:
  struct Record {
    TracePhase phase{TracePhase::Execute};
    std::string node;
    std::string detail;
    FrameId frame_id{0};
    StreamId stream_id{0};
    std::int64_t start_us{0};
    std::int64_t duration_us{0};
    std::uint64_t thread_id{0};
  };

  static std::string escape(const std::string &raw) {
    std::string escaped;
    escaped.reserve(raw.size());
    for (const char character : raw) {
      switch (character) {
      case '"':
        escaped += "\\\"";
        break;
      case '\\':
        escaped += "\\\\";
        break;
      case '\n':
        escaped += "\\n";
        break;
      case '\t':
        escaped += "\\t";
        break;
      default:
        if (static_cast<unsigned char>(character) < 0x20) {
          escaped += ' ';
        } else {
          escaped += character;
        }
        break;
      }
    }
    return escaped;
  }

  mutable std::mutex m_mutex;
  std::vector<Record> m_records;
};

} // namespace ai_pipe

#endif // AI_PIPE_TRACE_HPP
