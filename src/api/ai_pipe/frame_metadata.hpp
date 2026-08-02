/**
 * @file frame_metadata.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Frame identity types and metadata abstractions for cross-branch
 * synchronization
 * @version 0.1
 * @date 2025-12-24
 *
 * This file defines the metadata abstraction required for synchronized
 * frame dropping across parallel branches in a DAG pipeline.
 *
 * @copyright Copyright (c) 2025
 *
 */

#ifndef AI_PIPE_FRAME_METADATA_HPP
#define AI_PIPE_FRAME_METADATA_HPP

#include <atomic>
#include <chrono>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

namespace ai_pipe {

// =============================================================================
// Frame Identifier Types
// =============================================================================

/**
 * @brief Unique frame identifier for synchronization
 *
 * Frame IDs should be monotonically increasing within a stream.
 * Special values:
 *   - 0: Invalid/unset frame ID
 *   - UINT64_MAX: End-of-stream marker
 */
using FrameId = std::uint64_t;

/**
 * @brief Stream identifier for multi-source scenarios
 */
using StreamId = std::uint32_t;

/**
 * @brief High-resolution timestamp type
 */
using Timestamp = std::chrono::steady_clock::time_point;

// =============================================================================
// Constants
// =============================================================================

namespace frame_constants {

/// Invalid frame ID marker
constexpr FrameId k_invalid_frame_id = 0;

/**
 * End-of-stream frame ID marker.
 *
 * Scope note (settled by R6.1): this id is NOT how the engine signals
 * end of stream. The engine-level protocol is a per-input-port EOS
 * latch (Pipeline::signalEndOfStream / ILogicNode::onEndOfStream), not
 * a packet id - an in-band marker packet would be silently eaten by the
 * queue drop policies it has to survive. See docs/design/eos_flush.md
 * §4 for why that alternative was rejected.
 *
 * The constant therefore retains exactly two uses:
 *
 * 1. The sentinel maximum in the sync coordinator's watermark
 *    computation (engine-internal).
 * 2. A payload-level "last frame" tag that nodes may set and read
 *    themselves, via IFrameMetadata::isEndOfStream() /
 *    createEndOfStream(). The engine does not interpret it: stamping a
 *    packet with this id does not trigger flush or EOS propagation.
 */
constexpr FrameId k_end_of_stream_frame_id =
    std::numeric_limits<FrameId>::max();

/// Default stream ID for single-source scenarios
constexpr StreamId k_default_stream_id = 0;

/// Maximum allowed frame ID drift for synchronization
constexpr FrameId k_max_frame_drift = 100;

} // namespace frame_constants

// =============================================================================
// Frame Metadata Interface
// =============================================================================

/**
 * @brief Abstract interface for frame metadata
 *
 * Provides the essential information needed for:
 * - Frame identification and ordering
 * - Multi-stream synchronization
 * - Aligned frame dropping across parallel branches
 *
 * Implementations can extend this with domain-specific metadata
 * (e.g., video codec info, audio sample rate, etc.)
 */
class IFrameMetadata {
public:
  virtual ~IFrameMetadata() = default;

  // -------------------------------------------------------------------------
  // Core Identification
  // -------------------------------------------------------------------------

  /**
   * @brief Get the unique frame identifier
   * @return Frame ID (monotonically increasing within stream)
   */
  [[nodiscard]] virtual FrameId frameId() const = 0;

  /**
   * @brief Get the source stream identifier
   * @return Stream ID for multi-source scenarios
   */
  [[nodiscard]] virtual StreamId streamId() const = 0;

  /**
   * @brief Get the frame timestamp
   * @return Timestamp when the frame was captured/created
   */
  [[nodiscard]] virtual Timestamp timestamp() const = 0;

  // -------------------------------------------------------------------------
  // Synchronization Support
  // -------------------------------------------------------------------------

  /**
   * @brief Check if this frame should synchronize with another
   * @param other The other frame metadata to compare
   * @return true if frames should be processed together
   */
  [[nodiscard]] virtual bool
  shouldSyncWith(const IFrameMetadata &other) const = 0;

  /**
   * @brief Compare frame ordering
   * @param other The other frame metadata
   * @return <0 if this is earlier, 0 if same, >0 if this is later
   */
  [[nodiscard]] virtual int compareTo(const IFrameMetadata &other) const = 0;

  // -------------------------------------------------------------------------
  // Validity Checks
  // -------------------------------------------------------------------------

  /**
   * @brief Check if frame ID is valid
   */
  [[nodiscard]] virtual bool isValid() const {
    return frameId() != frame_constants::k_invalid_frame_id;
  }

  /**
   * @brief Check if this is a payload-level end-of-stream tag
   *
   * This is a node-to-node convention, not the engine's EOS protocol:
   * the engine never inspects it. For real end-of-stream handling use
   * Pipeline::signalEndOfStream() and ILogicNode::onEndOfStream() - see
   * the scope note at frame_constants::k_end_of_stream_frame_id.
   */
  [[nodiscard]] virtual bool isEndOfStream() const {
    return frameId() == frame_constants::k_end_of_stream_frame_id;
  }

  // -------------------------------------------------------------------------
  // Cloning
  // -------------------------------------------------------------------------

  /**
   * @brief Create a deep copy of this metadata
   */
  [[nodiscard]] virtual std::unique_ptr<IFrameMetadata> clone() const = 0;

  // -------------------------------------------------------------------------
  // Debug Support
  // -------------------------------------------------------------------------

  /**
   * @brief Get string representation for logging
   */
  [[nodiscard]] virtual std::string toString() const = 0;
};

// =============================================================================
// Basic Frame Metadata Implementation
// =============================================================================

/**
 * @brief Basic frame metadata implementation
 *
 * Suitable for most use cases where synchronization is based on
 * frame ID matching within configurable tolerance.
 */
class BasicFrameMetadata final : public IFrameMetadata {
public:
  /**
   * @brief Default constructor (invalid metadata)
   */
  BasicFrameMetadata()
      : m_frameId(frame_constants::k_invalid_frame_id),
        m_streamId(frame_constants::k_default_stream_id),
        m_timestamp(std::chrono::steady_clock::now()) {}

  /**
   * @brief Construct with frame ID
   */
  explicit BasicFrameMetadata(
      FrameId frame_id,
      StreamId stream_id = frame_constants::k_default_stream_id)
      : m_frameId(frame_id), m_streamId(stream_id),
        m_timestamp(std::chrono::steady_clock::now()) {}

  /**
   * @brief Construct with all parameters
   */
  BasicFrameMetadata(FrameId frame_id, StreamId stream_id, Timestamp timestamp)
      : m_frameId(frame_id), m_streamId(stream_id), m_timestamp(timestamp) {}

  // Copy and move
  BasicFrameMetadata(const BasicFrameMetadata &) = default;
  BasicFrameMetadata &operator=(const BasicFrameMetadata &) = default;
  BasicFrameMetadata(BasicFrameMetadata &&) noexcept = default;
  BasicFrameMetadata &operator=(BasicFrameMetadata &&) noexcept = default;

  // IFrameMetadata interface
  [[nodiscard]] FrameId frameId() const override { return m_frameId; }
  [[nodiscard]] StreamId streamId() const override { return m_streamId; }
  [[nodiscard]] Timestamp timestamp() const override { return m_timestamp; }

  [[nodiscard]] bool
  shouldSyncWith(const IFrameMetadata &other) const override {
    // Sync if frame IDs match exactly
    return frameId() == other.frameId();
  }

  [[nodiscard]] int compareTo(const IFrameMetadata &other) const override {
    if (frameId() < other.frameId())
      return -1;
    if (frameId() > other.frameId())
      return 1;

    // If frame IDs are equal, compare by timestamp
    if (timestamp() < other.timestamp())
      return -1;
    if (timestamp() > other.timestamp())
      return 1;

    return 0;
  }

  [[nodiscard]] std::unique_ptr<IFrameMetadata> clone() const override {
    return std::make_unique<BasicFrameMetadata>(*this);
  }

  [[nodiscard]] std::string toString() const override {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  m_timestamp.time_since_epoch())
                  .count();
    return "Frame{id=" + std::to_string(m_frameId) +
           ", stream=" + std::to_string(m_streamId) +
           ", ts=" + std::to_string(ms) + "ms}";
  }

  // Setters for modification
  void setFrameId(FrameId id) { m_frameId = id; }
  void setStreamId(StreamId id) { m_streamId = id; }
  void setTimestamp(Timestamp ts) { m_timestamp = ts; }

private:
  FrameId m_frameId;
  StreamId m_streamId;
  Timestamp m_timestamp;
};

// =============================================================================
// Timestamp-Based Frame Metadata
// =============================================================================

/**
 * @brief Timestamp-based frame metadata for time-synchronized streams
 *
 * Useful when multiple streams don't share frame IDs but need to be
 * synchronized based on capture time (e.g., multi-camera systems).
 */
class TimestampFrameMetadata final : public IFrameMetadata {
public:
  /**
   * @brief Synchronization tolerance for timestamp matching
   */
  static constexpr auto k_default_sync_tolerance =
      std::chrono::milliseconds{33};

  TimestampFrameMetadata()
      : m_frameId(frame_constants::k_invalid_frame_id),
        m_streamId(frame_constants::k_default_stream_id),
        m_timestamp(std::chrono::steady_clock::now()),
        m_syncTolerance(k_default_sync_tolerance) {}

  TimestampFrameMetadata(
      FrameId frame_id, StreamId stream_id, Timestamp timestamp,
      std::chrono::milliseconds sync_tolerance = k_default_sync_tolerance)
      : m_frameId(frame_id), m_streamId(stream_id), m_timestamp(timestamp),
        m_syncTolerance(sync_tolerance) {}

  [[nodiscard]] FrameId frameId() const override { return m_frameId; }
  [[nodiscard]] StreamId streamId() const override { return m_streamId; }
  [[nodiscard]] Timestamp timestamp() const override { return m_timestamp; }

  [[nodiscard]] bool
  shouldSyncWith(const IFrameMetadata &other) const override {
    // Sync based on timestamp proximity
    auto diff = std::chrono::abs(timestamp() - other.timestamp());
    return diff <= m_syncTolerance;
  }

  [[nodiscard]] int compareTo(const IFrameMetadata &other) const override {
    // Compare primarily by timestamp for time-based sync
    if (timestamp() < other.timestamp())
      return -1;
    if (timestamp() > other.timestamp())
      return 1;
    return 0;
  }

  [[nodiscard]] std::unique_ptr<IFrameMetadata> clone() const override {
    return std::make_unique<TimestampFrameMetadata>(*this);
  }

  [[nodiscard]] std::string toString() const override {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  m_timestamp.time_since_epoch())
                  .count();
    return "TimestampFrame{id=" + std::to_string(m_frameId) +
           ", stream=" + std::to_string(m_streamId) +
           ", ts=" + std::to_string(ms) + "ms}";
  }

  void setSyncTolerance(std::chrono::milliseconds tolerance) {
    m_syncTolerance = tolerance;
  }

private:
  FrameId m_frameId;
  StreamId m_streamId;
  Timestamp m_timestamp;
  std::chrono::milliseconds m_syncTolerance;
};

// =============================================================================
// Frame Metadata Factory
// =============================================================================

/**
 * @brief Factory for creating frame metadata instances
 */
class FrameMetadataFactory {
public:
  /**
   * @brief Create basic frame metadata with auto-incrementing frame ID
   */
  static BasicFrameMetadata
  createBasic(StreamId stream_id = frame_constants::k_default_stream_id) {
    static std::atomic<FrameId> next_id{1};
    return BasicFrameMetadata(next_id.fetch_add(1, std::memory_order_relaxed),
                              stream_id);
  }

  /**
   * @brief Create a payload-level end-of-stream tag
   *
   * A node-to-node convention the engine does not interpret; the
   * engine's own protocol is Pipeline::signalEndOfStream(). See the
   * scope note at frame_constants::k_end_of_stream_frame_id.
   */
  static BasicFrameMetadata
  createEndOfStream(StreamId stream_id = frame_constants::k_default_stream_id) {
    return BasicFrameMetadata(frame_constants::k_end_of_stream_frame_id,
                              stream_id);
  }
};

// =============================================================================
// Comparison Operators for Frame Metadata
// =============================================================================

inline bool operator<(const IFrameMetadata &lhs, const IFrameMetadata &rhs) {
  return lhs.compareTo(rhs) < 0;
}

inline bool operator>(const IFrameMetadata &lhs, const IFrameMetadata &rhs) {
  return lhs.compareTo(rhs) > 0;
}

inline bool operator<=(const IFrameMetadata &lhs, const IFrameMetadata &rhs) {
  return lhs.compareTo(rhs) <= 0;
}

inline bool operator>=(const IFrameMetadata &lhs, const IFrameMetadata &rhs) {
  return lhs.compareTo(rhs) >= 0;
}

inline bool operator==(const IFrameMetadata &lhs, const IFrameMetadata &rhs) {
  return lhs.compareTo(rhs) == 0;
}

inline bool operator!=(const IFrameMetadata &lhs, const IFrameMetadata &rhs) {
  return lhs.compareTo(rhs) != 0;
}

} // namespace ai_pipe

#endif // AI_PIPE_FRAME_METADATA_HPP