#ifndef AI_PIPE_UTILS_DATA_PACKET_HPP
#define AI_PIPE_UTILS_DATA_PACKET_HPP

#include "ai_pipe/error.hpp"
#include "ai_pipe/frame_metadata.hpp"
#include <any>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace ai_pipe {
using DataPacketId = uint64_t;

/**
 * Type-erased parameters and frame identity carried through pipeline ports.
 *
 * Parameter values own their storage through `std::any`. Copying a packet
 * copies every stored value according to that value's copy semantics.
 */
struct DataPacket {
  /**
   * Identity used for cross-branch synchronization. Zero means unassigned;
   * the engine assigns a monotonic ID at ingress and propagates it to output
   * packets that do not set an ID explicitly.
   */
  DataPacketId id{0};
  StreamId stream_id{frame_constants::k_default_stream_id}; ///< Source stream.
  Timestamp timestamp{}; ///< Capture or production time on `steady_clock`.

  [[nodiscard]] FrameId frameId() const { return id; }
  [[nodiscard]] bool hasFrameId() const {
    return id != frame_constants::k_invalid_frame_id;
  }

  /**
   * Returns a copy of the parameter stored under `key`.
   *
   * A missing key or a value whose exact `std::any` type is not `T` returns
   * `ErrorCode::InvalidArgument`. Use `valueOr()` on the result when a fallback
   * is appropriate.
   */
  template <typename T> Result<T> param(const std::string &key) const {
    const std::any *value = findParam(key);
    if (!value) {
      return Result<T>::err(ErrorCode::InvalidArgument,
                            "Missing required parameter: " + key);
    }
    if (const T *typed = std::any_cast<T>(value)) {
      return *typed;
    }
    return Result<T>::err(ErrorCode::InvalidArgument,
                          "Invalid parameter type for key '" + key +
                              "'. Expected type: " + typeid(T).name());
  }

  /** Inserts or replaces `key` with an owned value of type `T`. */
  template <typename T> void setParam(const std::string &key, T value) {
    if (std::any *existing = findParam(key)) {
      *existing = std::move(value);
      return;
    }
    m_params.emplace_back(key, std::any(std::move(value)));
  }

  /** Returns whether any value is stored under `key`. */
  bool has(const std::string &key) const { return findParam(key) != nullptr; }

  /** Returns whether at least one parameter has the exact type `T`. */
  template <typename T> bool has() const {
    for (const auto &entry : m_params) {
      if (entry.second.type() == typeid(T)) {
        return true;
      }
    }
    return false;
  }

  /** Returns whether `key` exists and has the exact type `T`. */
  template <typename T> bool has(const std::string &key) const {
    const std::any *value = findParam(key);
    return value && value->type() == typeid(T);
  }

  [[nodiscard]] std::size_t paramCount() const { return m_params.size(); }

  [[nodiscard]] std::vector<std::string> paramKeys() const {
    std::vector<std::string> keys;
    keys.reserve(m_params.size());
    for (const auto &entry : m_params) {
      keys.push_back(entry.first);
    }
    return keys;
  }

private:
  [[nodiscard]] const std::any *findParam(const std::string &key) const {
    for (const auto &entry : m_params) {
      if (entry.first == key) {
        return &entry.second;
      }
    }
    return nullptr;
  }

  [[nodiscard]] std::any *findParam(const std::string &key) {
    for (auto &entry : m_params) {
      if (entry.first == key) {
        return &entry.second;
      }
    }
    return nullptr;
  }

  // Flat storage: packets carry few params, so linear scan over
  // contiguous memory outperforms std::map and allocates less.
  std::vector<std::pair<std::string, std::any>> m_params;
};

/**
 * Binds a parameter key to `T` so access sites do not repeat either contract.
 *
 * @code
 *   // In the node class:
 *   static inline const TypedParam<cv::Mat> k_image{"image"};
 *   static inline const TypedParam<int64_t> k_ts{"capture_ts"};
 *
 *   // In process():
 *   auto img = k_image.read(*packet);   // Result<cv::Mat>
 *   k_ts.set(*out_packet, now_us);
 * @endcode
 */
template <typename T> class TypedParam {
public:
  explicit TypedParam(std::string key) : m_key(std::move(key)) {}

  [[nodiscard]] const std::string &key() const { return m_key; }

  /** Reads the parameter, preserving `DataPacket::param()` error semantics. */
  [[nodiscard]] Result<T> read(const DataPacket &packet) const {
    return packet.param<T>(m_key);
  }

  void set(DataPacket &packet, T value) const {
    packet.setParam(m_key, std::move(value));
  }

  [[nodiscard]] bool in(const DataPacket &packet) const {
    return packet.has<T>(m_key);
  }

private:
  std::string m_key;
};

} // namespace ai_pipe

#endif
