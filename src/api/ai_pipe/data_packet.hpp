/**
 * @file data_packet.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Type-erased key-value container passed between pipeline nodes
 * @version 0.2
 * @date 2026-07-04
 *
 * v0.2 (P3.1): storage switched from std::map<std::string, std::any> to a
 * flat vector scanned linearly. Packets typically carry a handful of
 * params, where a contiguous scan beats red-black tree traversal and
 * per-node heap allocations. The accessor API is unchanged; the storage
 * itself is now private (no external code accessed `.params` directly).
 *
 * Also introduces TypedParam<T>: a small declarative handle that binds a
 * param key to its C++ type once, giving nodes typed get/set access
 * without repeating <T> and key strings at every call site.
 *
 * @copyright Copyright (c) 2025
 */
#ifndef AI_PIPE_UTILS_DATA_PACKET_HPP
#define AI_PIPE_UTILS_DATA_PACKET_HPP

#include "ai_pipe/error.hpp"
#include "ai_pipe/frame_metadata.hpp"
#include <any>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace ai_pipe {
using DataPacketId = uint64_t;

struct DataPacket {
  /**
   * Frame identity header (P3.4). `id` doubles as the FrameId used for
   * cross-branch synchronization: 0 means "unassigned", and the engine
   * stamps a monotonically increasing id when an unassigned packet
   * enters the pipeline. Mid-pipeline output packets inherit the frame
   * identity of the inputs that produced them unless the node sets one
   * explicitly.
   */
  DataPacketId id{0};
  StreamId stream_id{frame_constants::k_default_stream_id};
  Timestamp timestamp{};

  [[nodiscard]] FrameId frameId() const { return id; }
  [[nodiscard]] bool hasFrameId() const {
    return id != frame_constants::k_invalid_frame_id;
  }

  template <typename T> T getParam(const std::string &key) const {
    const std::any *value = findParam(key);
    if (!value) {
      throw std::runtime_error("Missing required parameter: " + key);
    }
    try {
      return std::any_cast<T>(*value);
    } catch (const std::bad_any_cast &) {
      throw std::runtime_error("Invalid parameter type for key '" + key +
                               "'. Expected type: " + typeid(T).name());
    }
  }

  /**
   * @brief Exception-free typed access returning Result<T>
   *
   * The preferred accessor for new code: missing keys and type
   * mismatches come back as Error values (InvalidArgument) instead of
   * thrown exceptions, matching the framework-wide Result convention.
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

  template <typename T>
  std::optional<T> getOptionalParam(const std::string &key) const {
    const std::any *value = findParam(key);
    if (!value) {
      return std::nullopt;
    }
    try {
      return std::any_cast<T>(*value);
    } catch (const std::bad_any_cast &) {
      throw std::runtime_error("Invalid parameter type for optional key '" +
                               key + "'. Expected type: " + typeid(T).name());
    }
  }

  template <typename T> void setParam(const std::string &key, T value) {
    if (std::any *existing = findParam(key)) {
      *existing = std::move(value);
      return;
    }
    m_params.emplace_back(key, std::any(std::move(value)));
  }

  bool has(const std::string &key) const { return findParam(key) != nullptr; }

  template <typename T> bool has() const {
    for (const auto &entry : m_params) {
      if (entry.second.type() == typeid(T)) {
        return true;
      }
    }
    return false;
  }

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
 * @brief Declarative typed handle for a DataPacket parameter
 *
 * Binds a key to its C++ type once, at declaration, so access sites are
 * typo- and type-safe without repeating the template argument:
 *
 * @code
 *   // In the node class:
 *   static inline const TypedParam<cv::Mat> k_image{"image"};
 *   static inline const TypedParam<int64_t> k_ts{"capture_ts"};
 *
 *   // In process():
 *   cv::Mat img = k_image.get(*packet);
 *   k_ts.set(*out_packet, now_us);
 * @endcode
 */
template <typename T> class TypedParam {
public:
  explicit TypedParam(std::string key) : m_key(std::move(key)) {}

  [[nodiscard]] const std::string &key() const { return m_key; }

  /** @brief Read the param; throws if missing or of the wrong type */
  [[nodiscard]] T get(const DataPacket &packet) const {
    return packet.getParam<T>(m_key);
  }

  /** @brief Read the param; nullopt if missing, throws on type mismatch */
  [[nodiscard]] std::optional<T> tryGet(const DataPacket &packet) const {
    return packet.getOptionalParam<T>(m_key);
  }

  /** @brief Exception-free read: Error value on missing key or mismatch */
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
