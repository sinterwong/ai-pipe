#ifndef AI_PIPE_DATA_TYPES_HPP
#define AI_PIPE_DATA_TYPES_HPP

#include "ai_pipe/data_packet.hpp"
#include <map>
#include <memory>
#include <string>

namespace ai_pipe {

using PortData = DataPacket;

/// Creation-side handle: mutate while exclusively owned, then hand off.
using MutablePortDataPtr = std::shared_ptr<PortData>;

/// Flow-side handle: packets received from the pipeline are immutable.
using PortDataPtr = std::shared_ptr<const PortData>;

using PortDataMap = std::map<std::string, PortDataPtr>;

/**
 * @brief Copy-on-write escape hatch
 *
 * Deep-copies a received (immutable) packet into a fresh mutable one,
 * preserving frame identity and params. Use when a node must modify
 * data in place conceptually; the original stays untouched for any
 * parallel consumers.
 */
inline MutablePortDataPtr mutableCopy(const PortDataPtr &packet) {
  return packet ? std::make_shared<PortData>(*packet) : nullptr;
}

} // namespace ai_pipe

#endif
