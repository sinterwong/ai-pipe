#ifndef AI_PIPE_DATA_TYPES_HPP
#define AI_PIPE_DATA_TYPES_HPP

#include "ai_pipe/data_packet.hpp"
#include <map>
#include <memory>
#include <string>

namespace ai_pipe {

/** Packet type exchanged through node ports. */
using PortData = DataPacket;

/** Creation-side handle. Mutate only before handing it to the pipeline. */
using MutablePortDataPtr = std::shared_ptr<PortData>;

/** Flow-side handle. A node cannot mutate packets received from the pipeline.
 */
using PortDataPtr = std::shared_ptr<const PortData>;

/** Input or output packets keyed by declared port name. */
using PortDataMap = std::map<std::string, PortDataPtr>;

/**
 * Deep-copies an immutable packet into mutable, independently owned storage.
 * Frame identity and parameters are preserved. Returns `nullptr` for a null
 * input.
 */
inline MutablePortDataPtr mutableCopy(const PortDataPtr &packet) {
  return packet ? std::make_shared<PortData>(*packet) : nullptr;
}

} // namespace ai_pipe

#endif
