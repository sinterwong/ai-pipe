/**
 * @file data_types.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Core data-flow type aliases and the packet ownership model
 * @version 0.2
 * @date 2026-07-04
 *
 * v0.2 (P3.3) — Ownership model:
 *
 * Packets flowing through the graph are READ-ONLY. A node (or external
 * producer) creates a packet mutably, fills it, and hands it off; from
 * that point every consumer shares the same immutable object. This is
 * what makes zero-copy fan-out safe: parallel branches receive aliases
 * of one packet, so concurrent mutation would be a data race.
 *
 *   - MutablePortDataPtr: the creation-side handle. Obtained from
 *     std::make_shared<PortData>(); mutate freely while you are the
 *     only owner, then insert into an output map / push into the
 *     pipeline (implicitly converts to PortDataPtr).
 *   - PortDataPtr: the flow-side handle (shared_ptr<const PortData>).
 *     Everything a node receives in its inputs map is this type.
 *   - mutableCopy(): the copy-on-write escape hatch when a node truly
 *     needs to modify a received packet - copy it, then mutate the copy.
 *
 * @copyright Copyright (c) 2025
 */

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
