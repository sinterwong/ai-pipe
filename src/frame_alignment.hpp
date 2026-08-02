/**
 * @file frame_alignment.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief Aligned-gather skeleton and per-policy alignment rules (R4.3)
 * @version 0.1
 * @date 2026-07-18
 *
 * This is an INTERNAL header file. Users should not include this directly.
 *
 * The engine's three aligned gathers (FrameId / StreamFrameId /
 * Timestamp) shared one loop skeleton - peek every head, test
 * alignment, pop the pair or discard unpairable heads, repeat - with a
 * fourth copy of the pairing predicate living in the join-timeout
 * degradation. This component factors the skeleton into
 * gatherAligned() parameterized by a policy providing:
 *
 *   - aligned(heads):      do the current heads form a deliverable set?
 *   - forEachStale(heads): select the heads that can never be paired
 *                          (must select >= 1 whenever !aligned, which
 *                          guarantees loop progress)
 *   - pairsWith(a, ref):   the pairing predicate, reused by the
 *                          join-timeout degradation path
 *
 * The engine supplies the environment as callables (peek with
 * coordinated-drop consumption, pop-and-deliver, stale-drop
 * accounting), so this header stays free of engine internals.
 *
 * @copyright Copyright (c) 2026
 */
#ifndef AI_PIPE_INTERNAL_FRAME_ALIGNMENT_HPP
#define AI_PIPE_INTERNAL_FRAME_ALIGNMENT_HPP

#include "ai_pipe/data_types.hpp"
#include "ai_pipe/frame_metadata.hpp"
#include <chrono>
#include <utility>
#include <vector>

namespace ai_pipe::frame_alignment {

/**
 * @brief FrameId policy: heads pair when their FrameIds match exactly
 *
 * Unassigned ids (0) act as wildcards. Heads lagging behind the newest
 * head id can never be paired - their partner was dropped on a sibling
 * branch - and are discarded.
 */
struct FrameIdPolicy {
  static const char *dropReason() { return "frame alignment drop"; }

  [[nodiscard]] static FrameId headId(const PortDataPtr &head) {
    return head ? head->frameId() : frame_constants::k_invalid_frame_id;
  }

  [[nodiscard]] static FrameId newestId(const std::vector<PortDataPtr> &heads) {
    FrameId target = frame_constants::k_invalid_frame_id;
    for (const auto &head : heads) {
      target = std::max(target, headId(head));
    }
    return target;
  }

  [[nodiscard]] static bool aligned(const std::vector<PortDataPtr> &heads) {
    const FrameId target = newestId(heads);
    for (const auto &head : heads) {
      const FrameId frame = headId(head);
      if (frame != frame_constants::k_invalid_frame_id && frame != target) {
        return false;
      }
    }
    return true;
  }

  template <typename StaleFn>
  static void forEachStale(const std::vector<PortDataPtr> &heads,
                           StaleFn &&stale) {
    // Progress: !aligned implies some non-wildcard id below the newest.
    const FrameId target = newestId(heads);
    for (std::size_t i = 0; i < heads.size(); ++i) {
      const FrameId frame = headId(heads[i]);
      if (frame != frame_constants::k_invalid_frame_id && frame < target) {
        stale(i);
      }
    }
  }

  [[nodiscard]] static bool pairsWith(const PortData &head,
                                      const PortData &reference) {
    return !head.hasFrameId() || head.id == reference.id;
  }
};

/**
 * @brief (stream, frame) policy for AlignmentPolicy::StreamFrameId
 *
 * Heads pair only when every non-wildcard head carries the same
 * (stream_id, frame_id). When heads disagree, the head(s) that can
 * never be paired are discarded: any head strictly older (by entry
 * timestamp) than the newest head - a FIFO port never rewinds in time,
 * so its partner can no longer arrive. If no head is strictly older
 * (identical timestamps), the smallest (stream, frame) heads are
 * dropped as a deterministic tie-break.
 */
struct StreamFrameIdPolicy {
  static const char *dropReason() { return "stream alignment drop"; }

  [[nodiscard]] static bool aligned(const std::vector<PortDataPtr> &heads) {
    bool have_key = false;
    StreamId key_stream = frame_constants::k_default_stream_id;
    FrameId key_frame = frame_constants::k_invalid_frame_id;
    for (const auto &head : heads) {
      if (!head || !head->hasFrameId()) {
        continue;
      }
      if (!have_key) {
        have_key = true;
        key_stream = head->stream_id;
        key_frame = head->id;
      } else if (head->stream_id != key_stream || head->id != key_frame) {
        return false;
      }
    }
    return true;
  }

  template <typename StaleFn>
  static void forEachStale(const std::vector<PortDataPtr> &heads,
                           StaleFn &&stale) {
    Timestamp newest_ts{};
    for (const auto &head : heads) {
      if (head && head->hasFrameId() && head->timestamp > newest_ts) {
        newest_ts = head->timestamp;
      }
    }

    bool dropped_by_time = false;
    for (std::size_t i = 0; i < heads.size(); ++i) {
      if (heads[i] && heads[i]->hasFrameId() &&
          heads[i]->timestamp < newest_ts) {
        stale(i);
        dropped_by_time = true;
      }
    }
    if (dropped_by_time) {
      return;
    }

    // Deterministic tie-break: drop the smallest (stream, frame) heads.
    // Progress: misaligned non-wildcard heads cannot all be equal.
    std::size_t min_index = heads.size();
    for (std::size_t i = 0; i < heads.size(); ++i) {
      if (!heads[i] || !heads[i]->hasFrameId()) {
        continue;
      }
      if (min_index == heads.size() ||
          std::make_pair(heads[i]->stream_id, heads[i]->id) <
              std::make_pair(heads[min_index]->stream_id,
                             heads[min_index]->id)) {
        min_index = i;
      }
    }
    if (min_index == heads.size()) {
      return;
    }
    for (std::size_t i = 0; i < heads.size(); ++i) {
      if (heads[i] && heads[i]->hasFrameId() &&
          heads[i]->stream_id == heads[min_index]->stream_id &&
          heads[i]->id == heads[min_index]->id) {
        stale(i);
      }
    }
  }

  [[nodiscard]] static bool pairsWith(const PortData &head,
                                      const PortData &reference) {
    return !head.hasFrameId() ||
           (head.stream_id == reference.stream_id && head.id == reference.id);
  }
};

/**
 * @brief Timestamp-tolerance policy for AlignmentPolicy::Timestamp
 *
 * Heads pair when (max_ts - min_ts) <= tolerance across all non-null
 * port heads (null packets are wildcards, mirroring unassigned frame
 * ids under id alignment). Otherwise every head with
 * ts < max_ts - tolerance is discarded: per-port timestamps are
 * non-decreasing (stamped at ingress in arrival order), so such a head
 * can never fall within tolerance of the newest port's future frames.
 * Frame ids are ignored by this policy.
 */
struct TimestampPolicy {
  std::chrono::microseconds tolerance{0};

  static const char *dropReason() { return "timestamp alignment drop"; }

  [[nodiscard]] bool aligned(const std::vector<PortDataPtr> &heads) const {
    Timestamp min_ts = Timestamp::max();
    Timestamp max_ts = Timestamp::min();
    for (const auto &head : heads) {
      if (!head) {
        continue;
      }
      min_ts = std::min(min_ts, head->timestamp);
      max_ts = std::max(max_ts, head->timestamp);
    }
    return min_ts > max_ts || max_ts - min_ts <= tolerance;
  }

  template <typename StaleFn>
  void forEachStale(const std::vector<PortDataPtr> &heads,
                    StaleFn &&stale) const {
    // Progress: at least the min_ts head satisfies the drop condition
    // whenever !aligned.
    Timestamp max_ts = Timestamp::min();
    for (const auto &head : heads) {
      if (head) {
        max_ts = std::max(max_ts, head->timestamp);
      }
    }
    for (std::size_t i = 0; i < heads.size(); ++i) {
      if (heads[i] && max_ts - heads[i]->timestamp > tolerance) {
        stale(i);
      }
    }
  }

  [[nodiscard]] bool pairsWith(const PortData &head,
                               const PortData &reference) const {
    const auto diff = head.timestamp >= reference.timestamp
                          ? head.timestamp - reference.timestamp
                          : reference.timestamp - head.timestamp;
    return diff <= tolerance;
  }
};

/**
 * @brief The unified aligned-gather loop
 *
 * @param port_count Number of input ports
 * @param policy     Alignment policy (see the structs above)
 * @param peek       (std::size_t port) -> std::optional<PortDataPtr>;
 *                   peek the poppable head (consuming pending
 *                   coordinated drops); nullopt = port not ready
 * @param pop        (std::size_t port) -> bool; pop the head and
 *                   deliver it into the caller's input map
 * @param drop       (std::size_t port, const PortDataPtr &head,
 *                   const char *reason); discard an unpairable head
 * @param closed     (std::size_t port) -> bool; the port has reached
 *                   end of stream and drained (R6.1). Closed ports
 *                   leave the pairing set entirely: they contribute a
 *                   null head, which every policy treats as a wildcard,
 *                   and are not popped from. Without this, one branch
 *                   finishing would block the join forever waiting for
 *                   a partner that can no longer arrive.
 *
 * @return true when an aligned set was delivered across the still-open
 *         ports; false when some open port has no poppable data yet
 *         (transient; caller reschedules) or every port is closed
 *
 * Termination: each pass either delivers, returns on a dry port, or
 * discards >= 1 frame (policy progress guarantee), so the loop ends
 * once queues stabilize or run dry.
 */
template <typename Policy, typename PeekFn, typename PopFn, typename DropFn,
          typename ClosedFn>
bool gatherAligned(std::size_t port_count, const Policy &policy, PeekFn &&peek,
                   PopFn &&pop, DropFn &&drop, ClosedFn &&closed) {
  std::vector<PortDataPtr> heads(port_count);
  std::vector<bool> is_closed(port_count, false);

  for (;;) {
    // Phase A: peek every open head; any dry open port aborts the
    // attempt. Closed ports stay null (wildcard) and are never popped.
    std::size_t open_ports = 0;
    for (std::size_t i = 0; i < port_count; ++i) {
      is_closed[i] = closed(i);
      if (is_closed[i]) {
        heads[i] = nullptr;
        continue;
      }
      ++open_ports;
      auto head = peek(i);
      if (!head.has_value()) {
        return false;
      }
      heads[i] = std::move(head.value());
    }

    // Every port closed: nothing left to pair, and the caller's EOS
    // path owns the node from here.
    if (open_ports == 0) {
      return false;
    }

    // Phase B: aligned? Pop and deliver the whole set.
    if (policy.aligned(heads)) {
      for (std::size_t i = 0; i < port_count; ++i) {
        if (is_closed[i]) {
          continue;
        }
        if (!pop(i)) {
          return false; // Should not happen under single-consumer contract
        }
      }
      return true;
    }

    // Phase C: discard unpairable heads, then re-peek.
    bool dropped = false;
    policy.forEachStale(heads, [&](std::size_t i) {
      drop(i, heads[i], Policy::dropReason());
      dropped = true;
    });
    if (!dropped) {
      // Defensive: a policy failing its progress guarantee must not
      // spin the worker forever.
      return false;
    }
  }
}

} // namespace ai_pipe::frame_alignment

#endif // AI_PIPE_INTERNAL_FRAME_ALIGNMENT_HPP
