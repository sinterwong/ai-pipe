/**
 * @file lock_free_queue.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief High-performance Lock-Free MPMC Queue for AI Pipe scheduling
 * @version 1.0
 * @date 2026-02-06
 *
 * Implementation based on Dmitry Vyukov's bounded MPMC queue algorithm
 * using sequence-tagged ring buffer slots for ABA prevention.
 *
 * Key design decisions:
 * - Capacity forced to power-of-2 for branchless index wrapping (bitwise AND)
 * - Cache-line aligned slots to prevent false sharing between
 * producers/consumers
 * - Relaxed memory ordering where safe, acquire/release at synchronization
 * points
 * - Integrated drop policy support (DropHead, DropTail) for backpressure
 * handling
 * - Zero third-party dependencies, pure C++20 standard library
 *
 * Performance target: <100ns per operation under MPMC contention
 *
 * @copyright Copyright (c) 2026
 */

#ifndef AI_PIPE_LOCK_FREE_QUEUE_HPP
#define AI_PIPE_LOCK_FREE_QUEUE_HPP

#include "ai_pipe/frame_metadata.hpp"
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <new>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>

namespace ai_pipe {

/**
 * @brief Information about a drop event for callbacks/logging
 */
struct DropEvent {
  FrameId frame_id{frame_constants::k_invalid_frame_id};
  StreamId stream_id{frame_constants::k_default_stream_id};
  Timestamp drop_time{std::chrono::steady_clock::now()};
  std::string node_name;
  std::string port_name;
  std::string reason;
  std::size_t queue_size_before{0};
  std::size_t queue_size_after{0};
  std::size_t total_drops{0};

  [[nodiscard]] std::string toString() const {
    return "DropEvent{frame=" + std::to_string(frame_id) +
           ", stream=" + std::to_string(stream_id) + ", node=" + node_name +
           ", port=" + port_name + ", reason=" + reason + "}";
  }
};

/**
 * @brief Callback type for drop event notifications
 */
using DropEventCallback = std::function<void(const DropEvent &)>;

inline constexpr std::size_t k_cache_line_size = 64;

// =============================================================================
// Drop Policy Enumeration (lightweight alternative to IDropStrategy for
// lock-free context)
// =============================================================================

/**
 * @brief Drop policy for lock-free queue overflow handling
 *
 * Unlike IDropStrategy which requires deque random-access, these policies
 * are implemented via atomic CAS loops on the ring buffer head/tail.
 */
enum class LockFreeDropPolicy {
  DropTail, ///< Reject new items when queue is full (preserve oldest)
  DropHead, ///< Overwrite oldest item to make room for new (preserve latest)
  /**
   * Aggressive variant of DropHead: evict down to a window of the
   * keep_latest_n newest items on every push. The window bound is
   * strict for a single producer but only eventually enforced under
   * concurrent producers - see pushKeepLatest() for the precise
   * contract.
   */
  KeepLatest,
};

// =============================================================================
// Lock-Free Queue Statistics (atomic, wait-free counters)
// =============================================================================

struct LockFreeQueueStatistics {
  std::atomic<std::uint64_t> total_pushed{0};
  std::atomic<std::uint64_t> total_popped{0};
  std::atomic<std::uint64_t> total_dropped{0};
  std::atomic<std::uint64_t> total_rejected{0};
  std::atomic<std::uint64_t> peak_size{0};
  std::chrono::steady_clock::time_point created_time{
      std::chrono::steady_clock::now()};

  void reset() {
    total_pushed.store(0, std::memory_order_relaxed);
    total_popped.store(0, std::memory_order_relaxed);
    total_dropped.store(0, std::memory_order_relaxed);
    total_rejected.store(0, std::memory_order_relaxed);
    peak_size.store(0, std::memory_order_relaxed);
    created_time = std::chrono::steady_clock::now();
  }

  [[nodiscard]] double dropRate() const {
    auto pushed = total_pushed.load(std::memory_order_relaxed);
    auto dropped = total_dropped.load(std::memory_order_relaxed);
    if (pushed == 0)
      return 0.0;
    return static_cast<double>(dropped) / static_cast<double>(pushed) * 100.0;
  }

  [[nodiscard]] double throughput() const {
    auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                       std::chrono::steady_clock::now() - created_time)
                       .count();
    if (elapsed == 0)
      return 0.0;
    return static_cast<double>(total_popped.load(std::memory_order_relaxed)) /
           static_cast<double>(elapsed);
  }
};

// =============================================================================
// Core Lock-Free MPMC Queue (Vyukov Ring Buffer)
// =============================================================================

/**
 * @brief Lock-free bounded MPMC queue based on Vyukov's sequence-tagged
 *        ring buffer algorithm.
 *
 * Algorithm overview:
 * Each ring buffer cell contains an atomic sequence counter and a data slot.
 * - Producers read the enqueue position, find a cell where
 *   cell.sequence == enqueue_pos, write data, then set
 *   cell.sequence = enqueue_pos + 1.
 * - Consumers read the dequeue position, find a cell where
 *   cell.sequence == dequeue_pos + 1, read data, then set
 *   cell.sequence = dequeue_pos + capacity.
 *
 * This sequence tagging eliminates the ABA problem because each slot
 * monotonically progresses through sequence space, preventing stale
 * CAS from succeeding after wrap-around.
 *
 * @tparam T Must be movable. Typically std::shared_ptr<PortData>.
 */
template <typename T> class LockFreeMPMCQueue {
  static_assert(std::is_move_constructible_v<T>,
                "LockFreeMPMCQueue<T> requires T to be move constructible");

public:
  using value_type = T;
  using size_type = std::size_t;

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  /**
   * @brief Construct with requested capacity (rounded up to next power of 2)
   * @param min_capacity Minimum capacity; actual will be >= this and power of 2
   */
  explicit LockFreeMPMCQueue(size_type min_capacity)
      : m_capacity(roundUpPowerOf2(min_capacity < 2 ? 2 : min_capacity)),
        m_mask(m_capacity - 1),
        m_buffer(static_cast<Cell *>(::operator new(
            sizeof(Cell) * m_capacity, std::align_val_t{k_cache_line_size}))),
        m_enqueuePos(0), m_dequeuePos(0) {
    // Initialize sequence counters
    for (size_type i = 0; i < m_capacity; ++i) {
      new (&m_buffer[i]) Cell();
      m_buffer[i].sequence.store(i, std::memory_order_relaxed);
    }
  }

  ~LockFreeMPMCQueue() {
    // Drain remaining items to call destructors
    T dummy;
    while (tryPop(dummy)) {
    }

    // Destroy cells
    for (size_type i = 0; i < m_capacity; ++i) {
      m_buffer[i].~Cell();
    }

    ::operator delete(m_buffer, std::align_val_t{k_cache_line_size});
  }

  // Non-copyable, non-movable (atomic members)
  LockFreeMPMCQueue(const LockFreeMPMCQueue &) = delete;
  LockFreeMPMCQueue &operator=(const LockFreeMPMCQueue &) = delete;
  LockFreeMPMCQueue(LockFreeMPMCQueue &&) = delete;
  LockFreeMPMCQueue &operator=(LockFreeMPMCQueue &&) = delete;

  // -------------------------------------------------------------------------
  // Core Operations
  // -------------------------------------------------------------------------

  /**
   * @brief Non-blocking enqueue
   * @param item Item to enqueue (moved in on success)
   * @return true if successfully enqueued, false if queue is full
   */
  bool tryPush(T &&item) {
    Cell *cell;
    size_type pos = m_enqueuePos.load(std::memory_order_relaxed);

    for (;;) {
      cell = &m_buffer[pos & m_mask];
      size_type seq = cell->sequence.load(std::memory_order_acquire);
      auto diff =
          static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos);

      if (diff == 0) {
        // Slot is available for writing
        if (m_enqueuePos.compare_exchange_weak(pos, pos + 1,
                                               std::memory_order_relaxed)) {
          break; // Claimed this slot
        }
        // CAS failed, another producer claimed it retry with updated pos
      } else if (diff < 0) {
        // Queue is full (consumer hasn't freed this slot yet)
        return false;
      } else {
        // Another producer already advanced past this slot reload pos
        pos = m_enqueuePos.load(std::memory_order_relaxed);
      }
    }

    // We own this slot; write data and publish
    cell->data = std::move(item);
    cell->sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  /**
   * @brief Non-blocking dequeue
   * @param[out] item Receives the dequeued item on success
   * @return true if successfully dequeued, false if queue is empty
   */
  bool tryPop(T &item) {
    Cell *cell;
    size_type pos = m_dequeuePos.load(std::memory_order_relaxed);

    for (;;) {
      cell = &m_buffer[pos & m_mask];
      size_type seq = cell->sequence.load(std::memory_order_acquire);
      auto diff =
          static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1);

      if (diff == 0) {
        // Data is ready for reading
        if (m_dequeuePos.compare_exchange_weak(pos, pos + 1,
                                               std::memory_order_relaxed)) {
          break; // Claimed this slot
        }
        // CAS failed, another consumer claimed it retry
      } else if (diff < 0) {
        // Queue is empty (producer hasn't written this slot yet)
        return false;
      } else {
        // Another consumer already advanced past this slot reload pos
        pos = m_dequeuePos.load(std::memory_order_relaxed);
      }
    }

    // We own this slot; read data and release for reuse
    item = std::move(cell->data);
    cell->sequence.store(pos + m_capacity, std::memory_order_release);
    return true;
  }

  /**
   * @brief Force-push by evicting the oldest item if queue is full
   *
   * Used by DropHead policy. Atomically advances dequeue_pos to make room.
   * The evicted item is returned via the out parameter.
   *
   * @param item Item to push
   * @param[out] evicted Receives the evicted item if one was dropped
   * @return true if an item was evicted (evicted is valid), false if no
   * eviction
   */
  /**
   * @brief Force-push by evicting the oldest item if queue is full.
   * GUARANTEE: After eviction, the new item WILL be pushed.
   *
   * Design note: After eviction we use aggressive non-yielding spin to
   * push into the freed slot. Yielding here would allow other threads
   * to fill the slot, causing livelock on low-core CI machines where
   * every yield-and-resume cycle finds the queue full again.
   * If the aggressive spin fails (another thread grabbed the slot),
   * we re-evict to guarantee forward progress.
   */
  bool forcePush(T item, T &evicted) {
    // Fast path: queue has space
    if (tryPush(std::move(item))) {
      return false;
    }

    constexpr int k_spin_limit = 256;

    // Queue was full but tryPop also failed — transient state from
    // concurrent evictions. Spin briefly then give up without eviction.
    if (!tryPop(evicted)) {
      for (int spin = 0; spin < k_spin_limit; ++spin) {
        if (tryPush(std::move(item)))
          return false;
        spinPause();
      }
      std::this_thread::yield();
      (void)tryPush(std::move(item));
      return false;
    }

    // Evicted one item — MUST push ours to maintain the guarantee.
    // Phase 1: Aggressive non-yielding spin on the slot we just freed.
    // NOT yielding is intentional — it prevents other threads from
    // stealing the freed slot between our eviction and push.
    for (int spin = 0; spin < k_spin_limit; ++spin) {
      if (tryPush(std::move(item)))
        return true;
      spinPause();
    }

    // Phase 2: The freed slot was stolen by a concurrent forcePush.
    // Re-evict to create a fresh slot. Bounded retry ensures forward
    // progress: each eviction linearly increases available slots.
    constexpr int k_max_reevictions = 8;
    for (int round = 0; round < k_max_reevictions; ++round) {
      T re_evicted;
      if (tryPop(re_evicted)) {
        // Discard re-evicted item (counted as collateral eviction)
        for (int spin = 0; spin < k_spin_limit; ++spin) {
          if (tryPush(std::move(item)))
            return true;
          spinPause();
        }
      }
      // Brief yield to let concurrent operations settle
      std::this_thread::yield();
      if (tryPush(std::move(item)))
        return true;
    }

    // Phase 3: Final fallback — should be astronomically rare.
    // Use yield-and-retry but with a bounded eviction assist.
    while (!tryPush(std::move(item))) {
      T discard;
      (void)tryPop(discard); // Assist by creating space
      spinPause();
    }
    return true;
  }

  /**
   * @brief Copy the front element without consuming it
   *
   * SINGLE-CONSUMER CONTRACT: safe only when no other thread pops
   * concurrently (the execution engine guarantees one consumer per node
   * queue via the EXECUTING state machine). Producers never touch a
   * cell the consumer sees as ready, so the copy is stable.
   *
   * @param[out] item Receives a copy of the front element
   * @return true if an element was available
   */
  bool tryPeek(T &item) const {
    const size_type pos = m_dequeuePos.load(std::memory_order_relaxed);
    const Cell &cell = m_buffer[pos & m_mask];
    const size_type seq = cell.sequence.load(std::memory_order_acquire);
    if (static_cast<std::intptr_t>(seq) - static_cast<std::intptr_t>(pos + 1) !=
        0) {
      return false; // Empty (or slot not yet published)
    }
    item = cell.data;
    return true;
  }

  // -------------------------------------------------------------------------
  // Query Operations (approximate under concurrency)
  // -------------------------------------------------------------------------

  /**
   * @brief Approximate current size
   *
   * Not linearizable the result is a snapshot that may be stale by the time
   * it is observed. Useful for monitoring and heuristics.
   */
  [[nodiscard]] size_type size() const noexcept {
    auto enq = m_enqueuePos.load(std::memory_order_relaxed);
    auto deq = m_dequeuePos.load(std::memory_order_relaxed);
    return enq >= deq ? (enq - deq) : 0;
  }

  /**
   * @brief Approximate emptiness check
   */
  [[nodiscard]] bool empty() const noexcept { return size() == 0; }

  /**
   * @brief Check if queue appears full
   */
  [[nodiscard]] bool isFull() const noexcept { return size() >= m_capacity; }

  /**
   * @brief Get the actual capacity (power-of-2, may exceed requested)
   */
  [[nodiscard]] size_type capacity() const noexcept { return m_capacity; }

  /**
   * @brief Get fill ratio (approximate)
   */
  [[nodiscard]] double fillRatio() const noexcept {
    if (m_capacity == 0)
      return 0.0;
    return static_cast<double>(size()) / static_cast<double>(m_capacity);
  }

private:
  // -------------------------------------------------------------------------
  // Internal Types
  // -------------------------------------------------------------------------

  /**
   * @brief Ring buffer cell with sequence tag for ABA prevention
   *
   * Padded to cache-line boundary to prevent false sharing between
   * adjacent cells accessed by different threads.
   */
  struct alignas(k_cache_line_size) Cell {
    std::atomic<size_type> sequence{0};
    T data{};
  };

  // -------------------------------------------------------------------------
  // Utility
  // -------------------------------------------------------------------------

  static constexpr size_type roundUpPowerOf2(size_type v) {
    // Ensure minimum of 2
    if (v <= 2)
      return 2;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    if constexpr (sizeof(size_type) > 4) {
      v |= v >> 32;
    }
    v++;
    return v;
  }

  static void spinPause() {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) ||             \
    defined(_M_IX86)
    __builtin_ia32_pause();
#elif defined(__aarch64__) || defined(_M_ARM64)
    asm volatile("yield" ::: "memory");
#else
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
  }

  // -------------------------------------------------------------------------
  // Members
  // -------------------------------------------------------------------------

  const size_type m_capacity;
  const size_type m_mask; // capacity - 1, for branchless modulo

  Cell *const m_buffer;

  // Separate cache lines for enqueue and dequeue to avoid false sharing
  alignas(k_cache_line_size) std::atomic<size_type> m_enqueuePos;
  alignas(k_cache_line_size) std::atomic<size_type> m_dequeuePos;
};

// =============================================================================
// LockFreeNodeQueue Drop-policy aware wrapper for ExecutionEngine integration
// =============================================================================

/**
 * @brief Node-level queue wrapper providing drop policy, statistics, and
 *        callback support on top of the core LockFreeMPMCQueue.
 *
 * This class serves as a drop-in replacement for BoundedDropQueue<T> and
 * ThreadSafeQueue<T> in the ExecutionEngine's NodeState.
 *
 * Interface compatibility:
 * push(T) bool (replaces BoundedDropQueue::push)
 * tryPop() optional<T> (replaces BoundedDropQueue::tryPop)
 * size() size_t (replaces BoundedDropQueue::size)
 * empty() bool (replaces BoundedDropQueue::empty)
 * capacity() size_t (replaces BoundedDropQueue::capacity)
 * isFull() bool (replaces BoundedDropQueue::isFull)
 * clear() void (replaces BoundedDropQueue::clear)
 *
 * @tparam T Typically PortDataPtr (std::shared_ptr<PortData>)
 */
template <typename T> class LockFreeNodeQueue {
public:
  using value_type = T;
  using size_type = std::size_t;

  // -------------------------------------------------------------------------
  // Configuration
  // -------------------------------------------------------------------------

  struct Config {
    size_type capacity = 16;
    LockFreeDropPolicy drop_policy = LockFreeDropPolicy::DropHead;
    /**
     * Window size N for the KeepLatest policy. 0 is treated as 1;
     * N >= capacity degenerates to DropHead behavior. The "at most N"
     * bound is strict only for a single producer (see pushKeepLatest).
     */
    size_type keep_latest_n = 1;
    bool track_statistics = true;
    std::string node_name;
    std::string port_name;
  };

  // -------------------------------------------------------------------------
  // Construction
  // -------------------------------------------------------------------------

  explicit LockFreeNodeQueue(size_type capacity)
      : m_config{.capacity = capacity}, m_queue(capacity) {}

  explicit LockFreeNodeQueue(Config config)
      : m_config(std::move(config)), m_queue(m_config.capacity) {}

  ~LockFreeNodeQueue() = default;

  // Non-copyable, non-movable
  LockFreeNodeQueue(const LockFreeNodeQueue &) = delete;
  LockFreeNodeQueue &operator=(const LockFreeNodeQueue &) = delete;
  LockFreeNodeQueue(LockFreeNodeQueue &&) = delete;
  LockFreeNodeQueue &operator=(LockFreeNodeQueue &&) = delete;

  // -------------------------------------------------------------------------
  // Drop Policy Management
  // -------------------------------------------------------------------------

  void setDropPolicy(LockFreeDropPolicy policy) {
    m_config.drop_policy = policy;
  }

  [[nodiscard]] LockFreeDropPolicy dropPolicy() const {
    return m_config.drop_policy;
  }

  [[nodiscard]] std::string strategyName() const {
    switch (m_config.drop_policy) {
    case LockFreeDropPolicy::DropTail:
      return "DropTail";
    case LockFreeDropPolicy::DropHead:
      return "DropHead";
    case LockFreeDropPolicy::KeepLatest:
      return "KeepLatest" + std::to_string(m_config.keep_latest_n);
    }
    return "Unknown";
  }

  // -------------------------------------------------------------------------
  // Callback Management
  // -------------------------------------------------------------------------

  void setDropCallback(DropEventCallback callback) {
    m_dropCallback = std::move(callback);
  }

  template <typename Accessor> void setFrameIdAccessor(Accessor accessor) {
    m_frameIdAccessor = [accessor](const T &item) -> std::optional<FrameId> {
      auto result = accessor(item);
      if constexpr (std::is_same_v<decltype(result), std::optional<FrameId>>) {
        return result;
      } else if constexpr (std::is_same_v<decltype(result), FrameId>) {
        return result;
      } else {
        if (result) {
          return result->frameId();
        }
        return std::nullopt;
      }
    };
  }

  // -------------------------------------------------------------------------
  // Queue Operations
  // -------------------------------------------------------------------------

  /**
   * @brief Push an item with drop policy enforcement
   *
   * - DropTail: reject if full
   * - DropHead: evict oldest to make room
   * - KeepLatest: evict multiple oldest items to maintain keep_latest_n
   *   window (strict for one producer, eventual under concurrent
   *   producers - see pushKeepLatest for the precise contract)
   *
   * @param item Item to push
   * @return true if item was accepted, false if rejected (DropTail only)
   */
  bool push(T item) {
    switch (m_config.drop_policy) {
    case LockFreeDropPolicy::DropTail:
      return pushDropTail(std::move(item));

    case LockFreeDropPolicy::DropHead:
      return pushDropHead(std::move(item));

    case LockFreeDropPolicy::KeepLatest:
      return pushKeepLatest(std::move(item));
    }

    return pushDropHead(std::move(item)); // Default fallback
  }

  /**
   * @brief Copy the front item without consuming it
   *
   * Inherits the core queue's single-consumer contract: only the
   * thread that owns popping from this queue may peek.
   *
   * @return Copy of the front item, or std::nullopt if empty
   */
  [[nodiscard]] std::optional<T> tryPeek() const {
    T item;
    if (m_queue.tryPeek(item)) {
      return item;
    }
    return std::nullopt;
  }

  /**
   * @brief Non-blocking pop
   * @return The item, or std::nullopt if queue is empty
   */
  [[nodiscard]] std::optional<T> tryPop() {
    T item;
    if (m_queue.tryPop(item)) {
      if (m_config.track_statistics) {
        m_stats.total_popped.fetch_add(1, std::memory_order_relaxed);
      }
      return item;
    }
    return std::nullopt;
  }

  // -------------------------------------------------------------------------
  // Query Operations
  // -------------------------------------------------------------------------

  [[nodiscard]] bool empty() const { return m_queue.empty(); }

  [[nodiscard]] size_type size() const { return m_queue.size(); }

  [[nodiscard]] size_type capacity() const { return m_queue.capacity(); }

  [[nodiscard]] bool isFull() const { return m_queue.isFull(); }

  [[nodiscard]] double fillRatio() const { return m_queue.fillRatio(); }

  // -------------------------------------------------------------------------
  // Clear (drain all items)
  // -------------------------------------------------------------------------

  void clear() {
    T dummy;
    while (m_queue.tryPop(dummy)) {
    }
  }

  // -------------------------------------------------------------------------
  // Statistics
  // -------------------------------------------------------------------------

  [[nodiscard]] const LockFreeQueueStatistics &statistics() const {
    return m_stats;
  }

  void resetStatistics() { m_stats.reset(); }

  // -------------------------------------------------------------------------
  // Configuration Access
  // -------------------------------------------------------------------------

  [[nodiscard]] const Config &config() const { return m_config; }

private:
  // -------------------------------------------------------------------------
  // Drop Policy Implementations
  // -------------------------------------------------------------------------

  /**
   * @brief DropTail: reject new item if queue is full
   */
  bool pushDropTail(T item) {
    if (m_queue.tryPush(std::move(item))) {
      updatePeakSize();
      if (m_config.track_statistics) {
        m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
      }
      return true;
    }

    // Queue is full reject
    if (m_config.track_statistics) {
      m_stats.total_rejected.fetch_add(1, std::memory_order_relaxed);
    }
    return false;
  }

  /**
   * @brief DropHead: evict oldest to make room, then push.
   * GUARANTEE: item WILL be enqueued. Every eviction is counted.
   */
  bool pushDropHead(T item) {
    // Fast path
    if (m_queue.tryPush(std::move(item))) {
      updatePeakSize();
      if (m_config.track_statistics) {
        m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
      }
      return true;
    }

    // Slow path: evict + spin
    constexpr int k_max_evictions = 4;
    constexpr int k_spin_per_eviction = 128;

    for (int round = 0; round < k_max_evictions; ++round) {
      T evicted;
      if (m_queue.tryPop(evicted)) {
        if (m_config.track_statistics) {
          m_stats.total_dropped.fetch_add(1, std::memory_order_relaxed);
        }
        notifyDrop(evicted, "DropHead overflow");
      }
      for (int spin = 0; spin < k_spin_per_eviction; ++spin) {
        if (m_queue.tryPush(std::move(item))) {
          updatePeakSize();
          if (m_config.track_statistics) {
            m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
          }
          return true;
        }
        if ((spin & 31) == 31)
          std::this_thread::yield();
      }
    }

    // Eviction-assisted final fallback to prevent livelock
    while (!m_queue.tryPush(std::move(item))) {
      T discard;
      if (m_queue.tryPop(discard)) {
        if (m_config.track_statistics) {
          m_stats.total_dropped.fetch_add(1, std::memory_order_relaxed);
        }
        notifyDrop(discard, "DropHead overflow (fallback)");
      }
      std::this_thread::yield();
    }
    updatePeakSize();
    if (m_config.track_statistics) {
      m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  }

  /**
   * @brief KeepLatest: evict excess to maintain at most keep_latest_n
   *
   * Contract (F3). Let N = max(keep_latest_n, 1):
   *
   * - The push always succeeds (returns true); backpressure is
   *   expressed purely as evictions, and every evicted item is counted
   *   in total_dropped and reported through the drop callback.
   *
   * - Single producer: when push() returns and no other push is in
   *   flight, size() <= N. Concurrent consumer pops only shrink the
   *   queue, so the bound holds regardless of consumer activity.
   *
   * - P concurrent producers: the evict-then-push sequence is not
   *   atomic, so producers that pass the size check in the same race
   *   window can each add an item before any of them observes the
   *   others'. size() may therefore transiently reach N + P - 1
   *   (never beyond capacity), and if pushing stops inside that
   *   window the overshoot persists until the next operation.
   *
   * - Eventual enforcement (self-healing): every push first evicts
   *   until it observes size() < N, so a single subsequent push with
   *   no concurrent pushes restores size() <= N. Under a steady
   *   stream of pushes the window converges without external help.
   *
   * Callers needing a hard "never more than N" guarantee must
   * serialize producers externally; consumers should treat N as the
   * steady-state window, not an invariant of every instant.
   */
  bool pushKeepLatest(T item) {
    auto target_keep = m_config.keep_latest_n > 0 ? m_config.keep_latest_n : 1;

    auto current_size = m_queue.size();
    while (current_size >= target_keep) {
      T evicted;
      if (m_queue.tryPop(evicted)) {
        if (m_config.track_statistics) {
          m_stats.total_dropped.fetch_add(1, std::memory_order_relaxed);
        }
        notifyDrop(evicted, "KeepLatest overflow");
        current_size = m_queue.size();
      } else {
        break;
      }
    }

    if (m_queue.tryPush(std::move(item))) {
      updatePeakSize();
      if (m_config.track_statistics) {
        m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
      }
      return true;
    }

    constexpr int k_spin_limit = 128;
    T evicted;
    if (m_queue.tryPop(evicted)) {
      if (m_config.track_statistics) {
        m_stats.total_dropped.fetch_add(1, std::memory_order_relaxed);
      }
      notifyDrop(evicted, "KeepLatest force overflow");
    }
    for (int spin = 0; spin < k_spin_limit; ++spin) {
      if (m_queue.tryPush(std::move(item))) {
        updatePeakSize();
        if (m_config.track_statistics) {
          m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
      }
      if ((spin & 31) == 31)
        std::this_thread::yield();
    }

    // Eviction-assisted final fallback to prevent livelock
    while (!m_queue.tryPush(std::move(item))) {
      T discard;
      if (m_queue.tryPop(discard)) {
        if (m_config.track_statistics) {
          m_stats.total_dropped.fetch_add(1, std::memory_order_relaxed);
        }
        notifyDrop(discard, "KeepLatest overflow (fallback)");
      }
      std::this_thread::yield();
    }
    updatePeakSize();
    if (m_config.track_statistics) {
      m_stats.total_pushed.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  void updatePeakSize() {
    if (!m_config.track_statistics)
      return;

    size_type current = m_queue.size();
    size_type peak = m_stats.peak_size.load(std::memory_order_relaxed);
    while (current > peak) {
      if (m_stats.peak_size.compare_exchange_weak(peak, current,
                                                  std::memory_order_relaxed)) {
        break;
      }
    }
  }

  void notifyDrop(const T &dropped_item, const std::string &reason) {
    if (!m_dropCallback)
      return;

    FrameId frame_id = frame_constants::k_invalid_frame_id;
    if (m_frameIdAccessor) {
      auto fid = m_frameIdAccessor(dropped_item);
      if (fid.has_value()) {
        frame_id = fid.value();
      }
    }

    DropEvent event{
        .frame_id = frame_id,
        .drop_time = std::chrono::steady_clock::now(),
        .node_name = m_config.node_name,
        .port_name = m_config.port_name,
        .reason = reason,
        .queue_size_before = m_queue.capacity(),
        .queue_size_after = m_queue.size(),
        .total_drops = m_stats.total_dropped.load(std::memory_order_relaxed),
    };
    m_dropCallback(event);
  }

  // -------------------------------------------------------------------------
  // Members
  // -------------------------------------------------------------------------

  Config m_config;
  LockFreeMPMCQueue<T> m_queue;
  LockFreeQueueStatistics m_stats;

  DropEventCallback m_dropCallback;
  std::function<std::optional<FrameId>(const T &)> m_frameIdAccessor;
};

} // namespace ai_pipe

#endif // AI_PIPE_LOCK_FREE_QUEUE_HPP