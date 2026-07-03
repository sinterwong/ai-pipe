# Changelog

All notable changes to AI Pipe are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer
(pre-1.0: minor bumps may contain breaking changes, see the Migration
Guide).

## [0.4.0] - 2026-07-04

Delivered through the architecture-audit roadmap (`docs/TODO.md`,
Phases 0–6). Migration notes: `docs/Migration_Guide.md`.

### Fixed (correctness)

- **DropTail queue rejections were silent data loss**: `pushInput` now
  returns `QueueRejected`, rejections count into statistics and fire the
  drop callback.
- **`m_currentContext` data race** between `execute()` and worker-thread
  scheduling (now an atomic `shared_ptr`).
- **Unsound `ExecutionEngine::Impl` move operations** removed — in-flight
  worker tasks capture `this`; the engine stays movable via its PIMPL
  pointer.
- **Transient active-task dips** could wake streaming
  `execute(wait_for_completion=true)` callers early.
- **Streaming node failures were permanent**: an exception left the node
  `FAILED` forever, stranding queued frames. Failures are now per-frame
  events; the node returns to service.
- **22 ThreadSanitizer data races fixed** (thread-pool construction race,
  reset-vs-straggler counter races, lost-wakeup windows behind timeout
  polling). TSan and ASan run clean and gate CI.

### Added

- **CompiledGraph**: immutable indexed topology with precomputed routing
  tables, iterative-Kahn topological order, and source/sink sets.
- **Frame identity**: `DataPacket` carries `id` (FrameId), `stream_id`,
  `timestamp`; the engine stamps monotonic ids at injection and
  propagates identity through node outputs.
- **Frame-aligned joins**: multi-input nodes gather frame-aligned inputs;
  frames orphaned by sibling-branch drops are discarded and reported.
  The sync strategy (`shouldDrop`/`markProcessed`/watermark) is wired
  end to end.
- **Live observability**: latency histogram + percentiles, queue
  wait/pop counters, input-frame counts, schedule latency, and per-node
  statistics are now actually recorded (previously API-only).
- **`NodeRegistry`** with `AI_PIPE_REGISTER_NODE` /
  `AI_PIPE_REGISTER_NODE_WITH_CONFIG` macros for name-based,
  config-driven node construction.
- **Node lifecycle**: `ILogicNode::setup(context)` / `teardown()` run in
  topological/reverse order at execution start / reset / destruction.
- **Port payload typing**: `ILogicNode::portPayloadType()` lets
  `Graph::addEdge` reject type-mismatched connections at build time.
- **`TypedParam<T>`** declarative packet access and exception-free
  `DataPacket::param<T>()` returning `Result<T>`.
- **Engine-log bridging**: `PipelineContext::attachEngineLogs()` routes
  framework logs to the context's `ILoggerAdapter`.
- **Build/CI**: `AI_PIPE_SANITIZER` option, `CMakePresets.json`,
  `-Wall -Wextra -Wpedantic` (clean) with `AI_PIPE_WERROR`, static-lib
  builds, GCC+Clang CI matrix, ASan/TSan gates, clang-format gate.

### Changed (breaking — see Migration Guide)

- **Packet ownership**: `PortDataPtr` is now `shared_ptr<const PortData>`;
  create packets via `MutablePortDataPtr` (`make_shared`), modify
  received packets via `mutableCopy()`.
- **`SchedulingContext`** replaces its two port-name vectors with
  `expected_input_count` / `ready_input_count` / `ready_port_mask`.
- **`ExecutionEngine::initialize`** rejects empty graphs (`GraphEmpty`)
  and cyclic graphs (`GraphCycleDetected`).
- **`total_executions`** counts node execution attempts in all modes
  (previously: pipeline runs in batch, node schedules in stream).
- **`DataPacket`** storage is private (flat vector); use the accessor
  API. `id == 0` now means "unassigned frame id".

### Removed

- Deprecated `QueuePushResult` (use `Result<PushStatus>`).
- Legacy `ThreadPool` (`thread_pool.hpp`); the engine has used
  `WorkStealingThreadPool` since the lock-free queue migration.
- Unreferenced `CoordinatedSyncStrategy` (superseded by
  `JoinAwareSyncStrategy`).

### Performance

- Downstream edge resolution: O(E) scan → O(1) table (509ns → 6.2ns at
  1024 edges).
- Graph construction: O(E²) → O(E).
- Idle thread pool: ~8000 wakeups/s → 0 CPU (event-driven wakeup).
- Drain/stop waits: sleep-polling → condition variables.
- Hot-path scheduling: no virtual port-list calls, no string hashing,
  no per-task `packaged_task`/`future`, no per-node scheduling mutex.

## [0.3.1] - 2026-02

Baseline release before the architecture audit: lock-free MPMC queues,
work-stealing thread pool, `Result<T>` error handling, strategy-pattern
engine.
