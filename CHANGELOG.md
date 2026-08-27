# Changelog

All notable changes to AI Pipe are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/); versions follow SemVer
(pre-1.0: minor bumps may contain breaking changes, see the Migration
Guide).

## [Unreleased]

### Changed

- **Core/config boundary**: JSON graph assembly is now the optional
  `ai_pipe::config` library (`AI_PIPE_BUILD_CONFIG`) rather than a conditional
  implementation and stub inside `libai_pipe`.
- **Plugin ABI v2**: dynamic plugins use descriptor-first explicit,
  transactional registration with stable type ids and metadata. Successful
  libraries remain mapped until process exit; deactivation removes factories
  without invalidating existing objects.
- **Execution lifecycle**: successful batch completion callbacks are
  exactly-once, and strategies replaced after graph initialization are
  initialized immediately.

- **clang-tidy is now a required CI gate** (F2): the check set is
  curated in `.clang-tidy` (bugprone / concurrency / performance /
  portability groups + identifier naming; three false-positive/low-value
  checks excluded with rationale), covers headers under `src/`, and
  fails on any finding via `WarningsAsErrors`. All 31 actionable
  findings from the triage were fixed - notably fewer `shared_ptr`
  refcount bumps on the scheduling hot path (`executeNodeTask` /
  `routeToDownstream` now take const refs) - with 3 intentional
  deviations documented inline via NOLINT.

### Added

- Installed `ai_pipe_add_plugin()` helper, plugin discovery through
  `AI_PIPE_PLUGIN_PATH` and installation-relative directories, ABI/lifecycle
  policy documents, package-consumer verification, and a configurable soak
  runner.

- **JSON graph loader** (F1, `AI_PIPE_WITH_JSON`, default OFF):
  `ai_pipe/graph_loader.hpp` loads a declarative pipeline description -
  nodes (type/name/config), edges, engine options - and assembles a
  `Graph` through the `NodeRegistry` (`loadGraphFromJson`,
  `loadPipelineFromJson`, plus file variants). Vendored header-only
  nlohmann/json, consumed privately: the core stays dependency-free
  when the option is OFF (the API then returns `InvalidConfiguration`;
  probe with `jsonGraphLoaderAvailable()`). Strict schema - unknown
  keys are rejected by name. Reference: `docs/JSON_Graph_Loader.md`.
- **KeepLatest concurrency contract** (F3): the "keep newest N" bound
  is now precisely specified - strict for a single producer, eventual
  under P concurrent producers (transient overshoot bounded by
  N + P - 1, self-healing on the next uncontended push). Documented at
  `pushKeepLatest` / `QueueConfig` / design doc section 7.1 and locked in
  by four new tests (per-push strict window, N=0 as 1, concurrent
  overshoot bound + self-healing, producer/consumer conservation),
  TSan-verified. No behavior change.
- **aarch64 cross-compile gate** (F4): generic toolchain file
  `platforms/linux/aarch64-gnu.cmake` (distro `g++-aarch64-linux-gnu`,
  no external toolchain inputs), an `aarch64` CMake preset, and a
  build-only CI job that cross-compiles the core (with `-Werror` and
  the JSON loader) and verifies the artifact is really ARM aarch64.

## [0.5.0] - 2026-07-04

Closes the gaps identified in the post-0.4.0 goal review.

### Added

- **Graph connectivity validation**: `CompiledGraph::compile` (and thus
  engine initialize / pipeline build) rejects graphs where a non-source
  node declares an input port with no incoming edge - previously such
  nodes silently never executed. Error: `InvalidConfiguration` naming
  the starving port.
- **Benchmark regression gate in CI** (`scripts/check_benchmarks.py`):
  structural, same-run assertions (compiled routing >= 10x over legacy
  scan, size-independent lookups, O(E) construction/compile linearity).
  It caught a real O(V*E) regression on its first run.

### Performance

- **Fully indexed hot path**: the schedule -> execute -> gather ->
  propagate chain now passes CompiledGraph node indices and state
  references end to end. Per-execution node-state hashing and
  per-propagation port-name map lookups are gone; `OutEdge` carries the
  destination port's queue index. Hash lookups remain only at boundary
  APIs (one per external `pushInput`/`queueDepth` call).

## [0.4.0] - 2026-07-04

Delivered through the architecture-audit roadmap, Phases 0–6 (the
roadmap document `docs/TODO.md` was removed once every item was
completed; see it in git history at this tag). Migration notes:
`docs/Migration_Guide.md`.

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
