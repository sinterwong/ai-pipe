# Runtime lifecycle contract

## Engine and strategies

An engine follows `construct -> configure -> initialize(graph) -> run -> stop ->
destroy`. Scheduler, synchronization, trace, and queue configuration may change
only while the engine is idle.

Replacing a strategy after engine initialization immediately initializes the
replacement from the engine-owned `CompiledGraph` snapshot. Replacements are
rejected while running. Null strategies are rejected.

Each accepted successful batch execution publishes its result callback exactly
once. Streaming result and error callbacks are per-frame and may repeat until
the stream stops.

## Nodes

`setup()` runs in topological order before the first execution. Successfully set
up nodes remain active across subsequent runs of the same initialized engine.
If setup fails, the completed prefix is torn down in reverse order.

`process()` and `onEndOfStream()` never overlap for the same node. `teardown()`
runs in reverse topological order after worker tasks have joined, on reset,
reinitialization, or destruction. It must be `noexcept`; the engine guards and
logs a violating implementation.

## Destruction and callbacks

Destroying a running Pipeline requests cancellation or streaming stop and waits
for active engine tasks. The engine stops watchdog/defer threads, joins its
worker pool, then tears down nodes. No engine callback is invoked after the
engine destructor has joined its workers.

Applications must destroy pipelines and application-owned plugin nodes before
destroying services stored in `PipelineContext`. Plugin code remains mapped
until process exit, even after its factories are deactivated.
