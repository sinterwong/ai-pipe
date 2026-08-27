# Soak and production acceptance

The automated soak runner repeatedly exercises batch completion, async
callbacks, streaming backpressure, EOS, cancellation, and plugin activation in
one process:

```sh
cmake --preset release
cmake --build --preset release
AI_PIPE_SOAK_SECONDS=3600 scripts/soak_test.sh \
  build/release/x86_64/bin/tests/ai_pipe_tests
```

For the release-candidate gate use `AI_PIPE_SOAK_SECONDS=604800` and run a real
production graph separately for the same seven-day interval. Record:

- exact AI Pipe commit, compiler/runtime tuple, graph/config and plugin builds;
- input/output/drop/error counts and peak queue depths;
- RSS, thread count, CPU/GPU utilization and latency percentiles over time;
- every cancellation, EOS, reconnect, configuration change and external fault;
- sanitizer or core-dump evidence for any failure.

Acceptance requires no crash, deadlock, unbounded memory/thread growth,
duplicate batch completion, lost terminal EOS, or unexplained frame-accounting
gap. The automated runner is reproducible coverage, not a substitute for the
production 7x24 observation required by R5.1.
