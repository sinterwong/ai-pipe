#!/usr/bin/env bash
set -euo pipefail

duration_seconds="${AI_PIPE_SOAK_SECONDS:-3600}"
test_binary="${1:-build/x86_64/bin/tests/ai_pipe_tests}"

if [[ ! -x "${test_binary}" ]]; then
    echo "AI Pipe test binary is not executable: ${test_binary}" >&2
    exit 2
fi
if [[ ! "${duration_seconds}" =~ ^[1-9][0-9]*$ ]]; then
    echo "AI_PIPE_SOAK_SECONDS must be a positive integer" >&2
    exit 2
fi

test_directory="$(cd "$(dirname "${test_binary}")" && pwd)"
build_directory="$(cd "${test_directory}/../../.." && pwd)"
plugin_directory="${build_directory}/tests/plugins"
library_directory="${build_directory}/x86_64/lib"

export AI_PIPE_TEST_PLUGIN_DIR="${plugin_directory}"
export LD_LIBRARY_PATH="${library_directory}:${LD_LIBRARY_PATH:-}"

filter='ExecutionEngineTest.RapidStartStopStress:ExecutionEngineTest.CompletionCallbackFiresExactlyOncePerRun:PipelineAsyncTest.*:StreamBackpressureTest.*:EndOfStreamTest.*:PluginLoaderTest.*'

echo "Running AI Pipe soak suite for ${duration_seconds}s"
set +e
timeout --signal=TERM "${duration_seconds}s" "${test_binary}" \
    --gtest_filter="${filter}" \
    --gtest_repeat=-1 \
    --gtest_break_on_failure \
    --gtest_brief=1
status=$?
set -e

if [[ ${status} -eq 124 ]]; then
    echo "AI Pipe soak window completed without a test failure"
    exit 0
fi
exit "${status}"
