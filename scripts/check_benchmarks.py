#!/usr/bin/env python3
"""Benchmark regression gate (CI).

Runs the ai_pipe benchmark binary and asserts STRUCTURAL performance
properties. All assertions compare numbers from the same run on the same
machine - absolute time thresholds are deliberately avoided because
shared CI runners make them flaky. What this catches is architectural
regressions: an O(1) path degrading to O(N), construction going
quadratic again, or the compiled routing table losing its advantage.

Usage: check_benchmarks.py <path-to-ai_pipe_benchmarks>
"""

import json
import subprocess
import sys

# (name, minimum speedup) - compiled routing must beat the legacy edge
# scan by a wide margin; the measured ratio is ~80x, gate at 10x.
ROUTING_SPEEDUP_MIN = 10.0

# O(E) linearity: per-edge throughput at the largest size must retain at
# least this fraction of the smallest size's throughput. O(E^2) behavior
# collapses this ratio to near zero (measured: ~1.0 for linear).
LINEARITY_RETENTION_MIN = 0.30


def run_benchmarks(binary: str) -> dict:
    cmd = [
        binary,
        "--benchmark_filter=BM_Graph_GetOutgoingEdges_Scan|"
        "BM_CompiledGraph_OutEdges|BM_Graph_Construction_Chain|"
        "BM_CompiledGraph_Compile",
        "--benchmark_format=json",
        "--benchmark_min_time=0.2s",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr, file=sys.stderr)
        sys.exit(f"benchmark binary failed with rc={result.returncode}")
    return json.loads(result.stdout)


def index_by_name(report: dict) -> dict:
    return {b["name"]: b for b in report.get("benchmarks", [])}


def check(condition: bool, label: str, detail: str) -> bool:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}: {detail}")
    return condition


def main() -> int:
    if len(sys.argv) != 2:
        sys.exit(__doc__)

    by_name = index_by_name(run_benchmarks(sys.argv[1]))
    ok = True

    # 1. Compiled routing table vs legacy O(E) edge scan at 1024 edges
    scan = by_name.get("BM_Graph_GetOutgoingEdges_Scan/1024")
    table = by_name.get("BM_CompiledGraph_OutEdges/1024")
    if scan and table and table["cpu_time"] > 0:
        speedup = scan["cpu_time"] / table["cpu_time"]
        ok &= check(
            speedup >= ROUTING_SPEEDUP_MIN,
            "routing speedup @1024 edges",
            f"{speedup:.1f}x (gate: >= {ROUTING_SPEEDUP_MIN}x)",
        )
    else:
        ok = check(False, "routing speedup @1024 edges", "benchmarks missing")

    # 2. Compiled routing lookup must be size-independent (O(1)):
    #    1024-edge lookup no worse than 16-edge lookup by a wide margin
    small = by_name.get("BM_CompiledGraph_OutEdges/16")
    large = by_name.get("BM_CompiledGraph_OutEdges/1024")
    if small and large and small["cpu_time"] > 0:
        growth = large["cpu_time"] / small["cpu_time"]
        ok &= check(
            growth <= 10.0,
            "routing lookup size-independence",
            f"1024/16 time ratio {growth:.2f} (gate: <= 10)",
        )
    else:
        ok = check(False, "routing lookup size-independence", "missing")

    # 3. Graph construction stays O(E): per-edge throughput retention
    for prefix in ("BM_Graph_Construction_Chain", "BM_CompiledGraph_Compile"):
        small = by_name.get(f"{prefix}/128")
        large = by_name.get(f"{prefix}/8192")
        if small and large and small.get("items_per_second", 0) > 0:
            retention = large["items_per_second"] / small["items_per_second"]
            ok &= check(
                retention >= LINEARITY_RETENTION_MIN,
                f"{prefix} linearity",
                f"throughput retention 128->8192: {retention:.2f} "
                f"(gate: >= {LINEARITY_RETENTION_MIN}; O(E^2) collapses to ~0)",
            )
        else:
            ok = check(False, f"{prefix} linearity", "benchmarks missing")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
