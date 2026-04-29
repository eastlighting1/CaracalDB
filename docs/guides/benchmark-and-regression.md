---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Benchmark And Regression Checks

Use this guide when a performance-sensitive change touches graph traversal, vector search, neighbor sampling, storage layout, or benchmark infrastructure.

## Problem

Benchmarks are only useful if the baseline is repeatable and the failure threshold is explicit. CaracalDB stores baseline results in `bench/results/baseline.json` and compares fresh results with a tolerance.

## Steps

1. Run the benchmark harness.

```bash
uv run python -m bench.run --out bench/results/latest.json
```
2. Compare the latest run against the committed baseline.

```bash
uv run --with pytest python -c "
import json, pathlib, sys
from bench.harness import compare_against_baseline
baseline = json.loads(pathlib.Path('bench/results/baseline.json').read_text())
latest = json.loads(pathlib.Path('bench/results/latest.json').read_text())
regressions = compare_against_baseline(latest, baseline, tolerance=0.30)
if regressions:
    print('REGRESSIONS:')
    print('\\n'.join(f' - {line}' for line in regressions))
    sys.exit(1)
print('No regressions detected.')
"
```
3. Update the baseline only after measuring a representative run and reviewing the scenario parameters.

```json
{ "scenario": "knn", "metric": "ms", "value": 90.0, "n": 50000, "dim": 64, "k": 10, "repeats": 20 }
```
## Verification

A passing comparison means every scenario is at or below `baseline * 1.30`. It does not prove the engine is fast in absolute terms; it proves the current change did not exceed the agreed regression budget for the measured scenarios.

## Common Pitfalls

- Do not replace measured baselines with arbitrary round numbers unless the team has explicitly accepted them as temporary gates.
- Do not compare different scenario parameters under the same scenario name.
- Do not claim a speedup from CI noise. Keep the baseline conservative and explain the measurement context in the PR.

## Related ADR

The baseline policy will eventually move into an ADR once the Rust engine benchmark suite lands.
