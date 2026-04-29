---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Observability With EXPLAIN And PROFILE

Use this guide when you need to understand a plan shape or runtime behavior.

!!! warning "Experimental surface"
    Logical explain helpers are available in the Python reference implementation. Full Tuft `EXPLAIN` / `PROFILE` behavior and tracing exports are still experimental in v0.1.x.

## Problem

Graph queries can hide expensive scans, expansions, or sorts. CaracalDB separates logical explanation, profiling, and tracing so each layer can be tested.

## Steps

1. Render a logical plan with `explain_logical`.

```python
from caracaldb.observability import explain_logical, render_explain

text = render_explain(explain_logical(plan))
```
2. Use CLI `explain` for a quick wiring check.

```bash
uv run caracal explain graph.crcl Gene
```
3. Add profiling or tracing around physical operators when investigating runtime cost.

## Verification

An explain tree should show operator names, key attributes, and estimated rows when catalog statistics are available.

## Common Pitfalls

- `EXPLAIN` explains shape; it is not a benchmark.
- Profiling should be run on representative data.
- Keep output text stable when tests assert plan rendering.

## Related ADR

Tracing export format and profile span semantics should be captured once observability leaves the internal API stage.
