---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Exec API

Execution APIs are pull-based Arrow operators. A physical operator opens with an `ExecCtx`, emits `pyarrow.RecordBatch` values, and closes after the pipeline drains.

## Common Entry Points

| Name | Use |
|---|---|
| `ExecCtx` | Runtime execution context. |
| `PhysicalOperator` | Base class for pull-based operators. |
| `run_pipeline` | Drain an operator into batches. |

## Reference

::: caracaldb.exec
    options:
      show_root_heading: false
      show_source: true
