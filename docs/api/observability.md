---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Observability API

Observability APIs render logical plans, profile physical pipelines, and attach span-like traces around runtime work.

## Common Entry Points

| Name | Use |
|---|---|
| `explain_logical` | Convert a logical plan into an explain tree. |
| `render_explain` | Render an explain tree as text. |
| `profile_pipeline` | Measure operator execution. |
| `Tracer` | Record spans. |

## Reference

::: caracaldb.observability
    options:
      show_root_heading: false
      show_source: true
