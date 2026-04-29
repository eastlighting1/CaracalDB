---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Plan API

Plan APIs describe logical query trees before they are lowered to physical Arrow operators. They are primarily for planner work, explain output, and optimizer tests.

## Common Entry Points

| Name | Use |
|---|---|
| `LogicalOp` | Base logical plan node. |
| `LNodeScan` | Logical node scan. |
| `LSelection` | Logical filter. |
| `LProject` | Logical projection. |
| `LAggregate` | Logical aggregation. |
| `walk` | Traverse a logical plan tree. |

## Reference

::: caracaldb.plan
    options:
      show_root_heading: false
      show_source: true
