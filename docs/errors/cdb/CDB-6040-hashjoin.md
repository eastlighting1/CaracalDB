---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CDB-6040 Hash Join

## What You See

Execution fails while building or probing a hash join.

## Why It Happens

The join keys are unsupported for the current operator, the build side exceeded configured memory, or the planner selected a join shape that is not yet implemented.

## How To Fix

Check the plan with `EXPLAIN`, filter the build side earlier, or rewrite the query to use a supported pattern expansion when the relationship is graph-shaped.

## Cross-References

- [Observability](../../guides/observability-explain-profile.md)
- [API Reference](../../api/README.md)
