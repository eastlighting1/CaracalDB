---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CDB-6020 MVP Shape

## What You See

The executor rejects a logical or physical plan because its shape is outside the supported v0.1.x execution surface.

## Why It Happens

The planner produced an operation that CaracalDB has reserved for a later milestone, or an experimental path reached execution without a supported operator.

## How To Fix

Rewrite the query using supported pattern, filter, projection, aggregation, and snapshot operations. If the plan came from generated code, reduce it to a smaller supported query first.

## Cross-References

- [Pattern Queries](../../guides/pattern-queries.md)
- [Query Engine API](../../api/query-engine.md)
