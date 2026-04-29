---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# ADR 0001: Arrow-Native Storage

## Decision

CaracalDB stores node and edge properties in Arrow-compatible columnar structures instead of wrapping an external dataframe as the primary storage model.

## Consequences

Query, export, and ML workflows can share columnar memory layouts, while graph indexes remain explicit structures built from authoritative columns.
