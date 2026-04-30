---
applies_to: v0.2.x
status: stable
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# ADR 0001: Arrow-Native Storage

## Status

Accepted.

## Context

CaracalDB needs graph storage that can serve query execution, bundle inspection,
and ML handoff without copying every node or edge property through a
row-oriented adapter. The Python reference engine also needs a storage contract
that can later be implemented by a Rust core.

## Options Considered

- Store graph records in a property-graph object model and convert to columns
  only at export time.
- Wrap an external dataframe as the primary storage model and layer graph
  indexes beside it.
- Store authoritative node and edge properties in Arrow-compatible columns and
  build explicit graph indexes from those columns.

## Decision

CaracalDB stores node and edge properties in Arrow-compatible columnar structures instead of wrapping an external dataframe as the primary storage model.

## Consequences

Query, export, and ML workflows can share columnar memory layouts, while graph indexes remain explicit structures built from authoritative columns.
