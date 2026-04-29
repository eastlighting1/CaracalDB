---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# API

The API section documents public Python entry points first, then lower-level engine modules for contributors and advanced users.

## Primary Entry Points

- [caracaldb](caracaldb.md): `connect`, `Database`, `Connection`, and `Result`.

## Module Pages

- [Storage](storage.md): bundles, segments, packing, and low-level storage primitives.
- [Graph](graph.md): CSR, CSC, readers, and graph index helpers.
- [Plan](plan.md): logical query tree nodes.
- [Exec](exec.md): pull-based Arrow physical operators.
- [ML](ml.md): subgraphs and neighbor loading.
- [Feature](feature.md): online feature lookup.
- [Observability](observability.md): explain, profile, and tracing helpers.
- [Transactions](tx.md): transaction manager and conflicts.
- [UDF](udf.md): Python and Tuft UDF helpers.
