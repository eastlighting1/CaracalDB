---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# FAQ

## Is CaracalDB a Python-only database?

No. The package is Python-facing in v0.1.x, but the engine roadmap keeps a Rust implementation path explicit. The Python API is the first public implementation surface, not the whole identity of the project.

## Is Tuft Cypher?

No. Tuft intentionally keeps familiar graph pattern syntax where it helps, but its compatibility contract is the Tuft reference and specification.

## Can I use full relationship patterns today?

The parser reserves relationship syntax, but the v0.1.x public executor focuses on a single class-labeled node pattern with `WHERE`, `RETURN`, and `LIMIT`.

## Why Arrow?

Arrow keeps query results and subgraph batches close to the data shapes used by Python analytics, ML pipelines, and graph compute tools.

## What is the difference between CaracalDB and Lynxes?

CaracalDB owns storage, query, ontology, snapshots, and transaction boundaries. Lynxes is the graph analytics layer for lazy GraphFrame-style compute.

## Should I inspect `.crcl` files directly?

Only if you are working on storage tooling. The `.crcl` suffix can refer to a packed file or a directory bundle, so public application code should prefer the Python API.
