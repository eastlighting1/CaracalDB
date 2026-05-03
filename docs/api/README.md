---
applies_to: v0.2.x
status: stable
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# API Reference

The API section is organized by functional area. Each page covers a group of closely related
modules so that application code, engine contributors, and ML users each have a natural entry point.

## Pages

| Page | Covers | Audience |
|---|---|---|
| [Input / Output](io.md) | `connect`, `Database`, `Connection`, `Result`, bulk ingest | All users |
| [Storage & Transactions](storage.md) | Bundles, manifests, column segments, OCC transactions | Tooling, contributors |
| [Query Engine](query-engine.md) | Logical plan nodes, physical operators, pipeline execution | Engine contributors |
| [Graph](graph.md) | CSR/CSC adjacency index build and read | Analytics, GNN users |
| [Ontology](onto.md) | Catalog, class/property registry, closure index | Schema and ontology work |
| [Machine Learning](ml.md) | Neighbor sampling, subgraph container, online feature serving | ML practitioners |
| [Extensions](extensions.md) | Observability (explain/profile/trace), UDFs, Viewer | Advanced users |

## Stability notes

CaracalDB keeps the Python import path stable while the engine implementation matures.
Pages marked `experimental` have APIs that may change between minor versions.
Pages marked `stable` follow semver guarantees from v0.2.x onwards.
