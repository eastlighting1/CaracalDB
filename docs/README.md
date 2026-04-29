---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CaracalDB Documentation

CaracalDB documentation is organized for two audiences: users who build knowledge graph and GNN workflows on the database, and developers who extend the engine. This site starts from public-facing material only; internal design notes are not part of the documentation source.

## Where To Start

- New to the project: read [Why CaracalDB](start/why-caracaldb.md), run [Quickstart](start/quickstart.md), then take the [30-Minute Tour](start/30min-tour.md).
- Installing or verifying a package: read [Install](start/install.md).
- Learning Tuft query syntax: start with [Tuft](tuft/README.md).
- Looking for Python entry points: start with [API](api/README.md).
- Debugging an error code: start with [Errors](errors/index.md).
- Unsure where something belongs: check the [FAQ](start/faq.md).

## Documentation Map

| Area | Purpose |
|---|---|
| Start | Short entry points that get a working result quickly. |
| Concepts | Explanations of the data model, storage, ontology, snapshots, and ML shape. |
| Guides | Task recipes for ingestion, queries, reasoning, export, and operations. |
| Tutorials | End-to-end case studies based on runnable examples. |
| Tuft | Language reference and specification. |
| API | Python API reference and module entry points. |
| Errors | Error code index and recovery guidance. |
| Interop | Lynxes, Neo4j, PyG, DGL, jraph, and related integration paths. |
| Developers | Contribution, testing, benchmark, and engine-extension guidance. |

## Audience Paths

| Audience | Start with | Then read |
|---|---|---|
| Data and ML engineers | [Quickstart](start/quickstart.md) | [Ingest Parquet](guides/ingest-parquet.md), [Neighbor Loader PyG](guides/neighbor-loader-pyg.md), [Export Subgraph](guides/export-subgraph.md), [PyG and DGL](interop/pyg-and-dgl.md) |
| Graph engineers | [Data Model](concepts/data-model.md) | [Tuft](tuft/README.md), [Pattern Queries](guides/pattern-queries.md), [Neo4j Migration](interop/neo4j-migration.md) |
| Contributors | [Developers](developers/README.md) | [Contributing](developers/contributing.md), [Testing Strategy](developers/testing-strategy.md), [Error Policy](developers/error-policy.md) |

User-facing pages describe workflows and mental models. Engine internals belong in the developer section.
