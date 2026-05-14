---
applies_to: v0.2.x
status: experimental
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# PyG Interop

PyG integration turns a CaracalDB subgraph into tensors while keeping CaracalDB responsible for storage, query, reasoning, and repeatable snapshot selection.

## Problem

GNN training needs compact node features, edge indices, labels, and train/validation/test masks. Production graph data usually needs ontology filters, temporal snapshots, and schema-aware extraction before it is ready for a training loop.

For simple typed graph datasets, IRIs are not required. A node table with `node_id`, `type`, and feature columns plus an edge table with `src`, `dst`, and `type` is enough to establish the graph shape; CaracalDB can keep compact internal ids for adjacency while preserving `node_id` for labels and joins. The same applies when data starts as Neo4j-style JSON or triples: ingest normalizes it first, then ML export can read compact ids and stable external ids from the same graph.

## Shape

The planned adapter surface centers on `Subgraph` exports:

| Target | Expected payload |
|---|---|
| PyG | `Data` or `HeteroData` with `edge_index`, `x`, labels, and masks |
| CaracalDB | Snapshot-bound Arrow tables and graph identity columns |

## Workflow

1. Use Tuft to select the training subgraph.
2. Materialize node features in deterministic column order.
3. Export edges as integer id pairs.
4. Convert Arrow batches to framework tensors.
5. Persist the snapshot id and feature schema beside model artifacts.

## Verification

Check row counts before tensor conversion, verify that every edge endpoint exists in the exported node table, confirm `node_id` survives as the stable external id, and run one mini-batch through the model loader before launching a full training job.

## Common Pitfalls

- Mixing node ids from different snapshots.
- Training on ontology aliases without recording the canonical class set.
- Letting framework-specific tensors become the only stored copy of feature provenance.

## Related ADR

No dedicated GNN adapter ADR exists yet. This page follows the storage-first design captured by the Arrow-native architecture direction.
