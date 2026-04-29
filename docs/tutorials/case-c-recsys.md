---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Case C: Recommendation Graph

This tutorial follows the shape of `examples/recsys.ipynb` and the case-C golden tests. The goal is to sample user-item neighborhoods, run embedding lookup, and export batches for graph ML.

## Goal

Build a small recommendation workflow:

- sample heterogeneous user-item neighborhoods,
- find nearest items from a user tower embedding,
- create `Subgraph` batches,
- export a subgraph to Arrow and import it back,
- run short random walks over user views.

## Data Shape

| Class | Example role |
|---|---|
| `User` | seed nodes and user embeddings |
| `Item` | candidate recommendation targets |

| Edge | Meaning |
|---|---|
| `viewed` | user-to-item event |

## Walkthrough

1. Store users and items with stable node ids.
2. Build CSR for the `viewed` edge.
3. Use layered fan-out for neighbor sampling.
4. Use HNSW for user-item tower matching.
5. Emit Arrow-backed `Subgraph` batches for ML adapters.

```text
User seeds -> viewed CSR -> NeighborLoader -> Subgraph -> PyG / DGL / jraph
```
## Expected Result

The golden fixture emits sampled layers `[0, 1]`, finds the nearest item for a known user embedding, and round-trips an exported subgraph through Arrow IPC.

## Next Steps

- Use [Neighbor Loader For PyG](../guides/neighbor-loader-pyg.md).
- Use [Export Subgraph](../guides/export-subgraph.md).
- Use [ML Integration](../concepts/ml-integration.md) for the Arrow-first contract.
