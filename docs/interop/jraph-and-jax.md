---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# jraph And JAX

jraph integration is the JAX-oriented version of CaracalDB subgraph export. The goal is to feed pure, reproducible graph tensors into functional training and inference code.

## Problem

JAX workloads prefer immutable arrays and explicit batching. CaracalDB stores graph data with snapshot isolation, ontology-aware names, and Arrow-backed columns, so the export boundary needs to be precise.

## Shape

Export a snapshot-bound subgraph into the pieces required by `jraph.GraphsTuple`:

| GraphsTuple field | CaracalDB source |
|---|---|
| `nodes` | Selected node feature columns |
| `edges` | Selected edge feature columns |
| `senders` | Edge source ids remapped to dense integer ids |
| `receivers` | Edge target ids remapped to dense integer ids |
| `globals` | Snapshot metadata or graph-level features |
| `n_node` / `n_edge` | Export batch sizes |

## Workflow

1. Select the graph with a Tuft query and a fixed snapshot.
2. Remap stable graph ids to dense integer arrays.
3. Convert Arrow columns into JAX arrays.
4. Build `GraphsTuple` values at batch boundaries.
5. Store the remapping manifest with the model checkpoint.

## Verification

Confirm that dense ids are contiguous, endpoints are within range, and repeated exports from the same snapshot produce identical arrays.

## Common Pitfalls

- Assuming stable CaracalDB ids are already dense tensor ids.
- Losing the id remapping after model inference.
- Mixing graph-level metadata into node features without recording the schema.

## Related ADR

No dedicated JAX export ADR exists yet. Keep the export explicit and reproducible until a stable adapter is promoted.
