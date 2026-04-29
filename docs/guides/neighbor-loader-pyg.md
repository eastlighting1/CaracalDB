---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Neighbor Loader For PyG

Use this guide when training a GNN from sampled CaracalDB neighborhoods.

!!! warning "Experimental surface"
    CSR sampling and ML adapters are present as Python reference modules, but the PyG conversion path depends on optional ML packages and is not part of the minimal install. Check your environment before treating this guide as an executable workflow.

## Problem

GNN training wants layered fan-out from seed nodes. CaracalDB represents the graph with CSR indexes and exports sampled edges into Arrow-first subgraphs before framework conversion.

## Steps

1. Build CSR files for the edge types you want to sample.
2. Use `NeighborSampleOperator` with one or more `CsrReader` instances.
3. Convert sampled node and edge tables into `Subgraph`.
4. Use the PyG adapter when `torch` and `torch_geometric` are available.

```text
seed nodes -> CSR neighbor sample -> Subgraph -> PyG Data
```

Keep the CaracalDB node id as the canonical key through every stage. Framework tensors can be reindexed for training, but labels, masks, and sampled edges should keep a reversible mapping back to stored node ids.

## Verification

Check that sampled batches contain `src`, `dst`, `etype`, and `layer`, then verify the adapter preserves node ids used by your training labels.

Run a tiny deterministic sample before training: use one seed node, a fixed fan-out, and a CSR file whose neighbors you can inspect manually with `read_csr`. That catches direction mistakes before they become model-quality noise.

## Common Pitfalls

- Optional ML dependencies may be absent in minimal installs.
- Fan-out `0` means keep all neighbors, not zero neighbors.
- Deduplicate seeds before sampling to keep layers deterministic.
- Confirm whether you need outgoing CSR, incoming CSC, or both before choosing the reader set.

## Related ADR

Framework adapter stability should be documented once the PyG, DGL, and jraph APIs are promoted.
