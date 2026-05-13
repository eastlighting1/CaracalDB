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

1. Insert node and edge tables with stable `node_id` values.
2. Call `Database.sample_gnn_subgraph(...)` for one mini-batch or
   `Database.neighbor_loader(...)` for repeated batches.
3. Pass the returned `(edge_index, n_id)` pair to your training loop.
4. Gather features for `n_id` from your feature layer or Lynxes.

```text
seed nodes -> CSR/CSC neighbor sample -> (edge_index, n_id) -> PyG Data
```

Keep the CaracalDB node id as the canonical key through every stage. Framework tensors can be reindexed for training, but labels, masks, and sampled edges should keep a reversible mapping back to stored node ids.

## Official API

```python
with cdb.connect("citation") as db:
    edge_index, n_id = db.sample_gnn_subgraph(
        seeds=["paper/1", "paper/2"],
        fanouts=[15, 10],
        edge_types=["CITES"],
        seed=42,
    )
```

`edge_index` has shape `(2, E)` and uses local node ids into `n_id`. `n_id`
contains the global CaracalDB ids for every sampled node and always includes
the seed nodes, including isolated seeds.

For batched training:

```python
loader = db.neighbor_loader(
    "Paper",
    fanouts=[15, 10],
    edge_types=["CITES"],
    batch_size=1024,
    filter="split = 'train'",
    seed=42,
)

for edge_index, n_id in loader:
    train_step(edge_index, n_id)
```

`query_nodes(label, where)` can be used directly when you want to materialize
filtered seed ids. Simple equality predicates use property indexes when a
matching index exists and otherwise fall back to Arrow filtering.

## Sampling Semantics

- Fan-out applies per source node for every layer.
- `strategy="uniform"` is the default and samples without replacement unless
  `replace=True`.
- `strategy="first"` is the explicit deterministic first-neighbor fast path.
- `strategy="all"` or `fanout=0` expands all neighbors.
- `seed=...` makes uniform sampling and loader shuffling deterministic.
- `direction="out"`, `"in"`, and `"both"` use CSR/CSC readers and normalize
  output edges as stored graph directions.

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
