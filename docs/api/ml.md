---
applies_to: v0.2.x
status: experimental
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# Machine Learning

CaracalDB exposes Arrow-native ML surfaces: database-owned GNN sampling APIs,
`Subgraph` as the exchange container for framework adapters, and
`OnlineFeatureView` for low-latency point lookups during training or inference.

---

## GNN Sampling

`Database.sample_gnn_subgraph` samples layered neighborhoods directly from
CaracalDB-owned CSR/CSC graph indexes. It returns PyG-style `(edge_index,
n_id)` arrays: `edge_index` uses local ids into `n_id`, while `n_id` contains
global CaracalDB node ids.

```python
import caracaldb as cdb

with cdb.connect("citation") as db:
    edge_index, n_id = db.sample_gnn_subgraph(
        seeds=["paper/1", "paper/2"],
        fanouts=[15, 10],
        edge_types=["CITES"],
        seed=42,
    )
```

`Database.neighbor_loader` wraps the same sampler for mini-batch iteration and
can materialize seed ids from a node label plus a simple indexed predicate.

```python
with cdb.connect("citation") as db:
    loader = db.neighbor_loader(
        "Paper",
        fanouts=[15, 10],
        edge_types=["CITES"],
        batch_size=64,
        filter="split = 'train'",
        seed=42,
    )

    for edge_index, n_id in loader:
        print(edge_index.shape, n_id.shape)
```

### Sampling semantics

- Fan-out applies per source node per layer.
- `strategy="uniform"` is the default and samples without replacement unless
  `replace=True`.
- `strategy="first"` provides explicit deterministic first-neighbor truncation.
- `strategy="all"` or `fanout=0` expands all neighbors.
- `seed=...` makes uniform sampling and loader shuffling deterministic.
- Sparse graphs and isolated seed nodes return valid empty `edge_index` arrays
  while preserving seeds in `n_id`.

### Subgraph adapters

The lower-level `caracaldb.ml.Subgraph` container and PyG/DGL/jraph adapters
remain available for workloads that already assemble Arrow-backed node and
edge tables manually.

### Key objects

| Name | Description |
|---|---|
| `Database.sample_gnn_subgraph` | One-shot GNN neighborhood sampler returning `(edge_index, n_id)`. |
| `Database.neighbor_loader` | Iterable mini-batch sampler over explicit or filtered seed nodes. |
| `Database.query_nodes` | Predicate-based seed selection with property-index acceleration when available. |
| `Subgraph` | Arrow-backed container: node tables, edge tables, and seed ids. |

### Reference

::: caracaldb.ml
    options:
      show_root_heading: false
      show_source: true

---

## Online Feature Serving

`OnlineFeatureView` pre-loads selected columns into memory and serves point lookups by node id
without touching the storage layer on each call. Use it for online training or inference where
latency matters.

| Scenario | Recommendation |
|---|---|
| Full-graph batch export | `Connection.sql` and convert the result |
| Online training / inference serving | `OnlineFeatureView` for sub-millisecond latency |
| GNN mini-batch feature gathering | `NeighborLoader` (calls the feature layer internally) |

```python
import caracaldb as cdb
from caracaldb.feature import OnlineFeatureView

with cdb.connect("citation") as db:
    view = OnlineFeatureView(db, class_name="Paper", columns=["embedding", "label"])

    batch = view.lookup([0, 1, 2])
    print(batch.column("embedding"))

    stats = view.stats()
    print(f"Lookups: {stats.lookup_count}, avg: {stats.avg_latency_us:.1f} µs")
```

### Key objects

| Name | Description |
|---|---|
| `OnlineFeatureView` | Pre-loaded column cache with a `lookup(nids)` interface. |
| `OnlineLookupStats` | Cumulative lookup count and latency percentiles. |

### Reference

::: caracaldb.feature
    options:
      show_root_heading: false
      show_source: true

---

## See Also

- [ML Integration Concept](../concepts/ml-integration.md) — Arrow-native design philosophy
- [Neighbor Loader PyG Guide](../guides/neighbor-loader-pyg.md) — full PyG training loop
- [PyG and DGL Interop](../interop/pyg-and-dgl.md) — framework-specific conversion notes
- [Feature Store Guide](../guides/feature-store.md) — cache configuration and invalidation
- [Graph](graph.md) — CSR indexes that back neighbor sampling
