---
applies_to: v0.2.x
status: experimental
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# Machine Learning

CaracalDB exposes Arrow-native ML surfaces: the `NeighborLoader` for GNN mini-batch sampling,
`Subgraph` as the exchange container, and `OnlineFeatureView` for low-latency point lookups
during training or inference.

---

## Neighbor Sampling & Subgraph

`NeighborLoader` is a CaracalDB-native sampler that runs inside the embedded process, avoiding IPC
overhead. It produces `Subgraph` objects — pure Arrow containers mapping directly to the `Data` /
`HeteroData` objects expected by PyG and DGL.

```python
import caracaldb as cdb
from caracaldb.ml import NeighborLoader, NeighborLoaderConfig

with cdb.connect("citation") as db:
    config = NeighborLoaderConfig(
        seed_class="Paper",
        num_neighbors=[15, 10],   # 2-hop fan-out
        batch_size=64,
    )
    loader = NeighborLoader(db, config)

    for subgraph in loader:
        # subgraph.nodes: dict[str, pa.Table]
        # subgraph.edges: dict[str, pa.Table]
        print(subgraph.nodes["Paper"].num_rows)
```

### Exporting to frameworks

```python
# PyG
import torch
from torch_geometric.data import Data

sg = next(iter(loader))
paper_table = sg.nodes["Paper"]
x = torch.from_numpy(paper_table.column("embedding").to_pylist())
```

### Key objects

| Name | Description |
|---|---|
| `NeighborLoader` | Iterable sampler that yields mini-batch `Subgraph` objects. |
| `NeighborLoaderConfig` | Fan-out, seed class, batch size, and feature column configuration. |
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
