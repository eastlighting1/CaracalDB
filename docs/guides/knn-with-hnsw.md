---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# kNN With HNSW

Use this guide when node embeddings need approximate nearest-neighbor lookup.

## Problem

Vector search should keep node ids stable while delegating nearest-neighbor work to an HNSW index.

## Steps

Create an index, add two vectors keyed by node id, and query the nearest neighbor.

```python
import numpy as np

from caracaldb.graph.hnsw import HnswConfig, HnswIndex

index = HnswIndex(HnswConfig(dim=3, metric="l2", max_elements=10))
index.add(
    ids=[1, 2],
    vectors=np.array(
        [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
        ],
        dtype=np.float32,
    ),
)

labels, distances = index.search(np.array([1.0, 0.0, 0.0], dtype=np.float32), k=1, ef=8)
print(labels.tolist(), distances.round(4).tolist())
```

Expected output:

```text
[[1]] [[0.0]]
```
## Verification

`labels` should contain UInt64 node ids and `distances` should contain Float32 scores from the configured metric.

## Common Pitfalls

- Vector shape must be `(N, dim)`.
- Query dimension must match the index dimension.
- Persist index files under the bundle `vec/` area when the index belongs to stored graph data.

## Related ADR

HNSW persistence and Rust compatibility should be captured with the vector index format ADR.
