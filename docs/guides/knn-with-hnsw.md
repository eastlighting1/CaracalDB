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

1. Create an index config.

```python
from caracaldb.graph.hnsw import HnswConfig, HnswIndex

index = HnswIndex(HnswConfig(dim=64, metric="cosine", max_elements=10000))
```
2. Add vectors keyed by node id.

```python
index.add(ids=[1, 2], vectors=vectors)
```
3. Query nearest neighbors.

```python
labels, distances = index.search(query_vector, k=10, ef=64)
```
## Verification

`labels` should contain UInt64 node ids and `distances` should contain Float32 scores from the configured metric.

## Common Pitfalls

- Vector shape must be `(N, dim)`.
- Query dimension must match the index dimension.
- Persist index files under the bundle `vec/` area when the index belongs to stored graph data.

## Related ADR

HNSW persistence and Rust compatibility should be captured with the vector index format ADR.
