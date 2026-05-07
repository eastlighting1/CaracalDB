---
applies_to: v0.2.x
status: stable
last_updated: 2026-05-07
engine_status: python-reference; rust-engine-planned
---

# kNN With HNSW

Use this guide when node embeddings need approximate nearest-neighbor lookup.

## Problem

Vector search should keep node ids stable while delegating nearest-neighbor work to an HNSW index.

## Steps

For low-level experiments, create an HNSW index directly, add vectors keyed by
node id, and query the nearest neighbor.

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

## Database API

For stored graph data, prefer the database-level vector index lifecycle API so
metadata is persisted in the `.crcl` bundle:

```python
import pyarrow as pa
import caracaldb as cdb

with cdb.connect("semantic") as db:
    db.insert_node_table_arrow(
        pa.table(
            {
                "node_id": ["chunk/1", "chunk/2"],
                "type": ["Chunk", "Chunk"],
                "embedding": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]],
                "text": ["alpha", "beta"],
            }
        )
    )

    db.create_vector_index(
        name="chunk_embedding_hnsw",
        node_type="Chunk",
        property="embedding",
        dimension=3,
        metric="cosine",
    )

    result = db.vector_search(
        index="chunk_embedding_hnsw",
        query_vector=[1.0, 0.0, 0.0],
        top_k=1,
        return_properties=["text"],
    )
    print(result.rows())
```

## Common Pitfalls

- Vector shape must be `(N, dim)`.
- Query dimension must match the index dimension.
- Persist index files under the bundle `vec/` area when the index belongs to stored graph data.

## Related ADR

HNSW persistence and Rust compatibility should be captured with the vector index format ADR.
