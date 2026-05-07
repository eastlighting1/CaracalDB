---
applies_to: v0.2.x
status: stable
last_updated: 2026-05-07
engine_status: python-reference; rust-engine-planned
---

# Graph — Adjacency Indexes

The Graph API builds and reads compressed sparse row (CSR) and compressed sparse column (CSC)
adjacency indexes. These indexes power edge traversal in the physical execution layer and
GNN neighbor sampling.

## CSR vs CSC

| Index | Stores | Use case |
|---|---|---|
| CSR | Outgoing neighbors (`src → [dst]`) | Forward traversal, GNN fan-out sampling |
| CSC | Incoming neighbors (`dst → [src]`) | Reverse traversal, in-degree queries |

Both share the same physical file format — CSC is CSR with `src` and `dst` columns swapped.

## Database traversal API

`Database` exposes adjacency helpers that build and reuse CSR/CSC indexes lazily:

```python
import caracaldb as cdb

with cdb.connect("movies") as db:
    tom = db.nodes("Person").where(name="Tom Hanks").first()
    assert tom is not None

    acted_in = db.out(tom["node_id"], "ACTED_IN")
    actors = db.in_("movie/forrest-gump", "ACTED_IN")
    acted_count = db.degree(tom["node_id"], "ACTED_IN")
    shared_movies = db.common_neighbors("person/tom", "person/meg", "ACTED_IN")
    ranked = db.overlap("person/tom", ["person/meg", "person/kevin"], "ACTED_IN", top_k=10)
```

The traversal helpers accept an internal id, a `ResourceRef`, or a stored
`node_id` value and return Arrow tables. `out` and `in_` normalize traversal
results to `src`/`dst` columns, while `common_neighbors` and `overlap` return
internal neighbor ids for recommendation-style follow-up queries.

When node or edge batches are appended through `Database`, derived graph index
files are invalidated automatically so the next traversal rebuilds against the
latest bundle state.

## Graph ecosystem primitives

v0.2.7 adds higher-level graph ecosystem APIs for semantic retrieval and
evidence/path workloads:

```python
import pyarrow as pa
import caracaldb as cdb

with cdb.connect("knowledge") as db:
    db.upsert_node_table_arrow(
        pa.table(
            {
                "node_id": ["chunk/1", "chunk/2"],
                "type": ["Chunk", "Chunk"],
                "text": ["alpha evidence", "beta evidence"],
                "embedding": [[1.0, 0.0], [0.0, 1.0]],
            }
        )
    )

    db.create_vector_index(
        name="chunk_embedding_hnsw",
        node_type="Chunk",
        property="embedding",
        dimension=2,
        metric="cosine",
    )

    seeds = db.vector_search(
        index="chunk_embedding_hnsw",
        query_vector=[1.0, 0.0],
        top_k=1,
        return_properties=["text"],
    )

    subgraph = db.k_hop(
        seeds=[row["node_id"] for row in seeds.rows()],
        edge_types=["MENTIONS", "RELATED_TO"],
        depth=2,
    )
```

Path APIs return Arrow-native path artifacts with stable node ids, edge ids,
relation types, directions, edge properties, and optional weighted scores:

```python
paths = db.paths(
    source="entity/a",
    target="chunk/1",
    edge_types=["RELATED_TO", "EVIDENCED_BY"],
    max_depth=3,
    score="sum",
    score_property="weight",
)

path = db.shortest_path(
    source="entity/a",
    target="chunk/1",
    edge_types=["RELATED_TO", "EVIDENCED_BY"],
)
```

Tuft exposes the same substrate for semantic entry and bounded path matching:

```tuft
CALL vector.search('chunk_embedding_hnsw', [1.0, 0.0], 8)
YIELD node_id, score
RETURN node_id, score
ORDER BY score DESC
LIMIT 8
```

```tuft
MATCH p = (a:Entity)-[:RELATED_TO*1..3]->(b:Chunk)
RETURN p, length(p) AS hops, b.node_id
ORDER BY hops DESC
LIMIT 20
```

## Building indexes

```python
import pyarrow as pa
from caracaldb.graph import build_csr, build_csc, CsrReader

edges = pa.table({
    "src": [0, 0, 1, 2],
    "dst": [1, 2, 2, 3],
})

result = build_csr(edges, output_path="mydb.crcl/graph/INTERACTS_WITH.csr")
print(f"Built CSR with {result.edge_count} edges")

# Read back
reader = CsrReader("mydb.crcl/graph/INTERACTS_WITH.csr")
for batch in reader.neighbors([0, 1]):
    print(batch)
```

## Functions

| Name | Description |
|---|---|
| [`build_csr` | Build a forward (outgoing) adjacency index from an edge table. |
| [`build_csc` | Build a reverse (incoming) adjacency index from an edge table. |
| [`read_csr` | Read the raw CSR file as a pair of Arrow arrays (offsets, neighbors). |
| [`write_csr` | Write raw offset/neighbor arrays to a CSR file. |

## Classes

| Name | Description |
|---|---|
| [`CsrReader` | High-level reader: look up neighbor batches by seed node ids. |
| [`CSRBuildResult` | Summary returned by `build_csr` / `build_csc` (edge count, file size). |

## Constants

| Name | Description |
|---|---|
| `CSR_HEAD_FMT` / `CSR_HEAD_SIZE` | Struct format and byte size of the CSR file header. |
| `CSR_FOOTER_FMT` / `CSR_FOOTER_SIZE` | Struct format and byte size of the CSR file footer. |
| `CSR_FLAG_HAS_EIDS` | Flag bit indicating the CSR file stores edge ids alongside neighbors. |

## Reference

::: caracaldb.graph
    options:
      show_root_heading: false
      show_source: true

## See Also

- [Build CSR and CSC Guide](../guides/build-csr-and-csc.md) — step-by-step walkthrough
- [CSR/CSC Format](../format/csr-csc.md) — wire format specification
- [Pattern Queries](../guides/pattern-queries.md) — the query surface that consumes graph-oriented execution pieces
- [ML](ml.md) — neighbor sampling for GNN mini-batching
