---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Build CSR And CSC

Use this guide when an edge store or Arrow edge table needs adjacency indexes for traversal, sampling, or graph algorithms.

## Problem

Row-oriented edge tables are easy to ingest, but neighbor-oriented workloads need compact adjacency. CaracalDB uses CSR for outgoing adjacency and CSC for incoming adjacency.

## Steps

1. Prepare an edge table with `src`, `dst`, and optionally `eid`.

```python
import pyarrow as pa

edges = pa.table({"src": [0, 0, 1], "dst": [1, 2, 2], "eid": [10, 11, 12]})
```
2. Build CSR.

```python
from caracaldb.graph.csr_builder import build_csr

result = build_csr(edges, num_vertices=3, out_path="graph/forward.csr")
```
3. Build CSC for reverse adjacency.

```python
from caracaldb.graph.csc_builder import build_csc

reverse = build_csc(edges, num_vertices=3, out_path="graph/reverse.csc")
```
## Verification

The build result records the path, number of vertices, number of edges, and whether edge ids were stored. Use the CSR reader to verify checksum and layout before trusting a generated file.

For a smoke test, read the file back with `caracaldb.graph.csr_format.read_csr(path, mmap=True)` and compare `offsets[-1]` with the expected edge count. For CSC, check a node with known inbound edges so direction mistakes are obvious.

## Common Pitfalls

- `num_vertices` must be larger than every source vertex for CSR and every destination vertex for CSC.
- CSR arrays must be UInt64 and one-dimensional.
- A checksum mismatch is reported as `CDB-7081`; rebuild the index from trusted edge data.
- Rebuild CSR and CSC from the same trusted edge table when you need bidirectional traversal.
- Preserve `eid` when sampled or exported edges must map back to edge-store rows.

## Related ADR

The binary CSR/CSC format belongs in the public format ADR once the Rust engine file compatibility story is finalized.
