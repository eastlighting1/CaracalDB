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

Prepare a tiny edge table, build both adjacency files, then read the results back.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from caracaldb.graph.csc_builder import build_csc
from caracaldb.graph.csr_builder import build_csr
from caracaldb.graph.csr_format import read_csr

edges = pa.table({"src": [0, 0, 1], "dst": [1, 2, 2], "eid": [10, 11, 12]})

with TemporaryDirectory() as tmp:
    out = Path(tmp)
    csr = build_csr(edges, num_vertices=3, out_path=out / "forward.csr")
    csc = build_csc(edges, num_vertices=3, out_path=out / "reverse.csc")

    forward = read_csr(csr.path, mmap=False)
    reverse = read_csr(csc.path, mmap=False)
    print(csr.num_edges, forward.offsets.tolist(), forward.neighbors.tolist())
    print(csc.num_edges, reverse.offsets.tolist(), reverse.neighbors.tolist())
```

Expected output:

```text
3 [0, 2, 3, 3] [1, 2, 2]
3 [0, 0, 1, 3] [0, 0, 1]
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
