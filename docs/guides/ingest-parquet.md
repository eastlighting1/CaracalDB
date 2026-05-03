---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Ingest Parquet

Use this guide when source data already lives in Parquet and you want to load it into CaracalDB node or edge stores.

## Problem

Bulk loading should stream input in chunks, preserve Arrow-compatible columns, and isolate bad chunks without making every import an all-or-nothing operation.

## Steps

Create a bundle, write two tiny Parquet inputs, load nodes and edges, then read the stores back.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq

from caracaldb.ingest.parquet_loader import ingest_edges_from_parquet, ingest_nodes_from_parquet
from caracaldb.storage import create_bundle

with TemporaryDirectory() as tmp:
    root = Path(tmp)
    bundle = create_bundle(root / "graph", exist_ok=True)
    genes = root / "genes.parquet"
    interacts = root / "interacts.parquet"

    pq.write_table(pa.table({"symbol": ["TP53", "BRCA1"], "score": [0.91, 0.62]}), genes)
    pq.write_table(
        pa.table(
            {
                "src": pa.array([0], type=pa.uint64()),
                "dst": pa.array([1], type=pa.uint64()),
            }
        ),
        interacts,
    )

    store, report = ingest_nodes_from_parquet(
        bundle,
        parquet_path=genes,
        class_iri="http://example.org/Gene",
        local_name="Gene",
    )
    edge_store, edge_report = ingest_edges_from_parquet(
        bundle,
        parquet_path=interacts,
        property_iri="http://example.org/INTERACTS_WITH",
        local_name="INTERACTS_WITH",
    )

    print(report.rows_read, report.rows_written, report.rows_quarantined)
    print(store.to_table().to_pylist())
    print(edge_report.rows_read, edge_report.rows_written, edge_report.rows_quarantined)
    print(edge_store.to_table().to_pylist())
```

Expected output:

```text
2 2 0
[{'nid': 0, 'symbol': 'TP53', 'score': 0.91}, {'nid': 1, 'symbol': 'BRCA1', 'score': 0.62}]
1 1 0
[{'eid': 0, 'src': 0, 'dst': 1}]
```
## Verification

Check `rows_read`, `rows_written`, `rows_quarantined`, and `chunks` on the returned report. A clean import has zero quarantined rows.

After loading, open the target node or edge store and read a small batch back before building downstream indexes. For edges, verify that `src` and `dst` are `UInt64`, because CSR and CSC builders assume numeric node ids rather than external IRIs.

## Common Pitfalls

- Edge ingestion does not perform IRI-to-node-id lookup. Prepare `src` and `dst` before loading.
- Inbound `nid` and `eid` columns are stripped because stores assign those identities.
- Use `column_map` when source column names do not match CaracalDB's expected names.
- Keep `chunksize` positive and small enough that a quarantined chunk is easy to inspect.
- If rows are quarantined, inspect the first `report.quarantined` entry before retrying; it carries the CaracalDB error code and message for the failed chunk.

## Related ADR

The long-term import contract should be captured in a storage/ingest ADR when external schema mapping becomes stable.
