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

1. Create or open a bundle.

```python
from caracaldb.storage import create_bundle

bundle = create_bundle("graph", exist_ok=True)
```
2. Load node rows.

```python
from caracaldb.ingest.parquet_loader import ingest_nodes_from_parquet

store, report = ingest_nodes_from_parquet(
    bundle,
    parquet_path="genes.parquet",
    class_iri="http://example.org/Gene",
    local_name="Gene",
)
```
3. Load edge rows with `src` and `dst` columns that are already UInt64 node ids.

```python
from caracaldb.ingest.parquet_loader import ingest_edges_from_parquet

edge_store, edge_report = ingest_edges_from_parquet(
    bundle,
    parquet_path="interacts.parquet",
    property_iri="http://example.org/INTERACTS_WITH",
    local_name="INTERACTS_WITH",
)
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
