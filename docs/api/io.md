---
applies_to: v0.2.x
status: stable
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# Input / Output

This page covers the primary entry points for opening a database, running queries, consuming
results, and bulk-loading data from external sources. All application code starts here.

---

## Connections

### Opening a database

All application code begins with `connect`. It returns a `Database` handle which you use for all
subsequent operations — inserting data, defining classes, opening query cursors, and managing snapshots.

!!! tip "Use the context manager"
    Always open with `with` so that packed `.crcl` files are re-packed correctly on exit:

    ```python
    import caracaldb as cdb

    with cdb.connect("mydb") as db:
        conn = db.cursor()
        rows = conn.sql("MATCH (g:Gene) RETURN g.symbol LIMIT 5").rows()
    ```

### Key objects

| Name | Description |
|---|---|
| `connect` | Open or create a CaracalDB database. |
| `Database` | Handle to an open database. Owns storage, catalog, and lifecycle. |
| `Connection` | Query cursor bound to a `Database`. Execute Tuft via `.sql()`. |
| `Result` | Materialized query output. Convert to Arrow or Python rows. |
| `ResourceRef` | Resolved node identity with external id, internal id, and IRI. |

### Reference

::: caracaldb.api
    options:
      members:
        - connect
        - Database
        - Connection
        - Result
        - ResourceRef
      show_root_heading: false
      show_source: true

---

## Ingest

The Ingest API loads bulk node and edge data from Parquet files in a single pass, bypassing
the row-at-a-time insert path. Use it when seeding large datasets from files on disk.

!!! note "Ingest vs. `Database.insert_node_table`"
    Use `ingest_nodes_from_parquet` for thousands or more rows from a file.
    Use `Database.insert_node_table` for programmatic or small online writes.

### Example

```python
import caracaldb as cdb
from caracaldb.ingest import ingest_nodes_from_parquet, ingest_edges_from_parquet

with cdb.connect("biomedical") as db:
    report = ingest_nodes_from_parquet(db, "data/genes.parquet", class_name="Gene")
    print(f"Loaded {report.rows_written} rows")

    edge_report = ingest_edges_from_parquet(db, "data/interactions.parquet", relation="INTERACTS_WITH")
    print(f"Loaded {edge_report.rows_written} edges")
```

### Key objects

| Name | Description |
|---|---|
| `ingest_nodes_from_parquet` | Bulk-load a Parquet file as a node class. |
| `ingest_edges_from_parquet` | Bulk-load a Parquet file as a typed edge relation. |
| `ParquetLoadReport` | Summary of a completed ingest: row count, skipped rows, duration. |

### Reference

::: caracaldb.ingest
    options:
      show_root_heading: false
      show_source: true

---

## See Also

- [Quickstart](../start/quickstart.md) — first end-to-end walkthrough
- [Tuft Reference](../tuft/reference.md) — query language syntax
- [Ingest Parquet Guide](../guides/ingest-parquet.md) — step-by-step ingest walkthrough
- [Storage](storage.md) — lower-level bundle and transaction APIs
