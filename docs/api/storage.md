---
applies_to: v0.2.x
status: stable
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# Storage & Transactions

The Storage API is the low-level foundation for `.crcl` bundles, manifests, column segment I/O,
and packing. The Transaction API sits directly on top, providing OCC-based write-write conflict
detection over the MVCC storage layer.

!!! note "Application code does not need most of this"
    If you are connecting to a database and running queries, use
    `caracaldb.connect` instead. Use these APIs for importers, CLI tooling,
    custom exporters, engine contributors, and explicit transaction management.

---

## Storage

### Bundle lifecycle

A CaracalDB database lives in a `.crcl` bundle — either a **directory bundle** (a folder with
`MANIFEST`, `catalog.fb`, `nodes/`, `edges/`, etc.) or a **packed file** (a single ZIP-like
file produced by `pack_bundle`). The public `connect` function handles this automatically.

```python
from caracaldb.storage import open_bundle, pack_bundle, is_packed
from pathlib import Path

src = Path("mydb.crcl")
if is_packed(src):
    print("This is a packed file")

pack_bundle(Path("mydb.crcl"), output=Path("mydb_packed.crcl"))
```

### Key objects

| Name | Description |
|---|---|
| `create_bundle` | Create a new empty directory bundle. |
| `open_bundle` | Open an existing directory bundle. |
| `pack_bundle` / `unpack_bundle` | Convert between directory and packed forms. |
| `is_packed` | Return `True` if a path is a packed `.crcl` file. |
| `Bundle` | Open bundle handle (path + manifest). |
| `Manifest` | Bundle metadata: version, LSNs, snapshot list. |
| `read_column_segment` / `write_column_segment` | Read/write column segment files as Arrow RecordBatches. |
| `ColumnReader` / `ColumnWriter` | Streaming I/O for column segment files. |
| `ColumnSegmentFooter` | Parsed footer from a column segment file. |
| `BufferPool` | Optional page cache for read-heavy workloads. |
| `BufferPoolStats` | Hit/miss counters from the buffer pool. |
| `PageFrame` / `PageGuard` / `PageId` | Page management primitives. |

### Reference

::: caracaldb.storage
    options:
      show_root_heading: false
      show_source: true

---

## Transactions

CaracalDB uses **optimistic concurrency control (OCC)**:

1. A transaction opens at a snapshot LSN (`BEGIN`) — reads see data up to that point.
2. Writes accumulate in a write set without locks.
3. On `COMMIT`, conflicts are checked against commits since the snapshot.
4. `TxConflictError` (`CDB-8002`) signals a conflict; the caller should retry.

```python
from caracaldb.tx import TransactionManager, TxConflictError
import caracaldb as cdb

with cdb.connect("mydb") as db:
    tm = TransactionManager(db)
    try:
        tx = tm.begin()
        # ... perform writes ...
        tm.commit(tx)
    except TxConflictError:
        print("Conflict — retry")
```

### Key objects

| Name | Description |
|---|---|
| `TransactionManager` | Begin, commit, and roll back transactions; detect write-write conflicts. |
| `Transaction` | Snapshot-tagged transaction state and accumulated write set. |
| `TxConflictError` | Raised on write-write conflict (`CDB-8002`). |
| `BEGIN_KIND` / `COMMIT_KIND` / `ROLLBACK_KIND` | WAL record kind constants. |

### Reference

::: caracaldb.tx
    options:
      show_root_heading: false
      show_source: true

---

## See Also

- [Storage Layout Concept](../concepts/storage-layout.md) — visual overview of the bundle structure
- [Snapshots & MVCC Concept](../concepts/snapshots-and-mvcc.md) — MVCC and snapshot interaction
- [CRCL Bundle Format](../format/crcl-bundle.md) — wire format specification
- [Error CDB-8002](../errors/cdb/CDB-8002-tx-conflict.md) — transaction conflict reference
