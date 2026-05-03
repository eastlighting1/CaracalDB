---
applies_to: v0.2.x
status: shipped-partial
last_updated: 2026-05-02
engine_status: python-reference; rust-engine-planned
---

# Snapshots With `AS_OF`

Use this guide when reads need to refer to a named, stable graph view.

!!! note "Snapshot reads in v0.2.x"
    `AS_OF SNAPSHOT 'name'` is enforced for node and edge rows written
    through the public `Database` API:

    1. `MATCH (...) AS_OF SNAPSHOT 'name'` parses into the Tuft AST.
    2. `db.create_snapshot(...)`, `db.list_snapshots()`, and
       `db.release_snapshot(...)` manage named snapshots.
    3. A missing snapshot name raises `CDB-8013` from `Connection.sql(...)`
       instead of being silently ignored.
    4. Node and edge rows inserted after the snapshot LSN are hidden from
       `AS_OF` reads.

    Catalog definitions are not yet versioned. A class or property created
    after a snapshot may still be discoverable by the planner, even though
    rows written after the snapshot are filtered out.

## Problem

Training, release validation, and reproducible analytics need a read
boundary that does not move while writes continue.

## Steps

1. Create a named snapshot from the public `Database` API; no need to
   import from `caracaldb.storage.snapshot`:

   ```python
   import caracaldb as cdb
   from pathlib import Path
   from tempfile import TemporaryDirectory

   with TemporaryDirectory() as tmp:
       path = Path(tmp) / "graph.crcl"
       with cdb.connect(path) as db:
           db.define_class("Gene")
           db.insert_nodes("Gene", [{"symbol": "TP53"}])
           snap = db.create_snapshot("release-2026-04")
           print(snap.name, snap.lsn_high)
   ```

   Expected output:

   ```text
   release-2026-04 1
   ```

2. Reference the snapshot in Tuft:

   ```python
   import caracaldb as cdb
   from pathlib import Path
   from tempfile import TemporaryDirectory

   with TemporaryDirectory() as tmp:
       path = Path(tmp) / "graph.crcl"
       with cdb.connect(path) as db:
           db.define_class("Gene")
           db.insert_nodes("Gene", [{"symbol": "TP53"}])
           db.create_snapshot("release-2026-04")
           db.insert_nodes("Gene", [{"symbol": "BRCA1"}])

           rows = db.sql("""
           MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
           RETURN g.symbol
           """).rows()
           print(rows)
   ```

   Expected output:

   ```text
   [{'symbol': 'TP53'}]
   ```

   If the name is unknown the call raises `CaracalError` with code
   `CDB-8013` (`snapshot not found: 'release-2026-04'`).

3. List the registered snapshots:

   ```python
   import caracaldb as cdb
   from pathlib import Path
   from tempfile import TemporaryDirectory

   with TemporaryDirectory() as tmp:
       path = Path(tmp) / "graph.crcl"
       with cdb.connect(path) as db:
           db.define_class("Gene")
           db.insert_nodes("Gene", [{"symbol": "TP53"}])
           db.create_snapshot("release-2026-04")
           print([(entry.name, entry.lsn_high, entry.refcount) for entry in db.list_snapshots()])
   ```

   Expected output:

   ```text
   [('release-2026-04', 1, 1)]
   ```

4. Release the snapshot when it is no longer needed. The final release
   removes the entry from the registry:

   ```python
   import caracaldb as cdb
   from pathlib import Path
   from tempfile import TemporaryDirectory

   with TemporaryDirectory() as tmp:
       path = Path(tmp) / "graph.crcl"
       with cdb.connect(path) as db:
           db.define_class("Gene")
           db.insert_nodes("Gene", [{"symbol": "TP53"}])
           db.create_snapshot("release-2026-04")
           db.release_snapshot("release-2026-04")
           print(db.list_snapshots())
   ```

   Expected output:

   ```text
   []
   ```

## Verification

After step 1, `db.list_snapshots()` should report the new snapshot's
name, LSN high-water mark, creation time, and reference count. After
step 4 (and any other matching releases), the entry disappears.

Rows inserted after the snapshot should be absent from `AS_OF` reads:

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "graph.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene")
        db.insert_nodes("Gene", [{"symbol": "TP53"}])
        db.create_snapshot("v1")
        db.insert_nodes("Gene", [{"symbol": "BRCA1"}])

        rows = db.sql("MATCH (g:Gene) AS_OF SNAPSHOT 'v1' RETURN g.symbol").rows()
        print(rows)
```

Expected output:

```text
[{'symbol': 'TP53'}]
```

## Common Pitfalls

- A missing snapshot name raises `CDB-8013` rather than silently falling
  back to a latest read.
- `AS_OF DATETIME '...'` parses but raises `CDB-6021`; datetime-based
  resolution is reserved for the M4 cycle.
- Snapshot visibility applies to node and edge rows. Catalog metadata is
  still latest-version only.
- Bundles written before row-version metadata existed remain readable; their
  older rows are treated as visible to snapshots.

## Related ADR

Snapshot retention, garbage collection, and catalog-version visibility are
still tracked as follow-ups.
