---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Quickstart

This page is the shortest path from an empty environment to a CaracalDB query result. It is intentionally small: one database handle, one class, one row, one query.

## Goal

Open or create a packed `.crcl` database, insert one node, run the current MVP Tuft query shape, and return Python rows.

## Minimal Query

```python
import caracaldb as cdb

with cdb.connect("demo") as db:
    db.define_class("Gene")
    db.insert_nodes("Gene", [{"symbol": "TP53", "chromosome": "17"}])

    rows = db.sql("MATCH (g:Gene) RETURN g.symbol").rows()
    print(rows)
```
The query surface in v0.1.x supports a single node pattern with `WHERE`, `RETURN`, and `LIMIT`. Broader Tuft examples live in the language reference as the public API catches up with the planner.

## Next Steps

- Install and verify the package with [Install](install.md).
- Learn language shape in [Tuft Reference](../tuft/reference.md).
- Look up Python entry points in [API](../api/README.md).
