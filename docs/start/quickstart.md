---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Quickstart

This page is the shortest path from an empty environment to a CaracalDB query result. It is intentionally small: one database handle, one query shape, one Arrow table.

## Goal

Open or create a `.crcl` database, run the current MVP Tuft query shape, and return a `pyarrow.Table`.

## Minimal Query

```python
import pyarrow as pa
import caracaldb as cdb
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.node_store import open_node_store

bundle = create_bundle("demo", exist_ok=True)
catalog = Catalog.empty()
catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
save_catalog(bundle, catalog)

store = open_node_store(
    bundle,
    class_iri="http://example.org/Gene",
    local_name="Gene",
    create=True,
)
store.append(
    pa.record_batch({"symbol": pa.array(["TP53"]), "chromosome": pa.array(["17"])})
)

with cdb.connect(bundle.path, format="bundle") as db:
    table = db.cursor().sql("MATCH (g:Gene) RETURN g.symbol").arrow()
    print(table.to_pylist())
```
The query surface in v0.1.x supports a single node pattern with `WHERE`, `RETURN`, and `LIMIT`. Broader Tuft examples live in the language reference as the public API catches up with the planner.

## Next Steps

- Install and verify the package with [Install](install.md).
- Learn language shape in [Tuft Reference](../tuft/reference.md).
- Look up Python entry points in [API](../api/README.md).
