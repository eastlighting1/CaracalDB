---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Export Subgraph

Use this guide when a selected graph slice needs to move into another tool.

## Problem

Subgraph export should preserve node tables, edge tables, and metadata without forcing a framework-specific format.

## Steps

Build a `Subgraph`, export it to Arrow IPC, and import it back for a count check.

```python
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from caracaldb.exec.operators.export_arrow import (
    export_subgraph_to_arrow,
    import_subgraph_from_arrow,
)
from caracaldb.ml.subgraph import Subgraph

sg = Subgraph()
sg.add_nodes("http://example.org/Gene", pa.table({"nid": [0, 1], "symbol": ["TP53", "BRCA1"]}))
sg.add_edges("http://example.org/INTERACTS_WITH", pa.table({"src": [0], "dst": [1]}))
sg.meta["snapshot"] = "release-2026-04"

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "subgraph.arrow"
    export_subgraph_to_arrow(sg, path)
    loaded = import_subgraph_from_arrow(path)

    print(loaded.num_nodes(), loaded.num_edges(), loaded.meta["snapshot"])
```

Expected output:

```text
2 1 release-2026-04
```
## Verification

Import the file back with `import_subgraph_from_arrow` and compare node and edge counts.

## Common Pitfalls

- Empty subgraphs cannot be exported.
- Edge tables should preserve `src` and `dst`.
- Store snapshot id or seed metadata in `sg.meta` when reproducibility matters.

## Related ADR

Subgraph interchange should be locked down before promising cross-version Arrow IPC compatibility.
