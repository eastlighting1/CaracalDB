---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Feature Store

Use this guide when model-serving code needs fast point lookups for node features.

## Problem

Graph ML often needs feature vectors by node id. `OnlineFeatureView` preloads selected node columns and serves point-in-time lookups from memory.

## Steps

Open the weighted example database, then look up one node and a small batch.

```python
import caracaldb as cdb
import numpy as np

from caracaldb.feature import OnlineFeatureView

with cdb.connect("examples/data/example_weighted.crcl", mode="ro") as db:
    view = OnlineFeatureView(
        db.bundle,
        class_iri="http://example.org/GraphNode",
        local_name="GraphNode",
        feature_columns=["pagerank", "degree"],
    )

    one = {k: v.item() for k, v in view.lookup(0).items()}
    many = view.lookup_many(np.array([0, 1], dtype=np.uint64)).to_pylist()
    print(one)
    print(many)
```

Expected output:

```text
{'pagerank': 0.15, 'degree': 2}
[{'nid': 0, 'pagerank': 0.15, 'degree': 2}, {'nid': 1, 'pagerank': 0.85, 'degree': 10}]
```
## Verification

Use `view.stats()` to inspect lookup count, returned rows, p50, and p99 latency.

## Common Pitfalls

- Empty `feature_columns` is invalid.
- Missing node ids return empty results or null feature rows.
- The Python implementation preloads features; size your process accordingly.

## Related ADR

The Rust mmap-backed feature view should document memory and latency guarantees when promoted.
