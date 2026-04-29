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

1. Create the view.

```python
from caracaldb.feature import OnlineFeatureView

view = OnlineFeatureView(
    bundle,
    class_iri="http://example.org/Gene",
    local_name="Gene",
    feature_columns=["score", "degree"],
)
```
2. Look up one node.

```python
features = view.lookup(42)
```
3. Look up many nodes as an Arrow table.

```python
table = view.lookup_many(nids)
```
## Verification

Use `view.stats()` to inspect lookup count, returned rows, p50, and p99 latency.

## Common Pitfalls

- Empty `feature_columns` is invalid.
- Missing node ids return empty results or null feature rows.
- The Python implementation preloads features; size your process accordingly.

## Related ADR

The Rust mmap-backed feature view should document memory and latency guarantees when promoted.
