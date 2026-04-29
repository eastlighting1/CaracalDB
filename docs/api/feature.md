---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Feature API

Feature APIs provide in-memory point lookups for node features. The Python implementation preloads selected columns and reports lookup latency stats.

## Common Entry Points

| Name | Use |
|---|---|
| `OnlineFeatureView` | Serve feature lookups by `nid`. |
| `OnlineLookupStats` | Inspect lookup count and latency summaries. |

## Reference

::: caracaldb.feature
    options:
      show_root_heading: false
      show_source: true
