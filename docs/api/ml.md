---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# ML API

ML APIs expose the Arrow-first `Subgraph` container and neighbor-loading surfaces used by graph ML adapters.

## Common Entry Points

| Name | Use |
|---|---|
| `Subgraph` | Hold node tables, edge tables, and metadata. |
| `NeighborLoader` | Produce sampled subgraphs. |
| `NeighborLoaderConfig` | Configure fan-out and seed behavior. |

## Reference

::: caracaldb.ml
    options:
      show_root_heading: false
      show_source: true
