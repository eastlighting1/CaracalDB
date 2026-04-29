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

1. Build a `Subgraph`.

```python
from caracaldb.ml.subgraph import Subgraph

sg = Subgraph()
```
2. Add node and edge Arrow tables.

```python
sg.add_nodes("http://example.org/Gene", node_table)
sg.add_edges("http://example.org/INTERACTS_WITH", edge_table)
```
3. Export to Arrow IPC.

```python
from caracaldb.exec.operators.export_arrow import export_subgraph_to_arrow

export_subgraph_to_arrow(sg, "subgraph.arrow")
```
## Verification

Import the file back with `import_subgraph_from_arrow` and compare node and edge counts.

## Common Pitfalls

- Empty subgraphs cannot be exported.
- Edge tables should preserve `src` and `dst`.
- Store snapshot id or seed metadata in `sg.meta` when reproducibility matters.

## Related ADR

Subgraph interchange should be locked down before promising cross-version Arrow IPC compatibility.
