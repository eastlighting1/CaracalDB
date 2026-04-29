---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Graph API

Graph APIs build and read adjacency indexes. CSR represents outgoing adjacency; CSC uses the same physical format with `src` and `dst` swapped for incoming adjacency.

## Common Entry Points

| Name | Use |
|---|---|
| `build_csr` | Build forward adjacency from edge data. |
| `build_csc` | Build reverse adjacency from edge data. |
| `CsrReader` | Read adjacency batches. |
| `read_csr` / `write_csr` | Work with the raw CSR file format. |

## Reference

::: caracaldb.graph
    options:
      show_root_heading: false
      show_source: true
