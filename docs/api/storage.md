---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Storage API

Storage APIs are lower-level building blocks for bundles, manifests, column segments, packing, and in-memory page management. Application code should usually start with `caracaldb.connect`; storage APIs are for importers, tools, tests, and engine contributors.

## Common Entry Points

| Name | Use |
|---|---|
| `create_bundle` | Create a `.crcl` directory bundle. |
| `open_bundle` | Open an existing directory bundle. |
| `pack_bundle` / `unpack_bundle` | Convert between directory and packed forms. |
| `read_column_segment` / `write_column_segment` | Work with column segment files. |

## Reference

::: caracaldb.storage
    options:
      show_root_heading: false
      show_source: true
