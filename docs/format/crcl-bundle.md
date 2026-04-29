---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CRCL Bundle

A `.crcl` bundle is the durable unit of CaracalDB storage. It groups catalog metadata, column segments, graph indexes, and transaction logs under one directory.

## Layout

```text
graph.crcl/
  manifest.json
  catalog.fb
  nodes/
  edges/
  indexes/
  wal/
```
## Rules

- `manifest.json` is the entry point for readers.
- `catalog.fb` stores schema, classes, properties, and ids.
- Column and index files are content-addressed or manifest-addressed.
- Writers use temporary files and atomic rename for visible updates.

## Validation

Openers should validate manifest shape first, then catalog compatibility, then referenced file checksums.
