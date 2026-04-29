---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Manifest

The bundle manifest records which files belong to a CaracalDB bundle and how readers should validate them.

## Required Information

| Field | Purpose |
|---|---|
| Bundle version | Determines compatibility policy |
| Catalog path | Points to schema metadata |
| Segment entries | List node and edge column files |
| Index entries | List CSR, CSC, HNSW, or secondary indexes |
| Checksums | Detect partial or corrupted writes |

## Writer Rule

Writers should prepare new files first, validate their checksums, then publish a manifest update last. Readers should treat a manifest as authoritative only after it is parsed and validated.
