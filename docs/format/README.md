---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Format

Format pages document stable on-disk and wire contracts. They are for contributors, tool authors, and advanced users who need to inspect bundles without relying on private implementation details.

## Pages

- [CRCL bundle](crcl-bundle.md)
- [Manifest](manifest.md)
- [Column segment](column-segment.md)
- [CSR / CSC](csr-csc.md)
- [WAL](wal.md)
- [Catalog FlatBuffers](catalog-fb.md)

## Compatibility Rule

Readers should reject unsupported major versions, ignore reserved fields only when the page says they are forward-compatible, and preserve unknown manifest entries when rewriting metadata.
