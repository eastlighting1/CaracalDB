---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# WAL

The write-ahead log records transaction intent before bundle metadata becomes visible. Its public contract is recovery behavior, not byte-level stability.

## Recovery Contract

On startup, CaracalDB should be able to distinguish committed, aborted, and incomplete writes. Incomplete writes must not become visible through the manifest.

## Record Shape

WAL records identify transaction id, affected bundle resources, operation kind, and enough metadata to finish or roll back publication.

## Reader Guidance

External tools should not depend on WAL internals in v0.1.x. Use the manifest and catalog for stable inspection.
