---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Column Segment

Column segments store node and edge properties in Arrow-compatible chunks. They are the source of truth used to rebuild indexes and materialize query results.

## Shape

Each segment belongs to a logical table, column, and snapshot range. Segment metadata records type, row count, encoding, and checksum.

## Reader Expectations

- Validate the segment against the manifest before decoding.
- Preserve nullability and logical type.
- Avoid silently widening or narrowing values during load.
- Use the catalog to interpret column identity, not display names alone.

## Failure Mode

If a segment cannot be validated, report a storage error and rebuild from authoritative source data rather than returning partial query results.
