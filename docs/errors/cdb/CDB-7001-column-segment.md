---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CDB-7001 Column Segment

## What You See

CaracalDB cannot read or validate a stored column segment.

## Why It Happens

The segment file is missing, truncated, encoded with an unsupported format version, or inconsistent with the bundle manifest.

## How To Fix

Verify the bundle manifest, restore the segment from a known-good copy, or re-ingest the source data to rebuild the affected bundle.

## Cross-References

- [Storage Layout](../../concepts/storage-layout.md)
- [Storage API](../../api/storage.md)
