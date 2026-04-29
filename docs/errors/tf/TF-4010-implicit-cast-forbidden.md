---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-4010 Implicit Cast Forbidden

## What You See

Tuft refuses to coerce a value automatically between two types.

## Why It Happens

The language does not silently widen, narrow, parse, or stringify values when doing so could change query semantics.

## How To Fix

Use an explicit cast or convert the source data during ingest so the stored property already has the intended type.

## Cross-References

- [Tuft Specification](../../tuft/spec.md)
- [Ingest Parquet](../../guides/ingest-parquet.md)
