---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-3005 Unknown Property

## What You See

Tuft cannot resolve a property name used in a predicate, projection, aggregation, or path expression.

## Why It Happens

The property was not declared for the active graph schema, or the query is reading a snapshot where the property is not yet visible.

## How To Fix

Confirm the property exists in the active catalog and that the query snapshot includes it. If the property is optional, guard the expression before applying strict comparisons.

## Cross-References

- [Data Model](../../concepts/data-model.md)
- [Snapshots AS_OF](../../guides/snapshots-as-of.md)
