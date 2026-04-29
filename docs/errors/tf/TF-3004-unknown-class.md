---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-3004 Unknown Class

## What You See

A node label or ontology class name resolves syntactically but is not present in the active catalog.

## Why It Happens

The class has not been loaded, the active snapshot predates it, or the query uses a local alias that does not match the canonical class IRI.

## How To Fix

Check the catalog snapshot, load the ontology or schema that defines the class, and retry with the canonical class name.

## Cross-References

- [Data Model](../../concepts/data-model.md)
- [Snapshots and MVCC](../../concepts/snapshots-and-mvcc.md)
