---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-7004 Index Corruption

## What You See

Tuft reports that a graph or secondary index failed an integrity check while planning or executing a query.

## Why It Happens

The index metadata and stored index body disagree, or a checksum validation failed. This is treated as a hard error because continuing could return incomplete graph results.

## How To Fix

Stop using the affected bundle, rebuild the index from authoritative column segments, and keep the failing files for debugging if the error repeats.

## Cross-References

- [Build CSR and CSC](../../guides/build-csr-and-csc.md)
- [Storage Layout](../../concepts/storage-layout.md)
