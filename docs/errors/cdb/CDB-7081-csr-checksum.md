---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CDB-7081 CSR Checksum

## What You See

The CSR or CSC adjacency index fails checksum validation.

## Why It Happens

The index file does not match its manifest entry, or the index was written by an incompatible or interrupted build.

## How To Fix

Rebuild the adjacency index from edge columns and verify the bundle before serving queries from it.

## Cross-References

- [Build CSR and CSC](../../guides/build-csr-and-csc.md)
- [Graph API](../../api/graph.md)
