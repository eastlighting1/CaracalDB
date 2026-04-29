---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CDB-9001 Bundle Exists

## What You See

CaracalDB refuses to create a bundle because the target path already exists.

## Why It Happens

Bundle creation is intentionally conservative. Reusing an existing directory could mix old manifest data with new writes.

## How To Fix

Choose a new bundle path, remove the old bundle only after confirming it is no longer needed, or open the existing bundle instead of initializing it.

## Cross-References

- [Install](../../start/install.md)
- [Packaging and CLI](../../guides/packaging-and-cli.md)
