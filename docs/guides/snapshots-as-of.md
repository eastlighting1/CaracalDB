---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Snapshots With `AS_OF`

Use this guide when reads need to refer to a named, stable graph view.

!!! warning "Experimental surface"
    Named snapshot registry helpers exist in the Python reference implementation, but full `AS_OF` query execution is still an evolving surface. Verify support in the API page for your installed version before using this in production workflows.

## Problem

Training, release validation, and reproducible analytics need a read boundary that does not move while writes continue.

## Steps

1. Create a named snapshot.

```python
from caracaldb.storage.snapshot import create_snapshot

snapshot = create_snapshot(bundle, "release-2026-04")
```
2. Use `AS_OF` in Tuft to express the read boundary.

```tuft
MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
RETURN g.symbol
```
3. Release the snapshot when it is no longer needed.

```python
from caracaldb.storage.snapshot import release_snapshot

release_snapshot(bundle, "release-2026-04")
```
## Verification

The snapshot registry should list the name, LSN high-water mark, creation time, and reference count.

## Common Pitfalls

- `AS_OF` syntax can exist before every query path supports snapshot execution.
- Snapshot names must be non-empty and unique.
- Releasing a snapshot decrements its reference count and can remove it.

## Related ADR

Snapshot retention and garbage collection should be documented before long-lived production snapshots are encouraged.
