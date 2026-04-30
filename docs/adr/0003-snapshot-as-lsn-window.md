---
applies_to: v0.2.x
status: experimental
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# ADR 0003: Snapshot As LSN Window

## Status

Accepted for transaction and snapshot design; query syntax support remains
version-scoped in the API docs.

## Context

CaracalDB needs repeatable reads, named export snapshots, and clear conflict
diagnostics without copying a full bundle for every read view. WAL ordering
already gives the engine a durable sequence for committed writes.

## Options Considered

- Represent each snapshot as a physical copy of the bundle.
- Track snapshots as opaque names resolved by implementation-specific code.
- Represent snapshots as log sequence number windows over committed data.

## Decision

Snapshot visibility is represented as a log sequence number window over committed data.

## Consequences

Reads can be repeatable, transaction conflicts can be explained precisely, and export workflows can record the exact snapshot used for downstream artifacts.
