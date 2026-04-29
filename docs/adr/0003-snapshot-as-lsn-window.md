---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# ADR 0003: Snapshot As LSN Window

## Decision

Snapshot visibility is represented as a log sequence number window over committed data.

## Consequences

Reads can be repeatable, transaction conflicts can be explained precisely, and export workflows can record the exact snapshot used for downstream artifacts.
