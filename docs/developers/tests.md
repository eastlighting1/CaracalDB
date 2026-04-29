---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Tests

This page maps test directories to the kind of change they protect.

## Directory Map

| Directory | Purpose |
|---|---|
| `tests/lang` | Tuft parsing, binding, and diagnostics |
| `tests/storage` | Bundle, manifest, and storage behavior |
| `tests/exec` | Physical execution operators |
| `tests/plan` | Logical plans and planner behavior |
| `tests/golden` | Stable expected output |
| `tests/fuzz` | Property and fuzz-style parser checks |
| `tests/recovery` | Crash and recovery behavior |
| `tests/tx` | Transaction semantics |
| `tests/ml` | ML adapters and subgraph exports |
| `tests/feature` | Feature store behavior |
| `tests/observability` | Explain and profile output |
| `tests/cli` | Command-line behavior |
| `tests/e2e` | End-to-end workflows |

## Rule

Add the smallest test that would fail for the bug or behavior change. Broaden coverage when the change touches public contracts or shared infrastructure.
