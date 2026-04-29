---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-6012 Graph Budget

## What You See

Execution stops because a graph function or traversal exceeded the configured budget.

## Why It Happens

The query expanded too many neighbors, requested a path range with a large fanout, or called an algorithm without enough selectivity.

## How To Fix

Add labels, edge types, property filters, or a smaller hop range before the expansion. For intended large jobs, raise the budget only after checking the profile.

## Cross-References

- [Pattern Queries](../../guides/pattern-queries.md)
- [Observability](../../guides/observability-explain-profile.md)
