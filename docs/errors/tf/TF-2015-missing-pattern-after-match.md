---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-2015 Missing Pattern After MATCH

## What You See

The query contains `MATCH` but no pattern follows it.

## Why It Happens

`MATCH` introduces a graph pattern. Tuft cannot infer the node, edge, or path shape from later clauses.

## How To Fix

Add a pattern immediately after `MATCH`, then place filters in `WHERE` and projected values in `RETURN`.

## Cross-References

- [Pattern Queries](../../guides/pattern-queries.md)
- [Tuft Reference](../../tuft/reference.md)
