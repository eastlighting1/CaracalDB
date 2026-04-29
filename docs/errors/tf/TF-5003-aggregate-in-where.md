---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-5003 Aggregate In WHERE

## What You See

The query uses an aggregate expression inside `WHERE`.

## Why It Happens

`WHERE` filters rows before grouping. Aggregates are only valid after the grouping boundary, usually in `WITH`, `HAVING`-style filters, or `RETURN` projections depending on the query shape.

## How To Fix

Move the aggregate into a grouping step, bind it to a name, and filter the grouped result in the next clause.

## Cross-References

- [Aggregations and Top-K](../../guides/aggregations-and-topk.md)
- [Tuft Reference](../../tuft/reference.md)
