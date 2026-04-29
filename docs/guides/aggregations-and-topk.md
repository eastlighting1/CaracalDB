---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Aggregations And Top-K

Use this guide when a query plan or internal pipeline needs grouped aggregates, ordering, or a bounded top-k result.

## Problem

Aggregations and top-k are planner-level operations. The physical operators exist in v0.1.x, while the public `Connection.sql(...)` MVP surface remains focused on single-node `MATCH` queries.

## Steps

1. Use aggregate built-ins in Tuft where the planner supports them.

```tuft
MATCH (g:Gene)
RETURN g.chromosome, count(g)
```
2. Use ordering and limit for top-k style result shapes.

```tuft
MATCH (g:Gene)
RETURN g.symbol, g.score
ORDER BY g.score DESC
LIMIT 10
```
3. For internal execution work, map aggregate requests to `HashAggregateOperator` kernels: `count`, `sum`, `min`, `max`, `mean`, `avg`, `collect`, and `count_star`.

## Verification

For a public query, verify the API page for your version marks the aggregate shape executable. For internal operators, assert the output column names and row counts against a small Arrow table.

## Common Pitfalls

- Do not assume every parsed aggregate syntax is wired through `Connection.sql(...)`.
- `TopKOperator` materializes and sorts the input in v0.1.x; large external-sort behavior is planned later.
- Keep output names explicit when multiple aggregates target the same input column.

## Related ADR

Planner lowering for aggregate and top-k syntax should be documented when the public query surface expands beyond the MVP path.
