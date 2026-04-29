---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Pattern Queries

Use this guide when you want to read nodes from a CaracalDB bundle with Tuft and return an Arrow table.

## Problem

The v0.1.x executor intentionally supports a narrow query shape: one class-labeled node pattern, optional filtering, projected fields, and an optional literal limit.

## Steps

1. Match a class.

```tuft
MATCH (g:Gene)
RETURN g.symbol
```
2. Filter by a property.

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
```
3. Return multiple fields.

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol, g.chromosome
LIMIT 5
```
4. Execute from Python.

```python
with cdb.connect("demo", format="bundle") as db:
    table = db.cursor().sql("MATCH (g:Gene) RETURN g.symbol LIMIT 5").arrow()
```
## Verification

The result should be a `pyarrow.Table`. Projection names come from the returned property names unless `AS` aliases are used.

When a query returns no rows, verify the catalog class first, then inspect the node store columns. The MVP executor resolves class labels through the catalog and reads projected properties from the stored Arrow schema.

## Common Pitfalls

- Relationship patterns are parsed by Tuft but are outside the v0.1.x MVP executor.
- `LIMIT` must be an integer literal in the current executor.
- Class names resolve through the catalog. If a class is missing, expect `CDB-6021` or a Tuft binding diagnostic.
- Keep examples to one node binding until multi-hop execution is promoted into the stable executor.
- Prefer explicit projections over `RETURN *` in docs and tests so schema changes are visible.

## Related ADR

The broader pattern planner should get an ADR when multi-hop execution becomes part of the stable public surface.
