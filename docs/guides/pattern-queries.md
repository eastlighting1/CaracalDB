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

```python
import caracaldb as cdb
with cdb.connect("examples/data/example_simple.crcl", mode="ro") as db:
    rows = db.sql("MATCH (p:Person) RETURN p.name").rows()
    print(rows)
```

Expected output:

```text
[{'name': 'Alice'}, {'name': 'Bob'}, {'name': 'Charlie'}, {'name': 'Diana'}]
```

2. Filter by a property.

```python
import caracaldb as cdb
with cdb.connect("examples/data/example_simple.crcl", mode="ro") as db:
    rows = db.sql("""
    MATCH (p:Person)
    WHERE p.city = 'London'
    RETURN p.name, p.age
    """).rows()
    print(rows)
```

Expected output:

```text
[{'name': 'Bob', 'age': 34}]
```

3. Return multiple fields and an Arrow table.

```python
import caracaldb as cdb
with cdb.connect("examples/data/example_simple.crcl", mode="ro") as db:
    table = db.cursor().sql("""
    MATCH (p:Person)
    RETURN p.name, p.city
    LIMIT 2
    """).arrow()
    print(table.to_pylist())
```

Expected output:

```text
[{'name': 'Alice', 'city': 'New York'}, {'name': 'Bob', 'city': 'London'}]
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
