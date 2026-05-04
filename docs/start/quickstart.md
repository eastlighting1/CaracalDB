---
applies_to: v0.2.x
status: stable
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# Quickstart

This page is the shortest path from an empty environment to a CaracalDB query result. It is intentionally small: one database handle, one class, one row, one query.

## Goal

Open an existing example `.crcl` database, run the current MVP Tuft query shape, and return Python rows.

## Minimal Query

```python
import caracaldb as cdb
from pathlib import Path

with cdb.connect("examples/data/example_simple.crcl", mode="ro") as db:
    rows = db.sql("MATCH (p:Person) RETURN p.name, p.city LIMIT 2").rows()
    print(rows)
```

Expected output:

```text
[{'name': 'Alice', 'city': 'New York'}, {'name': 'Bob', 'city': 'London'}]
```
The query surface in v0.2.x supports a focused single-node pattern with `WHERE`, `RETURN`, and `LIMIT`. Broader Tuft examples live in the language reference as the public API catches up with the planner.

## Typed Graph Table

GNN-style data can describe graph meaning with columns instead of IRIs. `type` becomes the CaracalDB class name, `node_id` stays as the external stable id, and the remaining columns become node properties.

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "leaderboard.crcl"
    with cdb.connect(path) as db:
        db.insert_node_table(
            [
                {
                    "node_id": 0,
                    "type": "User",
                    "name": "Grandmaster_Ayasha_R",
                    "rank_points": 49908.0,
                },
                {
                    "node_id": 4691,
                    "type": "Competition",
                    "name": "Spring Open",
                    "rank_points": None,
                },
            ]
        )
        db.insert_edge_table([{"src": 0, "dst": 4691, "type": "HOSTED"}])

        rows = db.sql("MATCH (u:User) RETURN u.node_id, u.name").rows()
        print(rows)
```

Expected output:

```text
[{'node_id': 0, 'name': 'Grandmaster_Ayasha_R'}]
```

IRIs are optional in this path. CaracalDB keeps internal identifiers for catalog and storage compatibility, while user code can keep working with dataset ids such as `node_id`.

## Resource And Triple Ingest

CaracalDB can also normalize resource-shaped data. A Neo4j-style JSON object, an IRI resource, or a subject/predicate/object triple can all become the same internal graph resources. Dataset ids stay user-facing; CaracalDB assigns compact internal ids and can render them later as `caracaldb://resource/...`.

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "company.crcl"
    with cdb.connect(path) as db:
        db.insert_triples(
            [
                {"subject": "project/P9", "predicate": "rdf:type", "object": "Project"},
                {"subject": "project/P9", "predicate": "name", "object": "Risk Model"},
            ]
        )
        db.import_resource(
            {
                "id": "employee/E12345",
                "labels": ["Employee"],
                "properties": {"name": "Lukas Hoffman", "riskScore": 0.72},
                "relationships": {"worksOn": "project/P9"},
            }
        )

        ref = db.resource("employee/E12345")
        print(ref.display_iri)
        print(db.export_resource_turtle("employee/E12345").splitlines()[0])
```

Expected output:

```text
caracaldb://resource/employee/E12345
@prefix cdb: <caracaldb://resource/> .
```

Explicit ontology IRIs remain available when identity matters, but they are metadata rather than a requirement for loading property-graph or GNN-shaped data.

## Next Steps

- Install and verify the package with [Install](install.md).
- Learn language shape in [Tuft Reference](../tuft/reference.md).
- Look up Python entry points in [API](../api/README.md).
