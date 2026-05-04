---
applies_to: v0.2.x
status: experimental
last_updated: 2026-05-03
engine_status: python-reference; rust-engine-planned
---

# 30-Minute Tour

This tour gives you the shape of CaracalDB without pretending every planned surface is equally executable in v0.2.x. Follow it after the quickstart when you want the map: packed database, class definition, inserts, flexible resource ingest, query, ontology, snapshots, and ML handoff.

## 1. Open A Database

The repository includes small `.crcl` files under `examples/data/`. Use those for read-only examples unless a guide is specifically demonstrating database creation or writes.

```python
import caracaldb as cdb
from pathlib import Path

with cdb.connect("examples/data/example_simple.crcl", mode="ro") as db:
    print(type(db).__name__)
```

Expected output:

```text
Database
```

## 2. Define A Class

Classes are the names Tuft queries match. `define_class` creates the catalog entry; an explicit IRI is only needed when ontology identity matters.

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "tour.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene")
        print([cls.local_name for cls in db.catalog.classes])
```

Expected output:

```text
['Gene']
```

## 3. Insert Nodes

Rows are plain Python dictionaries. CaracalDB stores them as Arrow-compatible columns internally.

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "tour.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene")
        db.insert_nodes(
            "Gene",
            [
                {"symbol": "TP53", "chromosome": "17"},
                {"symbol": "BRCA1", "chromosome": "17"},
                {"symbol": "EGFR", "chromosome": "7"},
            ],
        )
        print(db.sql("MATCH (g:Gene) RETURN g.symbol").rows())
```

Expected output:

```text
[{'symbol': 'TP53'}, {'symbol': 'BRCA1'}, {'symbol': 'EGFR'}]
```

## 4. Run A Query

The v0.2.x query path supports a focused single-node pattern with `WHERE`, `RETURN`, and `LIMIT`.

```python
import caracaldb as cdb
from pathlib import Path

query = """
MATCH (p:Person)
WHERE p.city = 'London'
RETURN p.name, p.age
"""

with cdb.connect("examples/data/example_simple.crcl", mode="ro") as db:
    rows = db.sql(query).rows()
    print(rows)
```

Expected output:

```text
[{'name': 'Bob', 'age': 34}]
```

Close the handle when you are not using a `with` block. The examples above use `with`, so the handle is closed automatically.

## 5. Add Ontology Intent

Ontology metadata makes class names durable and explainable. In v0.2.x, the executable public API can register the class and IRI:

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "tour.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene", iri="http://example.org/Gene")
        db.define_class(
            "ProteinCodingGene",
            iri="http://example.org/ProteinCodingGene",
            superclass_iris=("http://example.org/Gene",),
        )
        print([cls.local_name for cls in db.catalog.classes])
```

Expected output:

```text
['Gene', 'ProteinCodingGene']
```

The focused `SUBCLASSOF*` class closure predicate is available in the v0.2.x query path. Broader reasoning features such as property closure and explicit `INFER CLOSURE` materialization are still experimental.

## 6. Import Resource-Shaped Data

Not every graph arrives as one node table and one edge table. `import_resource` accepts common resource shapes and normalizes them to CaracalDB nodes, edges, and internal resource ids.

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "tour.crcl"
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
                "properties": {"name": "Lukas Hoffman"},
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

Raw triples can land through the same model; use the same `insert_triples` shape with different subject ids.

## 7. Think In Snapshots

Snapshots name a read view by LSN. Create the snapshot first, then reference
that name from `AS_OF SNAPSHOT` reads.

```python
import caracaldb as cdb
from pathlib import Path
from tempfile import TemporaryDirectory

with TemporaryDirectory() as tmp:
    path = Path(tmp) / "tour.crcl"
    with cdb.connect(path) as db:
        db.define_class("Gene")
        db.insert_nodes("Gene", [{"symbol": "TP53"}])

        snap = db.create_snapshot("release-2026-04")
        db.insert_nodes("Gene", [{"symbol": "BRCA1"}])

        rows = db.sql("""
        MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
        RETURN g.symbol
        """).rows()
        print(snap.name, snap.lsn_high)
        print(rows)

        db.release_snapshot("release-2026-04")
```

Expected output:

```text
release-2026-04 1
[{'symbol': 'TP53'}]
```

## 8. Hand Off To Analytics Or ML

CaracalDB can return Arrow tables when you need columnar interop, while higher-level examples can stay with Python rows.

```text
Tuft query -> rows for app code
Tuft query -> Arrow table -> Subgraph -> Lynxes / PyG / DGL / jraph
```

## Next Steps

- Use [Pattern Queries](../guides/pattern-queries.md) for the executable query shape.
- Read [Ontology](../concepts/ontology.md) for hierarchy and closure.
- Read [Storage Layout](../concepts/storage-layout.md) before writing bundle tooling.
