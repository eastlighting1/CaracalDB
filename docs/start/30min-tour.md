---
applies_to: v0.2.x
status: experimental
last_updated: 2026-05-03
engine_status: python-reference; rust-engine-planned
---

# 30-Minute Tour

This tour gives you the shape of CaracalDB without pretending every planned surface is equally executable in v0.2.x. Follow it after the quickstart when you want the map: packed database, class definition, inserts, flexible resource ingest, query, ontology, snapshots, and ML handoff.

## 1. Open A Database

New databases use the packed `.crcl` format by default. The engine works through an internal bundle while the handle is open, but first-use code does not need to manage that detail.

```python
import caracaldb as cdb

db = cdb.connect("tour")
```

## 2. Define A Class

Classes are the names Tuft queries match. `define_class` creates the catalog entry; an explicit IRI is only needed when ontology identity matters.

```python
db.define_class("Gene")
```

## 3. Insert Nodes

Rows are plain Python dictionaries. CaracalDB stores them as Arrow-compatible columns internally.

```python
db.insert_nodes(
    "Gene",
    [
        {"symbol": "TP53", "chromosome": "17"},
        {"symbol": "BRCA1", "chromosome": "17"},
        {"symbol": "EGFR", "chromosome": "7"},
    ],
)
```

## 4. Run A Query

The v0.2.x query path supports a focused single-node pattern with `WHERE`, `RETURN`, and `LIMIT`.

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
LIMIT 5
```

```python
rows = db.sql("""
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
LIMIT 5
""").rows()
print(rows)
```

Close the handle when you are not using a `with` block.

```python
db.close()
```

## 5. Add Ontology Intent

Ontology metadata makes class names durable and explainable. In v0.2.x, the executable public API can register the class and IRI:

```python
db.define_class(
    "ProteinCodingGene",
    iri="http://example.org/ProteinCodingGene",
    superclass_iris=("http://example.org/Gene",),
)
```

The focused `SUBCLASSOF*` class closure predicate is available in the v0.2.x query path. Broader reasoning features such as property closure and explicit `INFER CLOSURE` materialization are still experimental.

## 6. Import Resource-Shaped Data

Not every graph arrives as one node table and one edge table. `import_resource` accepts common resource shapes and normalizes them to CaracalDB nodes, edges, and internal resource ids.

```python
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
print(ref.display_iri)  # caracaldb://resource/employee/E12345
```

Raw triples can land through the same model:

```python
db.insert_triples(
    [
        {"subject": "system/customer-data-lake", "predicate": "rdf:type", "object": "System"},
        {"subject": "system/customer-data-lake", "predicate": "name", "object": "Customer Data Lake"},
    ]
)
```

## 7. Think In Snapshots

Snapshots name a read view by LSN. Create the snapshot first, then reference
that name from `AS_OF SNAPSHOT` reads.

```python
snap = db.create_snapshot("release-2026-04")
print(snap.name, snap.lsn)
```

```tuft
MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
RETURN g.symbol
```

```python
rows = db.sql("""
MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
RETURN g.symbol
""").rows()
print(rows)
```

Release the named snapshot when the read view is no longer needed.

```python
db.release_snapshot("release-2026-04")
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
