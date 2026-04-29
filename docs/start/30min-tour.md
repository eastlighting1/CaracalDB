---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# 30-Minute Tour

This tour gives you the shape of CaracalDB without pretending every planned surface is equally executable in v0.1.x. Follow it after the quickstart when you want the map: packed database, class definition, inserts, query, ontology, snapshots, and ML handoff.

## 1. Open A Database

New databases use the packed `.crcl` format by default. The engine works through an internal bundle while the handle is open, but first-use code does not need to manage that detail.

```python
import caracaldb as cdb

db = cdb.connect("tour")
```

## 2. Define A Class

Classes are the names Tuft queries match. `define_class` creates the catalog entry and gives it a stable IRI.

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

The v0.1.x query path supports a focused single-node pattern with `WHERE`, `RETURN`, and `LIMIT`.

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

Ontology metadata makes class names durable and explainable. In v0.1.x, the executable public API can register the class and IRI:

```python
db.define_class(
    "ProteinCodingGene",
    iri="http://example.org/ProteinCodingGene",
)
```

Superclass closure is still an experimental surface in v0.1.x. `SUBCLASSOF*` examples in the ontology guide describe the intended query contract, not a query you should paste into the current `db.sql()` path.

## 6. Think In Snapshots

Snapshots name a read view by LSN. The language reserves `AS_OF` for snapshot-bound reads, while the storage layer already has a named snapshot registry.

```tuft
MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
RETURN g.symbol
```

## 7. Hand Off To Analytics Or ML

CaracalDB can return Arrow tables when you need columnar interop, while higher-level examples can stay with Python rows.

```text
Tuft query -> rows for app code
Tuft query -> Arrow table -> Subgraph -> Lynxes / PyG / DGL / jraph
```

## Next Steps

- Use [Pattern Queries](../guides/pattern-queries.md) for the executable query shape.
- Read [Ontology](../concepts/ontology.md) for hierarchy and closure.
- Read [Storage Layout](../concepts/storage-layout.md) before writing bundle tooling.
