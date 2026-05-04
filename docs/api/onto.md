---
applies_to: v0.2.x
status: stable
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# Ontology & Metadata

The Ontology API manages the catalog (class and property registry), the class hierarchy DAG,
and the materialized transitive closure index that powers `SUBCLASSOF*` queries.

## Key concepts

| Concept | API object | Description |
|---|---|---|
| **Catalog** | `Catalog` | Registry of all `ClassDef` and `PropertyDef` entries in a bundle. |
| **ClassDef** | `ClassDef` | A node class with an IRI, optional local name, field list, and superclass links. |
| **PropertyDef** | `PropertyDef` | A named edge relation with an IRI and optional domain/range metadata. |
| **Hierarchy** | `HierarchyDAG` / `OntologyHierarchy` | In-memory DAG built from superclass links; used for closure inference. |
| **Closure index** | `ClassClosureIndex` | Materialized reachable-ancestor index persisted to `closure/` in the bundle. |

## Working with the catalog

The catalog is normally accessed through `Database.catalog`:

```python
import caracaldb as cdb

with cdb.connect("biomedical") as db:
    catalog = db.catalog
    for cls in catalog.classes:
        print(cls.local_name, "→ IRIs:", cls.superclass_iris)
```

For advanced use (e.g. building tooling or CLI inspection), load the catalog directly:

```python
from caracaldb.storage import open_bundle
from caracaldb.onto import load_catalog, load_class_closure

bundle = open_bundle("biomedical.crcl")
catalog = load_catalog(bundle)
closure = load_class_closure(bundle)

# Resolve all subclasses of a given class
ancestors = closure.ancestors("caracaldb://class/Protein")
print(ancestors)
```

## Classes

| Name | Description |
|---|---|
| [`Catalog` | Registry of classes, properties, graphs, and indexes in a bundle. |
| [`ClassDef` | A single node class entry (IRI, local name, superclasses, fields). |
| [`PropertyDef` | A single property (edge relation) entry. |
| [`FieldDef` | A typed field on a class (name, type, nullability). |
| [`GraphDef` | A named named-graph compartment registered in the catalog. |
| [`IndexDef` | An index registered on a class field (B-tree, HNSW, etc.). |
| [`TypeRef` | A reference to a type (primitive or class). |
| [`HierarchyDAG` | Directed acyclic graph of class-to-superclass links. |
| [`OntologyHierarchy` | Higher-level helper for reasoning queries over a `HierarchyDAG`. |
| [`ClassClosureIndex` | Materialized transitive ancestor index for `SUBCLASSOF*` queries. |

## Enumerations

| Name | Description |
|---|---|
| [`IndexKind` | `BTREE`, `HNSW`, or `FULLTEXT`. |
| [`PropertyKind` | `OBJECT` (edge) or `DATA` (literal). |
| [`TypeKind` | Primitive type discriminant (`INT64`, `FLOAT64`, `STRING`, …). |

## Functions

| Name | Description |
|---|---|
| [`load_catalog` | Deserialize the `catalog.fb` FlatBuffers file from a bundle. |
| [`save_catalog` | Serialize the in-memory catalog back to `catalog.fb`. |
| [`load_class_closure` | Load the materialized closure index from `closure/`. |
| [`save_class_closure` | Persist the closure index to `closure/`. |

## Reference

::: caracaldb.onto
    options:
      show_root_heading: false
      show_source: true

## See Also

- [Ontology Concept](../concepts/ontology.md) — overview of how ontology metadata works in CaracalDB
- [Ontology Reasoning Guide](../guides/ontology-reasoning.md) — writing `SUBCLASSOF*` queries
- [Catalog FlatBuffers Format](../format/catalog-fb.md) — wire format for the catalog
