---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Ontology Reasoning

Use this guide when you need class or property hierarchy to affect graph queries. The v0.1.x documentation describes the workflow and syntax contract; execution support should be checked against the API page for your installed version.

!!! warning "Experimental surface"
    Catalog registration is available in the Python reference implementation, but hierarchy-aware Tuft execution and materialized closure are not yet stable public query features in v0.1.x. Treat the Tuft examples below as the intended contract unless your installed version documents support.

## Problem

Real datasets rarely agree on one flat label set. A biomedical graph might contain `Gene`, `ProteinCodingGene`, `DiseaseGene`, and imported classes from multiple ontologies. Reasoning lets a query ask for the broader concept without manually listing every child class.

## Steps

1. Register the classes that current queries can match.

```python
import caracaldb as cdb

with cdb.connect("bio") as db:
    db.define_class("Gene", iri="http://example.org/Gene")
    db.define_class(
        "ProteinCodingGene",
        iri="http://example.org/ProteinCodingGene",
    )
```

2. Record superclass intent in catalog metadata when you are building or testing ontology data.

```python
from caracaldb.onto.catalog import Catalog

catalog = Catalog.empty()
catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
catalog.register_class(
    iri="http://example.org/ProteinCodingGene",
    local_name="ProteinCodingGene",
    superclass_iris=("http://example.org/Gene",),
)
```
This lower-level catalog object is not automatically attached to an already-open database handle. Use the database API for runnable inserts and queries, and persist catalog metadata explicitly when writing internal tooling.

3. Use hierarchy-aware Tuft syntax when expressing the future query intent.

```tuft
MATCH (g:ProteinCodingGene)
WHERE g.class SUBCLASSOF* <http://example.org/Gene>
RETURN g.symbol
```
4. Materialize closure when the graph needs reusable hierarchy lookup.

```tuft
INFER CLOSURE (SUBCLASSOF) ON GRAPH biomedical
```
## Verification

Reasoning will be correct when a query for a parent class includes direct instances and instances of transitive child classes, while still preserving the original class identity for downstream analysis.

For the current Python reference path, verify the executable part first: define the class with `db.define_class`, insert rows with `db.insert_nodes`, and query the exact class label with `db.sql("MATCH (g:Gene) RETURN g.symbol")`. Then verify ontology metadata separately by loading the saved catalog, confirming each class IRI is present, and checking that superclass IRIs point at registered classes.

## Common Pitfalls

- Do not use local names as the durable ontology contract. Use IRIs in the catalog and local names for query readability.
- Do not assume `SUBCLASSOF*` means every OWL rule is active. It means transitive hierarchy closure in CaracalDB's supported model.
- Do not update hierarchy rules without rebuilding or invalidating any materialized closure index.
- Keep examples that rely on inferred closure separate from examples that only rely on catalog registration.

## Related ADR

Ontology closure storage and invalidation should receive an ADR once the closure index format is promoted into the public format documentation.
