---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# 30-Minute Tour

This tour gives you the shape of CaracalDB without pretending every planned surface is equally executable in v0.1.x. Follow it after the quickstart when you want the map: bundle, catalog, query, ontology, snapshots, and ML handoff.

## 1. Create A Bundle

CaracalDB stores graph data in a `.crcl` bundle. The public API can work with packed files, while engine-facing examples often use directory bundles.

```python
from caracaldb.storage import create_bundle

bundle = create_bundle("tour", exist_ok=True)
```
## 2. Register A Class

The catalog is the contract between stored data and Tuft names.

```python
from caracaldb.onto.catalog import Catalog, save_catalog

catalog = Catalog.empty()
catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
save_catalog(bundle, catalog)
```
## 3. Run A Query

The v0.1.x query path supports a focused single-node pattern.

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
LIMIT 5
```
## 4. Add Ontology Intent

Ontology links make class names durable and explainable.

```python
catalog.register_class(
    iri="http://example.org/ProteinCodingGene",
    local_name="ProteinCodingGene",
    superclass_iris=("http://example.org/Gene",),
)
```
## 5. Think In Snapshots

Snapshots name a read view by LSN. The language reserves `AS_OF` for snapshot-bound reads, while the storage layer already has a named snapshot registry.

```tuft
MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
RETURN g.symbol
```
## 6. Hand Off To Analytics Or ML

CaracalDB's interchange target is Arrow. Subgraphs keep nodes and edges as Arrow tables so adapters can hand them to graph analytics or ML tools.

```text
Tuft query -> Arrow table -> Subgraph -> Lynxes / PyG / DGL / jraph
```
## Next Steps

- Use [Pattern Queries](../guides/pattern-queries.md) for the executable query shape.
- Read [Ontology](../concepts/ontology.md) for hierarchy and closure.
- Read [Storage Layout](../concepts/storage-layout.md) before writing bundle tooling.
