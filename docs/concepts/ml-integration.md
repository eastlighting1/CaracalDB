---
applies_to: v0.2.x
status: experimental
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# ML Integration

CaracalDB treats ML integration as an Arrow-first handoff. The database should select, reason over, and snapshot graph data; ML tools should receive compact node and edge tables with stable identifiers.

## Mental Model

```mermaid
flowchart LR
    A["Tuft query"] --> B["Arrow node table"]
    A --> C["Arrow edge table"]
    B --> D["Subgraph"]
    C --> D
    D --> E["PyG / DGL / jraph"]
    D --> F["Lynxes GraphFrame"]
```
## Subgraph Contract

| Part | Shape |
|---|---|
| Nodes | `nodes[class_or_type] -> pyarrow.Table` with stable user columns such as `node_id` |
| Edges | `edges[relation_type] -> pyarrow.Table` with compact `src` and `dst` ids |
| Metadata | `meta[str] -> str` for snapshot id, seed set, or export notes |

## Code Shape

```python
import pyarrow as pa
from caracaldb.ml.subgraph import Subgraph

sg = Subgraph()
sg.add_nodes("http://example.org/Gene", pa.table({"nid": [1], "symbol": ["TP53"]}))
```
## Why This Shape

Arrow tables keep feature columns and graph identities in one place. For GNN-style tables, `node_id` is the stable id from the dataset; CaracalDB may also maintain compact internal ids for storage, adjacency, and sampling. In v0.2.3, resource-shaped input such as Neo4j JSON, IRI resources, and triples can normalize to the same `node_id` plus compact-id model before ML export. That makes the conversion boundary explicit and avoids baking one ML framework into the storage layer.

!!! note "Common misconception"
    CaracalDB does not need to become a training framework. Its job is to produce reproducible graph slices that training frameworks can consume.
