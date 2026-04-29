---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Lynxes GraphFrame

CaracalDB and Lynxes occupy different layers of the graph stack. CaracalDB owns storage, Tuft queries, ontology-aware reasoning, snapshots, and transactional boundaries. Lynxes is the analytic graph frame layer: lazy graph operations, algorithms, and Arrow-native compute.

## Division Of Responsibility

| Concern | CaracalDB | Lynxes |
|---|---|---|
| Durable graph storage | Primary owner | Reads exported frames |
| Query language | Tuft | Lazy graph expressions |
| Ontology and reasoning | Primary owner | Consumes materialized results |
| Graph algorithms | Exports candidate graph | Runs analytic workloads |
| Arrow interchange | Produces node and edge tables | Consumes and returns Arrow tables |

## Round Trip Pattern

The intended workflow is storage-first:

1. Query or materialize a subgraph in CaracalDB.
2. Convert the node and edge tables into a Lynxes `GraphFrame`.
3. Run analytic algorithms in Lynxes.
4. Write the resulting scores or labels back into CaracalDB.

```text
CaracalDB bundle -> Tuft subgraph -> Arrow tables -> Lynxes GraphFrame
Lynxes result -> Arrow table -> CaracalDB feature or node property
```
## Adapter Shape

The public adapter names are reserved as `to_graphframe` and `from_graphframe`. Until those functions are promoted into the stable API, document examples should describe the interchange contract rather than promising an import path.

The contract is simple:

| Table | Required columns | Notes |
|---|---|---|
| Nodes | `nid` | Additional columns remain Arrow-native properties. |
| Edges | `src`, `dst` | Edge type and weight columns are optional. |
| Results | `nid` or `src`/`dst` | Algorithm outputs should preserve graph identity columns. |

## What Not To Expect

Lynxes is not a replacement for CaracalDB storage, transactions, or ontology closure. CaracalDB is not trying to duplicate every analytic algorithm once a GraphFrame can handle the work cleanly.
