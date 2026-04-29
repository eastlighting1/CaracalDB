---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Neo4j Migration

This page maps familiar Neo4j and Cypher concepts into CaracalDB terms. The goal is practical migration: keep the graph model recognizable while making Tuft, IRIs, and Arrow-backed storage explicit.

## Concept Mapping

| Neo4j / Cypher | CaracalDB / Tuft | Migration note |
|---|---|---|
| Label | Class | Prefer stable IRIs, with local names for query ergonomics. |
| Relationship type | Edge type or property | Preserve direction explicitly. |
| Node property | Property column | Stored in Arrow-compatible node stores. |
| `MATCH (n:Gene)` | `MATCH (n:Gene)` | Single-node matching is available in v0.1.x. |
| `RETURN n.symbol` | `RETURN n.symbol` | Projection shape is intentionally similar. |
| Variable-length path | Planned Tuft pattern range | Document as planned until the planner exposes it publicly. |
| Transaction retry | `CDB-8002` | Retry from a fresh snapshot. |

## Query Examples

Cypher:

```cypher
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
LIMIT 5
```
Tuft:

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
LIMIT 5
```
The early surface is deliberately familiar. The larger difference is naming: CaracalDB treats ontology classes and properties as globally meaningful identifiers, then lets local names keep queries short.

## APOC And Procedures

| Neo4j/APOC habit | CaracalDB direction |
|---|---|
| Collection helpers | Tuft built-ins and Arrow compute kernels |
| Graph algorithms | Lynxes interop or dedicated graph operators |
| Import/export procedures | CLI packaging and Arrow/Parquet guides |
| Trigger-heavy workflows | Application-level orchestration for v0.1.x |

## Known Gaps

CaracalDB v0.1.x does not aim for full Cypher compatibility. Full-text indexes, triggers, APOC breadth, and every path-planning feature should be treated as migration work rather than drop-in behavior.
