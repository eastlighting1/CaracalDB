---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Why CaracalDB

CaracalDB is for teams that need a graph-shaped working set, ontology-aware names, and Arrow-friendly results without turning every workload into a remote database service. It starts from the assumption that graph data is often part of an ML or analytics pipeline, not a separate universe.

## Positioning

| Compared with | CaracalDB emphasizes | Tradeoff |
|---|---|---|
| General embedded stores | Graph classes, edges, Tuft, and Arrow result flow | Narrower than a general key-value or SQL engine |
| Neo4j | Local-first packaging, IRI-aware modeling, Arrow interchange | Not full Cypher or APOC compatibility |
| DuckDB | Graph patterns and ontology semantics | Not a relational analytics replacement |
| NetworkX-style scripts | Durable graph bundles and query surfaces | Less flexible than ad hoc Python objects |

## Mental Model

```mermaid
flowchart LR
    A[".crcl bundle"] --> B["Catalog and ontology"]
    A --> C["Node and edge stores"]
    B --> D["Tuft query"]
    C --> D
    D --> E["Arrow table"]
    E --> F["ML or analytics workflow"]
```
## A Small Example

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
LIMIT 5
```
This is the core promise of v0.1.x: keep the graph model explicit, run a focused Tuft query, and hand the result to Arrow-compatible Python code.

!!! note "Common misconception"
    CaracalDB is not positioned as “Python instead of Rust.” The public package is Python-facing today, while the engine roadmap keeps the Rust implementation path explicit.
