---
applies_to: v0.2.x
status: accepted
last_updated: 2026-05-01
engine_status: python-reference; rust-engine-planned
---

# ADR 0005: RDF as an Import Surface, Not an Engine Surface

## Status

Accepted. Defines the boundary between CaracalDB's ontology semantics and the W3C RDF / SPARQL / OWL-DL stack.

## Context

CaracalDB carries first-class IRIs, prefix declarations, and class/property
hierarchies (`SUBCLASSOF`, `SUBPROPERTYOF` with closure bitmaps). The Tuft
language adopts SPARQL-like ontology operators (`MATCH TRIPLES`, `INFER
CLOSURE`). At the same time, the engine targets analytical graph workloads
(traversal, neighbour sampling, GNN feature extraction, vector k-NN) and ships
as an embedded library, not a triple store.

This raises a recurring question: should CaracalDB implement a SPARQL endpoint
and full OWL-DL profile compliance? Several adjacent design conversations
(Palantir-style ontology platforms, "RDF/IRI is the friction point", Jena and
Blazegraph as comparables) push in that direction.

The cost of saying "yes" is large:

- A SPARQL parser and algebra are a full second language surface.
- OWL-DL reasoning is a separate research-grade problem space.
- Triple-store IO patterns (random property access, type-heterogeneous rows)
  conflict with the columnar, class-partitioned layout that underpins
  CaracalDB's bench targets.

## Options Considered

1. **Full SPARQL + OWL-DL.** Treat CaracalDB as a triple store with extras.
2. **No RDF interop at all.** Tuft only; users convert externally.
3. **RDF as an import-only surface.** A converter ingests RDF into the
   columnar `.crcl` bundle; the engine itself remains class-partitioned and
   exposes only Tuft.

## Decision

Option 3. CaracalDB supports OWL-RL-style class and property hierarchies and
IRI identity at the language and storage layer. RDF / Turtle / N-Triples
ingestion is handled by a converter (`tools/rdf_import.py` and `caracal
import-rdf`) that lowers triples into the catalog plus node/edge stores. The
engine does not expose a SPARQL endpoint, does not implement full OWL-DL, and
does not retain a triple-store layout once data is loaded.

## Consequences

- Users of RDF data sets can try CaracalDB without rewriting their data
  pipeline. The cost is a one-shot conversion at ingest time.
- The engine surface remains small: one query language (Tuft), one storage
  format (columnar `.crcl`).
- Workloads that genuinely need SPARQL semantics (federated queries, OWL-DL
  reasoning, named-graph quads) belong in a different product. CaracalDB's
  `MATCH TRIPLES` covers basic graph patterns over the imported triples but
  is not a SPARQL substitute.
- The class-partitioned layout means RDF data with no schema (untyped
  resources, mixed-type properties) imports best when given a hint about
  the dominant rdf:type structure. Schema-free RDF is a degraded path,
  not the recommended one.

## Non-goals (cross-reference)

This ADR pairs with the **Non-goals** section of [README.md](../../README.md):
no server, no auth, no SPARQL endpoint. RDF interop is import-only.
