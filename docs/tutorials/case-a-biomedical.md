---
applies_to: v0.2.x
status: experimental
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# Case A: Biomedical Graph

This page is a narrative companion to `examples/biomed.ipynb` and the case-A golden tests, not a replacement for the runnable notebook. The goal is to model genes, tissues, and interactions, then use graph operators to answer biomedical-style neighborhood questions.

## Goal

Start from a small `Gene` and `Tissue` graph, then answer:

- Which genes are on chromosome 17?
- Which genes interact with `TP53`?
- Which genes are reachable within two hops?
- Which seeds have the largest outgoing degree?

## Data Shape

| Class | Example columns |
|---|---|
| `Gene` | `symbol`, `chromosome` |
| `Tissue` | `name` |

| Edge | Meaning |
|---|---|
| `interactsWith` | gene-to-gene interaction |
| `expressedIn` | gene-to-tissue expression |

## Notebook-Backed Workflow

1. Register `Gene` and `Tissue` classes in the catalog.
2. Load nodes into class-specific node stores.
3. Load interaction edges with UInt64 `src` and `dst`.
4. Build CSR and CSC indexes for traversal.
5. Run scans, filters, expands, aggregates, and top-k operators.

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
```
The public Tuft MVP supports the single-node query above. The broader case-A operator chain mirrors what the planner will produce for multi-hop graph work.

For the executable end-to-end flow, run `examples/biomed.ipynb` or the tests under `tests/golden/case_a`.

## Expected Result

For the small golden fixture, chromosome 17 contains `BRCA1` and `TP53`, and `TP53` reaches `MDM2` and `BRCA1` in one hop.

## Next Steps

- Use [Pattern Queries](../guides/pattern-queries.md) for the public query path.
- Use [Build CSR And CSC](../guides/build-csr-and-csc.md) before neighbor traversal.
- Use [Ontology Reasoning](../guides/ontology-reasoning.md) when biomedical class hierarchy matters.
