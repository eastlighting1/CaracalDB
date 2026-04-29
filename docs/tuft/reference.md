---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Tuft Reference

Tuft is CaracalDB's graph query language. This page is organized by what a user wants to do: match graph data, filter rows, return values, call functions, and prepare for ontology-aware reads.

## Support Level

The grammar already reserves a broader language surface than the v0.1.x executor exposes through `cdb.connect(...).cursor().sql(...)`.

Specification link: [Evaluation Semantics](spec.md#evaluation-semantics).

| Surface | Status in v0.1.x |
|---|---|
| Single node `MATCH` | Executable |
| `WHERE` on simple expressions | Executable |
| `RETURN` projections | Executable |
| `LIMIT` | Executable |
| Relationship patterns | Parsed, not part of the public MVP executor |
| `WITH`, `UNWIND`, `CALL`, `EXPORT` | Parsed, not part of the public MVP executor |
| `INFER CLOSURE` | Parsed as utility syntax, execution is staged with ontology work |
| `AS_OF` | Parsed, execution is staged with snapshot reads |

## Minimal Query

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
LIMIT 5
```
This is the reference shape to use with the current Python API.

Specification link: [Grammar](spec.md#grammar).

## `MATCH`

`MATCH` introduces graph patterns. The v0.1.x executor supports a single node pattern with a variable and one class label.

```tuft
MATCH (g:Gene)
RETURN g.symbol
```
The variable, `g`, is used by later clauses. The class label, `Gene`, resolves through the catalog by IRI or local name.

Grammar-reserved relationship patterns look like this:

```tuft
MATCH (a:Gene)-[:INTERACTS_WITH]->(b:Gene)
RETURN a.symbol, b.symbol
```
Treat relationship execution as planned unless the API page for your version says otherwise.

Specification link: [Evaluation Semantics](spec.md#evaluation-semantics).

## `WHERE`

`WHERE` filters rows after a `MATCH`. The MVP executor supports simple comparisons over node properties.

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
```
Common operators include:

| Operator | Meaning |
|---|---|
| `=` | Equal |
| `!=`, `<>` | Not equal |
| `<`, `<=`, `>`, `>=` | Ordered comparison |
| `AND`, `OR` | Boolean composition |
| `IS NULL`, `IS NOT NULL` | Null checks, parsed in the language surface |

Specification link: [Type System](spec.md#type-system).

## `RETURN`

`RETURN` selects output expressions.

```tuft
MATCH (g:Gene)
RETURN g.symbol, g.chromosome
```
Aliases use `AS`:

```tuft
MATCH (g:Gene)
RETURN g.symbol AS gene_symbol
```
`RETURN DISTINCT` is part of the grammar surface, but distinct execution should be treated as planned unless the relevant API page marks it supported.

Specification link: [Evaluation Semantics](spec.md#evaluation-semantics).

## `LIMIT`

`LIMIT` caps the number of returned rows.

```tuft
MATCH (g:Gene)
RETURN g.symbol
LIMIT 10
```
The current executor expects an integer literal.

Specification link: [Evaluation Semantics](spec.md#evaluation-semantics).

## Prefixes And IRIs

Tuft accepts both local names and globally stable IRIs. Prefer IRIs in catalog definitions and local names in short queries.

```tuft
PREFIX bio: <http://example.org/bio/>
MATCH (g:bio:Gene)
RETURN g.symbol
```
When a prefix cannot be resolved, the binder reports `TF-3001`. When a class cannot be resolved, it reports `TF-3004`.

Specification link: [Names And Binding](spec.md#names-and-binding).

## Built-ins

Built-in functions are generated from the runtime registry. Use [Built-ins](builtins.md) for the current function list and arities.

```tuft
MATCH (g:Gene)
WHERE starts_with(g.symbol, 'TP')
RETURN upper(g.symbol)
```
Function names may parse before every runtime operator is wired to execute them. Check support level before relying on a built-in in production code.

Specification link: [Type System](spec.md#type-system).

## Ontology Predicates

Tuft reserves ontology-aware predicates for class and property hierarchy checks.

```tuft
MATCH (g:Gene)
WHERE g.kind SUBCLASSOF* <http://example.org/BiologicalEntity>
RETURN g.symbol
```
Use [Ontology](../concepts/ontology.md) for the model and [Ontology Reasoning](../guides/ontology-reasoning.md) for the operational workflow.

Specification link: [Grammar](spec.md#grammar).

## `INFER CLOSURE`

`INFER CLOSURE` is the language hook for materializing ontology closure over graph structures.

```tuft
INFER CLOSURE (SUBCLASSOF) ON GRAPH biomedical
```
In v0.1.x this syntax documents the intended workflow; production execution should follow the version-specific ontology guide.

Specification link: [Evaluation Semantics](spec.md#evaluation-semantics).

## `AS_OF`

`AS_OF` binds a match to a snapshot.

```tuft
MATCH (g:Gene) AS_OF SNAPSHOT 'release-2026-04'
RETURN g.symbol
```
Snapshot-bound reads are part of the language design and will be documented as executable once the public transaction and snapshot API is promoted.

Specification link: [Determinism](spec.md#determinism).
