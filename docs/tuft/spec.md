---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Tuft Specification

This page is for implementers. It defines the language contract that parser, binder, typer, planner, and executor changes must preserve. Users writing queries should start with the [Tuft Reference](reference.md).

## Lexical Structure

Tuft source is tokenized by the Lark grammar mirrored in [grammar.lark](grammar.lark). Identifiers may be plain names or escaped names. IRIs use `<...>` syntax. Strings may use single quotes, double quotes, or triple double-quoted blocks.

Reference link: [Prefixes And IRIs](reference.md#prefixes-and-iris).

## Grammar

The full grammar is not hand-copied into this page. It is mirrored from `caracaldb/lang/tuft/tuft.lark` into [grammar.lark](grammar.lark) by `tools/gen_tuft_grammar.py`.

Normal case:

```tuft
MATCH (g:Gene)
RETURN g.symbol
```
Error case:

```tuft
MATCH
```
The parser reports `TF-2001` for unexpected input.

Reference link: [Minimal Query](reference.md#minimal-query).

## Names And Binding

Class and property names bind through the catalog. Prefix declarations expand compact names into IRIs. Local names are acceptable in the v0.1.x MVP path when the catalog has a matching local name.

Normal case:

```tuft
PREFIX bio: <http://example.org/bio/>
MATCH (g:bio:Gene)
RETURN g.symbol
```
Error cases include undefined prefixes (`TF-3001`) and unknown classes (`TF-3004`).

Reference link: [Prefixes And IRIs](reference.md#prefixes-and-iris).

## Type System

The grammar reserves primitive, collection, vector, matrix, struct, union, node, edge, path, and ontology-facing types. The v0.1.x executor currently type-checks only the subset required by the public MVP query path and runtime operators.

Normal case:

```tuft
MATCH (g:Gene)
WHERE g.chromosome = '17'
RETURN g.symbol
```
Error cases include unsupported implicit casts (`TF-4010`) and type mismatches (`TF-4001`) as the typer surface expands.

Reference link: [WHERE](reference.md#where).

## Evaluation Semantics

The public MVP evaluates a single class-labeled node pattern, optional predicate, projections, and optional integer literal limit. Broader clauses may parse before they are executable through `Connection.sql(...)`.

Normal case:

```tuft
MATCH (g:Gene)
RETURN g.symbol
LIMIT 5
```
Error case:

```tuft
MATCH (a:Gene)-[:INTERACTS_WITH]->(b:Gene)
RETURN a.symbol, b.symbol
```
In v0.1.x this relationship pattern is grammar-reserved but outside the MVP executor.

Reference link: [Support Level](reference.md#support-level).

## Determinism

Tuft implementations should preserve deterministic parsing, binding, and output naming for the same catalog, query text, and snapshot boundary. Operators that sample or walk randomly must expose seed control at the execution layer.

Reference link: [AS_OF](reference.md#as_of).

## Compatibility And Versioning

Every public page declares `applies_to`. When Rust engine behavior diverges from the Python implementation, the page must state the version boundary explicitly instead of silently changing examples.

Reference link: [Tuft Reference](reference.md).
