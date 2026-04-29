---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-2001 Unexpected Token

## What You See

Tuft parsed part of the query, then found a token that does not fit the grammar at that position.

## Why It Happens

The surrounding clause is incomplete, clauses are ordered incorrectly, or syntax from Cypher or SPARQL was used where Tuft expects its own form.

## How To Fix

Read the query from the previous clause boundary and compare it with the clause examples in the reference. Fix the first highlighted token before changing later parts of the query.

## Cross-References

- [Tuft Reference](../../tuft/reference.md)
- [Tuft vs Cypher vs SPARQL](../../concepts/tuft-vs-cypher-vs-sparql.md)
