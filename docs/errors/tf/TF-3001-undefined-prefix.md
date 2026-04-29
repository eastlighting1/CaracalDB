---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-3001 Undefined Prefix

## What You See

Tuft sees a compact IRI prefix in a class, property, or value position but cannot resolve it.

## Why It Happens

The query references a prefix that was not declared in the query context or registered in the catalog.

## How To Fix

Declare the prefix before the query or use the full IRI form expected by your catalog. Keep prefix spelling and case consistent.

## Cross-References

- [Ontology](../../concepts/ontology.md)
- [Ontology Reasoning](../../guides/ontology-reasoning.md)
