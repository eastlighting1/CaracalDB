---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-9501 Ontology Constraint Violated

## What You See

An ontology operation would create an invalid class, property, or closure relationship.

## Why It Happens

The requested schema change conflicts with declared ontology rules, such as incompatible domains, ranges, or closure constraints.

## How To Fix

Inspect the ontology rule that rejected the change, correct the class or property declaration, and rebuild closure metadata after the schema is valid.

## Cross-References

- [Ontology](../../concepts/ontology.md)
- [Ontology Reasoning](../../guides/ontology-reasoning.md)
