---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-4001 Type Mismatch

## What You See

Tuft rejects an expression because the actual value type does not match the type required by the operator, function, or target column.

## Why It Happens

The expression mixes incompatible scalar, temporal, graph, or vector values. Tuft keeps casts explicit so query results remain deterministic.

## How To Fix

Use a value with the expected type, add an explicit cast where supported, or split the expression so each operator receives compatible inputs.

## Cross-References

- [Tuft Specification](../../tuft/spec.md)
- [Built-ins](../../tuft/builtins.md)
