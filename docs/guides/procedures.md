---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Procedures

Use this guide when a workflow looks like a named operation rather than a scalar expression.

!!! warning "Experimental surface"
    Procedure-shaped Tuft syntax is documented as a design target in v0.1.x. Prefer the current Python or CLI functions for operational work until named procedures are listed as stable API.

## Problem

Procedures are the right abstraction for multi-step graph operations such as import, export, reasoning, or maintenance. v0.1.x keeps procedure-shaped syntax in the language surface while stable public procedure APIs are still forming.

## Steps

1. Prefer built-in CLI or Python functions for current operational workflows.
2. Reserve `CALL name(...)` style Tuft syntax for procedure surfaces documented by your installed version.
3. Keep procedure outputs tabular so they can flow into Arrow and downstream tools.

```tuft
CALL graph.stats()
YIELD node_count, edge_count
```
## Verification

A stable procedure should document inputs, output columns, side effects, and error codes.

## Common Pitfalls

- Do not use procedures for scalar transforms; use built-ins or UDFs.
- Do not hide write side effects behind read-looking names.
- Treat procedure names as public API once they appear in docs.

## Related ADR

Procedure naming and side-effect rules should get an ADR before third-party procedures are supported.
