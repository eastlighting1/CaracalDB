---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Tuft

Tuft is CaracalDB's query language. This section separates user-facing reference material from implementation-facing specification material.

## Read This Way

- Use [Reference](reference.md) when writing queries.
- Use [Specification](spec.md) when changing parser, binder, type system, or evaluator behavior.
- Use [Built-ins](builtins.md) when checking function names and arities generated from the runtime registry.
- Use [grammar.lark](grammar.lark) when you need the exact grammar mirrored from source.

The grammar mirror is generated from the source grammar and checked in CI.
