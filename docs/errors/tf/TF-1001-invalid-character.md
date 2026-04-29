---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-1001 Invalid Character

## What You See

The Tuft parser stops before it can tokenize the query and reports an invalid character at a specific source position.

## Why It Happens

The query contains a byte or symbol that is not part of Tuft lexical syntax. This most often appears after copying punctuation from formatted text, using an unsupported quote mark, or pasting a hidden control character.

## How To Fix

Replace the highlighted character with plain Tuft syntax. Use ASCII quotes for string literals, keep identifiers alphanumeric with supported separators, and re-run the query after removing invisible characters around the highlighted span.

## Cross-References

- [Tuft Reference](../../tuft/reference.md)
- [Tuft Specification](../../tuft/spec.md)
