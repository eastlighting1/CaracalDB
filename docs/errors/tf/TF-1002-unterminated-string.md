---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-1002 Unterminated String

## What You See

The parser reaches the end of a line or query while still inside a string literal.

## Why It Happens

A string was opened but not closed, or the closing quote was escaped accidentally. This can also happen when a multi-line value is pasted into a single-line Tuft query.

## How To Fix

Close the string with the same quote style that opened it. If the value itself contains quotes, escape only the embedded quote and leave the final delimiter unescaped.

## Cross-References

- [Tuft Specification](../../tuft/spec.md)
