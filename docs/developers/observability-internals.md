---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Observability Internals

Observability internals define how `EXPLAIN`, `PROFILE`, and traces connect to planner and executor behavior.

## Span Model

Spans should identify the logical operation, physical operator, input cardinality, output cardinality, and elapsed time when available.

## Profile Rule

Profiling should wrap execution boundaries without changing result semantics. If adding instrumentation changes ordering, cardinality, or error behavior, treat it as an executor bug.

## Public Boundary

User-facing observability belongs in [Observability](../guides/observability-explain-profile.md). This page is for contributors maintaining the instrumentation path.
