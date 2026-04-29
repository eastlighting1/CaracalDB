---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# ADR 0002: Tuple IR For Expressions

## Decision

Expression lowering should use a compact intermediate representation that is easy to inspect, serialize, and compare in tests.

## Consequences

Planner tests can assert stable expression shape without depending on parser object identity or physical execution details.
