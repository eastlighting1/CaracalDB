---
applies_to: v0.2.x
status: experimental
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# ADR 0002: Tuple IR For Expressions

## Status

Accepted for the Python reference planner; still experimental as a public
contract.

## Context

Tuft parsing produces rich syntax objects, but planner tests need a smaller
shape that is easy to serialize and compare. Expression lowering also needs to
stay independent from object identity so generated plans remain stable across
parser refactors.

## Options Considered

- Assert directly against parser objects.
- Lower immediately into physical operator objects.
- Lower expressions into compact tuples before physical planning.

## Decision

Expression lowering should use a compact intermediate representation that is easy to inspect, serialize, and compare in tests.

## Consequences

Planner tests can assert stable expression shape without depending on parser object identity or physical execution details.
