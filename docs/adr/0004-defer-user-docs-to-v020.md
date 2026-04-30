---
applies_to: v0.2.x
status: superseded
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# ADR 0004: Defer User Docs To v0.2.0

## Status

Superseded.

## Context

Early development had a real risk of publishing user-facing pages before the
Python reference API was coherent enough to try. The competing risk was that
waiting too long would leave examples, errors, and design intent scattered
across implementation notes.

## Options Considered

- Defer all user documentation until the v0.2.0 surface was complete.
- Publish only internal engineering notes.
- Publish a version-scoped public documentation scaffold and mark unsupported
  surfaces explicitly.

## Decision

This placeholder records the earlier option of deferring broad user documentation. The current direction supersedes that by publishing a v0.1.x documentation scaffold.

## Consequences

Public pages should stay honest about experimental surfaces while still giving users a coherent path through install, query, interop, errors, and contributor workflows.
