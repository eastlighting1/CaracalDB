---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# `caracaldb`

This page documents the public Python entry points that are stable enough to use from applications. The v0.1.x surface is intentionally small: open a database, run a Tuft query through a connection, and materialize the result as Arrow.

## When To Use This Page

Use this reference when you already know the query you want to run and need the Python object model: `connect`, `Database`, `Connection`, and `Result`.

For task-oriented examples, start with [Quickstart](../start/quickstart.md). For language syntax, use [Tuft Reference](../tuft/reference.md).

## Public API

::: caracaldb.api
    options:
      members:
        - connect
        - Database
        - Connection
        - Result
      show_root_heading: false
      show_source: true

## Compatibility Notes

CaracalDB keeps the Python import path stable while the engine implementation matures. Pages that differ after the Rust engine lands will carry explicit version metadata rather than hiding the difference in prose.
