---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Rust Port Roadmap

CaracalDB is designed around a Rust engine direction. The current public Python surface is the first implementation vehicle, not the final engine boundary.

## Page-Level Status

Every public documentation page carries `engine_status` metadata. In v0.1.x the default value is `python-reference; rust-engine-planned`, which means the documented behavior is verified against the Python reference implementation and is intended to survive the Rust engine port unless a later page says otherwise.

## Roadmap Principles

- Preserve public semantics before replacing internals.
- Keep Arrow-oriented interchange stable.
- Port storage and execution where correctness and performance benefit most.
- Mark page-level differences once Rust-backed behavior diverges.

## Contributor Rule

Do not document Rust-only behavior as stable until the API, tests, and release notes identify the version where it becomes available.
