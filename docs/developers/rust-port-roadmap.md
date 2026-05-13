---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Rust Port Roadmap

CaracalDB currently ships a Python reference engine. The Rust core is a planned implementation direction, not a runtime component in v0.1.x.

## Page-Level Status

Every public documentation page carries `engine_status` metadata. In v0.1.x the default value is `python-reference; rust-engine-planned`, which means the documented behavior is verified against the Python reference implementation. Rust-backed behavior should not be implied unless a later page names the version where it lands.

## Roadmap Principles

- Preserve public semantics before replacing internals.
- Keep Arrow-oriented interchange stable.
- Port storage and execution where correctness and performance benefit most.
- Mark page-level differences once Rust-backed behavior diverges.

## Implementation Scaffold

The Rust port starts as an opt-in native core beside the Python reference engine:

- `crates/caracaldb-core` owns format-compatible storage and graph primitives.
- `crates/caracaldb-python` exposes the PyO3 extension module as `caracaldb._caracaldb_rust`.
- `CARACALDB_ENGINE=python|rust|auto` selects the requested runtime; `python` remains the default until Rust-backed compatibility coverage is complete.
- Rust wheel builds use `maturin` while ordinary source-tree development can still run through the existing Hatch Python workflow.

## Contributor Rule

Do not document Rust-only behavior as stable until the API, tests, and release notes identify the version where it becomes available.

## Migration And Rollback Policy

Rust-backed code must not silently upgrade `.crcl` bundles. Operators can run:

```bash
caracal migrate path/to/bundle --check
```

Today this is a no-op readiness check because format version `1` remains the only stable writer format. A future format-changing migration must provide all of the following before the writer default changes:

- reader for the existing format
- reader for the new format
- explicit migration command
- documented downgrade or rollback procedure
- corruption recovery test

Rollback for the current format is straightforward: the migration command leaves the directory bundle unchanged, so the pre-migration copy remains valid for Python reference and Rust readers.

## Rust Stability Gates

Rust-backed behavior is version-gated by feature area:

| Area | Status | Stable Version |
|---|---|---|
| Bundle/store manifest compatibility | experimental | not yet stable |
| Column segment read/write compatibility | experimental | not yet stable |
| CSR/CSC build and traversal helpers | experimental | not yet stable |
| Query execution operators | experimental | not yet stable |
| Rust parser subset | experimental, not default | not yet stable |

Rust-only syntax is forbidden in stable documentation until a release note names the version and compatibility contract.
