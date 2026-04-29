---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Testing Strategy

CaracalDB tests should make behavior cheap to change and hard to accidentally weaken. The suite combines focused unit tests, golden language cases, storage recovery checks, benchmark gates, and documentation checks.

## Test Layers

| Layer | Use it for | Typical command |
|---|---|---|
| Unit | Small parser, planner, storage, and API behavior | `uv run pytest tests/path -q` |
| Golden | Stable language or diagnostic output | `uv run pytest tests/golden -q` |
| Fuzz/property | Parser and storage invariants | `uv run pytest tests/fuzz -q` |
| Recovery | WAL, manifest, and crash-style behavior | `uv run pytest tests/recovery -q` |
| Docs | Public examples and generated indexes | `uv run mkdocs build --strict -f mkdocs.yml` |
| Bench | Performance regression budget | `uv run python -m bench.run` |

## Which Test To Add

| Change type | Expected test |
|---|---|
| Tuft grammar or binding | Parser/binder unit test plus a golden diagnostic when errors change |
| Runtime operator | Unit test for result shape and edge cases |
| Storage format | Round-trip test and recovery test |
| Public API | API smoke test and doc example if the workflow is user-facing |
| Error code | Diagnostic table entry plus generated docs check |
| Benchmark path | Harness scenario or baseline update rationale |

## Documentation Checks

The public quickstart code fence is executed by `tools/check_quickstart_code.py`. Error and built-in indexes are generated from runtime sources, then checked in CI so docs cannot drift quietly.

## Common Pitfall

Do not hide missing optional dependencies by weakening tests. If a feature needs `torch`, `dgl`, `jraph`, or `flatc`, the skip should explain the missing dependency and leave the core suite meaningful without it.
