---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Packaging And CLI

Use this guide when working from a repository checkout or packaging `.crcl` bundles.

## Problem

CaracalDB has both a Python API and a small CLI for common local tasks: initialize, run, explain, benchmark, pack, and unpack.

## Steps

1. Initialize a bundle.

```bash
uv run caracal init graph
```
2. Run a query file.

```bash
uv run caracal run graph.crcl -f query.tuft -o result.json
```
3. Run a benchmark scenario.

```bash
uv run caracal bench knn
```
4. Pack and unpack directory bundles.

```bash
uv run caracal pack graph.crcl -o graph-packed.crcl
uv run caracal unpack graph-packed.crcl -o graph-unpacked.crcl
```
## Verification

CLI commands return zero on success and print CaracalDB error codes on handled failures.

## Common Pitfalls

- New `connect(..., format="auto")` databases default to packed files.
- Use `format="bundle"` or CLI `unpack` when inspecting bundle internals.
- `caracal run` requires a query file.

## Related ADR

CLI stability and packaging guarantees should be documented before v1.0.
