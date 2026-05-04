---
applies_to: v0.2.x
status: stable
last_updated: 2026-05-04
engine_status: python-reference; rust-engine-planned
---

# API Reference

The API section is organized by functional area. Each page covers a group of closely related
modules so that application code, engine contributors, and ML users each have a natural entry point.

## Pages

| Page | Covers | Audience |
|---|---|---|
| [Storage & Transactions](storage.md) | Bundles, manifests, column segments, public connection path, OCC transactions | Tooling, contributors |
| [Graph](graph.md) | CSR/CSC adjacency index build and read; traversal-facing graph primitives | Analytics, GNN users |
| [Machine Learning](ml.md) | Neighbor sampling, subgraph container, online feature serving | ML practitioners |

## Stability notes

CaracalDB keeps the Python import path stable while the engine implementation matures.
Pages marked `experimental` have APIs that may change between minor versions.
Pages marked `stable` follow semver guarantees from v0.2.x onwards.
