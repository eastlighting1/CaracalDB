---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Architecture

This page orients contributors to module boundaries. User documentation explains concepts; this page explains where those concepts live in code.

## Layers

| Layer | Responsibility |
|---|---|
| Language | Tuft parsing, diagnostics, binding, and expressions |
| Plan | Logical operations and query lowering |
| Exec | Physical operators and result materialization |
| Storage | Bundles, manifests, columns, catalog, and WAL |
| Graph | CSR, CSC, HNSW, traversal helpers, and graph exports |
| Interop | Arrow, ML, and external graph ecosystem boundaries |

## Change Rule

Prefer changes that keep layer ownership clear. If a user-facing feature crosses layers, document the contract at the highest stable boundary and test each layer with a focused case.
