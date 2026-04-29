---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Interop

Interop pages explain how CaracalDB relates to graph analytics and graph database ecosystems.

## Planned Topics

- [Lynxes GraphFrame](lynxes-graphframe.md) integration.
- [Neo4j migration](neo4j-migration.md) patterns.
- [Neo4j Bolt bridge](neo4j-bolt-bridge.md) export flow.
- [PyG and DGL](pyg-and-dgl.md) export flows.
- [jraph and JAX](jraph-and-jax.md) integration.

The guiding model is simple: CaracalDB owns storage, query, and reasoning; external graph compute tools can own specialized analytics.
