---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Neo4j Bolt Bridge

The Bolt bridge page describes how to move CaracalDB content into tools that already expect the Neo4j driver ecosystem. In v0.1.x this is an export pattern, not a wire-compatible CaracalDB server.

## Problem

Teams often have dashboards, notebooks, or migration utilities built around the Neo4j Python driver. Rewriting every consumer before evaluating CaracalDB creates unnecessary migration risk.

## Shape

Use CaracalDB as the source of truth, export a selected subgraph, then load that projection into a Neo4j-compatible target for existing Bolt clients.

```text
CaracalDB snapshot -> Tuft export query -> node/edge batches -> Neo4j loader -> Bolt clients
```
## Workflow

1. Pick a stable snapshot with `AS_OF` when reproducibility matters.
2. Export node and edge tables with explicit identity columns.
3. Convert CaracalDB classes to labels and edge types to relationship types.
4. Load into the target Neo4j database using batched writes.
5. Keep a manifest that records snapshot id, export query, and row counts.

## Verification

Compare node counts, edge counts, and a small set of key lookups between CaracalDB and the target database. For ontology-derived classes, record whether closure was materialized before export.

## Common Pitfalls

- Treating the bridge as live replication.
- Exporting local display names instead of stable IRIs.
- Dropping snapshot metadata, which makes later debugging ambiguous.

## Related ADR

No dedicated bridge ADR exists yet. Until one lands, treat this page as an integration recipe layered on top of snapshot export.
