---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Catalog FlatBuffers

`catalog.fb` stores schema and ontology metadata in a compact FlatBuffers payload.

## Source Schema

The schema source lives at `schema/catalog.fbs`. Generated code should be treated as build output; the `.fbs` file is the reviewable contract.

## Contents

| Area | Examples |
|---|---|
| Classes | Class ids, names, hierarchy references |
| Properties | Property ids, domains, ranges, logical types |
| Graph metadata | Node and edge table mappings |
| Ontology metadata | Closure and constraint references |

## Compatibility

Additive fields should use FlatBuffers-compatible evolution. Removing or changing field meaning requires a format version decision.
