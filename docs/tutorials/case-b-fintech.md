---
applies_to: v0.2.x
status: experimental
last_updated: 2026-04-30
engine_status: python-reference; rust-engine-planned
---

# Case B: Fintech Graph

This page is a narrative companion to `examples/fraud.ipynb` and the case-B golden tests, not a replacement for the runnable notebook. The goal is to represent accounts and transfers, then derive features for risk or fraud workflows.

## Goal

Build a small account graph and compute:

- total balance across accounts,
- per-account outgoing transfer degree,
- nearest accounts by embedding,
- write-write transaction conflict behavior,
- snapshot stability.

## Data Shape

| Class | Example columns |
|---|---|
| `Account` | `name`, `balance`, embedding side data |

| Edge | Meaning |
|---|---|
| `transferredTo` | directed money movement |

## Notebook-Backed Workflow

1. Register the `Account` class.
2. Load account nodes and transfer edges.
3. Build a CSR index over `transferredTo`.
4. Build an HNSW index over account embeddings.
5. Use aggregate and kNN operators to refresh account features.
6. Use transactions and snapshots to keep refreshes reproducible.

```tuft
MATCH (a:Account)
RETURN a.name, a.balance
LIMIT 5
```
For the executable end-to-end flow, run `examples/fraud.ipynb` or the tests under `tests/golden/case_b`.

## Expected Result

The golden fixture computes a total balance of `1500.0`, detects account transfer fan-out, and raises `CDB-8002` when two transactions write the same account from competing snapshots.

## Next Steps

- Use [kNN With HNSW](../guides/knn-with-hnsw.md) for embedding lookup.
- Use [Transactions](../guides/transactions.md) for conflict handling.
- Use [Snapshots With AS_OF](../guides/snapshots-as-of.md) for stable read boundaries.
