---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Transactions

Use this guide when coordinating writes that may conflict with other writers.

!!! note "Current implementation"
    The Python reference transaction manager is available in `caracaldb.tx`. It records BEGIN, COMMIT, and ROLLBACK boundaries in the WAL and detects write-write conflicts for keys declared with `record_write`.

## Problem

CaracalDB uses snapshot-tagged transactions and write-write conflict detection. A commit fails when another transaction wrote the same key after your snapshot.

## Steps

1. Begin a transaction.

```python
tx = manager.begin()
```
2. Record the keys you write.

```python
tx.record_write("nodes/Gene", 42)
```
3. Commit or retry on conflict.

```python
try:
    manager.commit(tx)
except TxConflictError:
    tx = manager.begin()
```
## Verification

A successful commit returns a commit LSN. A conflicting commit raises `CDB-8002`.

Use a small two-transaction test when wiring a new writer: begin two transactions from the same snapshot, record the same `(table, key)` write in both, commit one, then confirm the second raises `TxConflictError`.

## Common Pitfalls

- Retry from a fresh snapshot after `CDB-8002`.
- Do not reuse a transaction after rollback.
- Keep write keys stable and specific enough to catch real conflicts.
- The manager only checks keys you record. Forgetting `record_write` means the conflict detector has nothing to compare.

## Related ADR

Transaction isolation and retry semantics should be captured when public write APIs stabilize.
