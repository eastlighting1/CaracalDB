---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# CDB-8002 Transaction Conflict

## What You See

A transaction cannot commit because its read or write set conflicts with a committed transaction.

## Why It Happens

CaracalDB uses snapshot isolation. A writer that starts from an older snapshot must fail rather than overwrite a newer committed value blindly.

## How To Fix

Retry from a fresh snapshot and make the write path idempotent. For high-contention keys, keep the transaction small and move expensive reads outside the retry loop when they are not part of the conflict set.

## Cross-References

- [TF-8002 Transaction Conflict](../tf/TF-8002-tx-conflict.md)
- [Transactions](../../guides/transactions.md)
