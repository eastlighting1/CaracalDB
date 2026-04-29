---
applies_to: v0.1.x
status: stable
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# TF-8002 Transaction Conflict

## What You See

A Tuft write or procedure fails because another transaction committed a conflicting change first.

## Why It Happens

The query read from one snapshot but attempted to commit against data that changed before the transaction finished.

## How To Fix

Retry the transaction from a fresh snapshot. Keep the retry idempotent and avoid reusing stale read results across attempts.

## Cross-References

- [CDB-8002 Transaction Conflict](../cdb/CDB-8002-tx-conflict.md)
- [Transactions](../../guides/transactions.md)
