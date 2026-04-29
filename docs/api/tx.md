---
applies_to: v0.1.x
status: experimental
last_updated: 2026-04-28
engine_status: python-reference; rust-engine-planned
---

# Transaction API

Transaction APIs provide snapshot-tagged transaction state and write-write conflict detection.

## Common Entry Points

| Name | Use |
|---|---|
| `TransactionManager` | Begin, commit, rollback, and context-manage transactions. |
| `Transaction` | Track a transaction snapshot and write set. |
| `TxConflictError` | Raised as `CDB-8002` on write-write conflict. |

## Reference

::: caracaldb.tx
    options:
      show_root_heading: false
      show_source: true
