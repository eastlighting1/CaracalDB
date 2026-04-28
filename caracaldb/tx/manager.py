"""BEGIN / COMMIT / ROLLBACK with single-writer write-write conflict detection.

Transactions are tagged with a snapshot id (the LSN the reader sees) and
declare which (table, key) tuples they touch. The manager serialises writers
through a re-entrant lock and rejects a commit when another transaction
already committed a write touching the same key after the writer's snapshot
was taken — this maps to the standard ``write-write`` conflict pattern that
Tuft's ``CDB-8002`` error covers.

The Python prototype is designed to fit alongside the WAL: BEGIN, COMMIT and
ROLLBACK each emit a WAL record so recovery (CDB-026) can reconstruct
transaction boundaries deterministically.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.mvcc import SnapshotId
from caracaldb.storage.wal import Wal

BEGIN_KIND = "TX_BEGIN"
COMMIT_KIND = "TX_COMMIT"
ROLLBACK_KIND = "TX_ABORT"


class TxConflictError(CaracalError):
    """Wrapper that pins the transaction-conflict error code."""

    def __init__(self, message: str, *, hint: str | None = None) -> None:
        super().__init__(code="CDB-8002", message=message, hint=hint)


@dataclass(slots=True)
class Transaction:
    tx_id: int
    snapshot: SnapshotId
    writes: set[tuple[str, object]] = field(default_factory=set)
    committed_lsn: int | None = None

    def record_write(self, table: str, key: object) -> None:
        self.writes.add((table, key))


class TransactionManager:
    def __init__(self, wal: Wal) -> None:
        self._wal = wal
        self._lock = threading.RLock()
        self._next_tx = 1
        # (table, key) → committed_lsn of the most recent committer.
        self._committed_writes: dict[tuple[str, object], int] = {}

    def begin(self, snapshot: SnapshotId | None = None) -> Transaction:
        with self._lock:
            tx_id = self._next_tx
            self._next_tx += 1
            snap = snapshot or SnapshotId(lsn_high=self._wal.last_lsn)
            self._wal.append(
                BEGIN_KIND,
                json.dumps({"tx_id": tx_id, "snapshot_lsn": snap.lsn_high}).encode("utf-8"),
            )
            return Transaction(tx_id=tx_id, snapshot=snap)

    def commit(self, tx: Transaction) -> int:
        with self._lock:
            for key in tx.writes:
                last_committed = self._committed_writes.get(key)
                if last_committed is not None and last_committed > tx.snapshot.lsn_high:
                    raise TxConflictError(
                        f"write-write conflict on {key!r}: "
                        f"another transaction committed at lsn={last_committed} "
                        f"after this transaction's snapshot lsn={tx.snapshot.lsn_high}",
                        hint="re-read the latest snapshot and retry the transaction",
                    )
            commit_lsn = self._wal.append(
                COMMIT_KIND,
                json.dumps({"tx_id": tx.tx_id, "writes": len(tx.writes)}).encode("utf-8"),
            )
            tx.committed_lsn = commit_lsn
            for key in tx.writes:
                self._committed_writes[key] = commit_lsn
            self._wal.flush()
            return commit_lsn

    def rollback(self, tx: Transaction) -> int:
        with self._lock:
            tx.writes.clear()
            lsn = self._wal.append(ROLLBACK_KIND, json.dumps({"tx_id": tx.tx_id}).encode("utf-8"))
            self._wal.flush()
            return lsn

    @contextmanager
    def transaction(self, snapshot: SnapshotId | None = None) -> Iterator[Transaction]:
        tx = self.begin(snapshot)
        try:
            yield tx
        except CaracalError:
            self.rollback(tx)
            raise
        except Exception:
            self.rollback(tx)
            raise
        else:
            self.commit(tx)


__all__ = [
    "BEGIN_KIND",
    "COMMIT_KIND",
    "ROLLBACK_KIND",
    "Transaction",
    "TransactionManager",
    "TxConflictError",
]
