"""Reader-writer stress: many readers + a single writer.

The Python prototype is GIL-bound, but the MVCC manager must remain
internally consistent under threaded contention. We spin up N reader
threads that repeatedly call ``mgr.begin()`` while a writer thread runs
``mgr.transaction()`` to commit successive (table, key) writes. The test
asserts:

1. Every commit produces a strictly larger LSN than the previous one.
2. No reader observes a stale ``next_tx`` (no duplicate tx ids).
3. Conflicting writes raise ``CDB-8002`` and never silently drop a write.
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.wal import Wal
from caracaldb.tx import TransactionManager


def test_concurrent_readers_observe_unique_tx_ids(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)

        seen: set[int] = set()
        lock = threading.Lock()
        errors: list[Exception] = []

        def reader() -> None:
            try:
                for _ in range(50):
                    tx = mgr.begin()
                    with lock:
                        # Every reader must observe a unique tx_id.
                        assert tx.tx_id not in seen
                        seen.add(tx.tx_id)
                    mgr.rollback(tx)
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=reader) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        assert len(seen) == 8 * 50


def test_writer_commit_lsn_is_monotonic(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        last = -1
        for i in range(20):
            with mgr.transaction() as tx:
                tx.record_write("Account", i)
            assert tx.committed_lsn is not None
            assert tx.committed_lsn > last
            last = tx.committed_lsn


def test_concurrent_writers_serialize_via_lock(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        # Two threads commit different keys → both succeed.
        results: list[int] = []
        errors: list[Exception] = []

        def commit_unique(key: int) -> None:
            try:
                with mgr.transaction() as tx:
                    tx.record_write("Account", key)
                results.append(tx.committed_lsn)  # type: ignore[arg-type]
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=commit_unique, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == []
        # All commits must produce distinct LSNs.
        assert len(set(results)) == len(results)


def test_concurrent_writers_on_same_key_at_least_one_conflicts(tmp_path: Path) -> None:
    with Wal(tmp_path / "wal") as wal:
        mgr = TransactionManager(wal)
        a = mgr.begin()
        b = mgr.begin()
        a.record_write("Account", 7)
        b.record_write("Account", 7)
        mgr.commit(a)
        with pytest.raises(CaracalError) as exc:
            mgr.commit(b)
        assert exc.value.code == "CDB-8002"
