"""Crash recovery: replay WAL from the last checkpoint.

The Python-prototype recovery path is intentionally narrow: column segments
are written atomically via tmp+rename, so durable on-disk state never reflects
a partially-written chunk. Recovery therefore only needs to (a) replay any
metadata-style WAL records that are not yet reflected in the manifest, and
(b) re-derive ``last_lsn``/``checkpoint_lsn`` from disk so that subsequent
appends pick up the correct sequence. Future milestones plug in domain-aware
redo handlers (node-store rollback, edge CSR rebuild, MVCC undo).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.checkpoint import CHECKPOINT_KIND
from caracaldb.storage.manifest import MANIFEST_NAME
from caracaldb.storage.wal import Wal, WalRecord

RedoHandler = Callable[[WalRecord], None]


@dataclass(slots=True)
class RecoveryReport:
    replayed: int = 0
    skipped_pre_checkpoint: int = 0
    last_lsn: int = 0
    checkpoint_lsn: int = 0
    handler_invocations: dict[str, int] = field(default_factory=dict)


def recover(
    bundle: Bundle,
    *,
    handlers: dict[str, RedoHandler] | None = None,
) -> RecoveryReport:
    """Replay WAL records strictly after the last manifest checkpoint.

    ``handlers`` is a ``{record.kind: callable}`` map. ``CHECKPOINT`` records
    advance the in-memory checkpoint pointer instead of being dispatched.
    Unknown kinds are tolerated (counted but not invoked) so older bundles can
    be opened by newer engines.
    """
    handlers = dict(handlers or {})
    report = RecoveryReport(checkpoint_lsn=bundle.manifest.checkpoint_lsn)

    last_seen = bundle.manifest.last_lsn
    boundary = bundle.manifest.checkpoint_lsn

    with Wal(bundle.path / "wal") as wal:
        for record in wal.iter_since(0):
            last_seen = max(last_seen, record.lsn)
            if record.lsn <= boundary:
                report.skipped_pre_checkpoint += 1
                continue
            if record.kind == CHECKPOINT_KIND:
                if record.lsn > boundary:
                    boundary = record.lsn
                continue
            handler = handlers.get(record.kind)
            if handler is not None:
                try:
                    handler(record)
                except Exception as exc:  # pragma: no cover - rethrow with context
                    raise CaracalError(
                        code="CDB-7070",
                        message=(
                            f"redo handler for {record.kind!r} failed "
                            f"at lsn={record.lsn}: {exc}"
                        ),
                    ) from exc
            report.handler_invocations[record.kind] = (
                report.handler_invocations.get(record.kind, 0) + 1
            )
            report.replayed += 1

    report.last_lsn = last_seen
    report.checkpoint_lsn = boundary

    if last_seen != bundle.manifest.last_lsn or boundary != bundle.manifest.checkpoint_lsn:
        refreshed = replace(
            bundle.manifest,
            last_lsn=last_seen,
            checkpoint_lsn=boundary,
        )
        refreshed.write_atomic(bundle.path / MANIFEST_NAME)
        object.__setattr__(bundle, "manifest", refreshed)

    return report


__all__ = ["RecoveryReport", "RedoHandler", "recover"]
