"""``AS_OF SNAPSHOT`` resolution helpers.

Lifts a parsed ``ta.AsOf`` AST node into a concrete ``SnapshotId`` resolved
against the bundle's snapshot registry, and pins the result onto the
ExecCtx's ``snapshot_id`` field so operators can consult it (today only the
NodeScan can ignore rows whose ``created_lsn`` is in the future, but the
plumbing is laid down so M5 row-version columns flow through unchanged).
"""

from __future__ import annotations

from caracaldb.exec.operator import ExecCtx
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.mvcc import SnapshotId
from caracaldb.storage.snapshot import open_snapshot


def resolve_as_of(bundle: Bundle, node: ta.AsOf | None) -> SnapshotId | None:
    if node is None:
        return None
    kind = (node.kind or "").upper()
    if kind == "SNAPSHOT":
        return open_snapshot(bundle, node.value)
    if kind == "DATETIME":
        # Datetime-based snapshot resolution lands when MVCC tracks per-row
        # commit timestamps; surface a clear error so callers see the gap.
        raise CaracalError(
            code="CDB-6021",
            message="AS_OF DATETIME is reserved for M4; use AS_OF SNAPSHOT 'name' for now",
        )
    raise CaracalError(code="CDB-6021", message=f"unknown AS_OF kind: {node.kind!r}")


def apply_as_of(ctx: ExecCtx, snap: SnapshotId | None) -> ExecCtx:
    if snap is None:
        return ctx
    ctx.snapshot_id = str(snap)
    ctx.metadata["snapshot_lsn"] = snap.lsn_high
    return ctx


__all__ = ["apply_as_of", "resolve_as_of"]
