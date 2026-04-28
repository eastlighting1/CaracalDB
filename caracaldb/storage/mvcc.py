"""MVCC primitives: snapshot ids and visibility metadata.

The Python prototype keeps the model intentionally narrow: every node store
write carries a ``created_lsn``; deletes set a ``deleted_lsn``. A snapshot is
an immutable ``(lsn_high, name)`` token that fixes the LSN window a reader
sees. Visibility is a pure function of the row's lsn metadata and the
snapshot's lsn — no per-row locks, no UNDO, no row-version chains.

The dataclass surface is finalised here; storage/snapshot.py owns the
on-disk catalogue of snapshots, and tx/manager.py drives the writer queue.
"""

from __future__ import annotations

from dataclasses import dataclass

from caracaldb.lang.diagnostics import CaracalError


@dataclass(frozen=True, slots=True)
class SnapshotId:
    """Immutable snapshot reference (committed up to ``lsn_high``)."""

    lsn_high: int
    name: str | None = None

    def __post_init__(self) -> None:
        if self.lsn_high < 0:
            raise CaracalError(code="CDB-8001", message="snapshot lsn must be >= 0")

    def __str__(self) -> str:
        if self.name:
            return f"snap:{self.name}@{self.lsn_high}"
        return f"snap:@{self.lsn_high}"


@dataclass(frozen=True, slots=True)
class RowVersion:
    """Per-row LSN metadata (logical view; physical layout is M5)."""

    created_lsn: int
    deleted_lsn: int | None = None

    def visible_to(self, snap: SnapshotId) -> bool:
        if self.created_lsn > snap.lsn_high:
            return False
        return not (self.deleted_lsn is not None and self.deleted_lsn <= snap.lsn_high)


def visibility_mask(versions: list[RowVersion], snap: SnapshotId) -> list[bool]:
    return [v.visible_to(snap) for v in versions]


__all__ = ["RowVersion", "SnapshotId", "visibility_mask"]
