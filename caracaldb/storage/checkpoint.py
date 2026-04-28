"""Fuzzy checkpoint coordination for `.crcl` bundles (M1).

The Python-prototype checkpoint is intentionally light: chunked column writers
are atomic per-chunk, so the only durability work the checkpointer has to do
is (1) record a ``CHECKPOINT`` marker in the WAL, (2) persist a refreshed
``MANIFEST`` whose ``checkpoint_lsn`` matches the marker, and (3) truncate WAL
segments whose records are entirely covered by the checkpoint. M3 widens this
to MVCC-aware dirty-page tracking; the API shape here is forward-compatible.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from pathlib import Path

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.manifest import MANIFEST_NAME, Manifest, utc_now_iso
from caracaldb.storage.wal import Wal

CHECKPOINT_KIND = "CHECKPOINT"


@dataclass(frozen=True, slots=True)
class CheckpointResult:
    checkpoint_lsn: int
    wal_segments_removed: int
    manifest_path: str
    timestamp: str


def checkpoint(bundle: Bundle, wal: Wal) -> CheckpointResult:
    """Record a CHECKPOINT marker, refresh the bundle MANIFEST, and prune WAL."""
    last_lsn = wal.last_lsn
    if last_lsn < bundle.manifest.checkpoint_lsn:
        raise CaracalError(
            code="CDB-7060",
            message=(
                f"WAL last_lsn={last_lsn} is behind manifest checkpoint_lsn="
                f"{bundle.manifest.checkpoint_lsn}; refusing to regress"
            ),
        )

    marker_lsn = wal.append(CHECKPOINT_KIND, b"")
    wal.flush()

    new_manifest = replace(
        bundle.manifest,
        last_lsn=marker_lsn,
        checkpoint_lsn=marker_lsn,
        created_at=bundle.manifest.created_at,
    )
    manifest_path = bundle.path / MANIFEST_NAME
    new_manifest.write_atomic(manifest_path)

    # Refresh in-memory Bundle.manifest to match disk (Bundle is frozen, mutate via __setattr__).
    object.__setattr__(bundle, "manifest", new_manifest)

    removed = wal.truncate_before(marker_lsn)
    return CheckpointResult(
        checkpoint_lsn=marker_lsn,
        wal_segments_removed=removed,
        manifest_path=str(manifest_path),
        timestamp=utc_now_iso(),
    )


def latest_checkpoint_lsn(bundle: Bundle) -> int:
    return bundle.manifest.checkpoint_lsn


def reload_manifest(bundle: Bundle) -> Manifest:
    """Re-read the manifest file from disk and bind it to ``bundle``."""
    refreshed = Manifest.read(bundle.path / MANIFEST_NAME)
    object.__setattr__(bundle, "manifest", refreshed)
    return refreshed


__all__ = [
    "CHECKPOINT_KIND",
    "CheckpointResult",
    "checkpoint",
    "latest_checkpoint_lsn",
    "reload_manifest",
]


def _coerce_path(p: str | Path) -> Path:
    return Path(p)
