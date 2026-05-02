"""Named snapshot catalogue.

Snapshots are recorded inside the bundle ``MANIFEST.snapshots`` field. Each
named snapshot is durable until the user releases it. The Python-prototype
implementation keeps a sidecar JSON registry that maps name → (lsn_high,
created_at) so reopening the bundle yields the same snapshot ids.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from pathlib import Path

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import Bundle
from caracaldb.storage.manifest import MANIFEST_NAME, utc_now_iso
from caracaldb.storage.mvcc import SnapshotId

SNAPSHOT_REGISTRY = "snapshots/_registry.json"


@dataclass(slots=True)
class SnapshotEntry:
    name: str
    lsn_high: int
    created_at: str
    refcount: int = 1


@dataclass(slots=True)
class SnapshotRegistry:
    entries: dict[str, SnapshotEntry] = field(default_factory=dict)

    def to_json(self) -> dict[str, object]:
        return {
            "entries": [
                {
                    "name": e.name,
                    "lsn_high": e.lsn_high,
                    "created_at": e.created_at,
                    "refcount": e.refcount,
                }
                for e in self.entries.values()
            ]
        }

    @classmethod
    def from_json(cls, value: dict[str, object]) -> SnapshotRegistry:
        registry = cls()
        for raw in value.get("entries", []):  # type: ignore[union-attr]
            entry = SnapshotEntry(
                name=str(raw["name"]),
                lsn_high=int(raw["lsn_high"]),
                created_at=str(raw["created_at"]),
                refcount=int(raw.get("refcount", 1)),
            )
            registry.entries[entry.name] = entry
        return registry


def _registry_path(bundle: Bundle) -> Path:
    return bundle.path / SNAPSHOT_REGISTRY


def load_registry(bundle: Bundle) -> SnapshotRegistry:
    target = _registry_path(bundle)
    if not target.is_file():
        return SnapshotRegistry()
    return SnapshotRegistry.from_json(json.loads(target.read_text(encoding="utf-8")))


def save_registry(bundle: Bundle, registry: SnapshotRegistry) -> None:
    target = _registry_path(bundle)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.name}.tmp")
    tmp.write_text(json.dumps(registry.to_json(), indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(target)


def create_snapshot(bundle: Bundle, name: str, *, lsn_high: int | None = None) -> SnapshotId:
    if not name:
        raise CaracalError(code="CDB-8011", message="snapshot name cannot be empty")
    registry = load_registry(bundle)
    if name in registry.entries:
        raise CaracalError(code="CDB-8012", message=f"snapshot {name!r} already exists")
    lsn = bundle.manifest.last_lsn if lsn_high is None else lsn_high
    entry = SnapshotEntry(name=name, lsn_high=lsn, created_at=utc_now_iso())
    registry.entries[name] = entry
    save_registry(bundle, registry)
    # Mirror into the bundle MANIFEST.snapshots list.
    new_manifest = replace(bundle.manifest, snapshots=tuple(sorted(registry.entries.keys())))
    new_manifest.write_atomic(bundle.path / MANIFEST_NAME)
    object.__setattr__(bundle, "manifest", new_manifest)
    return SnapshotId(lsn_high=lsn, name=name)


def peek_snapshot(bundle: Bundle, name: str) -> SnapshotId:
    """Read-only resolution of a named snapshot.

    Unlike :func:`open_snapshot`, this does not increment the refcount or
    rewrite the registry. Use it from query-time resolution (``AS_OF
    SNAPSHOT 'name'``) where the caller does not own the snapshot.
    """
    registry = load_registry(bundle)
    entry = registry.entries.get(name)
    if entry is None:
        raise CaracalError(code="CDB-8013", message=f"snapshot not found: {name!r}")
    return SnapshotId(lsn_high=entry.lsn_high, name=name)


def open_snapshot(bundle: Bundle, name: str) -> SnapshotId:
    registry = load_registry(bundle)
    entry = registry.entries.get(name)
    if entry is None:
        raise CaracalError(code="CDB-8013", message=f"snapshot not found: {name!r}")
    entry.refcount += 1
    save_registry(bundle, registry)
    return SnapshotId(lsn_high=entry.lsn_high, name=name)


def release_snapshot(bundle: Bundle, name: str) -> bool:
    """Decrement refcount; remove entry when it hits zero. Returns ``True``
    when the snapshot was removed.
    """
    registry = load_registry(bundle)
    entry = registry.entries.get(name)
    if entry is None:
        raise CaracalError(code="CDB-8013", message=f"snapshot not found: {name!r}")
    entry.refcount -= 1
    if entry.refcount <= 0:
        del registry.entries[name]
        save_registry(bundle, registry)
        new_manifest = replace(bundle.manifest, snapshots=tuple(sorted(registry.entries.keys())))
        new_manifest.write_atomic(bundle.path / MANIFEST_NAME)
        object.__setattr__(bundle, "manifest", new_manifest)
        return True
    save_registry(bundle, registry)
    return False


def list_snapshots(bundle: Bundle) -> list[SnapshotEntry]:
    registry = load_registry(bundle)
    return sorted(registry.entries.values(), key=lambda e: (e.lsn_high, e.name))


__all__ = [
    "SnapshotEntry",
    "SnapshotRegistry",
    "create_snapshot",
    "list_snapshots",
    "load_registry",
    "open_snapshot",
    "peek_snapshot",
    "release_snapshot",
    "save_registry",
]
