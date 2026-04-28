"""Directory-bundle helpers for CaracalDB `.crcl` stores."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.manifest import MANIFEST_NAME, Manifest

BUNDLE_SUFFIX = ".crcl"
BUNDLE_DIRS = (
    "dict",
    "nodes",
    "edges",
    "vec",
    "closure",
    "wal",
    "snapshots",
)


@dataclass(frozen=True, slots=True)
class Bundle:
    path: Path
    manifest: Manifest

    @property
    def manifest_path(self) -> Path:
        return self.path / MANIFEST_NAME

    def child(self, *parts: str) -> Path:
        return self.path.joinpath(*parts)


def create_bundle(path: str | Path, *, exist_ok: bool = False) -> Bundle:
    root = _normalize_bundle_path(path)
    if root.exists() and not exist_ok:
        raise CaracalError(
            code="CDB-9001",
            message=f"bundle already exists: {root}",
            hint="pass exist_ok=True to open or reuse an existing bundle",
        )
    if root.exists() and not root.is_dir():
        raise CaracalError(code="CDB-9002", message=f"bundle path is not a directory: {root}")

    root.mkdir(parents=True, exist_ok=True)
    for name in BUNDLE_DIRS:
        (root / name).mkdir(exist_ok=True)

    manifest_path = root / MANIFEST_NAME
    if manifest_path.exists():
        manifest = Manifest.read(manifest_path)
    else:
        manifest = Manifest.empty()
        manifest.write_atomic(manifest_path)
    return Bundle(path=root, manifest=manifest)


def open_bundle(path: str | Path) -> Bundle:
    root = _normalize_bundle_path(path)
    if not root.is_dir():
        raise CaracalError(code="CDB-9003", message=f"bundle directory not found: {root}")

    manifest_path = root / MANIFEST_NAME
    if not manifest_path.is_file():
        raise CaracalError(code="CDB-9004", message=f"manifest not found: {manifest_path}")

    missing = [name for name in BUNDLE_DIRS if not (root / name).is_dir()]
    if missing:
        raise CaracalError(
            code="CDB-9005",
            message=f"bundle is missing required directories: {', '.join(missing)}",
        )

    return Bundle(path=root, manifest=Manifest.read(manifest_path))


def _normalize_bundle_path(path: str | Path) -> Path:
    root = Path(path)
    if root.suffix != BUNDLE_SUFFIX:
        root = root.with_suffix(BUNDLE_SUFFIX)
    return root


__all__ = ["BUNDLE_DIRS", "BUNDLE_SUFFIX", "Bundle", "create_bundle", "open_bundle"]
