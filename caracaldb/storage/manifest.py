"""Manifest model and JSON serialization for `.crcl` bundles."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from caracaldb.storage.header import FORMAT_VERSION

MANIFEST_NAME = "MANIFEST"


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class ManifestFile:
    path: str
    size: int
    crc32: int

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> ManifestFile:
        return cls(path=str(value["path"]), size=int(value["size"]), crc32=int(value["crc32"]))

    def to_json(self) -> dict[str, Any]:
        return {"path": self.path, "size": self.size, "crc32": self.crc32}


@dataclass(frozen=True, slots=True)
class Manifest:
    format_version: int = FORMAT_VERSION
    created_at: str = field(default_factory=utc_now_iso)
    last_lsn: int = 0
    checkpoint_lsn: int = 0
    current_snapshot: str | None = None
    catalog_file: str = "catalog.fb"
    files: tuple[ManifestFile, ...] = ()
    snapshots: tuple[str, ...] = ()

    @classmethod
    def empty(cls) -> Manifest:
        return cls()

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> Manifest:
        return cls(
            format_version=int(value["format_version"]),
            created_at=str(value["created_at"]),
            last_lsn=int(value.get("last_lsn", 0)),
            checkpoint_lsn=int(value.get("checkpoint_lsn", 0)),
            current_snapshot=value.get("current_snapshot"),
            catalog_file=str(value.get("catalog_file", "catalog.fb")),
            files=tuple(ManifestFile.from_json(item) for item in value.get("files", [])),
            snapshots=tuple(str(item) for item in value.get("snapshots", [])),
        )

    @classmethod
    def read(cls, path: Path) -> Manifest:
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls.from_json(data)

    def to_json(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "format_version": self.format_version,
            "created_at": self.created_at,
            "last_lsn": self.last_lsn,
            "checkpoint_lsn": self.checkpoint_lsn,
            "catalog_file": self.catalog_file,
            "files": [item.to_json() for item in self.files],
            "snapshots": list(self.snapshots),
        }
        if self.current_snapshot is not None:
            value["current_snapshot"] = self.current_snapshot
        return value

    def write_atomic(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(f"{path.name}.tmp")
        text = json.dumps(self.to_json(), indent=2, sort_keys=True) + "\n"
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, path)


__all__ = ["MANIFEST_NAME", "Manifest", "ManifestFile", "utc_now_iso"]
