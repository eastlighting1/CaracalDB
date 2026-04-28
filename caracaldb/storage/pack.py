"""Pack / unpack helpers to convert between `.crcl` directory bundles and
single-file archives.

Engine spec §4.2 states:

    A database is supported both as a **directory bundle** ``graph.crcl/``
    and as a **single file** ``graph.crcl`` (a zip-ish container).

This module implements the single-file variant as a standard ZIP archive
(with ZIP64 extensions for large bundles).  The packed file is intended as
a **read-only exchange format** — the engine always operates on the
directory bundle at runtime.

Usage::

    from caracaldb.storage.pack import pack_bundle, unpack_bundle, is_packed

    packed = pack_bundle(Path("graph.crcl"))     # → graph.crcl (file)
    unpacked = unpack_bundle(packed)              # → graph.crcl/ (dir)
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.bundle import BUNDLE_SUFFIX
from caracaldb.storage.header import MAGIC

# Stored as the ZIP file comment so that ``is_packed`` can distinguish a
# packed ``.crcl`` file from an arbitrary ZIP archive.
_ZIP_COMMENT = MAGIC + b"PACKED"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def pack_bundle(
    bundle_path: str | Path,
    output: str | Path | None = None,
    *,
    codec: str = "deflate",
) -> Path:
    """Package a ``.crcl`` directory bundle into a single ZIP-based file.

    Parameters
    ----------
    bundle_path:
        Path to an existing ``.crcl/`` directory bundle.
    output:
        Destination file path.  Defaults to a sibling file with the same
        stem and a ``.crcl`` suffix (e.g. ``data.crcl/`` → ``data.crcl``
        placed next to the directory).
    codec:
        Compression method — ``"deflate"`` (default, good compatibility)
        or ``"stored"`` (no compression, fastest).

    Returns
    -------
    Path
        The path of the created packed file.

    Raises
    ------
    CaracalError
        If *bundle_path* does not exist or is not a directory bundle,
        or if *output* already exists.
    """
    root = Path(bundle_path)
    if not root.is_dir():
        raise CaracalError(
            code="CDB-9010",
            message=f"pack source is not a directory bundle: {root}",
        )

    dest = _resolve_pack_output(root, output)
    if dest.exists():
        raise CaracalError(
            code="CDB-9011",
            message=f"pack output already exists: {dest}",
            hint="remove the existing file or choose a different output path",
        )

    compression = _codec_to_compression(codec)

    dest.parent.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(dest, "w", compression=compression, allowZip64=True) as zf:
        zf.comment = _ZIP_COMMENT
        for dirpath, dirnames, filenames in os.walk(root):
            # Explicitly record directories so that empty sub-directories
            # (e.g. ``dict/``, ``wal/``) survive the round-trip.
            for dname in sorted(dirnames):
                abs_dir = Path(dirpath) / dname
                arcname = abs_dir.relative_to(root).as_posix() + "/"
                zf.mkdir(arcname)
            for fname in sorted(filenames):
                abs_file = Path(dirpath) / fname
                arcname = abs_file.relative_to(root).as_posix()
                zf.write(abs_file, arcname)

    return dest


def unpack_bundle(
    file_path: str | Path,
    output: str | Path | None = None,
) -> Path:
    """Restore a packed ``.crcl`` file back to a directory bundle.

    Parameters
    ----------
    file_path:
        Path to a packed ``.crcl`` file.
    output:
        Destination directory path.  Defaults to a sibling directory
        with the same stem and a ``.crcl`` suffix.

    Returns
    -------
    Path
        The path of the restored directory bundle.

    Raises
    ------
    CaracalError
        If *file_path* is not a valid packed file, or if *output*
        already exists.
    """
    src = Path(file_path)
    if not src.is_file():
        raise CaracalError(
            code="CDB-9012",
            message=f"unpack source is not a file: {src}",
        )
    if not is_packed(src):
        raise CaracalError(
            code="CDB-9013",
            message=f"file is not a valid packed .crcl archive: {src}",
            hint="ensure the file was created with 'caracal pack'",
        )

    dest = _resolve_unpack_output(src, output)
    if dest.exists():
        raise CaracalError(
            code="CDB-9014",
            message=f"unpack output already exists: {dest}",
            hint="remove the existing directory or choose a different output path",
        )

    with zipfile.ZipFile(src, "r") as zf:
        zf.extractall(dest)

    return dest


def is_packed(path: str | Path) -> bool:
    """Return ``True`` if *path* is a packed single-file ``.crcl`` archive."""
    p = Path(path)
    if not p.is_file():
        return False
    try:
        with zipfile.ZipFile(p, "r") as zf:
            return zf.comment == _ZIP_COMMENT
    except (zipfile.BadZipFile, OSError):
        return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_pack_output(bundle_dir: Path, output: Path | str | None) -> Path:
    """Compute the default output path for ``pack_bundle``."""
    if output is not None:
        out = Path(output)
        if out.suffix != BUNDLE_SUFFIX:
            out = out.with_suffix(BUNDLE_SUFFIX)
        return out
    # Place the file next to the directory, using a ``_packed`` infix to
    # avoid confusion when the directory and file share the same stem.
    return bundle_dir.parent / f"{bundle_dir.stem}_packed{BUNDLE_SUFFIX}"


def _resolve_unpack_output(packed_file: Path, output: Path | str | None) -> Path:
    """Compute the default output path for ``unpack_bundle``."""
    if output is not None:
        out = Path(output)
        if out.suffix != BUNDLE_SUFFIX:
            out = out.with_suffix(BUNDLE_SUFFIX)
        return out
    stem = packed_file.stem
    # Strip the ``_packed`` infix if present.
    if stem.endswith("_packed"):
        stem = stem[: -len("_packed")]
    return packed_file.parent / f"{stem}_unpacked{BUNDLE_SUFFIX}"


def _codec_to_compression(codec: str) -> int:
    """Map a human-readable codec name to a ``zipfile`` constant."""
    mapping = {
        "deflate": zipfile.ZIP_DEFLATED,
        "stored": zipfile.ZIP_STORED,
    }
    result = mapping.get(codec.lower())
    if result is None:
        raise CaracalError(
            code="CDB-9015",
            message=f"unsupported pack codec: {codec!r}",
            hint=f"choose one of: {', '.join(sorted(mapping))}",
        )
    return result


__all__ = ["is_packed", "pack_bundle", "unpack_bundle"]
