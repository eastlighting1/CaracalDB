"""CSR / CSC on-disk format helpers.

The wire layout follows ``docs/04_caracaldb_implementation.md §3.4``::

    [ CRCL header (24 B) ]
    [ u64 num_vertices ]
    [ u64 num_edges ]
    [ u32 flags ]              # bit0: has_eids
    [ u32 reserved ]
    [ u64 offsets[num_vertices + 1] ]
    [ u64 neighbors[num_edges] ]
    [ u64 eids[num_edges] ]    # only when flags & HAS_EIDS
    [ u32 footer_crc32 ]       # CRC32 over everything between the CRCL header
                                 and this trailer.

The 8-byte ``flags+reserved`` field keeps the body word-aligned without
expanding the published 04 §3.4 schema. ``write_csr`` is the
single source of truth used by both CSR and CSC writers; the only difference
between forward / reverse adjacency is the meaning of the source-side index.
"""

from __future__ import annotations

import struct
import zlib
from pathlib import Path
from typing import NamedTuple

import numpy as np

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.header import HEADER_SIZE, MAGIC, pack_header

CSR_HEAD_FMT = "<QQII"  # num_vertices, num_edges, flags, reserved
CSR_HEAD_SIZE = struct.calcsize(CSR_HEAD_FMT)
CSR_FOOTER_FMT = "<I"
CSR_FOOTER_SIZE = struct.calcsize(CSR_FOOTER_FMT)

CSR_FLAG_HAS_EIDS = 1 << 0


class CsrFile(NamedTuple):
    num_vertices: int
    num_edges: int
    flags: int
    offsets: np.ndarray
    neighbors: np.ndarray
    eids: np.ndarray | None


def write_csr(
    path: str | Path,
    *,
    offsets: np.ndarray,
    neighbors: np.ndarray,
    eids: np.ndarray | None = None,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if offsets.dtype != np.uint64 or neighbors.dtype != np.uint64:
        raise CaracalError(
            code="CDB-7080",
            message="CSR offsets/neighbors must be UInt64",
        )
    if offsets.ndim != 1 or neighbors.ndim != 1:
        raise CaracalError(code="CDB-7080", message="CSR arrays must be 1-D")
    if eids is not None and (eids.dtype != np.uint64 or eids.shape != neighbors.shape):
        raise CaracalError(
            code="CDB-7080",
            message="CSR eids must be UInt64 and same length as neighbors",
        )
    num_vertices = int(offsets.shape[0]) - 1
    num_edges = int(neighbors.shape[0])
    if num_vertices < 0:
        raise CaracalError(code="CDB-7080", message="CSR offsets must have >= 1 element")
    if int(offsets[-1]) != num_edges:
        raise CaracalError(
            code="CDB-7080",
            message=f"CSR offsets[-1]={int(offsets[-1])} != num_edges={num_edges}",
        )

    flags = 0
    if eids is not None:
        flags |= CSR_FLAG_HAS_EIDS

    header_bytes = pack_header()
    head = struct.pack(CSR_HEAD_FMT, num_vertices, num_edges, flags, 0)
    body = head + offsets.tobytes() + neighbors.tobytes()
    if eids is not None:
        body += eids.tobytes()
    crc = zlib.crc32(body) & 0xFFFFFFFF

    tmp = target.with_name(f"{target.name}.tmp")
    with tmp.open("wb") as f:
        f.write(header_bytes)
        f.write(body)
        f.write(struct.pack(CSR_FOOTER_FMT, crc))
    tmp.replace(target)
    return target


def read_csr(path: str | Path, *, mmap: bool = True) -> CsrFile:
    target = Path(path)
    data = target.read_bytes()
    if len(data) < HEADER_SIZE + CSR_HEAD_SIZE + CSR_FOOTER_SIZE:
        raise CaracalError(code="CDB-7081", message=f"CSR file too small: {target}")
    if data[: len(MAGIC)] != MAGIC:
        raise CaracalError(code="CDB-7081", message=f"invalid CSR magic: {target}")

    head_bytes = data[HEADER_SIZE : HEADER_SIZE + CSR_HEAD_SIZE]
    num_vertices, num_edges, flags, _reserved = struct.unpack(CSR_HEAD_FMT, head_bytes)
    body_start = HEADER_SIZE
    body_end = len(data) - CSR_FOOTER_SIZE
    body = data[body_start:body_end]
    expected_crc = struct.unpack(CSR_FOOTER_FMT, data[body_end:])[0]
    actual_crc = zlib.crc32(body) & 0xFFFFFFFF
    if actual_crc != expected_crc:
        raise CaracalError(code="CDB-7081", message=f"CSR checksum mismatch: {target}")

    offsets_off = HEADER_SIZE + CSR_HEAD_SIZE
    neighbors_off = offsets_off + (num_vertices + 1) * 8
    eids_off = neighbors_off + num_edges * 8

    if mmap:
        offsets = np.memmap(
            str(target), dtype=np.uint64, mode="r", offset=offsets_off, shape=(num_vertices + 1,)
        )
        neighbors = np.memmap(
            str(target), dtype=np.uint64, mode="r", offset=neighbors_off, shape=(num_edges,)
        )
        eids = (
            np.memmap(str(target), dtype=np.uint64, mode="r", offset=eids_off, shape=(num_edges,))
            if (flags & CSR_FLAG_HAS_EIDS)
            else None
        )
    else:
        offsets = np.frombuffer(data, dtype=np.uint64, count=num_vertices + 1, offset=offsets_off)
        neighbors = np.frombuffer(data, dtype=np.uint64, count=num_edges, offset=neighbors_off)
        eids = (
            np.frombuffer(data, dtype=np.uint64, count=num_edges, offset=eids_off)
            if (flags & CSR_FLAG_HAS_EIDS)
            else None
        )

    return CsrFile(
        num_vertices=num_vertices,
        num_edges=num_edges,
        flags=flags,
        offsets=offsets,
        neighbors=neighbors,
        eids=eids,
    )


__all__ = [
    "CSR_FLAG_HAS_EIDS",
    "CSR_FOOTER_FMT",
    "CSR_FOOTER_SIZE",
    "CSR_HEAD_FMT",
    "CSR_HEAD_SIZE",
    "CsrFile",
    "read_csr",
    "write_csr",
]
