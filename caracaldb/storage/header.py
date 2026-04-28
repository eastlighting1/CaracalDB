"""Common CaracalDB file header constants."""

from __future__ import annotations

import struct
import zlib

MAGIC = b"CRCL\x00\x00\x00\x01"
FORMAT_VERSION = 1
DEFAULT_PAGE_SIZE = 16 * 1024
HEADER_FMT = "<8sIIII"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


def header_crc32(
    *,
    magic: bytes = MAGIC,
    version: int = FORMAT_VERSION,
    page_size: int = DEFAULT_PAGE_SIZE,
    flags: int = 0,
) -> int:
    payload = struct.pack("<8sIII", magic, version, page_size, flags)
    return zlib.crc32(payload) & 0xFFFFFFFF


def pack_header(
    *,
    magic: bytes = MAGIC,
    version: int = FORMAT_VERSION,
    page_size: int = DEFAULT_PAGE_SIZE,
    flags: int = 0,
) -> bytes:
    return struct.pack(
        HEADER_FMT,
        magic,
        version,
        page_size,
        flags,
        header_crc32(
            magic=magic,
            version=version,
            page_size=page_size,
            flags=flags,
        ),
    )


__all__ = [
    "DEFAULT_PAGE_SIZE",
    "FORMAT_VERSION",
    "HEADER_FMT",
    "HEADER_SIZE",
    "MAGIC",
    "header_crc32",
    "pack_header",
]
