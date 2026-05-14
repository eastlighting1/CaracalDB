"""Strict release archive validation before upload.

PyPI rejects ZIP archives with bytes after the end-of-central-directory record
even when Python's ``zipfile`` module and ``pip install`` can still read them.
This check is intentionally stricter than a smoke install so corrupted wheels
fail before the publish step.
"""

from __future__ import annotations

import argparse
import pathlib
import struct
import tarfile
import zipfile

EOCD_SIGNATURE = b"PK\x05\x06"
EOCD_FIXED_SIZE = 22
MAX_ZIP_COMMENT = 65_535


def check_wheel(path: pathlib.Path, *, repair: bool = False) -> None:
    data = path.read_bytes()
    search_start = max(0, len(data) - (EOCD_FIXED_SIZE + MAX_ZIP_COMMENT))
    eocd_offset = data.rfind(EOCD_SIGNATURE, search_start)
    if eocd_offset < 0:
        raise SystemExit(f"{path}: ZIP end-of-central-directory record not found")
    if eocd_offset + EOCD_FIXED_SIZE > len(data):
        raise SystemExit(f"{path}: truncated ZIP end-of-central-directory record")

    comment_len = struct.unpack_from("<H", data, eocd_offset + 20)[0]
    expected_end = eocd_offset + EOCD_FIXED_SIZE + comment_len
    if expected_end != len(data):
        trailing = len(data) - expected_end
        if trailing < 0:
            raise SystemExit(f"{path}: ZIP archive is truncated")
        if not repair:
            raise SystemExit(f"{path}: ZIP archive has {trailing} trailing byte(s)")
        path.write_bytes(data[:expected_end])
        data = data[:expected_end]

    with zipfile.ZipFile(path) as zf:
        bad_member = zf.testzip()
        if bad_member is not None:
            raise SystemExit(f"{path}: corrupt ZIP member: {bad_member}")
        names = zf.namelist()

    if not any(
        name.startswith("caracaldb/_caracaldb_rust") and name.endswith((".so", ".pyd"))
        for name in names
    ):
        raise SystemExit(f"{path}: Rust extension missing from wheel")


def check_sdist(path: pathlib.Path) -> None:
    try:
        with tarfile.open(path, "r:gz") as tf:
            members = tf.getmembers()
            names = [member.name for member in members]
    except tarfile.TarError as exc:
        raise SystemExit(f"{path}: invalid sdist tarball: {exc}") from exc
    if not members:
        raise SystemExit(f"{path}: empty sdist")
    root = names[0].split("/", 1)[0]
    if f"{root}/LICENSE" not in names:
        raise SystemExit(f"{path}: LICENSE missing from sdist root")


def check_dist_dir(dist: pathlib.Path, *, repair: bool = False) -> None:
    files = sorted(path for path in dist.iterdir() if path.is_file())
    if not files:
        raise SystemExit(f"{dist}: no distribution files found")
    for path in files:
        if path.suffix == ".whl":
            check_wheel(path, repair=repair)
        elif path.name.endswith(".tar.gz"):
            check_sdist(path)
        else:
            raise SystemExit(f"{path}: unexpected distribution artifact")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repair",
        action="store_true",
        help="remove ZIP trailing data before validating wheels",
    )
    parser.add_argument("dist", type=pathlib.Path)
    args = parser.parse_args()
    check_dist_dir(args.dist, repair=args.repair)


if __name__ == "__main__":
    main()
