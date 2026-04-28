"""Tests for ``caracaldb.storage.pack`` — bundle pack/unpack round-trips."""

from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import Manifest, create_bundle, open_bundle
from caracaldb.storage.bundle import BUNDLE_DIRS
from caracaldb.storage.pack import is_packed, pack_bundle, unpack_bundle


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_sample_bundle(base: Path, name: str = "graph") -> Path:
    """Create a bundle with a small sentinel file for content verification."""
    bundle = create_bundle(base / name)
    # Write a small sentinel file so we can verify round-trip content.
    sentinel = bundle.path / "nodes" / "sentinel.txt"
    sentinel.write_text("hello caracal", encoding="utf-8")
    return bundle.path


# ---------------------------------------------------------------------------
# pack_bundle
# ---------------------------------------------------------------------------


def test_pack_bundle_creates_file(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    packed = pack_bundle(bundle_dir)

    assert packed.is_file()
    assert packed.suffix == ".crcl"


def test_pack_bundle_with_explicit_output(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    out = tmp_path / "export" / "my_graph.crcl"
    packed = pack_bundle(bundle_dir, output=out)

    assert packed == out
    assert packed.is_file()


def test_pack_bundle_stored_codec(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    packed = pack_bundle(bundle_dir, codec="stored")

    assert packed.is_file()
    assert is_packed(packed)


def test_pack_bundle_rejects_nonexistent_path(tmp_path: Path) -> None:
    with pytest.raises(CaracalError) as exc:
        pack_bundle(tmp_path / "nonexistent.crcl")

    assert exc.value.code == "CDB-9010"


def test_pack_bundle_rejects_existing_output(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    out = tmp_path / "existing.crcl"
    out.write_bytes(b"dummy")

    with pytest.raises(CaracalError) as exc:
        pack_bundle(bundle_dir, output=out)

    assert exc.value.code == "CDB-9011"


def test_pack_bundle_rejects_invalid_codec(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)

    with pytest.raises(CaracalError) as exc:
        pack_bundle(bundle_dir, codec="bzip2")

    assert exc.value.code == "CDB-9015"


# ---------------------------------------------------------------------------
# unpack_bundle
# ---------------------------------------------------------------------------


def test_unpack_bundle_restores_directory(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    packed = pack_bundle(bundle_dir)
    unpacked = unpack_bundle(packed)

    assert unpacked.is_dir()
    assert (unpacked / "MANIFEST").is_file()
    for dirname in BUNDLE_DIRS:
        assert (unpacked / dirname).is_dir()


def test_unpack_bundle_preserves_file_content(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    packed = pack_bundle(bundle_dir)
    unpacked = unpack_bundle(packed)

    sentinel = unpacked / "nodes" / "sentinel.txt"
    assert sentinel.read_text(encoding="utf-8") == "hello caracal"


def test_unpack_bundle_with_explicit_output(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    packed = pack_bundle(bundle_dir)
    out = tmp_path / "restored.crcl"
    unpacked = unpack_bundle(packed, output=out)

    assert unpacked == out
    assert unpacked.is_dir()


def test_unpack_bundle_rejects_nonexistent_file(tmp_path: Path) -> None:
    with pytest.raises(CaracalError) as exc:
        unpack_bundle(tmp_path / "missing.crcl")

    assert exc.value.code == "CDB-9012"


def test_unpack_bundle_rejects_non_packed_file(tmp_path: Path) -> None:
    fake = tmp_path / "fake.crcl"
    fake.write_bytes(b"not a zip")

    with pytest.raises(CaracalError) as exc:
        unpack_bundle(fake)

    assert exc.value.code == "CDB-9013"


def test_unpack_bundle_rejects_existing_output(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    packed = pack_bundle(bundle_dir)
    out = tmp_path / "occupied.crcl"
    out.mkdir()

    with pytest.raises(CaracalError) as exc:
        unpack_bundle(packed, output=out)

    assert exc.value.code == "CDB-9014"


# ---------------------------------------------------------------------------
# is_packed
# ---------------------------------------------------------------------------


def test_is_packed_true_for_packed_file(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)
    packed = pack_bundle(bundle_dir)

    assert is_packed(packed) is True


def test_is_packed_false_for_directory(tmp_path: Path) -> None:
    bundle_dir = _create_sample_bundle(tmp_path)

    assert is_packed(bundle_dir) is False


def test_is_packed_false_for_nonexistent(tmp_path: Path) -> None:
    assert is_packed(tmp_path / "nope.crcl") is False


def test_is_packed_false_for_arbitrary_file(tmp_path: Path) -> None:
    f = tmp_path / "random.bin"
    f.write_bytes(b"random bytes")

    assert is_packed(f) is False


# ---------------------------------------------------------------------------
# Round-trip: create → pack → unpack → open
# ---------------------------------------------------------------------------


def test_roundtrip_create_pack_unpack_open(tmp_path: Path) -> None:
    """Full round-trip: create a bundle, pack it, unpack it, and open it."""
    original = create_bundle(tmp_path / "rt_test")
    packed = pack_bundle(original.path)
    restored_dir = unpack_bundle(packed)

    reopened = open_bundle(restored_dir)

    assert isinstance(reopened.manifest, Manifest)
    assert reopened.manifest.format_version == original.manifest.format_version
    assert reopened.manifest.created_at == original.manifest.created_at
