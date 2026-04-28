"""CLI-level tests for ``caracal pack`` and ``caracal unpack``."""

from pathlib import Path

from caracaldb.cli.app import cmd_pack, cmd_unpack
from caracaldb.storage import create_bundle, open_bundle
from caracaldb.storage.pack import is_packed


def test_cmd_pack_creates_packed_file(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "db")
    rc = cmd_pack(bundle.path, output=tmp_path / "db_export.crcl", codec="deflate")

    assert rc == 0
    assert (tmp_path / "db_export.crcl").is_file()
    assert is_packed(tmp_path / "db_export.crcl")


def test_cmd_unpack_restores_bundle(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "db")
    packed = tmp_path / "db_export.crcl"
    cmd_pack(bundle.path, output=packed, codec="deflate")

    restored = tmp_path / "restored.crcl"
    rc = cmd_unpack(packed, output=restored)

    assert rc == 0
    assert restored.is_dir()
    reopened = open_bundle(restored)
    assert reopened.manifest.format_version == bundle.manifest.format_version


def test_cmd_pack_nonexistent_returns_error(tmp_path: Path) -> None:
    rc = cmd_pack(tmp_path / "missing.crcl", output=None, codec="deflate")
    assert rc == 1


def test_cmd_unpack_nonexistent_returns_error(tmp_path: Path) -> None:
    rc = cmd_unpack(tmp_path / "missing.crcl", output=None)
    assert rc == 1


def test_cmd_pack_unpack_roundtrip(tmp_path: Path, capsys) -> None:
    """Full CLI round-trip: init → pack → unpack → reopen."""
    bundle = create_bundle(tmp_path / "rt")
    packed = tmp_path / "rt_export.crcl"
    assert cmd_pack(bundle.path, output=packed, codec="stored") == 0

    captured = capsys.readouterr()
    assert "packed bundle to" in captured.out

    restored = tmp_path / "rt_restored.crcl"
    assert cmd_unpack(packed, output=restored) == 0

    captured = capsys.readouterr()
    assert "unpacked bundle to" in captured.out

    reopened = open_bundle(restored)
    assert reopened.manifest.created_at == bundle.manifest.created_at
