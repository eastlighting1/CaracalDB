from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import MAGIC, Bundle, Manifest, create_bundle, open_bundle
from caracaldb.storage.bundle import BUNDLE_DIRS


def test_create_bundle_writes_layout_and_manifest(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "graph")

    assert isinstance(bundle, Bundle)
    assert bundle.path.name == "graph.crcl"
    assert bundle.manifest_path.is_file()
    for dirname in BUNDLE_DIRS:
        assert (bundle.path / dirname).is_dir()

    opened = open_bundle(bundle.path)
    assert isinstance(opened.manifest, Manifest)
    assert opened.manifest.format_version == 1
    assert opened.manifest.catalog_file == "catalog.fb"


def test_create_existing_bundle_requires_exist_ok(tmp_path: Path) -> None:
    path = tmp_path / "graph.crcl"
    create_bundle(path)

    with pytest.raises(CaracalError) as exc:
        create_bundle(path)

    assert exc.value.code == "CDB-9001"


def test_create_existing_bundle_with_exist_ok_reuses_manifest(tmp_path: Path) -> None:
    path = tmp_path / "graph.crcl"
    first = create_bundle(path)
    second = create_bundle(path, exist_ok=True)

    assert second.manifest.created_at == first.manifest.created_at


def test_open_bundle_rejects_missing_manifest(tmp_path: Path) -> None:
    path = tmp_path / "broken.crcl"
    path.mkdir()

    with pytest.raises(CaracalError) as exc:
        open_bundle(path)

    assert exc.value.code == "CDB-9004"


def test_storage_magic_is_crcl() -> None:
    assert MAGIC == b"CRCL\x00\x00\x00\x01"
