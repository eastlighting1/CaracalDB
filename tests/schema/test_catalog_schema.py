import shutil
import subprocess
from pathlib import Path

import pytest

SCHEMA_PATH = Path("schema/catalog.fbs")


def test_catalog_schema_defines_required_root_and_tables() -> None:
    text = SCHEMA_PATH.read_text(encoding="utf-8")

    assert 'file_identifier "CRCL";' in text
    assert "root_type CatalogT;" in text
    for table_name in ["CatalogT", "ClassT", "PropertyT", "GraphT", "IndexT"]:
        assert f"table {table_name} " in text


def test_catalog_schema_defines_core_enums() -> None:
    text = SCHEMA_PATH.read_text(encoding="utf-8")

    for enum_name in [
        "TypeKind",
        "PropertyKind",
        "PropertyCharacteristic",
        "ConstraintKind",
        "IndexKind",
    ]:
        assert f"enum {enum_name} " in text


def test_catalog_schema_captures_property_hierarchy() -> None:
    text = SCHEMA_PATH.read_text(encoding="utf-8")

    assert "superclass_iris:[string];" in text
    assert "superproperty_iris:[string];" in text


def test_catalog_schema_compiles_with_flatc_when_available(tmp_path: Path) -> None:
    flatc = shutil.which("flatc")
    if flatc is None:
        pytest.skip("flatc is not installed")

    subprocess.run(
        [
            flatc,
            "--python",
            "--gen-object-api",
            "-o",
            str(tmp_path),
            str(SCHEMA_PATH),
        ],
        check=True,
    )
