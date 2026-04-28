"""Tests for the packed-default ``connect()`` workflow.

Validates that ``cdb.connect()`` creates packed single-file databases by
default and transparently handles pack/unpack on open/close.
"""

from pathlib import Path

import pyarrow as pa

import caracaldb as cdb
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage.node_store import open_node_store
from caracaldb.storage.pack import is_packed


def _seed_packed(tmp_path: Path, name: str = "bio") -> Path:
    """Create a packed database with some Gene data via context manager."""
    with cdb.connect(tmp_path / name) as db:
        catalog = Catalog.empty()
        catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
        save_catalog(db.bundle, catalog)
        store = open_node_store(
            db.bundle,
            class_iri="http://example.org/Gene",
            local_name="Gene",
            create=True,
        )
        store.append(
            pa.record_batch(
                {
                    "symbol": pa.array(["TP53", "MDM2"]),
                    "chromosome": pa.array(["17", "12"]),
                }
            )
        )
    return tmp_path / f"{name}.crcl"


# ---------------------------------------------------------------------------
# Default format (packed)
# ---------------------------------------------------------------------------


def test_connect_new_creates_packed_file(tmp_path: Path) -> None:
    """New ``connect()`` without format arg creates a packed single file."""
    with cdb.connect(tmp_path / "mydb") as db:
        assert db._packed_source is not None

    packed_path = tmp_path / "mydb.crcl"
    assert packed_path.is_file()
    assert is_packed(packed_path)


def test_packed_roundtrip_preserves_data(tmp_path: Path) -> None:
    """Data written in one session is readable in the next."""
    packed_path = _seed_packed(tmp_path, "rt")

    assert packed_path.is_file()
    assert is_packed(packed_path)

    with cdb.connect(tmp_path / "rt") as db:
        conn = db.cursor()
        table = conn.sql("MATCH (g:Gene) RETURN g.symbol").arrow()
        assert set(table["symbol"].to_pylist()) == {"TP53", "MDM2"}


def test_context_manager_repacks_on_exit(tmp_path: Path) -> None:
    packed_path = tmp_path / "ctx.crcl"

    with cdb.connect(tmp_path / "ctx") as db:
        # During the context, the packed file does not yet exist
        # (it's being worked on in a temp dir).
        assert not packed_path.is_file()

    # After exiting the context, the packed file should exist.
    assert packed_path.is_file()
    assert is_packed(packed_path)


def test_close_without_context_manager(tmp_path: Path) -> None:
    db = cdb.connect(tmp_path / "manual")
    db.close()

    packed_path = tmp_path / "manual.crcl"
    assert packed_path.is_file()
    assert is_packed(packed_path)


def test_double_close_is_safe(tmp_path: Path) -> None:
    db = cdb.connect(tmp_path / "dbl")
    db.close()
    db.close()  # should not raise

    assert (tmp_path / "dbl.crcl").is_file()


# ---------------------------------------------------------------------------
# Explicit format="bundle"
# ---------------------------------------------------------------------------


def test_connect_bundle_format_creates_directory(tmp_path: Path) -> None:
    db = cdb.connect(tmp_path / "bnd", format="bundle")
    assert db._packed_source is None
    assert (tmp_path / "bnd.crcl").is_dir()


# ---------------------------------------------------------------------------
# Auto-detect existing
# ---------------------------------------------------------------------------


def test_auto_detect_existing_directory(tmp_path: Path) -> None:
    """Auto mode opens an existing directory bundle without packing."""
    cdb.connect(tmp_path / "dir_test", format="bundle")
    assert (tmp_path / "dir_test.crcl").is_dir()

    # Re-open with auto — should detect the directory.
    db = cdb.connect(tmp_path / "dir_test")
    assert db._packed_source is None  # no packed source, just a plain bundle


def test_auto_detect_existing_packed_file(tmp_path: Path) -> None:
    """Auto mode opens an existing packed file transparently."""
    packed_path = _seed_packed(tmp_path, "pk")

    with cdb.connect(tmp_path / "pk") as db:
        assert db._packed_source == packed_path
        conn = db.cursor()
        table = conn.sql("MATCH (g:Gene) RETURN g.symbol").arrow()
        assert table.num_rows == 2
