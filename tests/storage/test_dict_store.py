from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.dict_store import DictStore


def test_dict_store_basic_lookup_round_trip(tmp_path: Path) -> None:
    store = DictStore()
    mapping = store.merge(
        [
            "http://example.org/Gene",
            "http://example.org/Tissue",
            "http://example.org/Gene",  # dup
        ]
    )

    assert len(store) == 2
    assert mapping["http://example.org/Gene"] == store.id_of("http://example.org/Gene")
    assert "http://example.org/Tissue" in store
    assert store.id_of("missing") is None


def test_dict_store_persists_round_trip(tmp_path: Path) -> None:
    target = tmp_path / "iri.dict"
    store = DictStore()
    store.merge(["b", "a", "c"])
    store.write_atomic(target)

    reopened = DictStore.read(target)
    assert len(reopened) == 3
    # sorted order on disk: a, b, c
    assert reopened.str_of(0) == "a"
    assert reopened.str_of(1) == "b"
    assert reopened.str_of(2) == "c"
    assert reopened.id_of("c") == 2


def test_dict_store_merge_preserves_existing_and_returns_ids() -> None:
    store = DictStore()
    store.merge(["alpha"])
    second = store.merge(["beta", "alpha", "gamma"])
    assert set(second.keys()) == {"beta", "alpha", "gamma"}
    assert all(store.id_of(v) is not None for v in ["alpha", "beta", "gamma"])


def test_dict_store_id_out_of_range_raises() -> None:
    store = DictStore()
    store.merge(["x"])
    with pytest.raises(CaracalError) as exc:
        store.str_of(99)
    assert exc.value.code == "CDB-7041"


def test_dict_store_rejects_corrupted_file(tmp_path: Path) -> None:
    target = tmp_path / "iri.dict"
    DictStore().write_atomic(target)
    raw = target.read_bytes()
    # flip one byte in the body to invalidate the CRC
    corrupted = raw[:30] + bytes([raw[30] ^ 0xFF]) + raw[31:]
    target.write_bytes(corrupted)

    with pytest.raises(CaracalError) as exc:
        DictStore.read(target)
    assert exc.value.code == "CDB-7040"
