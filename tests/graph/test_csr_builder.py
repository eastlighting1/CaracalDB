from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from caracaldb.graph import CsrReader, build_csr, read_csr
from caracaldb.graph.csr_builder import build_csr_arrays
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store


def test_build_csr_arrays_offsets_and_neighbors() -> None:
    src = np.array([2, 0, 0, 1], dtype=np.uint64)
    dst = np.array([3, 1, 2, 4], dtype=np.uint64)
    eids = np.array([10, 11, 12, 13], dtype=np.uint64)
    offsets, neighbors, eid = build_csr_arrays(src, dst, eids, num_vertices=5)
    assert offsets.tolist() == [0, 2, 3, 4, 4, 4]
    # Stable sort preserves the relative order of dst within a src group.
    assert neighbors.tolist() == [1, 2, 4, 3]
    assert eid.tolist() == [11, 12, 13, 10]


def test_build_csr_round_trips_to_disk(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "g")
    store = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2], type=pa.uint64()),
                "dst": pa.array([1, 2, 2, 0], type=pa.uint64()),
            }
        )
    )
    out = tmp_path / "p.csr"
    result = build_csr(store, num_vertices=3, out_path=out, with_eids=True)
    assert result.num_vertices == 3
    assert result.num_edges == 4
    assert result.has_eids

    file = read_csr(out, mmap=False)
    assert file.offsets.tolist() == [0, 2, 3, 4]
    assert file.neighbors.tolist() == [1, 2, 2, 0]
    assert file.eids is not None
    assert file.eids.tolist() == [0, 1, 2, 3]


def test_csr_reader_neighbors_of(tmp_path: Path) -> None:
    out = tmp_path / "p.csr"
    src = np.array([0, 0, 1, 2], dtype=np.uint64)
    dst = np.array([1, 2, 2, 0], dtype=np.uint64)
    offs, nbrs, _ = build_csr_arrays(src, dst, None, 3)
    from caracaldb.graph.csr_format import write_csr

    write_csr(out, offsets=offs, neighbors=nbrs)
    r = CsrReader(out)
    assert r.neighbors_of(0).tolist() == [1, 2]
    assert r.neighbors_of(1).tolist() == [2]
    assert r.neighbors_of(2).tolist() == [0]


def test_csr_reader_rejects_out_of_range_vertex(tmp_path: Path) -> None:
    out = tmp_path / "p.csr"
    offs, nbrs, _ = build_csr_arrays(
        np.array([0], dtype=np.uint64), np.array([0], dtype=np.uint64), None, 1
    )
    from caracaldb.graph.csr_format import write_csr

    write_csr(out, offsets=offs, neighbors=nbrs)
    r = CsrReader(out)
    with pytest.raises(CaracalError) as exc:
        r.neighbors_of(5)
    assert exc.value.code == "CDB-7083"


def test_csr_format_rejects_corrupt_crc(tmp_path: Path) -> None:
    out = tmp_path / "p.csr"
    offs, nbrs, _ = build_csr_arrays(
        np.array([0, 1], dtype=np.uint64), np.array([1, 0], dtype=np.uint64), None, 2
    )
    from caracaldb.graph.csr_format import write_csr

    write_csr(out, offsets=offs, neighbors=nbrs)
    raw = out.read_bytes()
    # Flip a body byte (after CRCL header).
    corrupt = raw[:30] + bytes([raw[30] ^ 0xFF]) + raw[31:]
    out.write_bytes(corrupt)
    with pytest.raises(CaracalError) as exc:
        read_csr(out, mmap=False)
    assert exc.value.code == "CDB-7081"
