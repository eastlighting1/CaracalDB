from pathlib import Path

import numpy as np
import pyarrow as pa

from caracaldb.graph import CsrReader, build_csc, build_csr
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import open_edge_store


def _seed(tmp_path: Path) -> tuple[Path, Path]:
    bundle = create_bundle(tmp_path / "g")
    store = open_edge_store(bundle, property_iri="http://x/p", local_name="p", create=True)
    store.append(
        pa.record_batch(
            {
                "src": pa.array([0, 0, 1, 2, 2], type=pa.uint64()),
                "dst": pa.array([1, 2, 2, 0, 1], type=pa.uint64()),
            }
        )
    )
    fwd = tmp_path / "p.csr"
    rev = tmp_path / "p.csc"
    build_csr(store, num_vertices=3, out_path=fwd, with_eids=True)
    build_csc(store, num_vertices=3, out_path=rev, with_eids=True)
    return fwd, rev


def test_csc_is_inverse_of_csr(tmp_path: Path) -> None:
    fwd_path, rev_path = _seed(tmp_path)
    fwd = CsrReader(fwd_path)
    rev = CsrReader(rev_path)

    # All edges round-trip in either direction.
    out_edges = set()
    for v in range(fwd.num_vertices):
        for n in fwd.neighbors_of(v).tolist():
            out_edges.add((v, int(n)))
    in_edges = set()
    for v in range(rev.num_vertices):
        for n in rev.neighbors_of(v).tolist():
            in_edges.add((int(n), v))  # rev stores in-edges keyed by dst
    assert out_edges == in_edges


def test_csr_reader_batch_neighbors_vectorised(tmp_path: Path) -> None:
    fwd_path, _ = _seed(tmp_path)
    r = CsrReader(fwd_path)
    seeds = np.array([0, 2], dtype=np.uint64)
    src_rep, dst = r.batch_neighbors(seeds)
    assert src_rep.tolist() == [0, 0, 2, 2]
    assert dst.tolist() == [1, 2, 0, 1]


def test_csr_reader_batch_neighbors_with_eids(tmp_path: Path) -> None:
    fwd_path, _ = _seed(tmp_path)
    r = CsrReader(fwd_path)
    seeds = np.array([0, 1], dtype=np.uint64)
    src_rep, dst, eid = r.batch_neighbors(seeds, return_eids=True)
    assert src_rep.tolist() == [0, 0, 1]
    # dst order matches stable sort within each source group; eids align by the same permutation
    assert len(eid) == 3


def test_csr_reader_handles_empty_seeds(tmp_path: Path) -> None:
    fwd_path, _ = _seed(tmp_path)
    r = CsrReader(fwd_path)
    src_rep, dst = r.batch_neighbors(np.empty(0, dtype=np.uint64))
    assert src_rep.size == 0 and dst.size == 0


def test_csr_reader_degrees(tmp_path: Path) -> None:
    fwd_path, _ = _seed(tmp_path)
    r = CsrReader(fwd_path)
    assert r.degrees().tolist() == [2, 1, 2]
