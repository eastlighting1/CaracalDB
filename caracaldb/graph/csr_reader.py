"""CSR reader with vectorised neighbour fan-out (mmap-only)."""

from __future__ import annotations

from pathlib import Path

import numpy as np

from caracaldb.graph.csr_format import CsrFile, read_csr
from caracaldb.lang.diagnostics import CaracalError


class CsrReader:
    """Read-only view over a ``.csr`` / ``.csc`` file.

    The reader holds NumPy memmaps of ``offsets``, ``neighbors``, and
    optionally ``eids``. Hot operators traverse it via ``batch_neighbors`` —
    a single call expands a seed batch to (src_repeat, dst_flat[, eid_flat])
    in pure NumPy without a Python for-loop.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._file: CsrFile = read_csr(self.path, mmap=True)

    @property
    def num_vertices(self) -> int:
        return self._file.num_vertices

    @property
    def num_edges(self) -> int:
        return self._file.num_edges

    @property
    def has_eids(self) -> bool:
        return self._file.eids is not None

    @property
    def offsets(self) -> np.ndarray:
        return self._file.offsets

    @property
    def neighbors(self) -> np.ndarray:
        return self._file.neighbors

    @property
    def eids(self) -> np.ndarray | None:
        return self._file.eids

    def neighbors_of(self, vid: int) -> np.ndarray:
        if vid < 0 or vid >= self._file.num_vertices:
            raise CaracalError(
                code="CDB-7083",
                message=f"vertex id out of range: {vid} (n={self._file.num_vertices})",
            )
        s = int(self._file.offsets[vid])
        e = int(self._file.offsets[vid + 1])
        return np.asarray(self._file.neighbors[s:e])

    def degrees(self, vids: np.ndarray | None = None) -> np.ndarray:
        offs = np.asarray(self._file.offsets)
        if vids is None:
            return (offs[1:] - offs[:-1]).astype(np.int64)
        v = vids.astype(np.int64)
        return (offs[v + 1] - offs[v]).astype(np.int64)

    def batch_neighbors(
        self,
        vids: np.ndarray,
        *,
        return_eids: bool = False,
        fanout: int | None = None,
        replace: bool = False,
        seed: int | np.random.Generator | None = None,
        strategy: str = "uniform",
    ) -> tuple[np.ndarray, np.ndarray] | tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Vectorised CSR fan-out.

        Returns ``(src_rep, dst_flat[, eid_flat])`` where ``src_rep`` repeats
        each input vid once per sampled neighbor so callers can join back to
        seeds. ``strategy="uniform"`` samples per source, ``"first"`` keeps
        the first neighbors in CSR order, and ``"all"`` ignores ``fanout``.
        """
        if strategy not in {"uniform", "first", "all"}:
            raise CaracalError(
                code="CDB-7083",
                message=f"unsupported CSR sampling strategy: {strategy!r}",
            )
        if fanout is not None and fanout < 0:
            raise CaracalError(code="CDB-7083", message="fanout must be >= 0")
        if vids.size == 0:
            empty = np.empty(0, dtype=np.uint64)
            return (empty, empty) if not return_eids else (empty, empty, empty)
        v = vids.astype(np.int64, copy=False)
        if v.min() < 0 or v.max() >= self._file.num_vertices:
            raise CaracalError(
                code="CDB-7083",
                message=(
                    f"vertex id range [{int(v.min())}, {int(v.max())}] outside CSR "
                    f"vertex space [0, {self._file.num_vertices})"
                ),
            )
        offs = np.asarray(self._file.offsets)
        starts = offs[v]
        ends = offs[v + 1]
        lens = (ends - starts).astype(np.int64)
        total = int(lens.sum())
        if total == 0:
            empty = np.empty(0, dtype=np.uint64)
            src_rep = np.repeat(vids.astype(np.uint64, copy=False), lens)
            return (src_rep, empty) if not return_eids else (src_rep, empty, empty)

        # Compute the flat index into neighbors[]: for each seed i with
        # length L_i, indices are starts[i] + [0, 1, ..., L_i-1]. Building
        # this without a Python loop uses the classic "repeat + arange" trick.
        cum_minus = np.repeat(np.concatenate(([0], np.cumsum(lens[:-1]))), lens)
        idx = np.repeat(starts.astype(np.int64), lens) + (np.arange(total) - cum_minus)
        dst = np.asarray(self._file.neighbors)[idx]
        src_rep = np.repeat(vids.astype(np.uint64, copy=False), lens)
        eid: np.ndarray | None = None
        if return_eids:
            if self._file.eids is None:
                raise CaracalError(
                    code="CDB-7083",
                    message="this CSR was written without eids; cannot return them",
                )
            eid = np.asarray(self._file.eids)[idx]

        effective_fanout = fanout
        if effective_fanout == 0:
            effective_fanout = None
        if strategy == "all":
            effective_fanout = None
        if effective_fanout is not None:
            dst = np.asarray(dst, dtype=np.uint64)
            if not replace and lens.max(initial=0) <= effective_fanout:
                # Sparse graph fast path: every requested segment already fits
                # inside the fanout, so sampling would only re-select all edges.
                pass
            else:
                src_rep, dst, eid = self._sample_segments(
                    src_rep,
                    dst,
                    eid,
                    effective_fanout,
                    replace=replace,
                    seed=seed,
                    strategy=strategy,
                )
        else:
            dst = np.asarray(dst, dtype=np.uint64)

        if not return_eids:
            return src_rep, dst
        assert eid is not None
        return src_rep, dst, np.asarray(eid, dtype=np.uint64)

    @staticmethod
    def _sample_segments(
        src_rep: np.ndarray,
        dst: np.ndarray,
        eid: np.ndarray | None,
        fanout: int,
        *,
        replace: bool,
        seed: int | np.random.Generator | None,
        strategy: str,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
        if src_rep.size == 0:
            return src_rep, dst, eid
        rng = seed if isinstance(seed, np.random.Generator) else np.random.default_rng(seed)
        change = np.concatenate(([True], src_rep[1:] != src_rep[:-1]))
        seg_start = np.flatnonzero(change)
        seg_end = np.concatenate((seg_start[1:], [src_rep.size]))
        keep_idx: list[np.ndarray] = []
        for start, end in zip(seg_start, seg_end, strict=True):
            size = int(end - start)
            if size <= fanout and not replace:
                chosen = np.arange(start, end, dtype=np.int64)
                keep_idx.append(chosen)
                continue
            elif strategy == "first":
                if replace and size < fanout:
                    base = np.arange(start, end, dtype=np.int64)
                    extra = np.resize(base, fanout - size)
                    chosen = np.concatenate((base, extra))
                else:
                    chosen = np.arange(start, start + fanout, dtype=np.int64)
            else:
                offsets = rng.choice(size, size=fanout, replace=replace)
                chosen = start + offsets.astype(np.int64, copy=False)
            keep_idx.append(chosen)
        idx = np.concatenate(keep_idx) if keep_idx else np.empty(0, dtype=np.int64)
        sampled_eid = None if eid is None else np.asarray(eid)[idx]
        return src_rep[idx], dst[idx], sampled_eid


__all__ = ["CsrReader"]
