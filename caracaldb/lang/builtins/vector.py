"""Vector / ML built-ins (01 §8.10).

The functions here operate on float32 vectors expressed as Arrow
``FixedSizeList<float32, dim>`` or plain ``ListArray``. Cross-vector
operations (``similarity``, ``vec_normalize``) are implemented in pure NumPy
on the per-row payload; the registry is shared with the binder so callers
can resolve names without binding directly to ``hnswlib``.
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa

from caracaldb.lang.builtins.scalar import BuiltinFn
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.vector import cosine_distance, cosine_similarity, dot_product, l2_distance


def _as_matrix(arr: pa.Array) -> np.ndarray:
    if isinstance(arr.type, pa.lib.FixedSizeListType):
        flat = arr.values.to_numpy(zero_copy_only=False).astype(np.float32, copy=False)
        return flat.reshape(len(arr), arr.type.list_size)
    if isinstance(arr.type, pa.lib.ListType):
        rows = arr.to_pylist()
        if not rows:
            return np.empty((0, 0), dtype=np.float32)
        dim = len(rows[0])
        for row in rows:
            if len(row) != dim:
                raise CaracalError(
                    code="CDB-6061", message="similarity() requires vectors of equal dimension"
                )
        return np.asarray(rows, dtype=np.float32)
    raise CaracalError(
        code="CDB-6061", message=f"vector function got non-vector input type: {arr.type}"
    )


def _to_arrow(matrix: np.ndarray) -> pa.Array:
    if matrix.ndim != 2:
        raise CaracalError(code="CDB-6061", message="expected 2-D matrix")
    dim = matrix.shape[1]
    flat = matrix.astype(np.float32, copy=False).reshape(-1)
    return pa.FixedSizeListArray.from_arrays(pa.array(flat, type=pa.float32()), dim)


def _similarity(args):
    a = _as_matrix(args[0])
    b = _as_matrix(args[1])
    if a.shape != b.shape:
        raise CaracalError(
            code="CDB-6061",
            message=f"similarity() shape mismatch: {a.shape} vs {b.shape}",
        )
    norm_a = np.linalg.norm(a, axis=1) + 1e-12
    norm_b = np.linalg.norm(b, axis=1) + 1e-12
    cos = np.einsum("ij,ij->i", a, b) / (norm_a * norm_b)
    return pa.array(cos.astype(np.float32), type=pa.float32())


def _binary_vector_scalar(args, fn):
    a = _as_matrix(args[0])
    b = _as_matrix(args[1])
    if a.shape != b.shape:
        raise CaracalError(
            code="CDB-6061",
            message=f"vector function shape mismatch: {a.shape} vs {b.shape}",
        )
    values = [fn(left, right) for left, right in zip(a, b, strict=True)]
    return pa.array(values, type=pa.float32())


def _cosine_similarity(args):
    return _binary_vector_scalar(args, cosine_similarity)


def _cosine_distance(args):
    return _binary_vector_scalar(args, cosine_distance)


def _dot_product(args):
    return _binary_vector_scalar(args, dot_product)


def _l2_distance(args):
    return _binary_vector_scalar(args, l2_distance)


def _vec_norm(args):
    m = _as_matrix(args[0])
    return pa.array(np.linalg.norm(m, axis=1).astype(np.float32), type=pa.float32())


def _vec_normalize(args):
    m = _as_matrix(args[0])
    norms = np.linalg.norm(m, axis=1, keepdims=True) + 1e-12
    return _to_arrow(m / norms)


def _top_k_unsupported(_args):  # pragma: no cover
    raise NotImplementedError("top_k() must be lowered to KnnOperator at plan time")


def _make(name, arity, fn) -> BuiltinFn:
    return BuiltinFn(name=name, arity=arity, kind="scalar", dispatch=fn)


VECTOR_FUNCTIONS: dict[str, BuiltinFn] = {
    fn.name: fn
    for fn in [
        _make("similarity", 2, _similarity),
        _make("cosine_similarity", 2, _cosine_similarity),
        _make("cosine_distance", 2, _cosine_distance),
        _make("dot_product", 2, _dot_product),
        _make("l2_distance", 2, _l2_distance),
        _make("vec_norm", 1, _vec_norm),
        _make("vec_normalize", 1, _vec_normalize),
        BuiltinFn(name="top_k", arity=(2, 3), kind="vector", dispatch=_top_k_unsupported),
    ]
}


__all__ = ["VECTOR_FUNCTIONS"]
