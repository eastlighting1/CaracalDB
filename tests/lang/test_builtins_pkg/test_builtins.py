import pyarrow as pa
import pytest

from caracaldb.lang.builtins import AGG_FUNCTIONS, GRAPH_FUNCTIONS, REGISTRY, SCALAR_FUNCTIONS
from caracaldb.lang.diagnostics import CaracalError


def test_scalar_abs_round_trip() -> None:
    arr = pa.array([-1, 2, -3])
    out = SCALAR_FUNCTIONS["abs"].dispatch([arr])
    assert out.to_pylist() == [1, 2, 3]


def test_scalar_starts_with() -> None:
    arr = pa.array(["foo", "bar", "foobar"])
    out = SCALAR_FUNCTIONS["starts_with"].dispatch([arr, pa.array(["foo"])])
    assert out.to_pylist() == [True, False, True]


def test_scalar_arity_violation_raises() -> None:
    fn = SCALAR_FUNCTIONS["abs"]
    with pytest.raises(CaracalError) as exc:
        fn.check_arity(2)
    assert exc.value.code == "CDB-6060"


def test_collection_size_and_head() -> None:
    lists = pa.array([[1, 2, 3], [9]])
    assert AGG_FUNCTIONS["size"].dispatch([lists]).to_pylist() == [3, 1]
    assert AGG_FUNCTIONS["head"].dispatch([lists]).to_pylist() == [1, 9]


def test_aggregate_kernels_are_recognised() -> None:
    for name in ("count", "sum", "avg", "min", "max", "collect"):
        assert AGG_FUNCTIONS[name].kind == "agg"


def test_graph_functions_registered_with_correct_arity() -> None:
    assert GRAPH_FUNCTIONS["degree"].kind == "graph"
    assert "neighbors" in GRAPH_FUNCTIONS
    # Variable arity is allowed (e.g. degree(node) or degree(node, type)).
    GRAPH_FUNCTIONS["degree"].check_arity(1)
    GRAPH_FUNCTIONS["degree"].check_arity(2)


def test_registry_merges_all_categories() -> None:
    for name in ("abs", "size", "degree", "count"):
        assert name in REGISTRY
