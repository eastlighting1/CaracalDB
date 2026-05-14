import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml import (
    Subgraph,
    from_graphframe,
    to_graphframe,
)
from caracaldb.ml.lynxes_adapter import from_graphframe as adapter_from_graphframe
from caracaldb.ml.lynxes_adapter import to_graphframe as adapter_to_graphframe


def test_lynxes_adapters_are_exported_from_ml_package() -> None:
    assert to_graphframe is adapter_to_graphframe
    assert from_graphframe is adapter_from_graphframe


def test_lynxes_adapter_raises_when_missing() -> None:
    try:
        import lynxes  # noqa: F401
    except ImportError:
        with pytest.raises(CaracalError) as exc:
            to_graphframe(Subgraph())
        assert exc.value.code == "CDB-6113"


def test_lynxes_adapter_or_skip() -> None:
    pytest.importorskip("lynxes")
    sg = Subgraph()
    gf = to_graphframe(sg)
    assert hasattr(gf, "nodes") and hasattr(gf, "edges")
