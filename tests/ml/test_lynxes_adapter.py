import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.ml import Subgraph
from caracaldb.ml.lynxes_adapter import to_graphframe


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
    assert hasattr(gf, "vertices") and hasattr(gf, "edges")
