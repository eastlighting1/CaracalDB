from pathlib import Path

import pytest

from caracaldb.exec.as_of import apply_as_of, resolve_as_of
from caracaldb.exec.operator import ExecCtx
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import ast as ta
from caracaldb.lang.tuft import parse_tuft
from caracaldb.storage import create_bundle
from caracaldb.storage.snapshot import create_snapshot


def test_parser_captures_as_of_snapshot() -> None:
    program = parse_tuft("MATCH (g:Gene) AS_OF SNAPSHOT 'v1' RETURN g")
    stmt = program.statements[0]
    assert isinstance(stmt, ta.QueryStmt) and stmt.query is not None
    match = stmt.query.clauses[0]
    assert isinstance(match, ta.MatchClause) and match.as_of is not None
    # Transformer wraps the literal in ta.Literal — pull the underlying value out.
    assert "v1" in match.as_of.value


def test_resolve_as_of_pins_snapshot_id(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "b")
    create_snapshot(bundle, "v1")
    snap = resolve_as_of(bundle, ta.AsOf(kind="SNAPSHOT", value="v1"))
    assert snap is not None and snap.name == "v1"

    ctx = apply_as_of(ExecCtx(), snap)
    assert ctx.snapshot_id is not None and "v1" in ctx.snapshot_id
    assert ctx.metadata["snapshot_lsn"] == snap.lsn_high


def test_resolve_as_of_datetime_is_reserved(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "b")
    with pytest.raises(CaracalError) as exc:
        resolve_as_of(bundle, ta.AsOf(kind="DATETIME", value="2026-04-26T00:00:00Z"))
    assert exc.value.code == "CDB-6021"
