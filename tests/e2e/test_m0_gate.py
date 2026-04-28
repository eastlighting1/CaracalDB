from pathlib import Path

from caracaldb.lang.tuft import parse_tuft
from caracaldb.lang.tuft.ast import MatchClause, QueryStmt
from caracaldb.storage import create_bundle, open_bundle


def test_m0_gate_parses_tuft_sample_and_reopens_empty_bundle(tmp_path: Path) -> None:
    source = """
    MATCH (g:Gene {symbol:'TP53'})
    WHERE g.symbol = 'TP53'
    RETURN g.symbol
    LIMIT 10;
    """

    program = parse_tuft(source)
    assert len(program.statements) == 1
    assert isinstance(program.statements[0], QueryStmt)
    assert program.statements[0].query is not None
    assert isinstance(program.statements[0].query.clauses[0], MatchClause)

    created = create_bundle(tmp_path / "m0_graph")
    reopened = open_bundle(created.path)

    assert reopened.path == created.path
    assert reopened.manifest.format_version == 1
    assert reopened.manifest.catalog_file == "catalog.fb"
