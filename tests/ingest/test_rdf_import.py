"""Tests for the N-Triples → `.crcl` importer (ADR-0005).

The fixture below is a 4-triple snippet that exercises the four code paths
the importer has to get right:
- ``rdf:type`` triples set the subject's class
- IRI-object triples become edges
- literal-object triples become node properties
- subjects mentioned only as edge targets still materialise as nodes
"""

from __future__ import annotations

from pathlib import Path

import caracaldb as cdb
from caracaldb.ingest.rdf_import import (
    RDF_TYPE_IRI,
    import_ntriples,
    lower_to_tables,
    parse_ntriples,
)

SAMPLE_NT = """\
<http://x/tp53> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://x/Gene> .
<http://x/tp53> <http://x/symbol> "TP53" .
<http://x/tp53> <http://x/expressed_in> <http://x/liver> .
<http://x/liver> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://x/Tissue> .
"""


def test_parse_ntriples_classifies_iri_vs_literal() -> None:
    triples = list(parse_ntriples(SAMPLE_NT.splitlines()))
    # rdf:type, literal symbol, IRI expressed_in, rdf:type — 4 in total
    assert len(triples) == 4
    has_iri = [is_iri for _, _, _, _, is_iri in triples]
    assert has_iri == [True, False, True, True]
    # The literal triple decodes to the bare value, no quotes
    literal_row = next(t for t in triples if not t[4])
    assert literal_row[3] == "TP53"


def test_lower_to_tables_assigns_classes_and_collects_props() -> None:
    triples = parse_ntriples(SAMPLE_NT.splitlines())
    nodes, edges = lower_to_tables(triples)
    by_id = {n["node_id"]: n for n in nodes}
    assert by_id["http://x/tp53"]["type"] == "Gene"
    assert by_id["http://x/tp53"]["symbol"] == "TP53"
    assert by_id["http://x/liver"]["type"] == "Tissue"
    assert len(edges) == 1
    assert edges[0]["src"] == "http://x/tp53"
    assert edges[0]["dst"] == "http://x/liver"
    assert edges[0]["type"] == "expressed_in"


def test_import_ntriples_round_trips_through_database(tmp_path: Path) -> None:
    nt_file = tmp_path / "sample.nt"
    nt_file.write_text(SAMPLE_NT, encoding="utf-8")

    db = cdb.connect(tmp_path / "out")
    stats = import_ntriples(db, nt_file)
    assert stats.triples_kept == 4
    # Two distinct subjects → two nodes; one IRI-object triple → one edge.
    assert stats.nodes_emitted == 2
    assert stats.edges_emitted == 1

    rows = db.sql("MATCH (g:Gene) RETURN g.symbol").rows()
    assert sorted(r["symbol"] for r in rows) == ["TP53"]


def test_blank_nodes_are_skipped(tmp_path: Path) -> None:
    nt = (
        SAMPLE_NT
        + '_:b1 <http://x/symbol> "ignored" .\n'
        + "<http://x/tp53> <http://x/related> _:b1 .\n"
    )
    nt_file = tmp_path / "blank.nt"
    nt_file.write_text(nt, encoding="utf-8")

    db = cdb.connect(tmp_path / "out2")
    stats = import_ntriples(db, nt_file)
    # The two blank-involving triples are dropped; the original 4 stay.
    assert stats.triples_kept == 4
    assert stats.blank_nodes_skipped >= 2


def test_default_class_used_when_no_rdf_type(tmp_path: Path) -> None:
    nt = '<http://x/foo> <http://x/label> "untyped" .\n'
    nt_file = tmp_path / "untyped.nt"
    nt_file.write_text(nt, encoding="utf-8")

    db = cdb.connect(tmp_path / "out3")
    stats = import_ntriples(db, nt_file, default_class="Thing")
    assert stats.nodes_emitted == 1
    rows = db.sql("MATCH (t:Thing) RETURN t.label").rows()
    assert rows and rows[0]["label"] == "untyped"


def test_rdf_type_iri_is_the_w3c_constant() -> None:
    assert RDF_TYPE_IRI == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
