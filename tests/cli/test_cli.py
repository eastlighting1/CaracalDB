import json
from pathlib import Path

import pyarrow as pa

from caracaldb.cli.app import cmd_bench, cmd_explain, cmd_init, cmd_run
from caracaldb.onto.catalog import Catalog, save_catalog
from caracaldb.storage import create_bundle, open_bundle
from caracaldb.storage.node_store import open_node_store


def _seed(tmp_path: Path) -> Path:
    bundle = create_bundle(tmp_path / "bio")
    catalog = Catalog.empty()
    catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
    save_catalog(bundle, catalog)
    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2"]),
                "chromosome": pa.array(["17", "12"]),
            }
        )
    )
    return bundle.path


def test_cmd_init_creates_empty_bundle(tmp_path: Path) -> None:
    rc = cmd_init(tmp_path / "fresh")
    assert rc == 0
    assert (tmp_path / "fresh.crcl" / "MANIFEST").is_file()
    open_bundle(tmp_path / "fresh.crcl")  # opens cleanly


def test_cmd_run_executes_query_and_writes_json(tmp_path: Path) -> None:
    bundle_path = _seed(tmp_path)
    query_file = tmp_path / "q.tuft"
    query_file.write_text("MATCH (g:Gene) RETURN g.symbol")
    output = tmp_path / "out.json"
    rc = cmd_run(bundle_path, query_file, output)
    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert {row["symbol"] for row in payload} == {"TP53", "MDM2"}


def test_cmd_run_without_query_file_returns_error(tmp_path: Path) -> None:
    bundle_path = _seed(tmp_path)
    assert cmd_run(bundle_path, None, None) == 2


def test_cmd_explain_prints_tree(tmp_path: Path, capsys) -> None:
    bundle_path = _seed(tmp_path)
    rc = cmd_explain(bundle_path, "Gene")
    captured = capsys.readouterr()
    assert rc == 0
    assert "NodeScan" in captured.out
    assert "bundle=" in captured.out


def test_cmd_bench_unknown_scenario_returns_error(capsys) -> None:
    rc = cmd_bench("does_not_exist")
    captured = capsys.readouterr()
    assert rc == 2
    assert "unknown scenario" in captured.err
