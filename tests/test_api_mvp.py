from pathlib import Path

import pyarrow as pa
import pytest

import caracaldb as cdb
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.catalog import save_catalog
from caracaldb.storage import create_bundle
from caracaldb.storage.edge_store import list_edge_stores, open_edge_store
from caracaldb.storage.node_store import open_node_store


def _seed_bundle(tmp_path: Path) -> Path:
    bundle_path = tmp_path / "bio"
    bundle = create_bundle(bundle_path)
    catalog = bundle and __import__("caracaldb.onto.catalog", fromlist=["Catalog"]).Catalog.empty()
    catalog.register_class(iri="http://example.org/Gene", local_name="Gene")
    save_catalog(bundle, catalog)

    store = open_node_store(
        bundle, class_iri="http://example.org/Gene", local_name="Gene", create=True
    )
    store.append(
        pa.record_batch(
            {
                "symbol": pa.array(["TP53", "MDM2", "BRCA1", "EGFR"]),
                "chromosome": pa.array(["17", "12", "17", "7"]),
            }
        )
    )
    return bundle_path


def test_connect_and_select_returns_arrow(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    db = cdb.connect(bundle_path, format="bundle")
    conn = db.cursor()
    table = conn.sql("MATCH (g:Gene) RETURN g.symbol").arrow()
    assert table.column_names == ["symbol"]
    assert table["symbol"].to_pylist() == ["TP53", "MDM2", "BRCA1", "EGFR"]


def test_where_filter_applies(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    conn = cdb.connect(bundle_path, format="bundle").cursor()
    table = conn.sql("MATCH (g:Gene) WHERE g.chromosome = '17' RETURN g.symbol").arrow()
    assert sorted(table["symbol"].to_pylist()) == ["BRCA1", "TP53"]


def test_limit_clips_result(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    conn = cdb.connect(bundle_path, format="bundle").cursor()
    table = conn.sql("MATCH (g:Gene) RETURN g.symbol LIMIT 2").arrow()
    assert table.num_rows == 2


def test_unknown_class_raises(tmp_path: Path) -> None:
    bundle_path = _seed_bundle(tmp_path)
    conn = cdb.connect(bundle_path, format="bundle").cursor()
    with pytest.raises(CaracalError) as exc:
        conn.sql("MATCH (x:Unknown) RETURN x.foo").arrow()
    assert exc.value.code in {"CDB-6021", "TF-3004"}


def test_database_convenience_api_inserts_and_queries_packed(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "quick") as db:
        db.define_class("Gene")
        db.insert_nodes("Gene", [{"symbol": "TP53", "chromosome": "17"}])

        result = db.sql("MATCH (g:Gene) RETURN g.symbol")

    assert result.rows() == [{"symbol": "TP53"}]
    assert (tmp_path / "quick.crcl").is_file()


def test_database_exec_supports_quickstart_shape(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "exec") as db:
        db.exec("""
            CREATE CLASS Gene;
            INSERT Gene { symbol: 'TP53', chromosome: '17' };
            """)

        rows = db.sql("MATCH (g:Gene) RETURN g.symbol").rows()

    assert rows == [{"symbol": "TP53"}]


def test_database_sql_supports_subclassof_star(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "ontology") as db:
        db.define_class("Gene", iri="http://example.org/Gene")
        db.define_class(
            "ProteinCodingGene",
            iri="http://example.org/ProteinCodingGene",
            superclass_iris=("http://example.org/Gene",),
        )
        db.insert_nodes(
            "ProteinCodingGene",
            [
                {"symbol": "TP53", "chromosome": "17"},
                {"symbol": "BRCA1", "chromosome": "17"},
            ],
        )

        rows = db.sql("""
            MATCH (g:ProteinCodingGene)
            WHERE g.class SUBCLASSOF* <http://example.org/Gene>
            RETURN g.symbol
            """).rows()

    assert rows == [{"symbol": "TP53"}, {"symbol": "BRCA1"}]


def test_database_sql_subclassof_star_allows_additional_filters(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "ontology-filter") as db:
        db.define_class("Gene", iri="http://example.org/Gene")
        db.define_class(
            "ProteinCodingGene",
            iri="http://example.org/ProteinCodingGene",
            superclass_iris=("http://example.org/Gene",),
        )
        db.insert_nodes(
            "ProteinCodingGene",
            [
                {"symbol": "TP53", "chromosome": "17"},
                {"symbol": "EGFR", "chromosome": "7"},
            ],
        )

        rows = db.sql("""
            MATCH (g:ProteinCodingGene)
            WHERE g.class SUBCLASSOF* <http://example.org/Gene>
              AND g.chromosome = '17'
            RETURN g.symbol
            """).rows()

    assert rows == [{"symbol": "TP53"}]


def test_define_class_merges_superclass_for_existing_class(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "ontology-upgrade") as db:
        db.define_class("Gene", iri="http://example.org/Gene")
        db.define_class("ProteinCodingGene", iri="http://example.org/ProteinCodingGene")
        db.insert_nodes("ProteinCodingGene", [{"symbol": "TP53"}])

    with cdb.connect(tmp_path / "ontology-upgrade") as db:
        db.define_class(
            "ProteinCodingGene",
            iri="http://example.org/ProteinCodingGene",
            superclass_iris=("http://example.org/Gene",),
        )

        rows = db.sql("""
            MATCH (g:ProteinCodingGene)
            WHERE g.class SUBCLASSOF* <http://example.org/Gene>
            RETURN g.symbol
            """).rows()

    assert rows == [{"symbol": "TP53"}]


def test_insert_node_table_groups_rows_by_type_and_preserves_node_id(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "typed-nodes") as db:
        db.insert_node_table(
            [
                {
                    "node_id": 0,
                    "type": "User",
                    "name": "Grandmaster_Ayasha_R",
                    "rank_points": 49908.0,
                },
                {
                    "node_id": 1,
                    "type": "User",
                    "name": "Grandmaster_Lucas_W",
                    "rank_points": 69138.0,
                },
                {
                    "node_id": 4691,
                    "type": "Competition",
                    "name": "Spring Open",
                    "rank_points": None,
                },
            ]
        )

        rows = db.sql("MATCH (u:User) RETURN u.node_id, u.name").rows()

    assert rows == [
        {"node_id": 0, "name": "Grandmaster_Ayasha_R"},
        {"node_id": 1, "name": "Grandmaster_Lucas_W"},
    ]


def test_insert_edge_table_resolves_external_node_ids(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "typed-edges") as db:
        db.insert_node_table(
            [
                {"node_id": 0, "type": "User", "name": "Grandmaster_Ayasha_R"},
                {"node_id": 4691, "type": "Competition", "name": "Spring Open"},
            ]
        )

        db.insert_edge_table(
            [
                {"src": 0, "dst": 4691, "type": "HOSTED", "weight": 1.0},
                {"src": 0, "dst": 4691, "type": "PLAYED", "weight": 2.0},
            ]
        )

        assert list_edge_stores(db.bundle) == ["HOSTED", "PLAYED"]
        store = open_edge_store(
            db.bundle,
            property_iri="caracaldb:local:HOSTED",
            local_name="HOSTED",
        )
        table = store.to_table()

    assert table["src"].to_pylist() == [0]
    assert table["dst"].to_pylist() == [1]
    assert table["type"].to_pylist() == ["HOSTED"]
    assert table["weight"].to_pylist() == [1.0]


def test_insert_edge_table_rejects_unknown_external_node_id(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "typed-edge-missing") as db:
        db.insert_node_table([{"node_id": 0, "type": "User", "name": "Grandmaster_Ayasha_R"}])

        with pytest.raises(CaracalError) as exc:
            db.insert_edge_table([{"src": 0, "dst": 999, "type": "HOSTED"}])

    assert exc.value.code == "CDB-7021"
    assert "unknown node_id" in exc.value.message


def test_import_resource_accepts_neo4j_json_and_creates_placeholder_targets(
    tmp_path: Path,
) -> None:
    with cdb.connect(tmp_path / "neo4j-resource", format="bundle") as db:
        db.import_resource(
            {
                "id": "employee/E12345",
                "labels": ["Employee"],
                "properties": {
                    "name": "Lukas Hoffman",
                    "email": "lukas@company.com",
                    "riskScore": 0.72,
                },
                "relationships": {
                    "worksOn": "project/P9",
                    "hasAccess": "system/customer-data-lake",
                },
            }
        )

        rows = db.sql("MATCH (e:Employee) RETURN e.name, e.riskScore").rows()
        project = db.sql("MATCH (p:Project) RETURN p.node_id").rows()
        ref = db.resource("employee/E12345")

    assert rows == [{"name": "Lukas Hoffman", "riskScore": 0.72}]
    assert project == [{"node_id": "project/P9"}]
    assert sorted(list_edge_stores(db.bundle)) == ["hasAccess", "worksOn"]
    assert ref.external_id == "employee/E12345"
    assert ref.display_iri == "caracaldb://resource/employee/E12345"


def test_import_resource_accepts_iri_resource_and_preserves_iri(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "iri-resource", format="bundle") as db:
        db.import_resource(
            {
                "@id": "https://ontology.company.com/employee/E12345",
                "type": "Employee",
                "name": "Lukas Hoffman",
            }
        )

        rows = db.sql("MATCH (e:Employee) RETURN e.node_id, e._iri, e.name").rows()
        ref = db.resource("https://ontology.company.com/employee/E12345")

    assert rows == [
        {
            "node_id": "https://ontology.company.com/employee/E12345",
            "_iri": "https://ontology.company.com/employee/E12345",
            "name": "Lukas Hoffman",
        }
    ]
    assert ref.iri == "https://ontology.company.com/employee/E12345"


def test_insert_triples_maps_type_literal_and_resource_edges(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "triples", format="bundle") as db:
        db.insert_triples(
            [
                {
                    "subject": "employee/E12345",
                    "predicate": "rdf:type",
                    "object": "Employee",
                },
                {"subject": "employee/E12345", "predicate": "name", "object": "Lukas Hoffman"},
                {"subject": "employee/E12345", "predicate": "worksOn", "object": "project/P9"},
            ]
        )

        rows = db.sql("MATCH (e:Employee) RETURN e.node_id, e.name").rows()
        project = db.sql("MATCH (p:Project) RETURN p.node_id").rows()
        store = open_edge_store(
            db.bundle,
            property_iri="caracaldb:local:worksOn",
            local_name="worksOn",
        )
        edges = store.to_table().to_pylist()

    assert rows == [{"node_id": "employee/E12345", "name": "Lukas Hoffman"}]
    assert project == [{"node_id": "project/P9"}]
    assert edges[0]["src"] == 0
    assert edges[0]["dst"] == 1


def test_import_resource_shape_detection_and_turtle_export(tmp_path: Path) -> None:
    with cdb.connect(tmp_path / "resource-export", format="bundle") as db:
        db.define_class("Employee", iri="https://ontology.company.com/Employee")
        db._define_property("worksOn", iri="https://ontology.company.com/worksOn")
        db.import_resource({"node_id": "project/P9", "type": "Project", "name": "Risk Model"})
        db.import_resource(
            {
                "id": "employee/E12345",
                "labels": ["Employee"],
                "properties": {"name": "Lukas Hoffman"},
                "relationships": {"worksOn": "project/P9"},
            }
        )

        turtle = db.export_resource_turtle("employee/E12345")

    assert "<caracaldb://resource/employee/E12345>" in turtle
    assert "<https://ontology.company.com/Employee>" in turtle
    assert "<https://ontology.company.com/worksOn>" in turtle
    assert "<caracaldb://resource/project/P9>" in turtle


def test_import_resource_rejects_unknown_shape(tmp_path: Path) -> None:
    with (
        cdb.connect(tmp_path / "unknown-resource", format="bundle") as db,
        pytest.raises(CaracalError) as exc,
    ):
        db.import_resource({"name": "Lukas Hoffman"})

    assert exc.value.code == "CDB-7010"
    assert "unsupported resource shape" in exc.value.message
