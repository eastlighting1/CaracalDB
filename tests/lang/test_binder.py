import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.lang.tuft import bind_program, parse_tuft
from caracaldb.onto import Catalog, PropertyKind


def _catalog() -> Catalog:
    catalog = Catalog.empty(catalog_id="bio")
    catalog.register_class("https://example.test/bio/Gene")
    catalog.register_class("https://example.test/bio/Protein")
    catalog.register_property(
        "https://example.test/bio/interactsWith",
        kind=PropertyKind.OBJECT,
        domain_iris=("https://example.test/bio/Gene",),
        range_iris=("https://example.test/bio/Protein",),
    )
    return catalog


def test_binder_expands_prefixes_and_validates_pattern_refs() -> None:
    source = """
    PREFIX bio: <https://example.test/bio/>;
    MATCH (g:bio:Gene)-[:bio:interactsWith]->(p:bio:Protein)
    RETURN g, p;
    """

    bound = bind_program(parse_tuft(source), _catalog(), source_text=source)

    assert bound.prefixes["bio"] == "https://example.test/bio/"
    assert [item.iri for item in bound.classes] == [
        "https://example.test/bio/Gene",
        "https://example.test/bio/Protein",
    ]
    assert [item.iri for item in bound.properties] == ["https://example.test/bio/interactsWith"]


def test_binder_reports_undefined_prefix() -> None:
    source = "MATCH (g:bio:Gene) RETURN g;"

    with pytest.raises(CaracalError) as exc:
        bind_program(parse_tuft(source), _catalog(), source_text=source)

    assert exc.value.code == "TF-3001"


def test_binder_reports_unknown_class() -> None:
    source = """
    PREFIX bio: <https://example.test/bio/>;
    MATCH (g:bio:Missing) RETURN g;
    """

    with pytest.raises(CaracalError) as exc:
        bind_program(parse_tuft(source), _catalog(), source_text=source)

    assert exc.value.code == "TF-3004"


def test_binder_reports_unknown_property() -> None:
    source = """
    PREFIX bio: <https://example.test/bio/>;
    MATCH (g:bio:Gene)-[:bio:missingEdge]->(p:bio:Protein)
    RETURN g, p;
    """

    with pytest.raises(CaracalError) as exc:
        bind_program(parse_tuft(source), _catalog(), source_text=source)

    assert exc.value.code == "TF-3005"


def test_binder_accepts_default_prefix_from_context() -> None:
    source = "MATCH (g:Gene) RETURN g;"

    bound = bind_program(
        parse_tuft(source),
        _catalog(),
        prefixes={"": "https://example.test/bio/"},
        source_text=source,
    )

    assert [item.iri for item in bound.classes] == ["https://example.test/bio/Gene"]


def test_binder_validates_ddl_superclasses_and_property_domains() -> None:
    source = """
    PREFIX bio: <https://example.test/bio/>;
    CREATE CLASS bio:Protein SUBCLASSOF bio:Gene;
    CREATE PROPERTY bio:interactsWith TYPE OBJECT DOMAIN bio:Gene RANGE IRI;
    """

    bound = bind_program(parse_tuft(source), _catalog(), source_text=source)

    assert [item.iri for item in bound.classes] == [
        "https://example.test/bio/Gene",
        "https://example.test/bio/Gene",
    ]
