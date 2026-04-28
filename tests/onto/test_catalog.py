from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto import (
    Catalog,
    FieldDef,
    IndexKind,
    PropertyKind,
    TypeKind,
    TypeRef,
    load_catalog,
    save_catalog,
)
from caracaldb.storage import MAGIC, create_bundle


def test_catalog_registers_classes_properties_graphs_and_indexes() -> None:
    catalog = Catalog.empty(catalog_id="bio")
    string_type = TypeRef(TypeKind.STRING, nullable=False)

    gene = catalog.register_class(
        "bio:Gene",
        fields=(FieldDef("symbol", string_type),),
        superclass_iris=("bio:BiologicalEntity",),
    )
    interacts = catalog.register_property(
        "bio:interactsWith",
        kind=PropertyKind.OBJECT,
        superproperty_iris=("bio:relatedTo",),
        domain_iris=("bio:Gene",),
        range_iris=("bio:Gene",),
    )
    graph = catalog.register_graph(
        "bio",
        class_iris=(gene.iri,),
        property_iris=(interacts.iri,),
    )
    index = catalog.register_index(
        "gene_symbol",
        kind=IndexKind.BTREE,
        target_class_iri=gene.iri,
        fields=("symbol",),
        unique=True,
    )

    assert gene.cid == 1
    assert gene.local_name == "Gene"
    assert catalog.class_by_iri("bio:Gene") == gene
    assert catalog.property_by_iri("bio:interactsWith") == interacts
    assert interacts.superproperty_iris == ("bio:relatedTo",)
    assert catalog.graph_by_name("bio") == graph
    assert index.iid == 1


def test_catalog_round_trips_through_bytes() -> None:
    catalog = Catalog.empty(catalog_id="bio")
    catalog.register_class("bio:Gene")

    payload = catalog.to_bytes()
    restored = Catalog.from_bytes(payload)

    assert payload.startswith(MAGIC)
    assert restored.catalog_id == "bio"
    assert restored.class_by_iri("bio:Gene") is not None


def test_catalog_persists_at_bundle_manifest_path(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "graph")
    catalog = load_catalog(bundle)
    catalog.register_class("bio:Gene")

    path = save_catalog(bundle, catalog)
    restored = load_catalog(bundle)

    assert path == bundle.child(bundle.manifest.catalog_file)
    assert restored.class_by_iri("bio:Gene") is not None


def test_catalog_rejects_duplicate_class_iri() -> None:
    catalog = Catalog.empty()
    catalog.register_class("bio:Gene")

    with pytest.raises(CaracalError) as exc:
        catalog.register_class("bio:Gene")

    assert exc.value.code == "CDB-9503"
