import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto import Catalog, OntologyHierarchy, PropertyKind


def test_ontology_hierarchy_builds_class_and_property_dags() -> None:
    catalog = Catalog.empty()
    catalog.register_class("bio:Entity")
    catalog.register_class("bio:BiologicalEntity", superclass_iris=("bio:Entity",))
    catalog.register_class("bio:Gene", superclass_iris=("bio:BiologicalEntity",))
    catalog.register_property("bio:relatedTo", kind=PropertyKind.OBJECT)
    catalog.register_property(
        "bio:interactsWith",
        kind=PropertyKind.OBJECT,
        superproperty_iris=("bio:relatedTo",),
    )

    hierarchy = OntologyHierarchy.from_catalog(catalog)

    assert hierarchy.classes.topological_order == (
        "bio:Entity",
        "bio:BiologicalEntity",
        "bio:Gene",
    )
    assert hierarchy.classes.parents("bio:Gene") == ("bio:BiologicalEntity",)
    assert hierarchy.classes.ancestors("bio:Gene") == ("bio:BiologicalEntity", "bio:Entity")
    assert hierarchy.classes.descendants("bio:Entity") == ("bio:BiologicalEntity", "bio:Gene")
    assert hierarchy.classes.is_subtype("bio:Gene", "bio:Entity")
    assert hierarchy.properties.is_subtype("bio:interactsWith", "bio:relatedTo")


def test_ontology_hierarchy_rejects_unknown_parent() -> None:
    catalog = Catalog.empty()
    catalog.register_class("bio:Gene", superclass_iris=("bio:Missing",))

    with pytest.raises(CaracalError) as exc:
        OntologyHierarchy.from_catalog(catalog)

    assert exc.value.code == "CDB-9504"


def test_ontology_hierarchy_rejects_cycles() -> None:
    catalog = Catalog.empty()
    catalog.register_class("bio:A", superclass_iris=("bio:B",))
    catalog.register_class("bio:B", superclass_iris=("bio:A",))

    with pytest.raises(CaracalError) as exc:
        OntologyHierarchy.from_catalog(catalog)

    assert exc.value.code == "CDB-9505"


def test_ontology_hierarchy_rejects_unknown_lookup_node() -> None:
    catalog = Catalog.empty()
    catalog.register_class("bio:Gene")
    hierarchy = OntologyHierarchy.from_catalog(catalog)

    with pytest.raises(CaracalError) as exc:
        hierarchy.classes.parents("bio:Missing")

    assert exc.value.code == "CDB-9504"
