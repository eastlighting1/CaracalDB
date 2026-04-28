from pathlib import Path

import pytest

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto import (
    Catalog,
    ClassClosureIndex,
    load_class_closure,
    save_class_closure,
)
from caracaldb.storage import create_bundle


def _catalog() -> Catalog:
    catalog = Catalog.empty(catalog_id="bio")
    catalog.register_class("bio:Entity")
    catalog.register_class("bio:BiologicalEntity", superclass_iris=("bio:Entity",))
    catalog.register_class("bio:Gene", superclass_iris=("bio:BiologicalEntity",))
    catalog.register_class("bio:Protein", superclass_iris=("bio:BiologicalEntity",))
    return catalog


def test_class_closure_materializes_descendant_and_ancestor_bitmaps() -> None:
    closure = ClassClosureIndex.from_catalog(_catalog())

    descendants = closure.descendants_bitmap("bio:BiologicalEntity")
    ancestors = closure.ancestors_bitmap("bio:Gene")

    assert set(descendants) == {2, 3, 4}
    assert set(ancestors) == {1, 2, 3}
    assert closure.descendant_iris("bio:BiologicalEntity") == (
        "bio:BiologicalEntity",
        "bio:Gene",
        "bio:Protein",
    )
    assert closure.ancestor_iris("bio:Gene") == ("bio:Entity", "bio:BiologicalEntity", "bio:Gene")
    assert closure.is_subclass("bio:Gene", "bio:Entity")
    assert not closure.is_subclass("bio:Entity", "bio:Gene", reflexive=False)


def test_class_closure_can_exclude_self_from_reflexive_queries() -> None:
    closure = ClassClosureIndex.from_catalog(_catalog())

    assert set(closure.descendants_bitmap("bio:BiologicalEntity", include_self=False)) == {3, 4}
    assert set(closure.ancestors_bitmap("bio:Gene", include_self=False)) == {1, 2}
    assert not closure.is_subclass("bio:Gene", "bio:Gene", reflexive=False)


def test_class_closure_persists_under_bundle_closure_directory(tmp_path: Path) -> None:
    bundle = create_bundle(tmp_path / "graph")
    catalog = _catalog()
    closure = ClassClosureIndex.from_catalog(catalog)

    path = save_class_closure(bundle, closure)
    restored = load_class_closure(bundle, catalog)

    assert path == bundle.child("closure", "subclassof.bitmap")
    assert restored.descendant_iris("bio:Entity") == (
        "bio:Entity",
        "bio:BiologicalEntity",
        "bio:Gene",
        "bio:Protein",
    )


def test_class_closure_rejects_stale_catalog_ids() -> None:
    catalog = _catalog()
    payload = ClassClosureIndex.from_catalog(catalog).to_bytes()
    changed = Catalog.empty(catalog_id="bio")
    changed.register_class("bio:Entity")

    with pytest.raises(CaracalError) as exc:
        ClassClosureIndex.from_bytes(payload, catalog=changed)

    assert exc.value.code == "CDB-9507"


def test_class_closure_rejects_unknown_class_lookup() -> None:
    closure = ClassClosureIndex.from_catalog(_catalog())

    with pytest.raises(CaracalError) as exc:
        closure.descendants_bitmap("bio:Missing")

    assert exc.value.code == "CDB-9504"
