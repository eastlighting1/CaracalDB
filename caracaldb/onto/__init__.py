"""Ontology and catalog services for CaracalDB."""

from caracaldb.onto.catalog import (
    Catalog,
    ClassDef,
    FieldDef,
    GraphDef,
    IndexDef,
    IndexKind,
    PropertyDef,
    PropertyKind,
    TypeKind,
    TypeRef,
    load_catalog,
    save_catalog,
)
from caracaldb.onto.closure import (
    CLASS_CLOSURE_FILE,
    CLOSURE_ENCODING,
    ClassClosureIndex,
    load_class_closure,
    save_class_closure,
)
from caracaldb.onto.hierarchy import HierarchyDAG, OntologyHierarchy

__all__ = [
    "CLASS_CLOSURE_FILE",
    "CLOSURE_ENCODING",
    "Catalog",
    "ClassDef",
    "ClassClosureIndex",
    "FieldDef",
    "GraphDef",
    "HierarchyDAG",
    "IndexDef",
    "IndexKind",
    "OntologyHierarchy",
    "PropertyDef",
    "PropertyKind",
    "TypeKind",
    "TypeRef",
    "load_class_closure",
    "load_catalog",
    "save_class_closure",
    "save_catalog",
]
