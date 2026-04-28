"""Catalog model and loader for `.crcl` bundles.

The M1 catalog API mirrors `schema/catalog.fbs` while keeping the serialization
adapter isolated. Until generated FlatBuffers bindings are available in-tree,
the loader intentionally stores a deterministic JSON envelope under the
manifest's `catalog_file`; callers use the dataclass API rather than depending
on the temporary wire representation.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Any, TypeVar

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage import Bundle, open_bundle
from caracaldb.storage.header import FORMAT_VERSION, MAGIC
from caracaldb.storage.manifest import utc_now_iso

# Temporary storage adapter used before generated FlatBuffers bindings become
# part of the runtime path. Keep this value explicit so on-disk dev fixtures can
# be rejected cleanly when the FlatBuffers adapter replaces it.
CATALOG_ENCODING = "caracal.catalog.v1+json"
T = TypeVar("T")


class TypeKind(IntEnum):
    UNKNOWN = 0
    BOOL = 1
    INT8 = 2
    INT16 = 3
    INT32 = 4
    INT64 = 5
    UINT8 = 6
    UINT16 = 7
    UINT32 = 8
    UINT64 = 9
    FLOAT32 = 10
    FLOAT64 = 11
    DECIMAL = 12
    STRING = 13
    BYTES = 14
    DATE = 15
    TIME = 16
    DATETIME = 17
    DURATION = 18
    IRI = 19
    UUID = 20
    NODE = 21
    EDGE = 22
    TRIPLE = 23
    PATH = 24
    SUBGRAPH = 25
    CLASS = 26
    PROPERTY = 27
    EMBEDDING = 28
    ADJACENCY = 29
    LIST = 30
    MAP = 31
    STRUCT = 32
    UNION = 33
    VECTOR = 34
    MATRIX = 35


class PropertyKind(IntEnum):
    OBJECT = 0
    DATATYPE = 1


class PropertyCharacteristic(IntEnum):
    SYMMETRIC = 0
    TRANSITIVE = 1
    REFLEXIVE = 2
    INVERSE = 3
    FUNCTIONAL = 4


class ConstraintKind(IntEnum):
    REQUIRED = 0
    UNIQUE = 1
    CHECK = 2
    DEFAULT = 3


class IndexKind(IntEnum):
    CSR = 0
    CSC = 1
    HNSW = 2
    IVF = 3
    BTREE = 4
    HASH = 5
    BITMAP = 6
    ZONE_MAP = 7


class SortDirection(IntEnum):
    NONE = 0
    ASC = 1
    DESC = 2


@dataclass(frozen=True, slots=True)
class TypeRef:
    kind: TypeKind = TypeKind.UNKNOWN
    name: str | None = None
    params: tuple[TypeRef, ...] = ()
    int_params: tuple[int, ...] = ()
    precision: int = 0
    scale: int = 0
    nullable: bool = True

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> TypeRef:
        return cls(
            kind=TypeKind(int(value.get("kind", TypeKind.UNKNOWN))),
            name=_optional_str(value.get("name")),
            params=tuple(cls.from_dict(item) for item in value.get("params", [])),
            int_params=tuple(int(item) for item in value.get("int_params", [])),
            precision=int(value.get("precision", 0)),
            scale=int(value.get("scale", 0)),
            nullable=bool(value.get("nullable", True)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": int(self.kind),
            "name": self.name,
            "params": [item.to_dict() for item in self.params],
            "int_params": list(self.int_params),
            "precision": self.precision,
            "scale": self.scale,
            "nullable": self.nullable,
        }


@dataclass(frozen=True, slots=True)
class ConstraintDef:
    kind: ConstraintKind
    expr: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ConstraintDef:
        return cls(kind=ConstraintKind(int(value["kind"])), expr=_optional_str(value.get("expr")))

    def to_dict(self) -> dict[str, Any]:
        return {"kind": int(self.kind), "expr": self.expr}


@dataclass(frozen=True, slots=True)
class FieldDef:
    name: str
    type: TypeRef
    constraints: tuple[ConstraintDef, ...] = ()
    default_expr: str | None = None
    doc: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FieldDef:
        return cls(
            name=str(value["name"]),
            type=TypeRef.from_dict(value["type"]),
            constraints=tuple(
                ConstraintDef.from_dict(item) for item in value.get("constraints", [])
            ),
            default_expr=_optional_str(value.get("default_expr")),
            doc=_optional_str(value.get("doc")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type.to_dict(),
            "constraints": [item.to_dict() for item in self.constraints],
            "default_expr": self.default_expr,
            "doc": self.doc,
        }


@dataclass(frozen=True, slots=True)
class ClassDef:
    cid: int
    iri: str
    local_name: str | None = None
    superclass_iris: tuple[str, ...] = ()
    fields: tuple[FieldDef, ...] = ()
    doc: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ClassDef:
        return cls(
            cid=int(value["cid"]),
            iri=str(value["iri"]),
            local_name=_optional_str(value.get("local_name")),
            superclass_iris=tuple(str(item) for item in value.get("superclass_iris", [])),
            fields=tuple(FieldDef.from_dict(item) for item in value.get("fields", [])),
            doc=_optional_str(value.get("doc")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "cid": self.cid,
            "iri": self.iri,
            "local_name": self.local_name,
            "superclass_iris": list(self.superclass_iris),
            "fields": [item.to_dict() for item in self.fields],
            "doc": self.doc,
        }


@dataclass(frozen=True, slots=True)
class PropertyDef:
    pid: int
    iri: str
    local_name: str | None = None
    kind: PropertyKind = PropertyKind.OBJECT
    superproperty_iris: tuple[str, ...] = ()
    domain_iris: tuple[str, ...] = ()
    range_type: TypeRef | None = None
    range_iris: tuple[str, ...] = ()
    characteristics: tuple[PropertyCharacteristic, ...] = ()
    fields: tuple[FieldDef, ...] = ()
    inverse_iri: str | None = None
    doc: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PropertyDef:
        range_type = value.get("range_type")
        return cls(
            pid=int(value["pid"]),
            iri=str(value["iri"]),
            local_name=_optional_str(value.get("local_name")),
            kind=PropertyKind(int(value.get("kind", PropertyKind.OBJECT))),
            superproperty_iris=tuple(str(item) for item in value.get("superproperty_iris", [])),
            domain_iris=tuple(str(item) for item in value.get("domain_iris", [])),
            range_type=TypeRef.from_dict(range_type) if range_type is not None else None,
            range_iris=tuple(str(item) for item in value.get("range_iris", [])),
            characteristics=tuple(
                PropertyCharacteristic(int(item)) for item in value.get("characteristics", [])
            ),
            fields=tuple(FieldDef.from_dict(item) for item in value.get("fields", [])),
            inverse_iri=_optional_str(value.get("inverse_iri")),
            doc=_optional_str(value.get("doc")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "pid": self.pid,
            "iri": self.iri,
            "local_name": self.local_name,
            "kind": int(self.kind),
            "superproperty_iris": list(self.superproperty_iris),
            "domain_iris": list(self.domain_iris),
            "range_type": self.range_type.to_dict() if self.range_type is not None else None,
            "range_iris": list(self.range_iris),
            "characteristics": [int(item) for item in self.characteristics],
            "fields": [item.to_dict() for item in self.fields],
            "inverse_iri": self.inverse_iri,
            "doc": self.doc,
        }


@dataclass(frozen=True, slots=True)
class GraphDef:
    gid: int
    name: str
    ontology_iris: tuple[str, ...] = ()
    class_iris: tuple[str, ...] = ()
    property_iris: tuple[str, ...] = ()
    doc: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> GraphDef:
        return cls(
            gid=int(value["gid"]),
            name=str(value["name"]),
            ontology_iris=tuple(str(item) for item in value.get("ontology_iris", [])),
            class_iris=tuple(str(item) for item in value.get("class_iris", [])),
            property_iris=tuple(str(item) for item in value.get("property_iris", [])),
            doc=_optional_str(value.get("doc")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "gid": self.gid,
            "name": self.name,
            "ontology_iris": list(self.ontology_iris),
            "class_iris": list(self.class_iris),
            "property_iris": list(self.property_iris),
            "doc": self.doc,
        }


@dataclass(frozen=True, slots=True)
class IndexOptionDef:
    key: str
    value: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> IndexOptionDef:
        return cls(key=str(value["key"]), value=_optional_str(value.get("value")))

    def to_dict(self) -> dict[str, Any]:
        return {"key": self.key, "value": self.value}


@dataclass(frozen=True, slots=True)
class IndexDef:
    iid: int
    name: str
    kind: IndexKind
    target_class_iri: str | None = None
    target_property_iri: str | None = None
    fields: tuple[str, ...] = ()
    sort: SortDirection = SortDirection.NONE
    options: tuple[IndexOptionDef, ...] = ()
    unique: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> IndexDef:
        return cls(
            iid=int(value["iid"]),
            name=str(value["name"]),
            kind=IndexKind(int(value["kind"])),
            target_class_iri=_optional_str(value.get("target_class_iri")),
            target_property_iri=_optional_str(value.get("target_property_iri")),
            fields=tuple(str(item) for item in value.get("fields", [])),
            sort=SortDirection(int(value.get("sort", SortDirection.NONE))),
            options=tuple(IndexOptionDef.from_dict(item) for item in value.get("options", [])),
            unique=bool(value.get("unique", False)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "iid": self.iid,
            "name": self.name,
            "kind": int(self.kind),
            "target_class_iri": self.target_class_iri,
            "target_property_iri": self.target_property_iri,
            "fields": list(self.fields),
            "sort": int(self.sort),
            "options": [item.to_dict() for item in self.options],
            "unique": self.unique,
        }


@dataclass(frozen=True, slots=True)
class PrefixDef:
    prefix: str
    iri: str

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> PrefixDef:
        return cls(prefix=str(value["prefix"]), iri=str(value["iri"]))

    def to_dict(self) -> dict[str, Any]:
        return {"prefix": self.prefix, "iri": self.iri}


@dataclass(frozen=True, slots=True)
class OntologyDef:
    iri: str
    base_iri: str | None = None
    prefixes: tuple[PrefixDef, ...] = ()
    class_iris: tuple[str, ...] = ()
    property_iris: tuple[str, ...] = ()
    imported_iris: tuple[str, ...] = ()
    doc: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> OntologyDef:
        return cls(
            iri=str(value["iri"]),
            base_iri=_optional_str(value.get("base_iri")),
            prefixes=tuple(PrefixDef.from_dict(item) for item in value.get("prefixes", [])),
            class_iris=tuple(str(item) for item in value.get("class_iris", [])),
            property_iris=tuple(str(item) for item in value.get("property_iris", [])),
            imported_iris=tuple(str(item) for item in value.get("imported_iris", [])),
            doc=_optional_str(value.get("doc")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "iri": self.iri,
            "base_iri": self.base_iri,
            "prefixes": [item.to_dict() for item in self.prefixes],
            "class_iris": list(self.class_iris),
            "property_iris": list(self.property_iris),
            "imported_iris": list(self.imported_iris),
            "doc": self.doc,
        }


@dataclass(slots=True)
class Catalog:
    format_version: int = FORMAT_VERSION
    catalog_id: str | None = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    ontologies: tuple[OntologyDef, ...] = ()
    classes: tuple[ClassDef, ...] = ()
    properties: tuple[PropertyDef, ...] = ()
    graphs: tuple[GraphDef, ...] = ()
    indexes: tuple[IndexDef, ...] = ()

    @classmethod
    def empty(cls, *, catalog_id: str | None = None) -> Catalog:
        now = utc_now_iso()
        return cls(catalog_id=catalog_id, created_at=now, updated_at=now)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Catalog:
        return cls(
            format_version=int(value.get("format_version", FORMAT_VERSION)),
            catalog_id=_optional_str(value.get("catalog_id")),
            created_at=str(value.get("created_at", utc_now_iso())),
            updated_at=str(value.get("updated_at", utc_now_iso())),
            ontologies=tuple(OntologyDef.from_dict(item) for item in value.get("ontologies", [])),
            classes=tuple(ClassDef.from_dict(item) for item in value.get("classes", [])),
            properties=tuple(PropertyDef.from_dict(item) for item in value.get("properties", [])),
            graphs=tuple(GraphDef.from_dict(item) for item in value.get("graphs", [])),
            indexes=tuple(IndexDef.from_dict(item) for item in value.get("indexes", [])),
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> Catalog:
        if not data.startswith(MAGIC):
            raise CaracalError(code="CDB-9501", message="catalog file has invalid magic header")
        payload = json.loads(data[len(MAGIC) :].decode("utf-8"))
        if payload.get("encoding") != CATALOG_ENCODING:
            raise CaracalError(code="CDB-9501", message="unsupported catalog encoding")
        return cls.from_dict(payload["catalog"])

    @classmethod
    def read(cls, path: str | Path) -> Catalog:
        catalog_path = Path(path)
        if not catalog_path.is_file():
            raise CaracalError(code="CDB-9502", message=f"catalog file not found: {catalog_path}")
        return cls.from_bytes(catalog_path.read_bytes())

    def to_dict(self) -> dict[str, Any]:
        return {
            "format_version": self.format_version,
            "catalog_id": self.catalog_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "ontologies": [item.to_dict() for item in self.ontologies],
            "classes": [item.to_dict() for item in self.classes],
            "properties": [item.to_dict() for item in self.properties],
            "graphs": [item.to_dict() for item in self.graphs],
            "indexes": [item.to_dict() for item in self.indexes],
        }

    def to_bytes(self) -> bytes:
        envelope = {
            "encoding": CATALOG_ENCODING,
            "catalog": self.to_dict(),
        }
        payload = json.dumps(envelope, indent=2, sort_keys=True).encode("utf-8")
        return MAGIC + payload

    def write_atomic(self, path: str | Path) -> None:
        catalog_path = Path(path)
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = catalog_path.with_name(f"{catalog_path.name}.tmp")
        tmp_path.write_bytes(self.to_bytes())
        os.replace(tmp_path, catalog_path)

    def register_class(
        self,
        iri: str,
        *,
        local_name: str | None = None,
        superclass_iris: tuple[str, ...] = (),
        fields: tuple[FieldDef, ...] = (),
        doc: str | None = None,
    ) -> ClassDef:
        self._ensure_unique("class", iri, self.classes, attr="iri")
        class_def = ClassDef(
            cid=_next_id(item.cid for item in self.classes),
            iri=iri,
            local_name=local_name or _local_name(iri),
            superclass_iris=superclass_iris,
            fields=fields,
            doc=doc,
        )
        self.classes = (*self.classes, class_def)
        self._touch()
        return class_def

    def register_property(
        self,
        iri: str,
        *,
        local_name: str | None = None,
        kind: PropertyKind = PropertyKind.OBJECT,
        superproperty_iris: tuple[str, ...] = (),
        domain_iris: tuple[str, ...] = (),
        range_type: TypeRef | None = None,
        range_iris: tuple[str, ...] = (),
        characteristics: tuple[PropertyCharacteristic, ...] = (),
        fields: tuple[FieldDef, ...] = (),
        inverse_iri: str | None = None,
        doc: str | None = None,
    ) -> PropertyDef:
        self._ensure_unique("property", iri, self.properties, attr="iri")
        property_def = PropertyDef(
            pid=_next_id(item.pid for item in self.properties),
            iri=iri,
            local_name=local_name or _local_name(iri),
            kind=kind,
            superproperty_iris=superproperty_iris,
            domain_iris=domain_iris,
            range_type=range_type,
            range_iris=range_iris,
            characteristics=characteristics,
            fields=fields,
            inverse_iri=inverse_iri,
            doc=doc,
        )
        self.properties = (*self.properties, property_def)
        self._touch()
        return property_def

    def register_graph(
        self,
        name: str,
        *,
        ontology_iris: tuple[str, ...] = (),
        class_iris: tuple[str, ...] = (),
        property_iris: tuple[str, ...] = (),
        doc: str | None = None,
    ) -> GraphDef:
        self._ensure_unique("graph", name, self.graphs, attr="name")
        graph = GraphDef(
            gid=_next_id(item.gid for item in self.graphs),
            name=name,
            ontology_iris=ontology_iris,
            class_iris=class_iris,
            property_iris=property_iris,
            doc=doc,
        )
        self.graphs = (*self.graphs, graph)
        self._touch()
        return graph

    def register_index(
        self,
        name: str,
        *,
        kind: IndexKind,
        target_class_iri: str | None = None,
        target_property_iri: str | None = None,
        fields: tuple[str, ...] = (),
        sort: SortDirection = SortDirection.NONE,
        options: tuple[IndexOptionDef, ...] = (),
        unique: bool = False,
    ) -> IndexDef:
        self._ensure_unique("index", name, self.indexes, attr="name")
        index = IndexDef(
            iid=_next_id(item.iid for item in self.indexes),
            name=name,
            kind=kind,
            target_class_iri=target_class_iri,
            target_property_iri=target_property_iri,
            fields=fields,
            sort=sort,
            options=options,
            unique=unique,
        )
        self.indexes = (*self.indexes, index)
        self._touch()
        return index

    def class_by_iri(self, iri: str) -> ClassDef | None:
        return _find_by_attr(self.classes, "iri", iri)

    def property_by_iri(self, iri: str) -> PropertyDef | None:
        return _find_by_attr(self.properties, "iri", iri)

    def graph_by_name(self, name: str) -> GraphDef | None:
        return _find_by_attr(self.graphs, "name", name)

    def _touch(self) -> None:
        self.updated_at = utc_now_iso()

    def _ensure_unique(self, kind: str, value: str, items: tuple[Any, ...], *, attr: str) -> None:
        if _find_by_attr(items, attr, value) is not None:
            raise CaracalError(code="CDB-9503", message=f"{kind} already exists: {value}")


def load_catalog(source: Bundle | str | Path) -> Catalog:
    bundle = _as_bundle(source)
    catalog_path = bundle.child(bundle.manifest.catalog_file)
    if not catalog_path.exists():
        return Catalog.empty(catalog_id=bundle.path.stem)
    return Catalog.read(catalog_path)


def save_catalog(target: Bundle | str | Path, catalog: Catalog) -> Path:
    bundle = _as_bundle(target)
    catalog_path = bundle.child(bundle.manifest.catalog_file)
    catalog.write_atomic(catalog_path)
    return catalog_path


def _as_bundle(value: Bundle | str | Path) -> Bundle:
    if isinstance(value, Bundle):
        return value
    return open_bundle(value)


def _find_by_attr(items: tuple[T, ...], attr: str, value: str) -> T | None:
    for item in items:
        if getattr(item, attr) == value:
            return item
    return None


def _next_id(values: Any) -> int:
    return max(values, default=0) + 1


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _local_name(iri: str) -> str:
    return iri.rstrip("/#").rsplit("/", 1)[-1].rsplit("#", 1)[-1].rsplit(":", 1)[-1]


__all__ = [
    "CATALOG_ENCODING",
    "Catalog",
    "ClassDef",
    "ConstraintDef",
    "ConstraintKind",
    "FieldDef",
    "GraphDef",
    "IndexDef",
    "IndexKind",
    "IndexOptionDef",
    "OntologyDef",
    "PrefixDef",
    "PropertyCharacteristic",
    "PropertyDef",
    "PropertyKind",
    "SortDirection",
    "TypeKind",
    "TypeRef",
    "load_catalog",
    "save_catalog",
]
