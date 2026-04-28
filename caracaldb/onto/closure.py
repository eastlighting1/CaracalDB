"""Class closure bitmaps for ontology hierarchy lookups.

This module is deliberately class-only. `SUBPROPERTYOF*` closure should live in
a separate property closure index so query planning can choose class and
property reasoning paths independently.
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from pyroaring import BitMap

from caracaldb.lang.diagnostics import CaracalError
from caracaldb.onto.catalog import Catalog
from caracaldb.onto.hierarchy import HierarchyDAG, OntologyHierarchy
from caracaldb.storage import Bundle, open_bundle
from caracaldb.storage.header import MAGIC

CLASS_CLOSURE_FILE = "subclassof.bitmap"
CLOSURE_ENCODING = "caracal.class-closure.v1+roaring"


@dataclass(slots=True)
class ClassClosureIndex:
    """Lazy `SUBCLASSOF*` cache keyed by class IRI.

    Each cached bitmap maps a superclass IRI to the `cid` values of all known
    subclasses. The reflexive form includes the superclass itself, matching the
    `SUBCLASSOF*` query semantics used by Tuft.
    """

    hierarchy: HierarchyDAG
    class_id_by_iri: dict[str, int]
    class_iri_by_id: dict[int, str]
    _descendants_by_iri: dict[str, BitMap] = field(default_factory=dict)
    _ancestors_by_iri: dict[str, BitMap] = field(default_factory=dict)

    @classmethod
    def from_catalog(cls, catalog: Catalog) -> ClassClosureIndex:
        hierarchy = OntologyHierarchy.from_catalog(catalog).classes
        id_by_iri = {item.iri: item.cid for item in catalog.classes}
        return cls(
            hierarchy=hierarchy,
            class_id_by_iri=id_by_iri,
            class_iri_by_id={cid: iri for iri, cid in id_by_iri.items()},
        )

    @classmethod
    def from_bytes(cls, data: bytes, *, catalog: Catalog) -> ClassClosureIndex:
        index = cls.from_catalog(catalog)
        if not data.startswith(MAGIC):
            raise CaracalError(code="CDB-9506", message="class closure file has invalid magic")
        payload = json.loads(data[len(MAGIC) :].decode("utf-8"))
        if payload.get("encoding") != CLOSURE_ENCODING:
            raise CaracalError(code="CDB-9506", message="unsupported class closure encoding")

        stored_ids = {
            str(key): int(value) for key, value in payload.get("class_id_by_iri", {}).items()
        }
        if stored_ids != index.class_id_by_iri:
            raise CaracalError(
                code="CDB-9507",
                message="class closure cache is stale for the current catalog",
            )

        for iri, encoded in payload.get("descendants_by_iri", {}).items():
            index._descendants_by_iri[str(iri)] = BitMap.deserialize(base64.b64decode(encoded))
        for iri, encoded in payload.get("ancestors_by_iri", {}).items():
            index._ancestors_by_iri[str(iri)] = BitMap.deserialize(base64.b64decode(encoded))
        return index

    @classmethod
    def read(cls, path: str | Path, *, catalog: Catalog) -> ClassClosureIndex:
        closure_path = Path(path)
        if not closure_path.is_file():
            raise CaracalError(
                code="CDB-9508",
                message=f"class closure file not found: {closure_path}",
            )
        return cls.from_bytes(closure_path.read_bytes(), catalog=catalog)

    def descendants_bitmap(self, iri: str, *, include_self: bool = True) -> BitMap:
        self._require_class(iri)
        if iri not in self._descendants_by_iri:
            ids = [self.class_id_by_iri[item] for item in self.hierarchy.descendants(iri)]
            self._descendants_by_iri[iri] = BitMap(ids)
        bitmap = self._descendants_by_iri[iri].copy()
        if include_self:
            bitmap.add(self.class_id_by_iri[iri])
        return bitmap

    def ancestors_bitmap(self, iri: str, *, include_self: bool = True) -> BitMap:
        self._require_class(iri)
        if iri not in self._ancestors_by_iri:
            ids = [self.class_id_by_iri[item] for item in self.hierarchy.ancestors(iri)]
            self._ancestors_by_iri[iri] = BitMap(ids)
        bitmap = self._ancestors_by_iri[iri].copy()
        if include_self:
            bitmap.add(self.class_id_by_iri[iri])
        return bitmap

    def descendant_iris(self, iri: str, *, include_self: bool = True) -> tuple[str, ...]:
        return self._iris_from_bitmap(self.descendants_bitmap(iri, include_self=include_self))

    def ancestor_iris(self, iri: str, *, include_self: bool = True) -> tuple[str, ...]:
        return self._iris_from_bitmap(self.ancestors_bitmap(iri, include_self=include_self))

    def is_subclass(self, child_iri: str, parent_iri: str, *, reflexive: bool = True) -> bool:
        self._require_class(child_iri)
        bitmap = self.descendants_bitmap(parent_iri, include_self=reflexive)
        return self.class_id_by_iri[child_iri] in bitmap

    def invalidate(self, iri: str | None = None) -> None:
        if iri is None:
            self._descendants_by_iri.clear()
            self._ancestors_by_iri.clear()
            return
        self._require_class(iri)
        affected = (iri, *self.hierarchy.ancestors(iri), *self.hierarchy.descendants(iri))
        for item in affected:
            self._descendants_by_iri.pop(item, None)
            self._ancestors_by_iri.pop(item, None)

    def materialize_all(self) -> None:
        for iri in self.hierarchy.nodes:
            self.descendants_bitmap(iri)
            self.ancestors_bitmap(iri)

    def to_bytes(self) -> bytes:
        self.materialize_all()
        envelope = {
            "encoding": CLOSURE_ENCODING,
            "class_id_by_iri": self.class_id_by_iri,
            "descendants_by_iri": _encode_bitmaps(self._descendants_by_iri),
            "ancestors_by_iri": _encode_bitmaps(self._ancestors_by_iri),
        }
        payload = json.dumps(envelope, indent=2, sort_keys=True).encode("utf-8")
        return MAGIC + payload

    def write_atomic(self, path: str | Path) -> None:
        closure_path = Path(path)
        closure_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = closure_path.with_name(f"{closure_path.name}.tmp")
        tmp_path.write_bytes(self.to_bytes())
        os.replace(tmp_path, closure_path)

    def _iris_from_bitmap(self, bitmap: BitMap) -> tuple[str, ...]:
        return tuple(self.class_iri_by_id[int(class_id)] for class_id in bitmap)

    def _require_class(self, iri: str) -> None:
        if iri not in self.class_id_by_iri:
            raise CaracalError(code="CDB-9504", message=f"unknown class hierarchy node: {iri}")


def load_class_closure(source: Bundle | str | Path, catalog: Catalog) -> ClassClosureIndex:
    bundle = _as_bundle(source)
    closure_path = bundle.child("closure", CLASS_CLOSURE_FILE)
    if not closure_path.exists():
        return ClassClosureIndex.from_catalog(catalog)
    return ClassClosureIndex.read(closure_path, catalog=catalog)


def save_class_closure(target: Bundle | str | Path, closure: ClassClosureIndex) -> Path:
    bundle = _as_bundle(target)
    closure_path = bundle.child("closure", CLASS_CLOSURE_FILE)
    closure.write_atomic(closure_path)
    return closure_path


def _as_bundle(value: Bundle | str | Path) -> Bundle:
    if isinstance(value, Bundle):
        return value
    return open_bundle(value)


def _encode_bitmaps(bitmaps: dict[str, BitMap]) -> dict[str, str]:
    return {
        iri: base64.b64encode(bitmap.serialize()).decode("ascii") for iri, bitmap in bitmaps.items()
    }


__all__ = [
    "CLASS_CLOSURE_FILE",
    "CLOSURE_ENCODING",
    "ClassClosureIndex",
    "load_class_closure",
    "save_class_closure",
]
