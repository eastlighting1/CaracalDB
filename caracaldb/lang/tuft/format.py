"""Debug formatting helpers for Tuft AST values."""

from __future__ import annotations

import json
from dataclasses import fields, is_dataclass
from enum import Enum
from typing import Any

from caracaldb.lang.tuft.ast import Span


def to_jsonable(value: Any, *, include_spans: bool = False) -> Any:
    """Convert an AST value into a stable JSON-compatible structure."""

    if value is None:
        return None
    if isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple | list):
        return [to_jsonable(item, include_spans=include_spans) for item in value]
    if isinstance(value, dict):
        return {
            str(key): to_jsonable(item, include_spans=include_spans)
            for key, item in value.items()
            if _include_value(item, include_spans=include_spans)
        }
    if isinstance(value, Span) and not include_spans:
        return None
    if is_dataclass(value):
        result: dict[str, Any] = {"node": type(value).__name__}
        for field in fields(value):
            if field.name == "span" and not include_spans:
                continue
            item = getattr(value, field.name)
            if not _include_value(item, include_spans=include_spans):
                continue
            result[field.name] = to_jsonable(item, include_spans=include_spans)
        return result
    return str(value)


def to_json(value: Any, *, include_spans: bool = False) -> str:
    return json.dumps(to_jsonable(value, include_spans=include_spans), indent=2, ensure_ascii=False)


def _include_value(value: Any, *, include_spans: bool) -> bool:
    if value is None:
        return False
    if isinstance(value, Span) and not include_spans:
        return False
    return not (value == () or value == [] or value == {})


__all__ = ["to_json", "to_jsonable"]
