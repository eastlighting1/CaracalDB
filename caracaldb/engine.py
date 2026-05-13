"""Engine selection helpers for the Python reference and Rust core."""

from __future__ import annotations

import importlib
import os
from dataclasses import dataclass
from types import ModuleType
from typing import Literal

from caracaldb.lang.diagnostics import CaracalError

EngineMode = Literal["python", "rust", "auto"]
RuntimeEngine = Literal["python", "rust"]

_ENGINE_ENV = "CARACALDB_ENGINE"


@dataclass(frozen=True, slots=True)
class EngineSelection:
    """Engine selection configuration.

    Examples:
        >>> selection = EngineSelection(requested="python", active="python", rust_available=False)
        >>> selection.active
        'python'
    """
    requested: EngineMode
    active: RuntimeEngine
    rust_available: bool
    rust_error: str | None = None


def rust_module() -> ModuleType | None:
    try:
        return importlib.import_module("caracaldb._caracaldb_rust")
    except ImportError:
        return None


def rust_available() -> bool:
    """Check if the rust core module is available.

    Examples:
        >>> available = rust_available()
    """
    return rust_module() is not None


def resolve_engine() -> EngineSelection:
    """Resolve the active execution engine.

    Examples:
        >>> selection = resolve_engine()
        >>> selection.active in ("python", "rust")
        True
    """
    raw = os.environ.get(_ENGINE_ENV, "python").strip().lower()
    if raw not in {"python", "rust", "auto"}:
        raise CaracalError(
            code="CDB-9007",
            message=(
                f"unsupported {_ENGINE_ENV} value: {raw!r}; " "expected 'python', 'rust', or 'auto'"
            ),
        )
    requested = raw  # type: ignore[assignment]
    rust = rust_module()
    if requested == "python":
        return EngineSelection(requested="python", active="python", rust_available=rust is not None)
    if requested == "auto":
        # The Rust core is intentionally opt-in until compatibility coverage is complete.
        return EngineSelection(requested="auto", active="python", rust_available=rust is not None)
    if rust is None:
        raise CaracalError(
            code="CDB-9008",
            message="CARACALDB_ENGINE=rust requested but caracaldb._caracaldb_rust is unavailable",
            hint="install a wheel with the Rust extension or use CARACALDB_ENGINE=python",
        )
    return EngineSelection(requested="rust", active="rust", rust_available=True)


__all__ = ["EngineMode", "EngineSelection", "RuntimeEngine", "resolve_engine", "rust_available"]
