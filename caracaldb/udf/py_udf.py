"""Python UDF: ``@cdb.udf`` decorator with type-contract validation.

Python UDFs are registered against a ``UdfRegistry`` and invoked from the
expression compiler with Arrow batches. The decorator captures the declared
return type so the registry can validate the output before passing it on.
The runtime contract:

    * ``@udf(returns=pa.float32(), arg_types=(pa.string(),))`` — explicit form.
    * ``@udf`` — minimal form, accepts whatever the function returns.

The function is called once per batch; the implementation is responsible for
returning a ``pa.Array`` (or anything convertible via ``pa.array``) of the
same length as the batch column it operates over.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass

import pyarrow as pa

from caracaldb.lang.diagnostics import CaracalError


@dataclass(slots=True)
class PyUdf:
    name: str
    fn: Callable[..., object]
    returns: pa.DataType | None = None
    arg_types: tuple[pa.DataType, ...] | None = None

    def __call__(self, *args: pa.Array) -> pa.Array:
        if self.arg_types is not None:
            if len(args) != len(self.arg_types):
                raise CaracalError(
                    code="CDB-6131",
                    message=f"{self.name}() expects {len(self.arg_types)} args, got {len(args)}",
                )
            for i, (arr, expected) in enumerate(zip(args, self.arg_types, strict=True)):
                if arr.type != expected:
                    raise CaracalError(
                        code="CDB-6131",
                        message=(f"{self.name}() arg {i}: expected {expected}, got {arr.type}"),
                    )
        result = self.fn(*args)
        out = result if isinstance(result, pa.Array) else pa.array(result)
        if self.returns is not None and out.type != self.returns:
            raise CaracalError(
                code="CDB-6131",
                message=f"{self.name}() returned {out.type}, expected {self.returns}",
            )
        return out


def udf(
    arg: Callable[..., object] | None = None,
    *,
    returns: pa.DataType | None = None,
    arg_types: Sequence[pa.DataType] | None = None,
    name: str | None = None,
) -> Callable[..., object]:
    """Register a function as a Python UDF.

    Supports two forms::

        @udf
        def add_one(x): ...

        @udf(returns=pa.int64(), arg_types=(pa.int64(),))
        def add_one(x): ...
    """

    def _wrap(fn: Callable[..., object]) -> PyUdf:
        return PyUdf(
            name=name or fn.__name__,
            fn=fn,
            returns=returns,
            arg_types=tuple(arg_types) if arg_types is not None else None,
        )

    if callable(arg):
        return _wrap(arg)
    return _wrap


@dataclass(slots=True)
class UdfRegistry:
    fns: dict[str, PyUdf] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.fns is None:
            self.fns = {}

    def register(self, fn: PyUdf) -> None:
        if fn.name in self.fns:
            raise CaracalError(code="CDB-6131", message=f"UDF already registered: {fn.name!r}")
        self.fns[fn.name] = fn

    def call(self, name: str, *args: pa.Array) -> pa.Array:
        if name not in self.fns:
            raise CaracalError(code="CDB-6131", message=f"unknown UDF: {name!r}")
        return self.fns[name](*args)


__all__ = ["PyUdf", "UdfRegistry", "udf"]
