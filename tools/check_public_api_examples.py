"""Check that the stable public Python API exports include Examples."""

from __future__ import annotations

import inspect

import caracaldb


def main() -> int:
    failures: list[str] = []
    for name in caracaldb.__all__:
        if name == "__version__":
            continue
        obj = getattr(caracaldb, name)
        doc = inspect.getdoc(obj) or ""
        if "Examples" not in doc:
            failures.append(f"caracaldb.{name}: missing Examples section")
    if failures:
        raise SystemExit("\n".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
