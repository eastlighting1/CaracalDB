"""Generate a lightweight API symbol index for documentation search."""

from __future__ import annotations

import argparse
import inspect
import json
from pathlib import Path

import caracaldb


def render_symbols() -> str:
    records = []
    for name in sorted(caracaldb.__all__):
        obj = getattr(caracaldb, name)
        if name == "__version__":
            kind = "constant"
            summary = "Package version."
        elif inspect.isclass(obj):
            kind = "class"
            lines = (inspect.getdoc(obj) or "").splitlines()
            summary = lines[0] if lines else ""
        elif callable(obj):
            kind = "function"
            lines = (inspect.getdoc(obj) or "").splitlines()
            summary = lines[0] if lines else ""
        else:
            kind = "object"
            summary = ""
        records.append(
            {
                "name": f"caracaldb.{name}",
                "kind": kind,
                "summary": summary,
                "url": f"api/caracaldb/#{name.lower()}",
            }
        )
    return json.dumps(records, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("docs/api/symbols.json"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    content = render_symbols()
    if args.check:
        existing = args.out.read_text(encoding="utf-8") if args.out.exists() else None
        if existing != content:
            raise SystemExit(f"{args.out} is out of date")
        return 0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
