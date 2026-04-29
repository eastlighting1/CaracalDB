"""Generate the public Tuft built-ins index from the runtime registry."""

from __future__ import annotations

import argparse
from pathlib import Path

from caracaldb.lang.builtins import REGISTRY


def _arity(value: int | tuple[int, int]) -> str:
    if isinstance(value, int):
        return str(value)
    return f"{value[0]}..{value[1]}"


def render_index() -> str:
    lines = [
        "---",
        "applies_to: v0.1.x",
        "status: generated",
        "last_updated: 2026-04-28",
        "engine_status: python-reference; rust-engine-planned",
        "---",
        "",
        "# Built-ins",
        "",
        "| Name | Kind | Arity |",
        "|---|---|---:|",
    ]
    for name in sorted(REGISTRY):
        fn = REGISTRY[name]
        lines.append(f"| `{fn.name}` | {fn.kind} | {_arity(fn.arity)} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("docs/tuft/builtins.md"))
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    content = render_index()
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
