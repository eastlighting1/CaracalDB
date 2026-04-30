"""Generate the public error index from CaracalDB diagnostics."""

from __future__ import annotations

import argparse
from pathlib import Path

from caracaldb.lang.diagnostics import ERROR_TABLE


def render_index() -> str:
    lines = [
        "---",
        "applies_to: v0.2.x",
        "status: generated",
        "last_updated: 2026-04-30",
        "engine_status: python-reference; rust-engine-planned",
        "---",
        "",
        "# Error Index",
        "",
        "| Code | Title | Hint |",
        "|---|---|---|",
    ]
    for code in sorted(ERROR_TABLE):
        info = ERROR_TABLE[code]
        hint = info.hint or ""
        lines.append(f"| `{info.code}` | {info.title} | {hint} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("docs/errors/index.md"))
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
