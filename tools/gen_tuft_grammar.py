"""Mirror the Tuft Lark grammar into the public documentation tree."""

from __future__ import annotations

import argparse
from pathlib import Path

SOURCE = Path("caracaldb/lang/tuft/tuft.lark")
DEFAULT_OUT = Path("docs/tuft/grammar.lark")


def render_grammar() -> str:
    return SOURCE.read_text(encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    content = render_grammar()
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
