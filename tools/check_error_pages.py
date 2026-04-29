"""Check that public error documentation exists for registered diagnostics."""

from __future__ import annotations

from pathlib import Path

from caracaldb.lang.diagnostics import ERROR_TABLE

DOCS_ROOT = Path("docs/errors")


def pages_for(code: str) -> list[Path]:
    prefix = code.split("-", 1)[0].lower()
    return sorted((DOCS_ROOT / prefix).glob(f"{code}-*.md"))


def main() -> int:
    missing = [code for code in sorted(ERROR_TABLE) if not pages_for(code)]
    if missing:
        lines = ["Missing public error documentation:"]
        lines.extend(f"- {code}" for code in missing)
        raise SystemExit("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
