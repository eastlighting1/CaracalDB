"""Check required front matter for public documentation pages."""

from __future__ import annotations

from pathlib import Path

SKIP_NAMES = {
    "01_language_spec.md",
    "02_engine_spec.md",
    "03_user_modeling_case_study.md",
    "04_caracaldb_implementation.md",
    "05_wbs.md",
    "TF-INDEX.md",
}
LEGACY_PATHS = {
    Path("docs/01_language_spec.md"),
    Path("docs/02_engine_spec.md"),
    Path("docs/03_user_modeling_case_study.md"),
    Path("docs/04_caracaldb_implementation.md"),
    Path("docs/05_wbs.md"),
    Path("docs/errors/TF-INDEX.md"),
    Path("docs/format/csr.md"),
    Path("docs/milestones/M0-gate.md"),
    Path("docs/milestones/M1-gate.md"),
    Path("docs/milestones/M2-gate.md"),
    Path("docs/milestones/M3-gate.md"),
    Path("docs/milestones/M4-gate.md"),
    Path("docs/milestones/M5-gate.md"),
}
REQUIRED_KEYS = ("applies_to:", "status:", "last_updated:", "engine_status:")


def is_public_page(path: Path) -> bool:
    if "_generated" in path.parts or "legacy" in path.parts or "release" in path.parts:
        return False
    if path in LEGACY_PATHS:
        return False
    if path.name in SKIP_NAMES:
        return False
    return not (
        path.parent.name == "milestones"
        and path.name.startswith("M")
        and path.name.endswith("-gate.md")
    )


def front_matter(text: str) -> str | None:
    if not text.startswith("---\n"):
        return None
    parts = text.split("---", 2)
    if len(parts) < 3:
        return None
    return parts[1]


def main() -> int:
    failures: list[str] = []
    for path in sorted(Path("docs").rglob("*.md")):
        if not is_public_page(path):
            continue
        meta = front_matter(path.read_text(encoding="utf-8"))
        if meta is None:
            failures.append(f"{path}: missing front matter")
            continue
        for key in REQUIRED_KEYS:
            if key not in meta:
                failures.append(f"{path}: missing {key.rstrip(':')}")
    if failures:
        raise SystemExit("\n".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
