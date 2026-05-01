"""N-Triples → ``.crcl`` importer.

This is a small, deliberately narrow converter that lowers an N-Triples file
into the columnar ``.crcl`` storage format. It is the engine surface required
by ADR-0005 ("RDF as an import surface, not an engine surface"): users with
RDF data can try CaracalDB without rewriting their pipeline, and the engine
itself stays free of SPARQL / OWL-DL machinery.

What we support
---------------
- One triple per line, ``<s> <p> <o> .`` form
- Object can be an IRI ``<o>`` or a string literal ``"value"`` (with optional
  ``@lang`` or ``^^<datatype>`` decoration; we keep the lexical value, drop
  the decoration with a warning)
- ``rdf:type`` is special-cased: it sets the *class* of the subject. A subject
  with no ``rdf:type`` triple falls back to the class ``Resource``.
- All other IRI predicates lower to **edges** (one row in ``insert_edge_table``)
- All other literal predicates lower to **node properties** (one column in
  ``insert_node_table`` keyed by the predicate's local name)

What we do not support
----------------------
- Turtle / TriG / RDF/XML — pre-convert with ``rapper`` / ``riot``
- Blank nodes — skipped with a warning (governance use cases want stable IRIs)
- Named graphs / quads — collapsed to a single graph
- Datatype-aware coercion (``xsd:integer`` → int) — everything stays a string;
  the host can cast at query time

The intent is "make ingest possible," not "implement the RDF stack." If the
limitations above bite, the right answer is a proper Turtle parser as a
separate tool, not bloating this one.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from caracaldb.lang.diagnostics import CaracalError

RDF_TYPE_IRI = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

# Crude N-Triples line parser. The official EBNF allows escaped characters,
# UTF-16 surrogate pairs, and various whitespace conventions; the regex
# below covers the 95% of real-world N-Triples corpora that come out of
# rapper / riot serialisers. Anything weirder gets reported as a parse
# error with the line number, so the caller can fix the source.
_TRIPLE_RE = re.compile(
    r"""
    ^\s*
    (?P<subject><[^>]+>|_:[A-Za-z0-9_]+)
    \s+
    (?P<predicate><[^>]+>)
    \s+
    (?P<object>
        <[^>]+>                                  # IRI object
        | _:[A-Za-z0-9_]+                        # blank object
        | "(?:[^"\\]|\\.)*"(?:@[A-Za-z\-]+|\^\^<[^>]+>)?  # literal
    )
    \s*\.\s*$
    """,
    re.VERBOSE,
)

_LITERAL_RE = re.compile(
    r'^"(?P<value>(?:[^"\\]|\\.)*)"(?:@[A-Za-z\-]+|\^\^<[^>]+>)?$'
)


@dataclass(slots=True)
class RdfImportStats:
    triples_seen: int = 0
    triples_kept: int = 0
    nodes_emitted: int = 0
    edges_emitted: int = 0
    blank_nodes_skipped: int = 0
    parse_errors: list[tuple[int, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "triples_seen": self.triples_seen,
            "triples_kept": self.triples_kept,
            "nodes_emitted": self.nodes_emitted,
            "edges_emitted": self.edges_emitted,
            "blank_nodes_skipped": self.blank_nodes_skipped,
            "parse_errors": list(self.parse_errors),
        }


def _local_name(iri: str) -> str:
    """Extract the local part of an IRI, suitable as a class/property name."""
    bare = iri.strip("<>")
    for sep in ("#", "/"):
        idx = bare.rfind(sep)
        if idx != -1 and idx + 1 < len(bare):
            return bare[idx + 1 :]
    return bare


def _is_iri(token: str) -> bool:
    return token.startswith("<") and token.endswith(">")


def _is_blank(token: str) -> bool:
    return token.startswith("_:")


def _strip_iri(token: str) -> str:
    return token[1:-1] if _is_iri(token) else token


def _decode_literal(token: str) -> str | None:
    m = _LITERAL_RE.match(token)
    if m is None:
        return None
    raw = m.group("value")
    # Minimal escape handling: \\ \" \n \t \r — anything else stays literal.
    return (
        raw.replace("\\\\", "\\")
        .replace('\\"', '"')
        .replace("\\n", "\n")
        .replace("\\t", "\t")
        .replace("\\r", "\r")
    )


def parse_ntriples(lines: Iterable[str]) -> Iterator[tuple[int, str, str, str, bool]]:
    """Yield ``(line_no, subject_iri, predicate_iri, object_token, object_is_iri)``.

    Blank-node subjects/objects are skipped. Parse failures become
    ``CaracalError`` with the line number — callers decide whether to halt
    or accumulate. ``object_token`` is the *raw* lexical form (for IRIs:
    bare IRI without ``<>``; for literals: decoded string).
    """
    for line_no, raw in enumerate(lines, start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _TRIPLE_RE.match(line)
        if m is None:
            raise CaracalError(
                code="CDB-7050",
                message=f"line {line_no}: not a valid N-Triples line",
                hint="re-serialise with rapper / riot in ntriples mode",
            )
        subj = m.group("subject")
        pred = m.group("predicate")
        obj = m.group("object")
        if _is_blank(subj):
            continue  # caller will count via stats
        pred_iri = _strip_iri(pred)
        if _is_iri(obj):
            yield (line_no, _strip_iri(subj), pred_iri, _strip_iri(obj), True)
        elif _is_blank(obj):
            continue
        else:
            literal = _decode_literal(obj)
            if literal is None:
                raise CaracalError(
                    code="CDB-7050",
                    message=f"line {line_no}: malformed literal {obj!r}",
                )
            yield (line_no, _strip_iri(subj), pred_iri, literal, False)


def lower_to_tables(
    triples: Iterator[tuple[int, str, str, str, bool]],
    *,
    default_class: str = "Resource",
    stats: RdfImportStats | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Materialise N-Triples into ``(nodes, edges)`` row lists.

    Two passes over a buffered triple list: the first establishes each
    subject's class (from ``rdf:type``, falling back to ``default_class``)
    and accumulates literal properties; the second emits edges. We must
    buffer because a literal triple can appear before its subject's
    ``rdf:type`` triple in the source file.
    """
    if stats is None:
        stats = RdfImportStats()

    triples_buf = list(triples)
    stats.triples_seen += len(triples_buf)

    subject_class: dict[str, str] = {}
    subject_props: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []

    for _, subj, pred, obj, is_iri in triples_buf:
        if pred == RDF_TYPE_IRI and is_iri:
            subject_class[subj] = _local_name(obj)
            stats.triples_kept += 1
            continue
        if is_iri:
            edges.append(
                {"node_id": subj, "src": subj, "dst": obj, "type": _local_name(pred)}
            )
            # The object also becomes a node we need to materialise; we let
            # it get its class either from a later rdf:type triple or
            # default_class below.
            subject_props.setdefault(obj, {})
            stats.triples_kept += 1
        else:
            props = subject_props.setdefault(subj, {})
            col = _local_name(pred)
            props[col] = obj
            stats.triples_kept += 1

    # Make sure every subject we saw at least exists as a node.
    for subj in list(subject_props.keys()) + list(subject_class.keys()):
        subject_props.setdefault(subj, {})

    nodes: list[dict[str, Any]] = []
    for subj, props in subject_props.items():
        cls = subject_class.get(subj, default_class)
        row: dict[str, Any] = {"node_id": subj, "type": cls}
        row.update(props)
        nodes.append(row)
    stats.nodes_emitted = len(nodes)
    stats.edges_emitted = len(edges)
    # Strip src/dst columns from edges that are not native; insert_edge_table
    # expects ``src`` / ``dst`` (we already use those).
    return nodes, edges


def import_ntriples(
    db,  # caracaldb.Database — typed loosely to avoid a circular import
    source: str | Path,
    *,
    default_class: str = "Resource",
) -> RdfImportStats:
    """Import an N-Triples file into the database in one shot.

    Convenience wrapper: opens the file, parses it, lowers to node/edge
    tables, and feeds them through ``Database.insert_node_table`` /
    ``insert_edge_table``. Returns import statistics for governance /
    audit logging.
    """
    path = Path(source)
    if not path.is_file():
        raise CaracalError(code="CDB-7050", message=f"N-Triples file not found: {path}")
    stats = RdfImportStats()
    blank_skipped = 0
    with path.open("r", encoding="utf-8") as fh:
        # Count blank-node lines as we stream, since parse_ntriples drops them.
        def _counted(lines: Iterable[str]) -> Iterator[str]:
            nonlocal blank_skipped
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("_:") or " _:" in stripped:
                    blank_skipped += 1
                yield line

        triples = parse_ntriples(_counted(fh))
        nodes, edges = lower_to_tables(triples, default_class=default_class, stats=stats)
    stats.blank_nodes_skipped = blank_skipped

    if nodes:
        db.insert_node_table(nodes)
    if edges:
        db.insert_edge_table(edges)
    return stats


__all__ = [
    "RDF_TYPE_IRI",
    "RdfImportStats",
    "import_ntriples",
    "lower_to_tables",
    "parse_ntriples",
]
