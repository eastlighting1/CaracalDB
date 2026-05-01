"""Stdlib HTTP server that exposes a `.crcl` pack as a web viewer.

The UI mimics a parquet-style inspector with four tabs: ``Query``, ``Data``,
``Schema``, and ``Metadata``. A left sidebar lists the classes recorded in
the catalog of the open database; selecting a class drives the Data and
Schema tabs.

Run from Python::

    from caracaldb.viewer import serve
    serve("graph.crcl", host="127.0.0.1", port=8765)

Or via the CLI::

    caracal view graph.crcl --port 8765
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, time
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pyarrow as pa

import caracaldb as cdb
from caracaldb.api import Database
from caracaldb.lang.diagnostics import CaracalError
from caracaldb.storage.pack import is_packed


def _find_crcl_files(root: Path) -> list[Path]:
    if root.is_file() or root.suffix == ".crcl":
        return [root.resolve()] if root.exists() else []
    if not root.exists():
        return []
    return sorted(
        (p.resolve() for p in root.rglob("*.crcl") if p.is_file() or p.is_dir()),
        key=lambda p: str(p).lower(),
    )


class _ViewerState:
    def __init__(self, root: Path, mode: str) -> None:
        self.root = root
        self.mode = mode
        self.source: Path | None = None
        self.db: Database | None = None

    def open(self, source: Path) -> None:
        target = source.expanduser()
        if not target.is_absolute():
            target = (self.root / target).resolve()
        if not target.exists():
            raise CaracalError(code="CDB-9101", message=f"viewer source not found: {target}")
        next_db = cdb.connect(target, mode=self.mode)
        if self.db is not None:
            self.db.close()
        self.source = target
        self.db = next_db

    def close(self) -> None:
        if self.db is not None:
            self.db.close()
        self.db = None
        self.source = None

    def require_db(self) -> tuple[Database, Path]:
        if self.db is None or self.source is None:
            raise CaracalError(code="CDB-9102", message="no .crcl file is open")
        return self.db, self.source

# ---------------------------------------------------------------------------
# JSON helpers
# ---------------------------------------------------------------------------


def _jsonify(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonify(v) for v in value]
    return str(value)


def _table_to_payload(table: pa.Table, *, limit: int, offset: int) -> dict[str, Any]:
    total = table.num_rows
    if offset:
        table = table.slice(offset)
    if limit >= 0:
        table = table.slice(0, limit)
    columns = [field.name for field in table.schema]
    types = [str(field.type) for field in table.schema]
    rows = [[_jsonify(row.get(name)) for name in columns] for row in table.to_pylist()]
    return {
        "columns": columns,
        "types": types,
        "rows": rows,
        "row_count": total,
        "returned": len(rows),
        "offset": offset,
    }


def _all_classes_to_payload(db: Database, *, limit: int, offset: int) -> dict[str, Any]:
    catalog = db.catalog
    columns: list[str] = ["_class"]
    rows: list[dict[str, Any]] = []
    for cls in catalog.classes:
        local = cls.local_name or _local(cls.iri)
        try:
            table = db.open_node_store(cls.iri).to_table()
        except CaracalError:
            continue
        for field in table.schema:
            if field.name not in columns:
                columns.append(field.name)
        for row in table.to_pylist():
            rows.append({"_class": local, **row})

    total = len(rows)
    visible_rows = rows[offset:]
    if limit >= 0:
        visible_rows = visible_rows[:limit]
    return {
        "columns": columns,
        "types": ["string", *["mixed" for _ in columns[1:]]],
        "rows": [[_jsonify(row.get(name)) for name in columns] for row in visible_rows],
        "row_count": total,
        "returned": len(visible_rows),
        "offset": offset,
    }


_ALL_NODES_QUERY_RE = re.compile(
    r"^\s*MATCH\s*\(\s*(?P<alias>[A-Za-z_][A-Za-z0-9_]*)?\s*\)\s+"
    r"RETURN\s+(?P=alias)\s*(?:LIMIT\s+(?P<limit>\d+))?\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _all_nodes_query_limit(text: str) -> int | None:
    match = _ALL_NODES_QUERY_RE.match(text)
    if not match:
        return None
    return int(match.group("limit") or "1000")


# ---------------------------------------------------------------------------
# Database snapshot
# ---------------------------------------------------------------------------


def _local(iri: str) -> str:
    for sep in ("#", "/"):
        if sep in iri:
            return iri.rsplit(sep, 1)[-1]
    return iri


def _bundle_metadata(db: Database, source: Path) -> dict[str, Any]:
    bundle = db.bundle
    catalog = db.catalog
    total_rows = 0
    class_summaries: list[dict[str, Any]] = []
    for cls in catalog.classes:
        local = cls.local_name or _local(cls.iri)
        try:
            store = db.open_node_store(cls.iri)
            num_rows = store.num_rows
            schema = store.schema
            columns = [
                {"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in schema
            ]
        except CaracalError:
            num_rows = 0
            columns = []
        total_rows += num_rows
        class_summaries.append(
            {
                "iri": cls.iri,
                "local_name": local,
                "superclasses": list(cls.superclass_iris),
                "row_count": num_rows,
                "columns": columns,
            }
        )

    src = Path(source)
    file_kind = "packed" if src.is_file() and is_packed(src) else "bundle"

    return {
        "source": str(src),
        "file_name": src.name,
        "file_kind": file_kind,
        "file_size": src.stat().st_size if src.exists() and src.is_file() else None,
        "bundle_path": str(bundle.path),
        "format_version": catalog.format_version,
        "catalog_id": catalog.catalog_id,
        "created_at": catalog.created_at,
        "updated_at": catalog.updated_at,
        "ontology_count": len(catalog.ontologies),
        "class_count": len(catalog.classes),
        "property_count": len(catalog.properties),
        "graph_count": len(catalog.graphs),
        "index_count": len(catalog.indexes),
        "total_rows": total_rows,
        "classes": class_summaries,
    }


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


_INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<title>CaracalDB &mdash; .crcl viewer</title>
<style>
  :root {
    --page: #eef2ef;
    --surface: #fbfcfb;
    --surface-2: #f4f7f4;
    --rail: #17201c;
    --border: #d7ded6;
    --border-strong: #b6c3b8;
    --text: #1d241f;
    --muted: #69736d;
    --accent: #137a68;
    --accent-2: #c7593c;
    --editor: #202823;
    --editor-line: #18201b;
    --editor-text: #f5fff7;
    --row-alt: #f5f8f5;
    --header: #e6eee7;
    --gutter: 28px;
  }
  body[data-theme="dark"] {
    --page: #101412;
    --surface: #171d1a;
    --surface-2: #202822;
    --rail: #48caaa;
    --border: #303b34;
    --border-strong: #4a5a50;
    --text: #edf4ef;
    --muted: #9ca9a1;
    --accent: #40c4a2;
    --accent-2: #ff8a65;
    --editor: #0d1110;
    --editor-line: #151b18;
    --editor-text: #f2fff7;
    --row-alt: #1c231f;
    --header: #242d27;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; background: var(--page); color: var(--text);
    font-family: Inter, "Segoe UI", system-ui, sans-serif; font-size: 15px; overflow: hidden; }
  .app { display: flex; flex-direction: column; height: 100vh; background: var(--page); }
  .topbar { display: grid; grid-template-columns: minmax(240px, 1fr) auto; gap: 24px;
    align-items: center; padding: 20px var(--gutter) 16px; background: var(--surface);
    border-bottom: 1px solid var(--border); }
  .file-title { display: flex; flex-direction: column; min-width: 0; gap: 4px; }
  .file-title .eyebrow { color: var(--accent); font-size: 12px; font-weight: 700;
    letter-spacing: .08em; text-transform: uppercase; }
  .file-title .name { color: var(--text); font-size: 24px; font-weight: 750; line-height: 1.15;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .file-title .path { color: var(--muted); font-size: 13px; overflow: hidden;
    text-overflow: ellipsis; white-space: nowrap; }
  .stats { display: flex; gap: 8px; flex-wrap: wrap; justify-content: flex-end; }
  .stat { min-width: 92px; padding: 8px 12px; border: 1px solid var(--border);
    border-radius: 8px; background: var(--surface-2); }
  .stat b { display: block; font-size: 18px; line-height: 1; color: var(--text); }
  .stat span { display: block; margin-top: 4px; font-size: 11px; color: var(--muted);
    text-transform: uppercase; letter-spacing: .04em; }
  .file-picker { display: grid; grid-template-columns: minmax(220px, 320px) minmax(320px, 1fr) auto auto; gap: 10px;
    align-items: center; padding: 12px var(--gutter); background: #e4ebe5;
    border-bottom: 1px solid var(--border); }
  body[data-theme="dark"] .file-picker { background: #141a17; }
  .file-picker select, .file-picker input { height: 36px; border: 1px solid var(--border-strong);
    border-radius: 8px; background: #fff; color: var(--text); padding: 0 10px; outline: none; }
  body[data-theme="dark"] .file-picker select,
  body[data-theme="dark"] .file-picker input {
    background: #0f1412;
    color: var(--text);
  }
  .file-picker input { min-width: 0; font-family: Consolas, "Courier New", monospace; }
  .file-picker .open-btn, .file-picker .theme-btn { justify-self: start; width: auto; min-width: 88px;
    height: 36px; padding: 0 13px; border-radius: 8px; font-weight: 700; cursor: pointer; white-space: nowrap; }
  .file-picker .open-btn { border: 1px solid var(--accent);
    background: var(--accent); color: #fff; }
  .file-picker .open-btn:hover { background: #0f6658; }
  body[data-theme="dark"] .file-picker .open-btn { color: #07110d; }
  body[data-theme="dark"] .file-picker .open-btn:hover { background: #67d7bd; }
  .file-picker .theme-btn { border: 1px solid var(--border-strong); background: var(--surface);
    color: var(--text); }
  .file-picker .theme-btn:hover { background: var(--surface-2); }
  @media (max-width: 900px) {
    .topbar { grid-template-columns: 1fr; }
    .stats { justify-content: flex-start; }
    .file-picker { grid-template-columns: 1fr auto auto; }
    .file-picker select { grid-column: 1 / -1; }
  }
  .main { display: flex; flex-direction: column; min-width: 0; min-height: 0; height: 100%; padding: 18px var(--gutter) 22px; gap: 14px; }
  .tabs { display: inline-flex; align-self: flex-start; gap: 4px; padding: 4px; background: #dfe8e0;
    border: 1px solid var(--border); border-radius: 10px; }
  .tab { padding: 9px 15px; cursor: pointer; color: #4c5851; border: 0; border-radius: 7px;
    font-size: 14px; line-height: 1.2; font-weight: 650; }
  .tab:hover { color: var(--text); background: rgba(255,255,255,.55); }
  .tab.active { color: #fff; background: var(--rail); box-shadow: 0 1px 2px rgba(0,0,0,.12); }
  body[data-theme="dark"] .tabs { background: #202923; }
  body[data-theme="dark"] .tab:hover { background: #29342d; }
  body[data-theme="dark"] .tab.active { color: #07110d; }
  .pane { flex: 1; min-height: 0; display: none; flex-direction: column; }
  .pane.active { display: flex; background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; overflow: hidden; box-shadow: 0 12px 32px rgba(23, 32, 28, .08); }
  .toolbar { padding: 14px var(--gutter) 15px; display: flex; align-items: center; gap: 10px;
    background: var(--surface); color: var(--muted); border-bottom: 1px solid var(--border); }
  .btn { background: #fff; color: var(--text); border: 1px solid var(--border-strong);
    padding: 7px 13px; border-radius: 7px; cursor: pointer; font-size: 14px; font-weight: 650;
    line-height: 1.1; min-height: 34px; }
  .btn:hover { background: var(--surface-2); border-color: #93a196; }
  .btn.primary { background: var(--accent); color: #fff; border-color: var(--accent); }
  .btn.primary:hover { background: #0f6658; }
  .btn.primary .play { color: #bff4d7; margin-right: 5px; }
  body[data-theme="dark"] .btn { background: #18201c; color: var(--text); border-color: var(--border-strong); }
  body[data-theme="dark"] .btn:hover { background: #223027; }
  body[data-theme="dark"] .btn.primary { background: var(--accent); color: #07110d; border-color: var(--accent); }
  body[data-theme="dark"] .btn.primary .play { color: #07110d; }
  .class-select { margin-left: auto; display: flex; align-items: center; gap: 10px;
    color: var(--muted); font-size: 14px; }
  .class-select select { min-width: 240px; max-width: 420px; background: #fff; color: var(--text);
    border: 1px solid var(--border-strong); border-radius: 7px; padding: 7px 10px; outline: none; }
  .editor { height: clamp(150px, 24vh, 240px); background: var(--editor); border-bottom: 1px solid var(--border); }
  .editor-shell { display: grid; grid-template-columns: 61px 1fr; height: 100%; overflow: hidden; }
  .line-nums { background: var(--editor-line); color: #8da092; padding-top: 12px;
    font-family: Consolas, "Courier New", monospace; font-size: 16px; line-height: 26px;
    text-align: right; user-select: none; overflow: hidden; }
  .line-nums div { padding-right: 18px; }
  .editor textarea { width: 100%; height: 100%; resize: none; background: var(--editor);
    color: var(--editor-text); border: 0; padding: 12px 14px; font-family: Consolas, "Courier New", monospace;
    font-size: 16px; line-height: 26px; outline: none; white-space: pre; overflow: auto; }
  .search { padding: 16px var(--gutter); border-bottom: 1px solid var(--border); background: var(--surface-2); }
  .searchbox { position: relative; }
  .searchbox::before { content: ""; position: absolute; left: 11px; top: 50%; width: 11px; height: 11px;
    border: 1.8px solid var(--muted); border-radius: 50%; transform: translateY(-58%); opacity: .95; }
  .searchbox::after { content: ""; position: absolute; left: 23px; top: 50%; width: 8px; height: 1.8px;
    background: var(--muted); transform: rotate(45deg); transform-origin: left center; opacity: .95; }
  .search input { width: 100%; background: #fff; color: var(--text);
    border: 1px solid var(--border-strong); padding: 3px 10px 4px 43px; border-radius: 8px; outline: none;
    font-size: 17px; line-height: 1.1; height: 40px; }
  .search input::placeholder { color: #8a938d; opacity: 1; }
  body[data-theme="dark"] .search { background: #18201c; }
  body[data-theme="dark"] .search input {
    background: #0f1412;
    color: var(--text);
    border-color: var(--border-strong);
  }
  body[data-theme="dark"] .search input::placeholder { color: var(--muted); }
  .table-wrap { flex: 1; overflow: auto; min-height: 0; background: #fff; }
  table { border-collapse: collapse; width: 100%; min-width: 100%; table-layout: auto; }
  th, td { text-align: left; padding: 10px 32px 10px 12px; border-right: 1px solid var(--border);
    border-bottom: 1px solid #edf1ee; white-space: nowrap; vertical-align: top; height: 44px;
    overflow: hidden; text-overflow: ellipsis; min-width: 110px; }
  th { background: var(--header); position: sticky; top: 0; font-weight: 700; height: 45px;
    border-bottom: 1px solid var(--border-strong); color: var(--text); }
  th .th-inner { display: flex; align-items: center; justify-content: space-between; gap: 14px; }
  tbody tr:nth-child(even) { background: var(--row-alt); }
  tbody tr:hover { background: #edf6ef; }
  body[data-theme="dark"] .table-wrap { background: var(--surface); }
  body[data-theme="dark"] th {
    background: var(--header);
    color: var(--text);
    border-bottom-color: var(--border-strong);
  }
  body[data-theme="dark"] td {
    color: var(--text);
    border-right-color: var(--border);
    border-bottom-color: var(--border);
  }
  body[data-theme="dark"] tbody tr { background: #141a17; }
  body[data-theme="dark"] tbody tr:nth-child(even) { background: var(--row-alt); }
  body[data-theme="dark"] tbody tr:hover { background: #26322b; }
  .status { padding: 9px 14px; font-size: 12px; color: var(--muted); border-top: 1px solid var(--border);
    background: var(--surface-2); }
  .query-status { margin-left: 8px; color: var(--muted); font-size: 13px; }
  .error { color: #a43b2f; padding: 12px; white-space: pre-wrap; font-family: Consolas, monospace; }
  .kv { width: 100%; }
  .kv th { width: 300px; }
  .empty { padding: 24px; color: var(--muted); }
</style>
</head>
<body>
<div class="app">
  <header class="topbar">
    <div class="file-title">
      <div class="eyebrow">CaracalDB Viewer</div>
      <div class="name" id="file-name">Loading...</div>
      <div class="path" id="file-path"></div>
    </div>
    <div class="stats">
      <div class="stat"><b id="stat-classes">0</b><span>Classes</span></div>
      <div class="stat"><b id="stat-rows">0</b><span>Rows</span></div>
      <div class="stat"><b id="stat-version">v-</b><span>Format</span></div>
    </div>
  </header>
  <div class="file-picker">
    <select id="file-select"></select>
    <input id="path-input" placeholder="data/example.crcl or C:\path\to\bundle.crcl" />
    <button class="open-btn" id="btn-open-file">Open</button>
    <button class="theme-btn" id="btn-theme" type="button">Dark</button>
  </div>
  <section class="main">
    <div class="tabs">
      <div class="tab active" data-tab="query">Query</div>
      <div class="tab" data-tab="data">Data</div>
      <div class="tab" data-tab="schema">Schema</div>
      <div class="tab" data-tab="metadata">Metadata</div>
    </div>

    <div class="pane active" id="pane-query">
      <div class="toolbar">
        <button class="btn primary" id="btn-run"><span class="play">&#9654;</span>Run</button>
        <button class="btn" id="btn-clear">Clear</button>
        <span class="query-status" id="query-status"></span>
      </div>
      <div class="editor"><div class="editor-shell">
        <div class="line-nums" id="query-lines"></div>
        <textarea id="query-text" spellcheck="false"
          placeholder="MATCH (n)
RETURN n
LIMIT 100">MATCH (n)
RETURN n
LIMIT 100</textarea>
      </div></div>
      <div class="search"><div class="searchbox"><input id="query-search" placeholder="Search rows" /></div></div>
      <div class="table-wrap" id="query-table"></div>
      <div class="status" id="query-rowcount"></div>
    </div>

    <div class="pane" id="pane-data">
      <div class="toolbar"><label class="class-select" style="margin-left:0">Class <select id="data-class-select"></select></label></div>
      <div class="search"><div class="searchbox"><input id="data-search" placeholder="Search rows" /></div></div>
      <div class="table-wrap" id="data-table"></div>
      <div class="status" id="data-status"></div>
    </div>

    <div class="pane" id="pane-schema">
      <div class="toolbar"><label class="class-select" style="margin-left:0">Class <select id="schema-class-select"></select></label></div>
      <div class="search"><div class="searchbox"><input id="schema-search" placeholder="Search rows" /></div></div>
      <div class="table-wrap" id="schema-table"></div>
    </div>

    <div class="pane" id="pane-metadata">
      <div class="table-wrap" id="metadata-table"></div>
    </div>
  </section>
</div>

<script>
const state = { info: null, files: [], classes: [], current: null };
const ALL_CLASSES = "__all__";

function el(html) { const t = document.createElement("template"); t.innerHTML = html.trim(); return t.content.firstChild; }
function escapeHtml(s) {
  if (s === null || s === undefined) return '<span style="color:var(--muted)">null</span>';
  return String(s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
}
function attrHtml(s) {
  if (s === null || s === undefined) return "";
  return String(s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
}
function setTheme(theme) {
  const resolved = theme === "dark" ? "dark" : "light";
  document.body.dataset.theme = resolved;
  localStorage.setItem("caracal-theme", resolved);
  document.getElementById("btn-theme").textContent = resolved === "dark" ? "Light" : "Dark";
}
function initTheme() {
  const saved = localStorage.getItem("caracal-theme");
  const preferred = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
  setTheme(saved || preferred);
}

function renderTable(target, columns, rows, opts) {
  opts = opts || {};
  const filter = (opts.filter || "").toLowerCase();
  const visible = filter
    ? rows.filter(r => r.some(v => v !== null && v !== undefined && String(v).toLowerCase().includes(filter)))
    : rows;
  if (!columns.length) {
    target.innerHTML = '<div class="empty">No columns.</div>';
    return;
  }
  const head = columns.map(c => `<th><span class="th-inner"><span>${escapeHtml(c)}</span></span></th>`).join("");
  const body = visible.map(r => `<tr>${r.map(v => `<td>${escapeHtml(v)}</td>`).join("")}</tr>`).join("");
  target.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function renderKv(target, pairs) {
  const rows = pairs.map(([k,v]) => `<tr><th>${escapeHtml(k)}</th><td>${escapeHtml(v)}</td></tr>`).join("");
  target.innerHTML = `<table class="kv"><thead><tr><th><span class="th-inner"><span>Key</span></span></th><th><span class="th-inner"><span>Value</span></span></th></tr></thead><tbody>${rows}</tbody></table>`;
}

async function loadInfo() {
  const r = await fetch("/api/info");
  const info = await r.json();
  state.info = info;
  state.classes = info.classes || [];
  document.title = `${info.file_name || ".crcl"} - CaracalDB viewer`;
  document.getElementById("file-name").textContent = info.file_name || "No .crcl selected";
  document.getElementById("file-path").textContent = info.source || info.root || "";
  document.getElementById("path-input").value = info.source || "";
  document.getElementById("stat-classes").textContent = info.class_count || 0;
  document.getElementById("stat-rows").textContent = info.total_rows || 0;
  document.getElementById("stat-version").textContent = info.format_version ? `v${info.format_version}` : "v-";
  hydrateClassSelects();

  renderMetadata(info);
  if (state.classes.length) {
    selectClass(state.classes[0].iri);
  } else {
    lastData = { columns: [], rows: [] };
    lastQuery = { columns: [], rows: [] };
    document.getElementById("data-table").innerHTML = '<div class="empty">Open a .crcl file.</div>';
    document.getElementById("schema-table").innerHTML = '<div class="empty">Open a .crcl file.</div>';
    document.getElementById("query-table").innerHTML = '<div class="empty">Open a .crcl file.</div>';
    document.getElementById("query-status").textContent = "";
    document.getElementById("query-rowcount").textContent = "";
  }
  updateLineNumbers();
  if (state.classes.length) runQuery();
}

async function loadFiles() {
  const r = await fetch("/api/files");
  const payload = await r.json();
  state.files = payload.files || [];
  const select = document.getElementById("file-select");
  select.innerHTML = "";
  if (!state.files.length) {
    select.appendChild(el('<option value="">No .crcl files under data/</option>'));
    return;
  }
  state.files.forEach(file => {
    select.appendChild(el(`<option value="${attrHtml(file.path)}">${escapeHtml(file.label)}</option>`));
  });
  if (state.info && state.info.source) select.value = state.info.source;
  if (state.info && state.info.source && select.value !== state.info.source) {
    select.selectedIndex = -1;
  }
}

async function openFile(path) {
  if (!path) return;
  const r = await fetch("/api/open", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path }),
  });
  const payload = await r.json();
  if (payload.error) {
    document.getElementById("query-table").innerHTML = `<div class="error">${escapeHtml(payload.error)}</div>`;
    return;
  }
  await loadFiles();
  await loadInfo();
}

function hydrateClassSelects() {
  const totalRows = state.info ? state.info.total_rows : 0;
  ["data-class-select", "schema-class-select"].forEach(id => {
    const select = document.getElementById(id);
    select.innerHTML = "";
    if (!state.classes.length) {
      select.innerHTML = '<option value="">No classes</option>';
      select.disabled = true;
      return;
    }
    select.appendChild(el(`<option value="${ALL_CLASSES}">All classes (${escapeHtml(totalRows)})</option>`));
    state.classes.forEach(cls => {
      const opt = el(`<option value="${attrHtml(cls.iri)}">${escapeHtml(cls.local_name)} (${escapeHtml(cls.row_count)})</option>`);
      select.appendChild(opt);
    });
    select.addEventListener("change", () => {
      if (select.value === ALL_CLASSES) {
        refreshDataTab();
        refreshSchemaTab();
        return;
      }
      selectClass(select.value);
    });
  });
}

function renderMetadata(info) {
  renderKv(document.getElementById("metadata-table"), [
    ["file_name", info.file_name],
    ["file_kind", info.file_kind],
    ["file_size", info.file_size],
    ["bundle_path", info.bundle_path],
    ["format_version", info.format_version],
    ["catalog_id", info.catalog_id || "(none)"],
    ["created_at", info.created_at],
    ["updated_at", info.updated_at],
    ["class_count", info.class_count],
    ["property_count", info.property_count],
    ["ontology_count", info.ontology_count],
    ["graph_count", info.graph_count],
    ["index_count", info.index_count],
    ["total_rows", info.total_rows],
  ]);
}

function selectClass(iri) {
  state.current = iri;
  ["data-class-select", "schema-class-select"].forEach(id => {
    const select = document.getElementById(id);
    if (select.value !== iri && select.value !== ALL_CLASSES) select.value = iri;
  });
  refreshDataTab();
  refreshSchemaTab();
  updateLineNumbers();
}

let lastData = { columns: [], rows: [] };
async function refreshDataTab() {
  const selected = document.getElementById("data-class-select").value;
  if (!state.current && selected !== ALL_CLASSES) return;
  const url = selected === ALL_CLASSES
    ? "/api/classes?limit=1000"
    : `/api/class?iri=${encodeURIComponent(selected || state.current)}&limit=1000`;
  const r = await fetch(url);
  const payload = await r.json();
  if (payload.error) {
    document.getElementById("data-table").innerHTML = `<div class="error">${escapeHtml(payload.error)}</div>`;
    document.getElementById("data-status").textContent = "";
    return;
  }
  lastData = payload;
  renderTable(document.getElementById("data-table"), payload.columns, payload.rows,
    { filter: document.getElementById("data-search").value });
  document.getElementById("data-status").textContent =
    `${payload.returned} of ${payload.row_count} rows`;
}

function refreshSchemaTab() {
  const selected = document.getElementById("schema-class-select").value;
  const target = document.getElementById("schema-table");
  if (selected === ALL_CLASSES) {
    const rows = [];
    state.classes.forEach(cls => {
      cls.columns.forEach((c, i) => rows.push([cls.local_name, i, c.name, c.type, c.nullable ? "YES" : "NO"]));
    });
    const filter = (document.getElementById("schema-search").value || "").toLowerCase();
    const visible = filter
      ? rows.filter(r => r.some(v => String(v).toLowerCase().includes(filter)))
      : rows;
    renderTable(target, ["Class", "#", "Column name", "Data type", "Nullable"], visible);
    return;
  }
  const cls = state.classes.find(c => c.iri === (selected || state.current));
  if (!cls) { target.innerHTML = '<div class="empty">Select a class.</div>'; return; }
  const filter = (document.getElementById("schema-search").value || "").toLowerCase();
  const cols = ["#", "Column name", "Data type", "Nullable"];
  const rows = cls.columns.map((c, i) => [i, c.name, c.type, c.nullable ? "YES" : "NO"]);
  const visible = filter
    ? rows.filter(r => r.some(v => String(v).toLowerCase().includes(filter)))
    : rows;
  renderTable(target, cols, visible);
}

async function runQuery() {
  const text = document.getElementById("query-text").value;
  document.getElementById("query-status").textContent = "running...";
  const r = await fetch("/api/query", { method: "POST",
    headers: { "Content-Type": "application/json" }, body: JSON.stringify({ query: text }) });
  const payload = await r.json();
  if (payload.error) {
    document.getElementById("query-table").innerHTML = `<div class="error">${escapeHtml(payload.error)}</div>`;
    document.getElementById("query-status").textContent = "error";
    document.getElementById("query-rowcount").textContent = "";
    return;
  }
  lastQuery = payload;
  renderTable(document.getElementById("query-table"), payload.columns, payload.rows,
    { filter: document.getElementById("query-search").value });
  document.getElementById("query-status").textContent = "ok";
  document.getElementById("query-rowcount").textContent = `${payload.returned} rows`;
}
let lastQuery = { columns: [], rows: [] };

function updateLineNumbers() {
  const textarea = document.getElementById("query-text");
  const count = Math.max(1, textarea.value.split("\n").length);
  document.getElementById("query-lines").innerHTML =
    Array.from({ length: count }, (_, i) => `<div>${i + 1}</div>`).join("");
}

document.querySelectorAll(".tab").forEach(t => t.addEventListener("click", () => {
  document.querySelectorAll(".tab").forEach(x => x.classList.toggle("active", x === t));
  document.querySelectorAll(".pane").forEach(p => p.classList.toggle("active",
    p.id === "pane-" + t.dataset.tab));
}));

document.getElementById("btn-run").addEventListener("click", runQuery);
document.getElementById("btn-clear").addEventListener("click", () => {
  document.getElementById("query-text").value = "";
  document.getElementById("query-table").innerHTML = "";
  document.getElementById("query-status").textContent = "";
  document.getElementById("query-rowcount").textContent = "";
  updateLineNumbers();
});

document.getElementById("data-search").addEventListener("input", () =>
  renderTable(document.getElementById("data-table"), lastData.columns, lastData.rows,
    { filter: document.getElementById("data-search").value }));
document.getElementById("schema-search").addEventListener("input", refreshSchemaTab);
document.getElementById("query-search").addEventListener("input", () =>
  renderTable(document.getElementById("query-table"), lastQuery.columns, lastQuery.rows,
    { filter: document.getElementById("query-search").value }));

document.getElementById("query-text").addEventListener("keydown", e => {
  if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); runQuery(); }
});
document.getElementById("query-text").addEventListener("input", updateLineNumbers);
document.getElementById("query-text").addEventListener("scroll", e => {
  document.getElementById("query-lines").scrollTop = e.target.scrollTop;
});

document.getElementById("file-select").addEventListener("change", e => openFile(e.target.value));
document.getElementById("btn-open-file").addEventListener("click", () =>
  openFile(document.getElementById("path-input").value));
document.getElementById("path-input").addEventListener("keydown", e => {
  if (e.key === "Enter") openFile(e.target.value);
});
document.getElementById("btn-theme").addEventListener("click", () =>
  setTheme(document.body.dataset.theme === "dark" ? "light" : "dark"));

(async function init() {
  initTheme();
  await loadInfo();
  await loadFiles();
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


def serve(
    path: str | Path = "data",
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    mode: str = "ro",
    open_browser: bool = True,
) -> None:
    """Serve a local viewer rooted at a data directory or opened `.crcl` file."""
    src = Path(path)
    root = src if src.is_dir() or not src.suffix else src.parent
    if not root.exists():
        root = Path.cwd() / "data" if str(path) == "data" else root
    state = _ViewerState(root.resolve(), mode)
    candidates = _find_crcl_files(src if src.exists() else root)
    if src.exists() and (src.is_file() or src.suffix == ".crcl"):
        state.open(src)
    elif candidates:
        state.open(candidates[0])

    handler_cls = _make_handler(state)
    httpd = ThreadingHTTPServer((host, port), handler_cls)
    url = f"http://{host}:{port}/"
    print(f"caracal viewer: serving {root} at {url}  (Ctrl-C to stop)")

    if open_browser:
        try:
            import webbrowser

            webbrowser.open(url)
        except Exception:  # pragma: no cover
            pass

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\ncaracal viewer: shutting down")
    finally:
        httpd.server_close()
        state.close()


def _make_handler(state: _ViewerState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        # Silence default access logging to keep stdout clean.
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def _send_json(self, payload: Any, status: int = 200) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, html: str) -> None:
            body = html.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            route = parsed.path
            qs = parse_qs(parsed.query)

            if route in ("/", "/index.html"):
                self._send_html(_INDEX_HTML)
                return
            if route == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
                return
            if route == "/api/info":
                try:
                    db, source = state.require_db()
                    payload = _bundle_metadata(db, source)
                    payload["root"] = str(state.root)
                    self._send_json(payload)
                except CaracalError as exc:
                    self._send_json(
                        {
                            "root": str(state.root),
                            "source": None,
                            "file_name": None,
                            "classes": [],
                            "class_count": 0,
                            "property_count": 0,
                            "ontology_count": 0,
                            "graph_count": 0,
                            "index_count": 0,
                            "total_rows": 0,
                            "error": f"{exc.code}: {exc.message}",
                        }
                    )
                return
            if route == "/api/files":
                files = []
                for file_path in _find_crcl_files(state.root):
                    try:
                        label = str(file_path.relative_to(state.root))
                    except ValueError:
                        label = str(file_path)
                    files.append({"path": str(file_path), "label": label})
                self._send_json(
                    {
                        "root": str(state.root),
                        "current": str(state.source) if state.source else None,
                        "files": files,
                    }
                )
                return
            if route == "/api/class":
                iri = (qs.get("iri") or [""])[0]
                limit = int((qs.get("limit") or ["1000"])[0])
                offset = int((qs.get("offset") or ["0"])[0])
                if not iri:
                    self._send_json({"error": "missing 'iri' parameter"}, status=400)
                    return
                try:
                    db, _source = state.require_db()
                    store = db.open_node_store(iri)
                    table = store.to_table()
                    self._send_json(_table_to_payload(table, limit=limit, offset=offset))
                except CaracalError as exc:
                    self._send_json({"error": f"{exc.code}: {exc.message}"}, status=400)
                return
            if route == "/api/classes":
                limit = int((qs.get("limit") or ["1000"])[0])
                offset = int((qs.get("offset") or ["0"])[0])
                try:
                    db, _source = state.require_db()
                    self._send_json(_all_classes_to_payload(db, limit=limit, offset=offset))
                except CaracalError as exc:
                    self._send_json({"error": f"{exc.code}: {exc.message}"}, status=400)
                return

            self.send_response(404)
            self.end_headers()

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path == "/api/open":
                length = int(self.headers.get("Content-Length") or "0")
                raw = self.rfile.read(length) if length else b"{}"
                try:
                    body = json.loads(raw.decode("utf-8") or "{}")
                except json.JSONDecodeError as exc:
                    self._send_json({"error": f"invalid JSON: {exc}"}, status=400)
                    return
                text = (body.get("path") or "").strip()
                if not text:
                    self._send_json({"error": "empty path"}, status=400)
                    return
                try:
                    state.open(Path(text))
                    db, source = state.require_db()
                    payload = _bundle_metadata(db, source)
                    payload["root"] = str(state.root)
                    self._send_json(payload)
                except CaracalError as exc:
                    self._send_json({"error": f"{exc.code}: {exc.message}"}, status=400)
                return

            if parsed.path != "/api/query":
                self.send_response(404)
                self.end_headers()
                return
            length = int(self.headers.get("Content-Length") or "0")
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError as exc:
                self._send_json({"error": f"invalid JSON: {exc}"}, status=400)
                return
            text = (body.get("query") or "").strip()
            if not text:
                self._send_json({"error": "empty query"}, status=400)
                return
            try:
                db, _source = state.require_db()
                all_nodes_limit = _all_nodes_query_limit(text)
                if all_nodes_limit is not None:
                    self._send_json(
                        _all_classes_to_payload(db, limit=all_nodes_limit, offset=0)
                    )
                    return
                result = db.cursor().sql(text)
                table = result.arrow()
                self._send_json(_table_to_payload(table, limit=1000, offset=0))
            except CaracalError as exc:
                hint = f"\nhint: {exc.hint}" if getattr(exc, "hint", None) else ""
                self._send_json({"error": f"{exc.code}: {exc.message}{hint}"}, status=400)
            except Exception as exc:  # noqa: BLE001
                self._send_json({"error": f"{type(exc).__name__}: {exc}"}, status=500)

    return Handler


__all__ = ["serve"]
