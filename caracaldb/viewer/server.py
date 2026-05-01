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
                {"name": f.name, "type": str(f.type), "nullable": f.nullable}
                for f in schema
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
    --bg: #1c1c1c;
    --panel: #232323;
    --panel-2: #2a2a2a;
    --border: #3a3a3a;
    --text: #e6e6e6;
    --muted: #9a9a9a;
    --accent: #4ec9b0;
    --kw: #c586c0;
    --num: #b5cea8;
    --str: #ce9178;
    --row-alt: #262626;
    --header: #303030;
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text);
    font-family: "Segoe UI", system-ui, sans-serif; font-size: 13px; }
  .app { display: grid; grid-template-columns: 260px 1fr; height: 100vh; }
  .sidebar { background: var(--panel); border-right: 1px solid var(--border); overflow: auto; }
  .sidebar h2 { margin: 0; padding: 10px 12px; font-size: 11px; font-weight: 600;
    letter-spacing: 0.08em; color: var(--muted); text-transform: uppercase;
    border-bottom: 1px solid var(--border); }
  .file-info { padding: 10px 12px; border-bottom: 1px solid var(--border); }
  .file-info .name { font-weight: 600; color: var(--accent); word-break: break-all; }
  .file-info .meta { font-size: 11px; color: var(--muted); margin-top: 2px; }
  .class-list { list-style: none; margin: 0; padding: 0; }
  .class-list li { padding: 8px 12px; cursor: pointer; border-bottom: 1px solid var(--border);
    display: flex; justify-content: space-between; gap: 8px; }
  .class-list li:hover { background: var(--panel-2); }
  .class-list li.active { background: #2d3a3a; color: var(--accent); }
  .class-list .row-count { color: var(--muted); font-variant-numeric: tabular-nums; }
  .main { display: flex; flex-direction: column; min-width: 0; }
  .tabs { display: flex; gap: 0; border-bottom: 1px solid var(--border); background: var(--panel); }
  .tab { padding: 10px 18px; cursor: pointer; color: var(--muted); border-right: 1px solid var(--border); }
  .tab.active { color: var(--text); background: var(--bg); font-weight: 600; }
  .pane { flex: 1; min-height: 0; display: none; flex-direction: column; }
  .pane.active { display: flex; }
  .toolbar { padding: 8px; display: flex; gap: 8px; border-bottom: 1px solid var(--border);
    background: var(--panel); }
  .btn { background: var(--panel-2); color: var(--text); border: 1px solid var(--border);
    padding: 4px 12px; border-radius: 3px; cursor: pointer; font-size: 13px; }
  .btn:hover { background: #353535; }
  .btn.primary { color: #6acf6a; }
  .editor { flex: 1; min-height: 140px; height: 35%; }
  .editor textarea { width: 100%; height: 100%; resize: none; background: var(--bg);
    color: var(--text); border: 0; padding: 12px; font-family: Consolas, "Courier New", monospace;
    font-size: 13px; outline: none; }
  .search { padding: 8px; border-bottom: 1px solid var(--border); background: var(--panel); }
  .search input { width: 100%; background: var(--panel-2); color: var(--text);
    border: 1px solid var(--border); padding: 6px 10px; border-radius: 3px; outline: none; }
  .table-wrap { flex: 1; overflow: auto; min-height: 0; }
  table { border-collapse: collapse; width: 100%; }
  th, td { text-align: left; padding: 6px 12px; border-right: 1px solid var(--border);
    border-bottom: 1px solid var(--border); white-space: nowrap; vertical-align: top; }
  th { background: var(--header); position: sticky; top: 0; font-weight: 600; }
  tbody tr:nth-child(even) { background: var(--row-alt); }
  .status { padding: 6px 12px; font-size: 12px; color: var(--muted); border-top: 1px solid var(--border);
    background: var(--panel); }
  .error { color: #f28b82; padding: 12px; white-space: pre-wrap; font-family: Consolas, monospace; }
  .kv { width: 100%; }
  .kv th { width: 220px; }
  .empty { padding: 24px; color: var(--muted); }
</style>
</head>
<body>
<div class="app">
  <aside class="sidebar">
    <div class="file-info" id="file-info">
      <div class="name" id="file-name">loading...</div>
      <div class="meta" id="file-meta"></div>
    </div>
    <h2>Classes</h2>
    <ul class="class-list" id="class-list"></ul>
  </aside>
  <section class="main">
    <div class="tabs">
      <div class="tab active" data-tab="query">Query</div>
      <div class="tab" data-tab="data">Data</div>
      <div class="tab" data-tab="schema">Schema</div>
      <div class="tab" data-tab="metadata">Metadata</div>
    </div>

    <div class="pane active" id="pane-query">
      <div class="toolbar">
        <button class="btn primary" id="btn-run">&#9654; Run</button>
        <button class="btn" id="btn-clear">Clear</button>
        <span class="status" id="query-status" style="border:0;background:transparent"></span>
      </div>
      <div class="editor"><textarea id="query-text" spellcheck="false"
        placeholder="MATCH (n:ClassName) RETURN n LIMIT 100">MATCH (n:Resource) RETURN n LIMIT 100</textarea></div>
      <div class="search"><input id="query-search" placeholder="Search rows" /></div>
      <div class="table-wrap" id="query-table"></div>
      <div class="status" id="query-rowcount"></div>
    </div>

    <div class="pane" id="pane-data">
      <div class="search"><input id="data-search" placeholder="Search rows" /></div>
      <div class="table-wrap" id="data-table"></div>
      <div class="status" id="data-status"></div>
    </div>

    <div class="pane" id="pane-schema">
      <div class="search"><input id="schema-search" placeholder="Search rows" /></div>
      <div class="table-wrap" id="schema-table"></div>
    </div>

    <div class="pane" id="pane-metadata">
      <div class="table-wrap" id="metadata-table"></div>
    </div>
  </section>
</div>

<script>
const state = { info: null, classes: [], current: null };

function el(html) { const t = document.createElement("template"); t.innerHTML = html.trim(); return t.content.firstChild; }
function escapeHtml(s) {
  if (s === null || s === undefined) return '<span style="color:var(--muted)">null</span>';
  return String(s).replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
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
  const head = columns.map(c => `<th>${escapeHtml(c)}</th>`).join("");
  const body = visible.map(r => `<tr>${r.map(v => `<td>${escapeHtml(v)}</td>`).join("")}</tr>`).join("");
  target.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function renderKv(target, pairs) {
  const rows = pairs.map(([k,v]) => `<tr><th>${escapeHtml(k)}</th><td>${escapeHtml(v)}</td></tr>`).join("");
  target.innerHTML = `<table class="kv"><thead><tr><th>Key</th><th>Value</th></tr></thead><tbody>${rows}</tbody></table>`;
}

async function loadInfo() {
  const r = await fetch("/api/info");
  const info = await r.json();
  state.info = info;
  state.classes = info.classes || [];
  document.getElementById("file-name").textContent = info.file_name || "(unknown)";
  const sizeKb = info.file_size != null ? (info.file_size / 1024).toFixed(1) + " KB" : "directory";
  document.getElementById("file-meta").textContent =
    `${info.file_kind} · ${sizeKb} · v${info.format_version}`;

  const list = document.getElementById("class-list");
  list.innerHTML = "";
  if (!state.classes.length) {
    list.innerHTML = '<li style="color:var(--muted);cursor:default">(no classes)</li>';
  }
  state.classes.forEach(cls => {
    const li = el(`<li><span>${escapeHtml(cls.local_name)}</span><span class="row-count">${cls.row_count}</span></li>`);
    li.addEventListener("click", () => selectClass(cls.iri));
    list.appendChild(li);
  });

  renderMetadata(info);
  if (state.classes.length) selectClass(state.classes[0].iri);
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
  document.querySelectorAll("#class-list li").forEach((li, i) => {
    li.classList.toggle("active", state.classes[i] && state.classes[i].iri === iri);
  });
  refreshDataTab();
  refreshSchemaTab();
  const local = (state.classes.find(c => c.iri === iri) || {}).local_name;
  if (local) document.getElementById("query-text").value = `MATCH (n:${local}) RETURN n LIMIT 100`;
}

let lastData = { columns: [], rows: [] };
async function refreshDataTab() {
  if (!state.current) return;
  const url = `/api/class?iri=${encodeURIComponent(state.current)}&limit=1000`;
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
  const cls = state.classes.find(c => c.iri === state.current);
  const target = document.getElementById("schema-table");
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

loadInfo();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


def serve(
    path: str | Path,
    *,
    host: str = "127.0.0.1",
    port: int = 8765,
    mode: str = "ro",
    open_browser: bool = True,
) -> None:
    """Open a `.crcl` file or bundle and serve a local viewer."""
    src = Path(path)
    if not src.exists():
        raise CaracalError(code="CDB-9101", message=f"viewer source not found: {src}")

    db = cdb.connect(src, mode=mode)

    handler_cls = _make_handler(db, src)
    httpd = ThreadingHTTPServer((host, port), handler_cls)
    url = f"http://{host}:{port}/"
    print(f"caracal viewer: serving {src} at {url}  (Ctrl-C to stop)")

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
        db.close()


def _make_handler(db: Database, source: Path) -> type[BaseHTTPRequestHandler]:
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
            if route == "/api/info":
                try:
                    self._send_json(_bundle_metadata(db, source))
                except CaracalError as exc:
                    self._send_json({"error": f"{exc.code}: {exc.message}"}, status=500)
                return
            if route == "/api/class":
                iri = (qs.get("iri") or [""])[0]
                limit = int((qs.get("limit") or ["1000"])[0])
                offset = int((qs.get("offset") or ["0"])[0])
                if not iri:
                    self._send_json({"error": "missing 'iri' parameter"}, status=400)
                    return
                try:
                    store = db.open_node_store(iri)
                    table = store.to_table()
                    self._send_json(_table_to_payload(table, limit=limit, offset=offset))
                except CaracalError as exc:
                    self._send_json({"error": f"{exc.code}: {exc.message}"}, status=400)
                return

            self.send_response(404)
            self.end_headers()

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
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
