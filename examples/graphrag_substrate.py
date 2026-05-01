"""GraphRAG substrate example — CaracalDB + external embedder + HNSW.

This is a deliberately small, runnable demonstration of how CaracalDB
integrates with a retrieval-augmented generation pipeline. It is *not* a
GraphRAG framework; that work belongs in the host application or a
LangChain/LlamaIndex integration. CaracalDB's contract for retrievers is
the Arrow record-batch output of ``Connection.sql`` (and the columnar
storage underneath it).

What this example shows
-----------------------
1. Build a tiny scientific-literature KG (papers, authors, citations).
2. Compute an embedding per paper title (here: deterministic stub vectors;
   in production: an LLM embedder, sentence-transformers, OpenAI, etc.).
3. Index the embeddings with ``caracaldb.graph.hnsw.HnswIndex`` so vector
   search is co-located with the graph.
4. Run a "GraphRAG query": vector-search for a question, then expand the
   top-k papers to their cited neighbours via ``MATCH ... -[:cites]-> ...``,
   then return the joined Arrow batch as the prompt context.

The point is that CaracalDB does *not* need to know about LLMs to be a good
GraphRAG substrate. It needs to make these three things cheap:

- **Vector search co-located with the graph** (HNSW lives next to CSR).
- **Property recovery on retrieved nodes** (HashJoin recovers titles, etc.).
- **Multi-hop expansion in one query** (Tuft pattern matching).

If your retriever needs more than this, you have a host-application problem,
not a database problem.

Run me
------
::

    uv run python examples/graphrag_substrate.py

This script intentionally uses no external embedding service so it can run
in CI. Replace ``embed_text`` with a real embedder for production use.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import numpy as np

import caracaldb as cdb
from caracaldb.graph.hnsw import HnswConfig, HnswIndex


# ---------------------------------------------------------------------------
# Embedder seam — replace this with an LLM/sentence-transformers call.
# ---------------------------------------------------------------------------

EMBED_DIM = 32


def embed_text(text: str, dim: int = EMBED_DIM) -> np.ndarray:
    """Deterministic pseudo-embedding for the example.

    Uses SHA-256 to seed a NumPy RNG, then draws ``dim`` floats. This gives
    each text a stable but topic-naive vector — good enough to demonstrate
    plumbing, useless for real retrieval. Swap in a real embedder.
    """
    seed = int.from_bytes(hashlib.sha256(text.encode("utf-8")).digest()[:8], "little")
    rng = np.random.default_rng(seed)
    v = rng.standard_normal(dim).astype(np.float32)
    return v / (np.linalg.norm(v) + 1e-9)


# ---------------------------------------------------------------------------
# Step 1: tiny KG build
# ---------------------------------------------------------------------------


PAPERS = [
    ("p1", "Attention Is All You Need", "transformer architecture"),
    ("p2", "BERT: Pre-training of Deep Bidirectional Transformers", "language model"),
    ("p3", "Graph Neural Networks: A Review", "graph learning survey"),
    ("p4", "GraphSAGE: Inductive Representation Learning", "graph sampling"),
    ("p5", "Knowledge Graph Embedding by TransE", "kg embedding"),
]
CITATIONS = [
    ("p2", "p1"),  # BERT cites Attention
    ("p4", "p3"),  # GraphSAGE cites GNN review
    ("p5", "p3"),  # TransE cites GNN review
]


def build_kg(bundle_path: Path) -> cdb.Database:
    db = cdb.connect(bundle_path)
    nodes = [
        {"node_id": pid, "type": "Paper", "title": title, "topic": topic}
        for pid, title, topic in PAPERS
    ]
    db.insert_node_table(nodes)
    edges = [
        {"node_id": src, "src": src, "dst": dst, "type": "cites"}
        for src, dst in CITATIONS
    ]
    db.insert_edge_table(edges)
    return db


# ---------------------------------------------------------------------------
# Step 2 + 3: embed and index
# ---------------------------------------------------------------------------


def build_hnsw_index(db: cdb.Database) -> tuple[HnswIndex, dict[int, str]]:
    """Embed every paper title and stash it in HNSW keyed by ``_cdb_gid``."""
    rows = db.sql("MATCH (p:Paper) RETURN p.title").rows()
    # We need the gid alongside title; the rows() helper drops it because
    # it's not in the projection. Pull it via a second arrow() call so the
    # example shows both APIs working.
    table = db.sql("MATCH (p:Paper) RETURN p.title").arrow()
    titles = table.column("title").to_pylist()

    # Recover gids by re-querying with the global identity column. In a real
    # app you'd already have these from the ingest path.
    from caracaldb.storage.node_store import open_node_store

    paper_class = next(c for c in db.catalog.classes if c.local_name == "Paper")
    store = open_node_store(
        db.bundle, class_iri=paper_class.iri, local_name="Paper"
    )
    gid_table = store.to_table(columns=["_cdb_gid", "title"])
    gids = [int(g) for g in gid_table.column("_cdb_gid").to_pylist()]
    titles_in_order = gid_table.column("title").to_pylist()

    config = HnswConfig(dim=EMBED_DIM, max_elements=max(16, len(gids)))
    index = HnswIndex(config)
    vectors = np.vstack([embed_text(t) for t in titles_in_order])
    index.add(np.asarray(gids, dtype=np.uint64), vectors)

    gid_to_title = dict(zip(gids, titles_in_order, strict=True))
    return index, gid_to_title


# ---------------------------------------------------------------------------
# Step 4: GraphRAG query
# ---------------------------------------------------------------------------


def graphrag_query(
    db: cdb.Database,
    index: HnswIndex,
    gid_to_title: dict[int, str],
    question: str,
    *,
    k: int = 2,
) -> dict[str, object]:
    """Vector-search the question, then graph-expand the seeds via :cites.

    Returns a structured prompt context with the seed papers and their
    cited neighbours. This is the Arrow-shaped payload a host application
    would feed to its LLM.
    """
    qvec = embed_text(question)
    labels, distances = index.search(qvec, k=k)
    seed_gids = [int(g) for g in labels[0].tolist()]
    seed_titles = [gid_to_title[g] for g in seed_gids]

    # Pull the cited neighbours of the seeds in one Tuft query. We keep the
    # query simple (one-hop) to keep the example focused; a real GraphRAG
    # pipeline can chain WITH-clauses or run multiple queries here.
    cited = db.sql(
        "MATCH (src:Paper)-[:cites]->(dst:Paper) RETURN src.title AS seed, dst.title AS cited"
    ).rows()
    cited_for_seeds = [
        row for row in cited if row["seed"] in set(seed_titles)
    ]
    return {
        "question": question,
        "seed_papers": [
            {"gid": g, "title": t, "distance": float(distances[0][i])}
            for i, (g, t) in enumerate(zip(seed_gids, seed_titles, strict=True))
        ],
        "cited": cited_for_seeds,
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def main(out_dir: Path | None = None) -> int:
    out_dir = out_dir or Path("./graphrag-demo.crcl")
    db = build_kg(out_dir)
    index, gid_to_title = build_hnsw_index(db)

    print(f"# CaracalDB GraphRAG substrate demo (bundle: {db.bundle.path})")
    print(f"# {len(gid_to_title)} papers indexed in HNSW (dim={EMBED_DIM})")
    print()

    for question in (
        "transformer language modeling",
        "graph neural network survey",
        "knowledge graph completion",
    ):
        ctx = graphrag_query(db, index, gid_to_title, question, k=2)
        print(f"Q: {ctx['question']}")
        for seed in ctx["seed_papers"]:
            print(f"  seed: {seed['title']!r}  d={seed['distance']:.3f}")
        for c in ctx["cited"]:
            print(f"    cites: {c['cited']!r}")
        print()

    print(
        "# Integration contract:\n"
        "#   - Connection.sql(...).arrow()           : Arrow Table for tools\n"
        "#   - Connection.sql(...).record_batches()  : streaming RecordBatches\n"
        "#   - HnswIndex                             : vector search, gid-keyed\n"
        "# CaracalDB does not bundle an LLM. Bring your own embedder + prompt\n"
        "# template; this script is the substrate, not the framework."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
