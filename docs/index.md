# HumemDB

HumemDB is an embedded multi-model runtime that keeps each engine doing the job it is
already good at.

- SQLite is the canonical OLTP and write path.
- DuckDB is the analytical read path.
- Cypher is lowered over SQLite-backed graph storage.
- Vector search spans exact and ANN paths, with exact NumPy over SQLite-backed
  vector storage today.

The current public surfaces are intentionally explicit:

- `db.query(...)` is the main public surface for explicit SQL and Cypher text.
- direct vector search lives on explicit vector methods such as `search_vectors(...)`.
- candidate-filtered vector search still exists when SQL or Cypher text defines the candidate set,
  but that is a narrower advanced path rather than the main public entry point.
- HumemDB applies routing internally; the public query surface no longer exposes
  `route` or `query_type`.

## Why HumemDB exists

HumemDB is not trying to force OLTP, analytics, graph traversal, and vector retrieval
through one engine just because that sounds elegant. The current runtime starts with a
thin orchestration layer over embedded engines so correctness stays obvious and routing
stays visible.

## What is shipped today

- Portable PostgreSQL-like SQL translation for a narrow `SELECT` / `INSERT` /
  `UPDATE` / `DELETE` / `CREATE` subset, including defended non-recursive CTE,
  window, `UNION ALL`, and `CASE` plus correlated `EXISTS` read shapes.
- SQLite-backed graph tables with generated-parser-backed Cypher `CREATE`, `MATCH`,
  `MATCH ... SET`, and narrow `MATCH ... DELETE` flows.
- Vector search with an exact SQLite-plus-NumPy baseline today and room for indexed
  ANN paths where the benchmark justifies them.
- Explicit transaction control through `db.transaction()` on the canonical SQLite
  write path.

## What is intentionally out of scope for `v0`

- Full PostgreSQL compatibility.
- Full Cypher compatibility.
- Public backend override knobs.
- Indexed ANN as the default vector path.
- A broad internal IR before mixed-mode composition actually requires it.

## Start here

- [Installation](getting-started/installation.md)
- [Quick Start](getting-started/quickstart.md)
- [SQL Example](examples/01_sql_basics.md)
- [Cypher Example](examples/02_cypher_social_graph.md)
- [Vector Example](examples/03_vector_search.md)
