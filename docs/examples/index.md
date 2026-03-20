# Examples

HumemDB ships small but real Python examples in the repository `examples/` directory,
and the docs pages in this section describe those exact files.

Like the ArcadeDB docs, each page is intended to be a companion to the real Python
script, not a disconnected hand-written snippet.

Current shipped examples:

- [01 - SQL Basics](01_sql_basics.md): generates 2,000 users and 50,000 orders, then runs both SQLite and DuckDB queries.
- [02 - Cypher Social Graph](02_cypher_social_graph.md): generates a few hundred graph nodes and edges with richer properties.
- [03 - Vector Search](03_vector_search.md): generates 36,000 vectors across two collections and exercises exact search.

Run them locally from the repository root:

```bash
uv sync
uv run python scripts/release/run_examples.py
```

Or run individual files directly:

```bash
uv run python examples/01_sql_basics.py
uv run python examples/02_cypher_social_graph.py
uv run python examples/03_vector_search.py
```
