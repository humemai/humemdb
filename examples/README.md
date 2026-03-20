# HumemDB Examples

These examples are intentionally small. They exercise the current public `v0` surfaces
without pulling in large datasets or extra infrastructure.

Run one example from the repository root:

```bash
uv sync
uv run python examples/01_sql_basics.py
```

Run all example scripts in sequence:

```bash
uv sync
uv run python scripts/release/run_examples.py
```

Current examples:

- `01_sql_basics.py`: 2,000 users, 50,000 orders, transactional inserts, joins, filters, and DuckDB analytical reads.
- `02_cypher_social_graph.py`: hundreds of generated nodes and edges with richer properties, named params, reverse-edge reads, and ordered graph queries.
- `03_vector_search.py`: 36,000 generated vectors across two collections, bucket filtering, direct vector queries, and cache-refresh behavior after inserts.
