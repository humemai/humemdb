# HumemDB Examples

These examples are intentionally representative rather than minimal. They exercise the
current public `v0` surfaces with larger relational, graph, and vector workloads while
still running from a clean checkout.

Run one example from the repository root:

```bash
python examples/01_sql_basics.py
```

Run all example scripts in sequence:

```bash
python scripts/release/run_examples.py
```

Current examples:

- `01_sql_basics.py`: 5,000 users, 50,000 orders, 150,000 order items, transactional
  inserts, selective reads, multi-join reads, windowed analytics, and per-step timing.
- `02_cypher_social_graph.py`: more than 50,000 graph nodes and more than 100,000
  edges across multiple labels and relationship families, plus per-step timing.
- `03_vector_search.py`: more than 60,000 direct vectors plus SQL-owned and graph-owned
  vector flows, with metadata filters, invalidation checks, and per-step timing.
