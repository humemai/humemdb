# 03 - Vector Search

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/03_vector_search.py){ .md-button }

## What the Python example does

The script generates a sizable exact-search workload rather than only a handful of
vectors.

- 36,000 generated vectors in one direct vector set
- 16 dimensions per vector
- exact search through `search_vectors(...)`
- direct metadata-filtered vector search
- SQL candidate-filtered vector search over candidate row ids
- Cypher candidate-filtered vector search over candidate node ids
- row-owned and node-owned vector writes through the SQL and Cypher frontends
- cache invalidation after inserting additional vectors

## Why this example matters

HumemDB currently positions the public vector runtime as an exact SQLite-plus-NumPy
baseline. This example exercises tens of thousands of generated points while staying
inside the current public `v0` surface.

## Representative flow

```python
with HumemDB("vectors.sqlite3") as db:
    direct_rows = build_vectors()
    direct_rows[0] = _direct_record(
    direct_rows[0],
    metadata={"cluster": "early", "tier": "primary"},
    )
    db.insert_vectors(direct_rows)

    result = db.search_vectors(
      _embedding(1.0, 0.0, 0.0),
      top_k=5,
      metric="cosine",
    )
```

## What this means in practice

- SQLite is the canonical vector store.
- The canonical vector identity is `target`, `namespace`, and `target_id`.
- The public path is exact, not ANN.
- The current direct vector path auto-assigns direct ids starting at `1`, accepts
  insert-time metadata records, and searches one vector-only set loaded from SQLite.
- Direct vectors are intentionally searched through `search_vectors(...)`, while
  SQL-row and Cypher-node vector search is expressed inside `db.query(...)` text.
- Narrow vector-only categorization comes from metadata equality filters.
- SQL rows and Cypher nodes can keep their ids system-assigned, then resolve candidate ids and reuse the same exact
  ranking path.
- Vector results expose their provenance explicitly, so mixed direct, row-owned, and
  graph-owned vectors do not rely on one shared global id space.
- When vectors belong to rows or nodes, the intended write surface is still SQL or
  Cypher themselves, not extra helper APIs.
- Exact-vector caches are invalidated automatically after SQL writes that touch vector
  storage.

## Metrics

- `cosine`
- `dot`
- `l2`
