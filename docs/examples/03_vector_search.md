# 03 - Vector Search

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/03_vector_search.py){ .md-button }

## What the Python example does

The script generates a sizable exact-search workload rather than only a handful of
vectors.

- 36,000 generated vectors in one direct vector set
- 16 dimensions per vector
- exact search through `search_vectors(...)`
- direct metadata-filtered vector search
- exact search through raw `query_type="vector"`
- SQL-scoped vector search over candidate row ids
- Cypher-scoped vector search over candidate node ids
- row-owned and node-owned vector writes through the SQL and Cypher frontends
- cache invalidation after inserting additional vectors

## Why this example matters

HumemDB currently positions the public vector runtime as an exact SQLite-plus-NumPy
baseline. This example exercises tens of thousands of generated points while staying
inside the current public `v0` surface.

## Representative flow

```python
with HumemDB("vectors.sqlite3") as db:
    db.insert_vectors(build_vectors())

    result = db.search_vectors(
        _embedding(1.0, 0.0, 0.0),
        top_k=5,
        metric="cosine",
    )
```

## What this means in practice

- SQLite is the canonical vector store.
- The public path is exact, not ANN.
- The current direct vector path searches one vector-only set loaded from SQLite.
- Narrow vector-only categorization comes from metadata equality filters.
- SQL and Cypher vector scope both resolve candidate ids, then reuse the same exact
  ranking path.
- When vectors belong to rows or nodes, the intended write surface is still SQL or
  Cypher themselves, not extra helper APIs.
- Exact-vector caches are invalidated automatically after SQL writes that touch vector
  storage.

## Metrics

- `cosine`
- `dot`
- `l2`
