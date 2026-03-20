# 03 - Vector Search

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/03_vector_search.py){ .md-button }

This page documents the real repository example `examples/03_vector_search.py`.

## What the Python example does

The script generates a realer exact-search workload rather than only a handful of
vectors.

- 24,000 vectors in the `default` collection
- 12,000 vectors in the `archive` collection
- 16 dimensions per vector
- multiple logical buckets
- exact search through `search_vectors(...)`
- exact search through raw `query_type="vector"`
- cache invalidation after inserting additional vectors

## Why this example matters

HumemDB currently positions the public vector runtime as an exact SQLite-plus-NumPy
baseline. That claim is not very useful if the only shipped example uses three points.
This example exercises tens of thousands of generated points while staying inside the
current public `v0` surface.

## Representative flow

```python
with HumemDB("vectors.sqlite3") as db:
    db.insert_vectors(build_default_vectors())
    db.insert_vectors(build_archive_vectors())

    result = db.search_vectors(
        "default",
        _embedding(1.0, 0.0, 0.0),
        top_k=5,
        metric="cosine",
    )
```

## What this means in practice

- SQLite is the canonical vector store.
- The public path is exact, not ANN.
- Bucket filters can narrow the candidate set before scoring.
- Collection caches are invalidated automatically after SQL writes that touch vector
  storage.

## Metrics

- `cosine`
- `dot`
- `l2`
