# 03 - Vector Search

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/03_vector_search.py){ .md-button }

## What the Python example does

The script exercises a mixed vector workload rather than only one standalone direct
vector set.

- 60,009 direct vectors in one exact-search set
- 8-dimensional embeddings reused across direct, SQL-owned, and graph-owned vectors
- direct vector inserts with explicit ids and inline metadata
- metadata updates through `set_vector_metadata(...)`
- exact search through `search_vectors(...)`
- SQL candidate-filtered vector search over row-owned embeddings
- Cypher candidate-filtered vector search over graph-node embeddings
- SQL `UPDATE` and `DELETE` flows that invalidate vector state
- Cypher `SET`, `DELETE`, and `DETACH DELETE` flows that keep graph-owned vectors in sync
- step-by-step timing printed from the script itself

## Why this example matters

HumemDB currently positions the public vector runtime as an exact SQLite-plus-NumPy
baseline. This example shows how that exact runtime behaves across all three public
vector ownership models while staying inside the current public `v0` surface.

## Representative flow

```python
with HumemDB("vectors") as db:
    db.insert_vectors(build_direct_rows())
    db.set_vector_metadata([(1001, {"fresh": True})])

    result = db.search_vectors(
        _embedding(1.0, 0.12, 0.0),
        top_k=4,
        metric="cosine",
    )
```

## What this means in practice

- SQLite is the canonical vector store.
- The canonical vector identity is `target`, `namespace`, and `target_id`.
- The public path is exact, not ANN.
- The current direct vector path can auto-assign ids or accept explicit import-style
  ids, and it accepts insert-time metadata records.
- Direct vectors are intentionally searched through `search_vectors(...)`, while
  SQL-row and Cypher-node vector search is expressed inside `db.query(...)` text.
- Narrow vector-only categorization comes from metadata equality filters.
- SQL rows and Cypher nodes can keep their ids system-assigned, then resolve candidate
  ids and reuse the same exact ranking path.
- Vector results expose their provenance explicitly, so mixed direct, row-owned, and
  graph-owned vectors do not rely on one shared global id space.
- When vectors belong to rows or nodes, the intended write surface is still SQL or
  Cypher themselves, not extra helper APIs.
- Exact-vector caches are invalidated automatically after direct inserts and after SQL
  or Cypher writes that touch vector storage.

## Metrics

- `cosine`
- `dot`
- `l2`
