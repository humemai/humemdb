# test_db.py

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/tests/test_db.py){ .md-button }

## What this test file covers

`test_db.py` is the broad public-surface regression suite for HumemDB.

It exercises the `HumemDB` entry point across SQL, Cypher, DuckDB routing,
transactions, environment-driven runtime settings, and vector search integration.

## Main areas covered

- PostgreSQL-like SQL translation such as casts and `ILIKE`
- unsupported SQL rejection for out-of-scope syntax
- SQLite and DuckDB route behavior
- transaction commit and rollback behavior
- Cypher `CREATE`, `MATCH`, relationship traversal, filtering, ordering, and limits
- direct-vector convenience calls through the public API
- SQL-owned vector writes and candidate-filtered vector search
- Cypher-owned vector writes and candidate-filtered vector search
- cache invalidation and preload behavior for vector search

## Why this test file exists

This file defends the top-level runtime contract. When HumemDB behavior changes across
query translation, routing, or public query surfaces, this suite is meant to catch the
regression from the user-facing API boundary.

## Representative themes

```python
with HumemDB("app.sqlite3", "analytics.duckdb") as db:
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    db.query("CREATE (u:User {name: 'Alice'})")

    result = db.query(
        "SELECT id FROM docs WHERE topic = $topic ORDER BY embedding <=> $query LIMIT 3",
        params={
            "query": [1.0, 0.0],
            "topic": "alpha",
        },
    )
```

## Current emphasis

- defend the documented `v0` public behavior
- keep SQLite as the source of truth for writes
- verify DuckDB stays read-only through the public API
- ensure vector behavior remains aligned across direct, SQL, and Cypher surfaces