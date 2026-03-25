# Quick Start

## SQL

Write the PostgreSQL-like `HumemSQL v0` surface once. HumemDB now applies its current
automatic routing policy internally, keeping writes on SQLite and sending only admitted
analytical reads to DuckDB. HumemDB prefers named `$name` SQL parameters publicly.
Positional DB-API params may still work in narrow cases underneath, but they are not the
main public style to design around.

```python
from humemdb import HumemDB

with HumemDB("app.sqlite3", "analytics.duckdb") as db:
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

    with db.transaction():
        db.executemany(
            "INSERT INTO users (id, name) VALUES ($id, $name)",
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
        )

    result = db.query("SELECT id, name FROM users ORDER BY id")

    print(result.columns)
    print(result.rows)
```

## Cypher

`db.query(...)` now supports both SQL and the current narrow Cypher subset directly.
The common path no longer needs any explicit query-type override or a separate Cypher
helper.

```python
from humemdb import HumemDB

with HumemDB("graph.sqlite3", "graph.duckdb") as db:
    db.query("CREATE (a:User {name: 'Alice'})-[:KNOWS]->(b:User {name: 'Bob'})")

    result = db.query("MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name")

    print(result.rows)
```

## Vector search

Direct vector search is intentionally separate from `db.query(...)`. Use
`search_vectors(...)` for the direct path. Candidate-filtered vector search is now inferred from
the query text itself, using PostgreSQL-like SQL vector ordering or Neo4j-like Cypher
`SEARCH ... VECTOR INDEX ...` syntax.

```python
from humemdb import HumemDB

with HumemDB("vectors.sqlite3") as db:
    assigned_direct_ids = db.insert_vectors(
        [
            {"embedding": [1.0, 0.0], "metadata": {"group": "alpha"}},
            {"embedding": [0.9, 0.1], "metadata": {"group": "alpha"}},
            {"embedding": [0.0, 1.0], "metadata": {"group": "beta"}},
        ]
    )
    db.query("CREATE TABLE docs (id INTEGER PRIMARY KEY, topic TEXT NOT NULL)")
    db.executemany(
        "INSERT INTO docs (topic) VALUES ($topic)",
        [
            {"topic": "alpha"},
            {"topic": "alpha"},
            {"topic": "beta"},
        ],
    )
    doc_rows = db.query("SELECT id, topic FROM docs ORDER BY id")

    direct_result = db.search_vectors(
        [1.0, 0.0],
        top_k=2,
        metric="cosine",
        filters={"group": "alpha"},
    )
    sql_candidate_filtered_result = db.query(
        "SELECT id FROM docs WHERE topic = $topic ORDER BY embedding <=> $query LIMIT 2",
        params={
            "query": [1.0, 0.0],
            "topic": "alpha",
        },
    )

    print(direct_result.rows)
    print(sql_candidate_filtered_result.rows)
    print(doc_rows.rows)
```

Vector result rows are explicit:

- direct search returns rows like `("direct", "", 1, score)`
- SQL candidate-filtered search returns rows like `("sql_row", "docs", 1, score)`
- Cypher candidate-filtered search returns rows like `("graph_node", "", 7, score)`

The direct API auto-assigns ids starting at `1` and returns them from
`insert_vectors(...)`. For the common path, insert record-like rows with an
`embedding` and optional `metadata` map. Explicit direct ids are still allowed when
you need them for import or migration.

If you want vectors to feel attached to rows or nodes at write time, use the narrow SQL
and Cypher frontends directly instead of the raw direct vector API:

```python
from humemdb import HumemDB

with HumemDB("app.sqlite3") as db:
    db.query(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
    )
    db.executemany(
        "INSERT INTO docs (title, embedding) VALUES ($title, $embedding)",
        [
            {"title": "Alpha", "embedding": [1.0, 0.0]},
            {"title": "Beta", "embedding": [0.0, 1.0]},
        ],
    )
    db.query(
        "UPDATE docs SET embedding = $embedding WHERE id = $id",
        params={"embedding": [0.8, 0.2], "id": 2},
    )

    db.query(
        "CREATE (u:User {name: $name, embedding: $embedding})",
        params={"name": "Alice", "embedding": [1.0, 0.0]},
    )
    db.query(
        "CREATE (u:User {name: $name, embedding: $embedding})",
        params={"name": "Bob", "embedding": [0.0, 1.0]},
    )
    db.query(
        "MATCH (u:User {name: 'Bob'}) SET u.embedding = $embedding",
        params={"embedding": [0.8, 0.2]},
    )
```

These write forms are intentionally narrow `v0` subset features. They follow a
PostgreSQL-like row ownership model and a Neo4j-style node-property ownership model
without claiming full pgvector or full Neo4j Cypher compatibility.

Internally, that ownership is stored as `target`, `namespace`, and `target_id` rather than
one shared bare integer id.

## Read the surface boundaries

These examples are deliberately simple because the public `v0` surfaces are still
intentionally narrow. Before building on a query mode, check the guide pages for what
is supported and what HumemDB still rejects explicitly.
