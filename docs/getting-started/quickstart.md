# Quick Start

## SQL

For `query_type="sql"`, write the PostgreSQL-like `HumemSQL v0` surface on both routes.
`route="sqlite"` and `route="duckdb"` choose the backend engine, not a backend-specific
SQL dialect.

```python
from humemdb import HumemDB

with HumemDB("app.sqlite3", "analytics.duckdb") as db:
    db.query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        route="sqlite",
    )

    with db.transaction(route="sqlite"):
        db.executemany(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            [(1, "Alice"), (2, "Bob")],
            route="sqlite",
        )

    result = db.query(
        "SELECT id, name FROM users ORDER BY id",
        route="sqlite",
    )

    print(result.columns)
    print(result.rows)
```

## Cypher

```python
from humemdb import HumemDB

with HumemDB("graph.sqlite3", "graph.duckdb") as db:
    db.query(
        "CREATE (a:User {name: 'Alice'})-[:KNOWS]->(b:User {name: 'Bob'})",
        route="sqlite",
        query_type="cypher",
    )

    result = db.query(
        "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
        route="sqlite",
        query_type="cypher",
    )

    print(result.rows)
```

## Vector search

```python
from humemdb import HumemDB

with HumemDB("vectors.sqlite3") as db:
    db.insert_vectors(
        [
            (1, [1.0, 0.0]),
            (2, [0.9, 0.1]),
            (3, [0.0, 1.0]),
        ]
    )
    db.set_vector_metadata(
        [
            (1, {"group": "alpha"}),
            (2, {"group": "alpha"}),
            (3, {"group": "beta"}),
        ]
    )
    db.query(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, topic TEXT NOT NULL)",
        route="sqlite",
    )
    db.executemany(
        "INSERT INTO docs (id, topic) VALUES (?, ?)",
        [(1, "alpha"), (2, "alpha"), (3, "beta")],
        route="sqlite",
    )

    direct_result = db.search_vectors(
        [1.0, 0.0],
        top_k=2,
        metric="cosine",
        filters={"group": "alpha"},
    )
    sql_scoped_result = db.query(
        "SELECT id FROM docs WHERE topic = ? ORDER BY id",
        route="sqlite",
        query_type="vector",
        params={
            "query": [1.0, 0.0],
            "top_k": 2,
            "scope_query_type": "sql",
            "scope_params": ("alpha",),
        },
    )

    print(direct_result.rows)
    print(sql_scoped_result.rows)
```

If you want vectors to feel attached to rows or nodes at write time, use the narrow SQL
and Cypher frontends directly instead of the raw direct vector API:

```python
from humemdb import HumemDB

with HumemDB("app.sqlite3") as db:
    db.query(
        (
            "CREATE TABLE docs ("
            "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
        ),
        route="sqlite",
    )
    db.executemany(
        "INSERT INTO docs (id, title, embedding) VALUES (?, ?, ?)",
        [
            (1, "Alpha", [1.0, 0.0]),
            (2, "Beta", [0.0, 1.0]),
        ],
        route="sqlite",
    )
    db.query(
        "UPDATE docs SET embedding = ? WHERE id = ?",
        route="sqlite",
        params=([0.8, 0.2], 2),
    )

    db.query(
        "CREATE (u:User {name: $name, embedding: $embedding})",
        route="sqlite",
        query_type="cypher",
        params={"name": "Alice", "embedding": [1.0, 0.0]},
    )
    db.query(
        "CREATE (u:User {name: $name, embedding: $embedding})",
        route="sqlite",
        query_type="cypher",
        params={"name": "Bob", "embedding": [0.0, 1.0]},
    )
    db.query(
        "MATCH (u:User {name: 'Bob'}) SET u.embedding = $embedding",
        route="sqlite",
        query_type="cypher",
        params={"embedding": [0.8, 0.2]},
    )
```

These write forms are intentionally narrow `v0` subset features. They follow a
PostgreSQL-like row ownership model and a Neo4j-style node-property ownership model
without claiming full pgvector or full Neo4j Cypher compatibility.

## Read the surface boundaries

These examples are deliberately simple because the public `v0` surfaces are still
intentionally narrow. Before building on a query mode, check the guide pages for what
is supported and what HumemDB still rejects explicitly.
