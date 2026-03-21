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
            (1, "default", 0, [1.0, 0.0]),
            (2, "default", 0, [0.9, 0.1]),
            (3, "default", 1, [0.0, 1.0]),
        ]
    )

    result = db.search_vectors(
        "default",
        [1.0, 0.0],
        top_k=2,
        metric="cosine",
    )

    print(result.rows)
```

## Read the surface boundaries

These examples are deliberately simple because the public `v0` surfaces are still
intentionally narrow. Before building on a query mode, check the guide pages for what
is supported and what HumemDB still rejects explicitly.
