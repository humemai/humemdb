# Quick Start

## SQL

Write the PostgreSQL-like `HumemSQL v0` surface once. HumemDB now applies its current
automatic routing policy internally, keeping writes on SQLite and sending only admitted
analytical reads to DuckDB. HumemDB prefers named `$name` SQL parameters publicly.
Positional DB-API params may still work in narrow cases underneath, but they are not the
main public style to design around.

Pass one base path to `HumemDB(...)`. HumemDB creates missing backing files on first
use and reopens them on later calls.

The stable package-level imports are:

```python
from humemdb import HumemDB, QueryResult, translate_sql
```

```python
from humemdb import HumemDB

with HumemDB("app") as db:
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

with HumemDB("graph") as db:
    db.query("CREATE (a:User {name: 'Alice'})-[:KNOWS]->(b:User {name: 'Bob'})")

    result = db.query("MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name")

    print(result.rows)
```

## Vector search

Direct vector search is intentionally separate from `db.query(...)`. Use
`search_vectors(...)` for the direct path. Candidate-filtered vector search is now inferred from
the query text itself, using PostgreSQL-like SQL vector ordering or Neo4j-like Cypher
`CALL db.index.vector.queryNodes(...)` syntax, including optional post-call `MATCH ...`
and `WHERE ...` filtering before `RETURN`.

```python
from humemdb import HumemDB

with HumemDB("vectors") as db:
    assigned_direct_ids = db.insert_vectors(
        [
            {"embedding": [1.0, 0.0], "metadata": {"group": "alpha"}},
            {"embedding": [0.9, 0.1], "metadata": {"group": "alpha"}},
            {"embedding": [0.0, 1.0], "metadata": {"group": "beta"}},
        ]
    )
    direct_index_state = db.build_vector_index()
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
    sql_index_state = db.query(
        "CREATE INDEX docs_embedding_idx ON docs USING ivfpq "
        "(embedding vector_cosine_ops)"
    )
    cypher_index_state = db.query("SHOW VECTOR INDEXES")

    print(direct_result.rows)
    print(sql_candidate_filtered_result.rows)
    print(doc_rows.rows)
    print(direct_index_state)
    print(sql_index_state.rows)
    print(cypher_index_state.rows)
```

Vector result rows are explicit:

- direct search returns rows like `("direct", "", 1, score)`
- SQL candidate-filtered search returns rows like `("sql_row", "docs", 1, score)`
- Cypher `CALL db.index.vector.queryNodes(...)` returns projected rows like `(7, score)`

The direct API auto-assigns ids starting at `1` and returns them from
`insert_vectors(...)`. For the common path, insert record-like rows with an
`embedding` and optional `metadata` map. Explicit direct ids are still allowed when
you need them for import or migration.

If you want vectors to feel attached to rows or nodes at write time, use the narrow SQL
and Cypher frontends directly instead of the raw direct vector API:

```python
from humemdb import HumemDB

with HumemDB("app") as db:
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

The indexed hot/cold runtime also has a narrow public lifecycle surface:

- direct Python: `build_vector_index()`, `inspect_vector_index()`,
  `refresh_vector_index()`, `drop_vector_index()`, and `await_vector_index_refresh()`
- SQL: `CREATE INDEX docs_embedding_idx ON docs USING ivfpq (embedding
    vector_cosine_ops)` or `CREATE INDEX docs_dot_idx ON docs USING ivfpq (embedding
    vector_ip_ops)`, `ALTER VECTOR INDEX docs_embedding_idx PAUSE MAINTENANCE`, `ALTER
    VECTOR INDEX docs_embedding_idx RESUME MAINTENANCE`, `REFRESH VECTOR INDEX
    docs_embedding_idx`, `REBUILD VECTOR INDEX docs_embedding_idx`, `DROP INDEX
    docs_embedding_idx`, and `SELECT * FROM humemdb_vector_indexes`
- Cypher: `CREATE VECTOR INDEX profile_embedding_idx IF NOT EXISTS FOR (p:Profile) ON
    (p.embedding) OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}`,
    `ALTER VECTOR INDEX profile_embedding_idx PAUSE MAINTENANCE`, `ALTER VECTOR INDEX
    profile_embedding_idx RESUME MAINTENANCE`, `REFRESH VECTOR INDEX
    profile_embedding_idx`, `REBUILD VECTOR INDEX profile_embedding_idx`, `SHOW VECTOR
    INDEXES`, and `DROP VECTOR INDEX profile_embedding_idx`

Internally, that ownership is stored as `target`, `namespace`, and `target_id` rather than
one shared bare integer id.

## CSV import

HumemDB also ships a CSV ingest surface for relational and graph-first workflows.

```python
from humemdb import HumemDB

with HumemDB("ingest") as db:
    db.import_table("users", "users.csv")
    db.import_nodes("User", "people.csv", id_column="id")
    db.import_edges(
        "KNOWS",
        "knows.csv",
        source_id_column="from_id",
        target_id_column="to_id",
    )
```

These import helpers are the current public file-ingest path. They stay on the canonical
SQLite write route and wrap chunked writes in explicit rollback-safe transactions.

## Read the surface boundaries

These examples are deliberately simple because the public `v0` surfaces are still
intentionally narrow. Before building on a query mode, check the guide pages for what
is supported and what HumemDB still rejects explicitly.
