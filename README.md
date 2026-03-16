# humemdb

HumemDB aims to be a multi-model embedded database for OLTP, OLAP, graphs,
tables, and vector search.

- SQLite for OLTP.
- DuckDB for OLAP.
- Cypher support over SQL-backed graph storage.
- Later, LanceDB for vector search.

Today, it starts as a thin Python orchestration layer over embedded engines.
The longer-term goal is a single embedded system that supports standard SQL,
Cypher, and vector search without forcing one engine to do every job.

## Install

Install from source:

```bash
uv pip install .
```

Install in editable mode for development:

```bash
uv pip install -e .
```

For contributors, use:

```bash
uv sync
```

`uv sync` makes the local environment match the project exactly using `pyproject.toml` and `uv.lock`.

When dependencies change:

```bash
uv lock
uv sync
```

`uv lock` updates the lockfile with exact resolved versions. `uv sync` installs that exact environment.

## Current Direction

- SQLite is the source of truth.
- DuckDB is the analytical replica.
- Direct writes through the public API go to SQLite, not DuckDB.
- DuckDB should first read SQLite directly.
- Materialization into DuckDB comes later if benchmarks justify it.
- Public SQL should become a portable HumemDB SQL subset.
- HumemDB SQL should feel closer to boring PostgreSQL-style SQL than to SQLite-specific or DuckDB-specific dialect features.
- The SQL layer should start as a small Python implementation.
- Only move toward heavier parser infrastructure or native acceleration if profiling proves it is needed.
- Graph data is stored in SQL tables.
- Cypher is parsed and translated into SQL.
- Vectors can be stored in SQLite.
- For small collections, vector search can start as exact NumPy search over cached vectors.
- Routing starts explicit, then becomes automatic.

At the beginning, the caller will likely specify both route and query type.

- Route: `sqlite` or `duckdb`
- Query type: `sql`, `cypher`, or `vector`

Long term, the goal is a more flexible interface where the input can be natural language, SQL, Cypher, vector search, or another query form, and HumemDB parses it into an internal representation before routing it.

For SQL specifically, the goal is not to expose SQLite SQL and DuckDB SQL separately. The goal is to accept a small portable SQL subset first, then gradually add validation, classification, and translation as needed.

This is the simplest model that keeps writes correct and analytics fast.

## Example

```python
from humemdb import HumemDB

with HumemDB("app.sqlite3", "analytics.duckdb") as db:
    db.query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        route="sqlite",
    )

    with db.transaction(route="sqlite"):
        db.query(
            "INSERT INTO users (name) VALUES (?)",
            route="sqlite",
            params=("Alice",),
        )

    result = db.query(
        "SELECT id, name FROM users",
        route="sqlite",
    )

    print(result.rows)
```

## Phases

### Phase 1 - Done

Build the minimal router.

Status: complete.

- Keep the system embedded and in-process.
- Open SQLite and DuckDB connections.
- Support explicit routing: `sqlite` and `duckdb`.
- Send writes to SQLite.

Initial package shape:

```text
src/humemdb/
    __init__.py
    db.py
    engines.py
    types.py
tests/
    test_db.py
```

Initial classes:

- `HumemDB`: main in-process entry point.
- `SQLiteEngine`: wrapper around the Python `sqlite3` connection.
- `DuckDBEngine`: wrapper around the Python `duckdb` connection.
- `QueryResult`: normalized result object returned by queries.

Initial methods:

- `HumemDB.__init__(sqlite_path, duckdb_path=None)`
- `HumemDB.query(text, *, route, query_type="sql", params=None)`
- `HumemDB.executemany(text, params_seq, *, route, query_type="sql")`
- `HumemDB.begin(route=...)`
- `HumemDB.commit(route=...)`
- `HumemDB.rollback(route=...)`
- `HumemDB.transaction(route=...)`
- `HumemDB.close()`
- `SQLiteEngine.execute(text, params=None)`
- `DuckDBEngine.execute(text, params=None)`

Phase 1 transaction behavior:

- Writes auto-commit unless they are inside an explicit transaction block.
- `with db.transaction(route="sqlite"):` and `with db.transaction(route="duckdb"):` are supported.
- Public writes target SQLite; DuckDB is read-only from the `HumemDB` API.
- Small to moderate batch writes go through SQLite with `executemany(...)`.

Phase 1 bulk ingest behavior:

- Start with transactional SQLite batch writes for in-memory Python data.
- Keep larger file-based or workload-specific ingestion strategies for later phases.

### Phase 2

Define HumemDB SQL.

- Start with a small portable SQL subset.
- Keep it close to common PostgreSQL-style SQL where practical.
- Avoid engine-specific syntax in the public contract.
- Reject unsupported SQL clearly instead of guessing.

### Phase 3

Use DuckDB over SQLite first.

- Let DuckDB read SQLite directly.
- Benchmark analytical queries on the direct path.
- Add materialization into DuckDB only if needed.

### Phase 4

Add graph storage and Cypher support.

- Parse Cypher queries.
- Translate Cypher to SQL over graph tables.
- Store nodes and edges in SQLite.
- Use DuckDB for graph analytics when useful.

### Phase 5

Add automatic routing.

- Point reads and transactional queries go to SQLite.
- Scans, aggregates, and analytics go to DuckDB.
- Keep routing explainable and overridable.

### Phase 6

Add SQL classification and validation.

- Classify read versus write queries safely.
- Detect simple OLTP versus OLAP query shapes.
- Validate the supported portable SQL subset.
- Keep using lightweight parsing before introducing a full SQL AST.
- Keep the first implementation in Python.

### Phase 7

Add SQL translation only when needed.

- Introduce an AST-based approach instead of string rewriting.
- Normalize the portable HumemDB SQL subset into backend-specific SQL.
- Keep engine-specific escape hatches explicit.
- Stay in Python unless profiling shows the translator itself is a bottleneck.

### Phase 8

Add vector support later.

- Start with vectors stored in SQLite plus exact NumPy search.
- Add LanceDB later only if scale requires it.

### Later Ingest Work

- Add larger ingestion strategies only when needed.
- Choose ingest paths based on data size, source format, and workload.
- Keep canonical data ingestion centered on SQLite.

## Principles

- Correctness before optimization.
- Explicit behavior before smart behavior.
- Each engine should do the job it is good at.
- The system should make routing decisions visible.
