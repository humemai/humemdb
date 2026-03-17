# HumemDB

HumemDB aims to be a multi-model embedded database for OLTP, OLAP, graphs, tables, and
vector search.

- SQLite for OLTP.
- DuckDB for OLAP.
- Cypher support over SQL-backed graph storage.
- Later, LanceDB for vector search.

Today, it starts as a thin Python orchestration layer over embedded engines. The
longer-term goal is a single embedded system that supports standard SQL, Cypher, and
vector search without forcing one engine to do every job.

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

`uv sync` makes the local environment match the project exactly using `pyproject.toml`
and `uv.lock`.

When dependencies change:

```bash
uv lock
uv sync
```

`uv lock` updates the lockfile with exact resolved versions. `uv sync` installs that
exact environment.

## Current Direction

- SQLite is the source of truth.
- DuckDB is the analytical replica.
- Direct writes through the public API go to SQLite, not DuckDB.
- DuckDB now reads SQLite directly first.
- Materialization into DuckDB comes later if benchmarks justify it.
- Initial benchmarks show SQLite stays better for point lookups and small
    filtered reads, while DuckDB wins for scan-heavy analytical queries.
- Public SQL should become a portable HumemSQL subset.
- HumemSQL should feel closer to boring PostgreSQL-style SQL than to
    SQLite-specific or DuckDB-specific dialect features.
- The SQL path should use `sqlglot` to parse PostgreSQL-like SQL and emit
    backend SQL for SQLite and DuckDB.
- String matching is not the plan beyond trivial edge cases.
- A full internal IR is not required at the beginning.
- Introduce a thin internal plan layer only when mixed SQL, graph, and vector
    queries make it necessary.
- Graph data is stored in SQL tables.
- Cypher is a separate frontend and should not be treated as just another SQL
    dialect.
- Vectors can be stored in SQLite.
- For small collections, vector search can start as exact NumPy search over cached
  vectors.
- Routing starts explicit, then becomes automatic.

At the beginning, the caller will likely specify both route and query type.

- Route: `sqlite` or `duckdb`
- Query type: `sql`, `cypher`, or `vector`

Long term, the goal is a more flexible interface where the input can be natural
language, SQL, Cypher, vector search, or another query form, and HumemDB parses it into
an internal representation before routing it.

For SQL specifically, the goal is not to expose SQLite SQL and DuckDB SQL separately.
The goal is to accept a small PostgreSQL-like portable subset first, parse it with
`sqlglot`, and then translate it into backend SQL as needed.

HumemSQL v0 is intentionally small. The currently implemented statement subset
is:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE`

Recursive CTEs and broader PostgreSQL compatibility are intentionally out of scope for
this first translation layer.

HumemDB will likely grow an internal plan or IR layer over time, but that is not the
first move. The current approach is to keep single-mode queries simple, keep translation
and backend emission behind clean boundaries, and only add a thin internal plan layer
when mixed SQL, graph, and vector queries create real composition pressure.

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
- `with db.transaction(route="sqlite"):` and `with db.transaction(route="duckdb"):` are
  supported.
- Public writes target SQLite; DuckDB is read-only from the `HumemDB` API.
- Small to moderate batch writes go through SQLite with `executemany(...)`.

Phase 1 bulk ingest behavior:

- Start with transactional SQLite batch writes for in-memory Python data.
- Keep larger file-based or workload-specific ingestion strategies for later phases.

### Phase 2 - Done

Define HumemSQL and the SQL translation layer.

Status: complete.

- Start with a small PostgreSQL-like portable SQL subset.
- Keep it close to common PostgreSQL-style SQL where practical.
- Avoid engine-specific syntax in the public contract.
- Parse SQL with `sqlglot` instead of string rewriting.
- Reject unsupported SQL clearly instead of guessing.
- HumemSQL v0 currently supports `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `CREATE`.
- HumemSQL v0 does not support recursive CTEs.

### Phase 3 - Done

Use DuckDB over SQLite first.

Status: complete.

- Let DuckDB read SQLite directly.
- Benchmark analytical queries on the direct path.
- Add materialization into DuckDB only if needed.
- Emit backend-specific SQL for SQLite and DuckDB from the HumemSQL layer.

Current benchmark utility:

```bash
python scripts/benchmarks/duckdb_direct_read.py --rows 50000 --repetitions 5
python scripts/benchmarks/duckdb_direct_read.py --rows 1000000 --batch-size 20000
```

The benchmark now compares several query shapes, including point lookup,
filtered range reads, aggregate top-k, and join-heavy aggregation.

Current takeaway:

- SQLite stays better for point lookups and smaller filtered reads.
- DuckDB is already faster on larger analytical aggregates and join-heavy reads.
- Direct DuckDB-over-SQLite reads are the default analytical path for now.
- Materialization is deferred until a future workload proves it is necessary.

### Phase 4

Add graph storage and Cypher support.

- Parse Cypher as its own frontend.
- Lower Cypher into graph and relational operations over graph tables.
- Store nodes and edges in SQLite.
- Use DuckDB for graph analytics when useful.

### Phase 5

Add vector support.

- Start with vectors stored in SQLite plus exact NumPy search.
- Add LanceDB later only if scale requires it.
- Keep vector queries as a separate frontend, not as forced SQL syntax.

### Phase 6

Introduce a thin internal plan layer only when needed.

- Expect HumemDB to eventually need an internal plan or IR layer.
- Do not start with a full IR just because it sounds clean.
- Keep Phase 2 through Phase 5 simple and single-mode where possible.
- Add a small internal plan layer when one user request needs multiple coordinated
    operations across SQL, graph, and vector execution.
- Design earlier phases with clean seams so that later IR work is an insertion, not a
    rewrite from scratch.

### Phase 7

Add automatic routing and lightweight planning.

- Point reads and transactional queries go to SQLite.
- Scans, aggregates, and analytics go to DuckDB.
- Keep routing explainable and overridable.

### Phase 8

Add SQL classification and validation.

- Classify read versus write queries safely.
- Detect simple OLTP versus OLAP query shapes.
- Validate the supported portable SQL subset.
- Keep the first implementation in Python.

### Phase 9

Add natural language support later.

- Start with a small model or parser that maps natural language into structured HumemDB
    requests.
- Compile that structured request into SQL, Cypher, or vector operations.
- Do not make raw natural-language-to-backend-SQL the core contract.

### Later Ingest Work

- Add larger ingestion strategies only when needed.
- Choose ingest paths based on data size, source format, and workload.
- Keep canonical data ingestion centered on SQLite.

## Release Notes

Before pushing `v0.1`, review the licenses of all direct and transitive dependencies and
document any required notices, attributions, or redistribution requirements in the
repository and package metadata.

## Principles

- Correctness before optimization.
- Explicit behavior before smart behavior.
- Each engine should do the job it is good at.
- The system should make routing decisions visible.
