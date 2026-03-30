# HumemDB

Multi-model embedded data orchestration for SQL, Cypher, and vector search.

[![Docs](https://img.shields.io/badge/docs-humem.ai-0f766e)](https://docs.humem.ai/humemdb/)
[![Test](https://github.com/humemai/humemdb/actions/workflows/test.yml/badge.svg)](https://github.com/humemai/humemdb/actions/workflows/test.yml)
[![Examples](https://github.com/humemai/humemdb/actions/workflows/test-examples.yml/badge.svg)](https://github.com/humemai/humemdb/actions/workflows/test-examples.yml)
[![Build Docs](https://github.com/humemai/humemdb/actions/workflows/build-docs.yml/badge.svg)](https://github.com/humemai/humemdb/actions/workflows/build-docs.yml)
[![Publish PyPI](https://github.com/humemai/humemdb/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/humemai/humemdb/actions/workflows/publish-pypi.yml)

---

## ✨ What HumemDB is

HumemDB is a Python-first embedded runtime that keeps each engine doing the job it is
already good at.

- SQLite for OLTP.
- DuckDB for OLAP.
- Cypher support over SQL-backed graph storage.
- Exact vector search, with the default runtime path starting from an exact
    SQLite-plus-NumPy baseline today.
- LanceDB later where the benchmark justifies an indexed ANN path.

Today, it starts as a thin Python orchestration layer over embedded engines. The
longer-term goal is a single embedded system that supports standard SQL, Cypher, and
vector search without forcing one engine to do every job.

The goal is not to force SQL, graph, and vector workloads through one backend just
because that sounds clean. The goal is a simple, explicit orchestration layer with
clear routing and defensible tradeoffs.

## ✅ Current status

HumemDB already ships a real `v0` surface built around explicit SQL/Cypher text-query
frontends plus a separate direct-vector API:

- `HumemSQL v0`
- `HumemCypher v0`
- CSV-backed `import_table(...)`, `import_nodes(...)`, and `import_edges(...)`
- direct vector methods for vector-only workflows

Current behavior is intentionally explicit:

- `db.query(...)` is the text-query surface for SQL, Cypher, and language-level vector
  search
- direct vector search lives on methods such as `search_vectors(...)`
- candidate-filtered vector search is expressed in SQL/Cypher syntax when SQL rows or graph nodes
  define the candidate set first
- `db.query(...)` infers a conservative execution route internally; the public query
  surface no longer exposes a backend override
- `query_type` is no longer part of the public `db.query(...)` surface
- Writes go to SQLite
- broad analytical SQL can route to DuckDB; ambiguous SQL and current Cypher reads stay
  on SQLite by default
- Vector search starts from the exact baseline path today

## Public API at a glance

Import the stable surface from `humemdb`:

```python
from humemdb import HumemDB, QueryResult, RuntimeThreadBudget, translate_sql
```

Main public entry points today:

- `HumemDB(base_path, *, preload_vectors=False)` creates or reopens one embedded
  database pair from a single base path
- `db.query(text, *, params=None)` executes SQL, Cypher, or language-level vector query
  text
- `db.executemany(text, params_seq)` handles SQLite-backed batch writes
- `db.transaction()`, `db.begin()`, `db.commit()`, and `db.rollback()` control the
- `db.import_table(...)`, `db.import_nodes(...)`, and `db.import_edges(...)` provide the
  current CSV ingest surface
  canonical SQLite write transaction
- `db.insert_vectors(...)`, `db.search_vectors(...)`, and `db.set_vector_metadata(...)`
  provide the direct-vector surface
- `db.preload_vectors()` and `db.vectors_cached()` expose the current exact-vector cache
  state

`QueryResult` is the normalized return shape for queries and exposes `rows`, `columns`,
`route`, `query_type`, `rowcount`, and `first()`.

## 🔗 Documentation

- [HumemDB docs](https://docs.humem.ai/humemdb/)

## Install

HumemDB supports Python 3.10 and newer.

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

## Libraries HumemDB relies on

HumemDB is a pure Python orchestration layer, but it relies on a small set of core
Python libraries and embedded engines:

- `sqlite3` from the Python standard library for the canonical local write path.
- `duckdb` for analytical reads over the SQLite-backed source-of-truth database.
- `numpy` for the exact in-memory vector search baseline.
- `sqlglot[c]` for the current PostgreSQL-like SQL translation layer.
- `lancedb` for benchmark work and future indexed ANN paths.
- `threadpoolctl` for thread-pool coordination around compute-heavy dependencies.

Those dependencies are part of the public runtime story. HumemDB does not try to hide
them behind a fake "single engine" narrative.

## 🧠 What is supported today

### SQL

- PostgreSQL-like portable subset translated with `sqlglot`
- callers write `HumemSQL v0`; HumemDB applies a conservative internal classifier so
  broad analytical SQL may route to DuckDB while writes and selective reads stay on
  SQLite
- public SQL params use named `$name` placeholders with mapping-style params
- backend-specific SQLite or DuckDB SQL is not part of the supported public contract
- statement coverage: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`
- defended read shapes now include non-recursive CTEs, `UNION ALL`, window functions,
  and `CASE` plus correlated `EXISTS` in the current portable subset
- recursive CTEs intentionally unsupported in `v0`

### Cypher

- generated-parser-backed narrow `CREATE`, `MATCH`, `MATCH ... SET`, and narrow
  `MATCH ... DELETE` flows
- labeled nodes, single directed relationships, relationship aliases, reverse-edge
  matches, anonymous relationship endpoints, and narrow relationship type alternation
- simple scalar comparisons plus narrow top-level boolean regrouping in `WHERE`
- string predicates with `STARTS WITH`, `ENDS WITH`, and `CONTAINS`
- property null predicates with `IS NULL` and `IS NOT NULL`
- `DISTINCT`, `ORDER BY`, `SKIP` or `OFFSET`, and `LIMIT` on the admitted read subset
- row ordering is only guaranteed when the query includes an explicit `ORDER BY`
- `SKIP` or `OFFSET` without `ORDER BY` is allowed, but unordered pagination is not
  a stable contract
- public Cypher execution currently stays on SQLite

### Vector

- SQLite-backed vector storage
- canonical vector identity uses `target`, `namespace`, and `target_id`
- exact NumPy baseline path
- row-filtered vector search through SQL candidate queries
- node-filtered vector search through Cypher candidate queries
- direct vector methods for vector-only use, with narrow metadata equality filters
- SQL INSERTs and Cypher CREATEs can carry vector values into the canonical store
- benchmark path toward indexed ANN where justified

Direct vector search returns explicit provenance columns so mixed direct, SQL-owned,
and graph-owned vectors can coexist safely in one SQLite database.

Direct vector search is intentionally separate from `db.query(...)`. Use
`search_vectors(...)` for the direct path, and use SQL or Cypher text through
`db.query(...)` when those language surfaces define the candidate set first.
That path exists, but it is not meant to be the main public starting point for
HumemDB.

## ⚡ Quick example

Pass one base path to `HumemDB(...)`. HumemDB creates missing backing files on first use
and reopens them on later calls.

```python
from humemdb import HumemDB

with HumemDB("app") as db:
    db.query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")

    with db.transaction():
        db.query(
            "INSERT INTO users (name) VALUES ($name)",
            params={"name": "Alice"},
        )

    result = db.query("SELECT id, name FROM users")

    print(result.rows)
```

## 📥 Quick ingest example

```python
from humemdb import HumemDB

with HumemDB("ingest") as db:
    db.query(
        "CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
    )

    imported_rows = db.import_table("accounts", "accounts.csv")
    print(imported_rows)
```

More examples live in [examples/](examples/) and in the docs site, including
[`examples/05_csv_ingest.py`](examples/05_csv_ingest.py) for the full relational plus
graph ingestion flow.

## 🔗 Quick links

- Docs: [docs.humem.ai/humemdb](https://docs.humem.ai/humemdb/)
- Repository: [github.com/humemai/humemdb](https://github.com/humemai/humemdb)
- Issues: [github.com/humemai/humemdb/issues](https://github.com/humemai/humemdb/issues)
- Internal roadmap notes: [things-to-do.md](things-to-do.md)

## 📦 Packaging

HumemDB itself is a pure Python package today. It does not ship platform-specific
project binaries, even though some dependencies may install native wheels on the user
side.

## 🗺️ Planning

Detailed internal roadmap notes now live in `things-to-do.md` instead of this README.

## 📄 License

HumemDB's own source code is licensed under MIT. See [LICENSE](LICENSE).

Third-party dependencies keep their own licenses. Installing HumemDB may also install
third-party Python packages and, in some cases, their native wheels. Those components
are not relicensed under MIT just because HumemDB depends on them.

For the concrete dependency set, see [pyproject.toml](pyproject.toml) and
[uv.lock](uv.lock).
