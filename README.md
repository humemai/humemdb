# HumemDB

Multi-model embedded data orchestration for SQL, Cypher, and vector search.

[![Docs](https://img.shields.io/badge/docs-humem.ai-0f766e)](https://docs.humem.ai/humemdb/)
[![Test](https://github.com/humemai/humemdb/actions/workflows/test.yml/badge.svg)](https://github.com/humemai/humemdb/actions/workflows/test.yml)
[![Build Docs](https://github.com/humemai/humemdb/actions/workflows/build-docs.yml/badge.svg)](https://github.com/humemai/humemdb/actions/workflows/build-docs.yml)
[![Publish PyPI](https://github.com/humemai/humemdb/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/humemai/humemdb/actions/workflows/publish-pypi.yml)

---

## ✨ What HumemDB is

HumemDB is a Python-first embedded runtime that keeps each engine doing the job it is
already good at.

- SQLite for OLTP.
- DuckDB for OLAP.
- Cypher support over SQL-backed graph storage.
- Exact and ANN vector search, with the default runtime path starting from an exact
  SQLite-plus-NumPy baseline today.
- LanceDB later where the benchmark justifies an indexed ANN path.

Today, it starts as a thin Python orchestration layer over embedded engines. The
longer-term goal is a single embedded system that supports standard SQL, Cypher, and
vector search without forcing one engine to do every job.

The goal is not to force SQL, graph, and vector workloads through one backend just
because that sounds clean. The goal is a simple, explicit orchestration layer with
clear routing and defensible tradeoffs.

## ✅ Current status

HumemDB already ships a real `v0` surface for three query modes:

- `HumemSQL v0`
- `HumemCypher v0`
- `HumemVector v0`

Current behavior is intentionally explicit:

- Route: `sqlite` or `duckdb`
- Query type: `sql`, `cypher`, or `vector`
- Writes go to SQLite
- DuckDB is the analytical read path
- Vector search starts from the exact baseline path today

## 🔗 Documentation

- [HumemDB docs](https://docs.humem.ai/humemdb/)

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

## 🧠 What is supported today

### SQL

- PostgreSQL-like portable subset translated with `sqlglot`
- statement coverage: `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `CREATE`
- recursive CTEs intentionally unsupported in `v0`

### Cypher

- narrow `CREATE` and `MATCH` flows
- labeled nodes and single directed relationships
- relationship aliases and reverse-edge matches
- simple `WHERE ... AND ...` equality filtering
- `ORDER BY` and `LIMIT`

### Vector

- SQLite-backed vector storage
- exact NumPy baseline path
- bucket filtering
- benchmark path toward indexed ANN where justified

## ⚡ Quick example

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

More examples live in the docs site.

## 🔗 Quick links

- Docs: [docs.humem.ai/humemdb](https://docs.humem.ai/humemdb/)
- Repository: [github.com/humemai/humemdb](https://github.com/humemai/humemdb)
- Issues: [github.com/humemai/humemdb/issues](https://github.com/humemai/humemdb/issues)
- Internal phase tracking: [things-to-do.md](things-to-do.md)

## 📦 Packaging

HumemDB itself is a pure Python package today. It does not ship platform-specific
project binaries, even though some dependencies may install native wheels on the user
side.

## 🗺️ Planning

The detailed internal roadmap and phase tracking now live in `things-to-do.md` instead
of this README.

## 📄 License

MIT.
