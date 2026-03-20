# 01 - SQL Basics

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/01_sql_basics.py){ .md-button }

This page documents the real repository example `examples/01_sql_basics.py`.

## What the Python example does

The script generates a moderately sized embedded workload instead of a tiny hand-made
toy dataset.

- 2,000 `users`
- 50,000 `orders`
- 2 SQLite tables
- transactional bulk inserts through the public API
- SQLite filtered OLTP-style reads
- SQLite joined reads
- DuckDB grouped analytical reads over the SQLite-backed tables

## Why this example exists

HumemDB is supposed to route small transactional work and broader analytical work to
different engines explicitly. This example demonstrates that split using the actual
public Python API.

## Main operations covered

- `CREATE TABLE` routed to SQLite
- `executemany(...)` inside an explicit transaction
- `ILIKE` translation on the SQLite route
- SQLite join queries for row-oriented reads
- DuckDB grouped aggregate queries for broader scans

## Representative flow

```python
with HumemDB("app.sqlite3", "analytics.duckdb") as db:
    db.query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, segment TEXT NOT NULL, city TEXT NOT NULL)",
        route="sqlite",
    )
    db.query(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, status TEXT NOT NULL, total_cents INTEGER NOT NULL)",
        route="sqlite",
    )

    with db.transaction(route="sqlite"):
        db.executemany(..., users, route="sqlite")
        db.executemany(..., orders, route="sqlite")

    sqlite_rows = db.query(..., route="sqlite")
    duckdb_rollup = db.query(..., route="duckdb")
```

## Supported statement kinds

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE`

## Explicit rejections

- recursive CTEs
- unsupported PostgreSQL syntax outside the current tested surface
- direct public writes routed to DuckDB
