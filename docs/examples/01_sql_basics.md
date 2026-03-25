# 01 - SQL Basics

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/01_sql_basics.py){ .md-button }

## What the Python example does

The script builds a larger commerce-style workload instead of a tiny two-table toy
dataset.

- 5,000 `users`
- 128 `products`
- 50,000 `orders`
- 150,000 `order_items`
- 4 relational tables
- transactional bulk inserts through the public API
- SQLite filtered OLTP-style reads with correlated `EXISTS`
- SQLite multi-join reads across users, orders, products, and order items
- DuckDB analytical reads with non-recursive `WITH` and window functions
- `UNION ALL` reporting over grouped product totals
- step-by-step timing printed from the script itself

## Why this example exists

HumemDB is supposed to handle transactional row work and broader analytical scans
through one public Python API. This example demonstrates that split using a schema
that is big enough to surface realistic join, ranking, and reporting shapes.

## Main operations covered

- `CREATE TABLE` routed to SQLite
- `executemany(...)` inside an explicit transaction
- `ILIKE` translation on the SQLite route
- correlated `EXISTS` subqueries
- SQLite join queries for row-oriented reads
- DuckDB grouped aggregate queries for broader scans
- non-recursive CTEs
- window functions through `ROW_NUMBER()`
- `UNION ALL`
- per-step elapsed timing output

## Representative flow

```python
with HumemDB("app.sqlite3", "analytics.duckdb") as db:
    db.query("CREATE TABLE users (...)")
    db.query("CREATE TABLE products (...)")
    db.query("CREATE TABLE orders (...)")
    db.query("CREATE TABLE order_items (...)")

    with db.transaction():
        db.executemany(..., users)
        db.executemany(..., products)
        db.executemany(..., orders)
        db.executemany(..., order_items)

    sqlite_rows = db.query(...)
    duckdb_rollup = db.query(...)
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
- direct DuckDB write execution through internal engine paths
