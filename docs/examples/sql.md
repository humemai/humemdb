# SQL Example

`HumemSQL v0` accepts a small PostgreSQL-like subset and emits backend SQL for SQLite
or DuckDB.

```python
from humemdb import HumemDB

with HumemDB("app.sqlite3", "analytics.duckdb") as db:
    db.query(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, amount INTEGER)",
        route="sqlite",
    )

    db.executemany(
        "INSERT INTO events (id, kind, amount) VALUES (?, ?, ?)",
        [
            (1, "click", 10),
            (2, "click", 15),
            (3, "purchase", 99),
        ],
        route="sqlite",
    )

    oltp = db.query(
        "SELECT id, kind FROM events WHERE id = 1",
        route="sqlite",
    )
    olap = db.query(
        "SELECT kind, SUM(amount) AS total FROM events GROUP BY kind ORDER BY total DESC",
        route="duckdb",
    )

    print(oltp.rows)
    print(olap.rows)
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