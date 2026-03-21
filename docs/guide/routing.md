# Routing

HumemDB starts with explicit routing because hidden routing rules would make the first
release hard to reason about.

## Routes

- `sqlite`: canonical write path and transactional source of truth
- `duckdb`: analytical read path over SQLite-backed data

## Current policy

- send writes to SQLite
- allow read-only SQL on DuckDB
- allow Cypher reads on SQLite or DuckDB
- keep vector search on SQLite

For `query_type="sql"`, the caller still writes `HumemSQL v0` on both routes. Choosing
`route="sqlite"` or `route="duckdb"` selects the backend engine, not a SQLite-specific
or DuckDB-specific SQL dialect.

## Why explicit routing comes first

Automatic routing is planned later, but the runtime needs benchmark evidence and stable
surface semantics before it should guess for the caller.

## Example

```python
result = db.query(
    "SELECT kind, COUNT(*) AS total FROM events GROUP BY kind",
    route="duckdb",
)
```

If that same call were a write, HumemDB would reject it instead of silently mutating the
analytical replica.
