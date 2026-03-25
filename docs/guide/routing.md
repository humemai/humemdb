# Routing

HumemDB now routes automatically at the public `db.query(...)` boundary. Callers write
one supported SQL or Cypher surface, and the runtime chooses the execution backend when
that choice is part of the current contract.

## Routes

- `sqlite`: canonical write path and transactional source of truth
- `duckdb`: analytical read path over SQLite-backed data

## Current policy

- writes always go to SQLite
- read-only SQL is classified automatically; broad analytical SQL may route to DuckDB
- current public Cypher execution stays on SQLite
- current vector execution stays on SQLite

There is no public `route=` override on `db.query(...)` anymore. Backend choice is now a
runtime concern, not part of the public language surface.

## Current benchmark evidence

The current routing sweep supports a conservative SQL policy.

- selective OLTP-style SQL such as point lookups, filtered ranges, and ordered hot-path
    lookups stayed SQLite-favored through the current sweep
- several analytical SQL shapes crossed to DuckDB as early as `10_000` rows, including
    grouped aggregates, CTE rollups, join-and-group shapes, `EXISTS` filters, and windowed
    ranking
- some document and memory analytical joins crossed later, around `100_000` to
    `1_000_000` rows
- raw backend Cypher benchmarks showed only limited crossovers on a few broad graph read
    shapes, so public Cypher routing remains SQLite-first for now
- the current vector sweep did not show an acceptable indexed crossover, so vector search
    remains on the SQLite/NumPy exact path

These measurements are evidence for the current conservative classifier, not a promise
that every analytical-looking query will route to DuckDB.

## Example

```python
result = db.query(
        "SELECT kind, COUNT(*) AS total FROM events GROUP BY kind"
)
```

If that call is a write, HumemDB keeps it on SQLite. If it is a read, HumemDB infers the
query surface and applies the current routing policy internally.
