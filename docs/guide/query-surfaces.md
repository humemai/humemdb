# Query Surfaces

HumemDB currently centers `db.query(...)` on explicit SQL or Cypher text and keeps the
direct vector path separate.

## `HumemSQL v0`

- PostgreSQL-like source syntax.
- Translated with `sqlglot` into backend SQL.
- `db.query(...)` is the public entry point for explicit SQL text.
- Users write `HumemSQL v0` regardless of route; `route` selects the execution backend,
  not a backend-specific SQL dialect.
- Backend-specific SQLite or DuckDB SQL is not part of the supported public SQL
  contract.
- Public writes go to SQLite.
- Read queries may go to SQLite or DuckDB.

## `HumemCypher v0`

- Separate frontend, not a SQL dialect.
- `db.query(...)` is the public entry point for explicit Cypher text.
- Graph data is stored in SQL tables.
- Cypher reads can run on SQLite or DuckDB.
- Cypher writes still go to SQLite.

## `HumemVector v0`

- Vector search is expressed inside HumemSQL or HumemCypher text rather than as a
  separate public query type.
- SQLite-backed vector table.
- Exact NumPy search over the cached vector matrix.
- Direct vector search still lives on explicit vector methods such as
  `search_vectors(...)` and can be narrowed with equality-style metadata filters.
- HumemSQL uses PostgreSQL-like vector ordering forms such as
  `ORDER BY embedding <->|<=>|<#> $query LIMIT ...`.
- HumemCypher uses Neo4j-like `SEARCH ... VECTOR INDEX embedding FOR $query LIMIT ...`
  forms.
- SQL vector queries define row-oriented vector candidate sets.
- Cypher vector queries define node-oriented vector candidate sets.
- Routed vector execution is currently fixed to `route="sqlite"`.

## Implementation notes

- `route` remains explicit.
- `db.query(...)` now infers SQL, the current narrow Cypher subset, and the current
  language-level vector forms directly from the query text.
- Internally the vector path still lowers to a candidate query plus exact vector
  ranking, but that split is no longer part of the public query API.

## Why the version label matters

The `v0` marker is attached to the language surfaces, not to the package version. A
package release can move forward while a surface is still intentionally narrow and
allowed to change incompatibly.
