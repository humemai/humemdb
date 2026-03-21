# Query Surfaces

HumemDB treats SQL, Cypher, and vector search as separate frontends.

## `HumemSQL v0`

- PostgreSQL-like source syntax.
- Translated with `sqlglot` into backend SQL.
- Users write `HumemSQL v0` regardless of route; `route` selects the execution backend,
  not a backend-specific SQL dialect.
- Backend-specific SQLite or DuckDB SQL is not part of the supported public SQL
  contract.
- Public writes go to SQLite.
- Read queries may go to SQLite or DuckDB.

## `HumemCypher v0`

- Separate frontend, not a SQL dialect.
- Graph data is stored in SQL tables.
- Cypher reads can run on SQLite or DuckDB.
- Cypher writes still go to SQLite.

## `HumemVector v0`

- Separate frontend, not forced SQL syntax.
- SQLite-backed vector table.
- Exact NumPy search over cached matrices.
- Routing is currently fixed to `route="sqlite"`.

## Why the version label matters

The `v0` marker is attached to the language surfaces, not to the package version. A
package release can move forward while a surface is still intentionally narrow and
allowed to change incompatibly.
