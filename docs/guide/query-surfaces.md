# Query Surfaces

HumemDB currently centers `db.query(...)` on explicit SQL or Cypher text and keeps the
direct vector path separate.

For the exact supported PostgreSQL-like SQL and Neo4j-like Cypher subset, see
[Supported Syntax](supported-syntax.md).

## `HumemSQL v0`

- PostgreSQL-like source syntax.
- Translated with `sqlglot` into backend SQL.
- `db.query(...)` is the public entry point for explicit SQL text.
- Users write `HumemSQL v0` regardless of backend; the public API no longer accepts a
  `route` override.
- HumemDB applies a conservative automatic classifier internally: broad analytical SQL
  may route to DuckDB, while writes and selective SQL stay on SQLite.
- Backend-specific SQLite or DuckDB SQL is not part of the supported public SQL
  contract.
- Public writes go to SQLite.
- Read queries may go to SQLite or DuckDB.

## `HumemCypher v0`

- Separate frontend, not a SQL dialect.
- `db.query(...)` is the public entry point for explicit Cypher text.
- Graph data is stored in SQL tables.
- Current public Cypher execution stays on SQLite.
- Cypher writes still go to SQLite.

### Current graph storage layout

HumemDB currently stores the property graph in four SQLite tables: one table for
nodes, one for edges, and one property table for each side.

```text
graph_nodes
  id (PK)
  label
      |
      | 1-to-many via node_id
      v
graph_node_properties
  node_id (PK, FK -> graph_nodes.id)
  key     (PK)
  value
  value_type


graph_edges
  id (PK)
  type
  from_node_id (FK -> graph_nodes.id)
  to_node_id   (FK -> graph_nodes.id)
      |
      | 1-to-many via edge_id
      v
graph_edge_properties
  edge_id (PK, FK -> graph_edges.id)
  key     (PK)
  value
  value_type
```

Practical consequences of this layout:

- one logical node becomes one `graph_nodes` row plus zero or more
  `graph_node_properties` rows
- one logical edge becomes one `graph_edges` row plus zero or more
  `graph_edge_properties` rows
- properties are stored as typed key/value rows rather than as one JSON blob per node
  or edge
- foreign keys keep node and edge relationships consistent, and SQLite indexes cover
  the common label, endpoint, and property lookup paths

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
- Vector execution is currently fixed to SQLite under the public API.

## Implementation notes

- `db.query(...)` now infers SQL, the current narrow Cypher subset, and the current
  language-level vector forms directly from the query text.
- The public query API does not expose a backend override.
- HumemDB uses the current conservative workload classifier internally: broad
  analytical SQL may route to DuckDB, while writes, ambiguous SQL, current Cypher
  reads, and vector execution stay on SQLite.
- Internally the vector path still lowers to a candidate query plus exact vector
  ranking, but that split is no longer part of the public query API.

## Why the version label matters

The `v0` marker is attached to the language surfaces, not to the package version. A
package release can move forward while a surface is still intentionally narrow and
allowed to change incompatibly.
