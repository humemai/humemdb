# Query Surfaces

HumemDB currently centers `db.query(...)` on explicit SQL or Cypher text and keeps the
direct vector path separate.

## Public Python methods

The stable public Python surface today is intentionally small:

- `HumemDB(base_path, *, preload_vectors=False)` opens or reopens one embedded database
  pair from a single base path
- `db.query(text, *, params=None)` is the text-query surface
- `db.executemany(text, params_seq)` is the current batch-write surface
- `db.transaction()`, `db.begin()`, `db.commit()`, and `db.rollback()` control the
  canonical SQLite write transaction
- `db.import_table(...)`, `db.import_nodes(...)`, and `db.import_edges(...)` are the
  current CSV ingest surface
- `db.insert_vectors(...)`, `db.search_vectors(...)`, and `db.set_vector_metadata(...)`
  are the direct-vector methods

`QueryResult` is the normalized result object returned by query-like calls.

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

### Relational indexes through the public API

HumemDB does not currently expose a separate `create_index(...)` helper. Instead,
app-owned relational indexes are created through ordinary SQL DDL on the same public
`db.query(...)` surface.

That means callers can stay entirely inside the public HumemDB API and write index DDL
such as:

```python
db.query(
    "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)"
)
```

Practical implications:

- relational indexing is part of the public SQL story, not something that requires
  reaching into internal SQLite handles
- callers are expected to add workload-specific indexes for app-owned tables when
  benchmark or application evidence justifies them
- HumemDB keeps a few internal default indexes for its own graph and vector storage,
  but ordinary relational table indexing remains explicit application DDL

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

### Current default graph indexes

HumemDB creates a small default set of SQLite indexes for the admitted Cypher read and
write surface. These are meant to support the common graph access paths now, not to be
an exhaustive graph-tuning layer for every future workload.

- `graph_nodes(label, id)` for label-constrained node lookup
- `graph_edges(from_node_id, type, to_node_id)` for outgoing typed traversal
- `graph_edges(to_node_id, type, from_node_id)` for incoming typed traversal
- `graph_node_properties(key, value_type, value, node_id)` for node-property equality
  lookup
- `graph_edge_properties(key, value_type, value, edge_id)` for edge-property equality
  lookup
- a partial unique index on `graph_node_properties(node_id)` where
  `value_type = 'vector'` so one node cannot accumulate multiple vector-valued
  properties in the current graph-owned embedding path

Practical implications:

- current default indexing is aimed at label lookup, endpoint-driven traversals,
  reverse-edge traversals, and equality-style property filters
- this default set is intentionally conservative; broader ordered traversals or other
  workload-specific graph reads may still justify app-owned SQLite indexes later
- HumemDB supports ordinary app-owned `CREATE INDEX IF NOT EXISTS ...` DDL through
  `db.query(...)`, so applications can add narrower indexes when benchmark evidence
  justifies them

## `HumemVector v0`

- Vector search is expressed inside HumemSQL or HumemCypher text rather than as a
  separate public query type.
- SQLite-backed vector table.
- Exact NumPy search over the cached vector matrix.
- Direct vector search still lives on explicit vector methods such as
  `search_vectors(...)` and can be narrowed with equality-style metadata filters.
- HumemSQL uses PostgreSQL-like vector ordering forms such as
  `ORDER BY embedding <->|<=>|<#> $query LIMIT ...`.
- HumemCypher uses Neo4j-like
  `CALL db.index.vector.queryNodes('user_embedding_idx', k, $query) YIELD node, score`
  forms, with optional `MATCH ...` or `WHERE ...` filtering before `RETURN`.
- SQL vector queries define row-oriented vector candidate sets.
- Cypher vector queries define node-oriented vector candidate sets.
- Vector execution is currently fixed to SQLite under the public API.

## Implementation notes

- `db.query(...)` now infers SQL, the current narrow Cypher subset, and the current
  language-level vector forms directly from the query text.
- The public query API does not expose a backend override.
- The public Python API does not expose SQLite or DuckDB engine handles directly.
- HumemDB uses the current conservative workload classifier internally: broad
  analytical SQL may route to DuckDB, while writes, ambiguous SQL, current Cypher
  reads, and vector execution stay on SQLite.
- Internally the vector path still lowers to a candidate query plus exact vector
  ranking, but that split is no longer part of the public query API.

## Why the version label matters

The `v0` marker is attached to the language surfaces, not to the package version. A
package release can move forward while a surface is still intentionally narrow and
allowed to change incompatibly.
