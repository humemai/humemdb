# Vectors

The current vector surface is deliberately conservative.

## Public direct-vector methods

The direct-vector Python API is:

- `db.insert_vectors(rows)` to append direct vectors and receive assigned ids
- `db.search_vectors(query, *, top_k=10, metric="cosine", filters=None)` to run the
  direct vector search path
- `db.inspect_vector_index(metric="cosine")` to inspect the public cold-index state
- `db.build_vector_index(metric="cosine")` and `db.refresh_vector_index(...)` to build
  or rebuild the public cold index for one metric
- `db.drop_vector_index(metric="cosine")` to disable and remove the current cold index
  for one metric
- `db.await_vector_index_refresh(metric="cosine")` to wait for one pending background
  refresh when you need deterministic lifecycle control
- `db.set_vector_metadata(rows)` to attach equality-filterable metadata to existing
  direct vectors
- `db.preload_vectors()` and `db.vectors_cached()` to control or inspect the current
  exact-vector cache

Use `db.query(...)` instead when SQL rows or graph nodes define the candidate set first.

## Storage model

- vectors are stored in SQLite as float32 blobs
- the current direct vector path loads one exact in-memory matrix from SQLite
- one canonical vector table can also be filtered by SQL rows or Cypher nodes through
  id-based candidate queries
- vector-only categorization stays narrow: equality-style metadata filters over the
  canonical vector table
- SQL row-owned vectors can enter through narrow `INSERT` and `UPDATE ... SET embedding
  = ... WHERE id = ...` forms
- Cypher node-owned vectors can enter through narrow `CREATE (...) {embedding: ...}` and
  `MATCH ... SET n.embedding = ...` forms
- dimensions must be consistent within the loaded vector set

## Search model

- exact search only on the public path
- the direct vector path can search the full stored vector set or a metadata-filtered
  subset
- SQL candidate-filtered vector search means SQL defines the candidate row ids, then the exact
  vector path ranks those ids
- Cypher candidate-filtered vector search means Cypher defines the candidate node ids, then the
  exact vector path ranks those ids
- metrics: `cosine`, `dot`, `l2`

## Current subset boundary

- HumemDB follows a PostgreSQL-like row model and a Neo4j-style node-property model for
  vectors, but only as a narrow subset
- SQL vector writes are intentionally limited to simple row-owned `embedding` columns
  rather than full pgvector compatibility
- SQL vector lifecycle admin is intentionally narrow but now follows a smaller
  pgvector-like shape: `CREATE INDEX docs_embedding_idx ON docs USING ivfpq
  (embedding vector_cosine_ops)` or `CREATE INDEX docs_ip_idx ON docs USING ivfpq
  (embedding vector_ip_ops)`, plus `ALTER VECTOR INDEX docs_embedding_idx PAUSE
  MAINTENANCE`, `ALTER VECTOR INDEX docs_embedding_idx RESUME MAINTENANCE`,
  `REFRESH VECTOR INDEX docs_embedding_idx`, `REBUILD VECTOR INDEX
  docs_embedding_idx`, `DROP INDEX docs_embedding_idx`, and
  `SELECT * FROM humemdb_vector_indexes`
- Cypher vector writes are intentionally limited to simple node-owned `embedding`
  property flows rather than full Neo4j Cypher compatibility
- Cypher vector lifecycle admin is intentionally narrow but now follows a smaller
  Neo4j-like shape: `CREATE VECTOR INDEX profile_embedding_idx IF NOT EXISTS FOR
  (p:Profile) ON (p.embedding) OPTIONS { indexConfig:
  {`vector.similarity_function`: 'cosine'} }`, plus `ALTER VECTOR INDEX
  profile_embedding_idx PAUSE MAINTENANCE`, `ALTER VECTOR INDEX
  profile_embedding_idx RESUME MAINTENANCE`, `REFRESH VECTOR INDEX
  profile_embedding_idx`, `REBUILD VECTOR INDEX profile_embedding_idx`,
  `SHOW VECTOR INDEXES`, and `DROP VECTOR INDEX profile_embedding_idx`
- Cypher vector search now supports a narrow Neo4j-like procedure subset through
  `CALL db.index.vector.queryNodes('profile_embedding_idx', 10, $query) YIELD node,
  score MATCH (node:Profile) RETURN node.id, score`
- internal vector normalization still uses one canonical SQLite-backed vector store, but
  that normalization is not the public conceptual center

## Why exact search is the default

HumemDB needs a defensible baseline before it broadens into indexed ANN routing. The
exact SQLite plus NumPy path is simple enough to benchmark honestly and strong enough to
set the first public contract.

## Relationship to LanceDB

LanceDB is in the dependency set because benchmark and future accelerated work already
exists around that direction. It is not the default public runtime path today.
