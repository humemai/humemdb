# Vectors

The current vector surface is deliberately conservative.

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
- Cypher vector writes are intentionally limited to simple node-owned `embedding`
  property flows rather than full Neo4j Cypher compatibility
- internal vector normalization still uses one canonical SQLite-backed vector store, but
  that normalization is not the public conceptual center

## Why exact search is the default

HumemDB needs a defensible baseline before it broadens into indexed ANN routing. The
exact SQLite plus NumPy path is simple enough to benchmark honestly and strong enough to
set the first public contract.

## Relationship to LanceDB

LanceDB is in the dependency set because benchmark and future accelerated work already
exists around that direction. It is not the default public runtime path today.