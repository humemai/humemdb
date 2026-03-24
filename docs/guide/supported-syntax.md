# Supported Syntax

HumemDB deliberately implements narrow, tested language subsets.

When the docs say PostgreSQL-like SQL or Neo4j-like Cypher, that means HumemDB borrows
familiar syntax shapes from those ecosystems. It does **not** mean broad PostgreSQL,
pgvector, Neo4j, or openCypher compatibility.

The supported contract is the subset documented here and defended by the test suite.

## `HumemSQL v0`

Current SQL direction:

- PostgreSQL-like source syntax
- translated through `sqlglot`
- executed on SQLite or DuckDB depending on the selected route
- backend-specific SQL is not part of the public contract

Current statement subset:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE`

Current parameter style:

- named mapping parameters such as `$name`

Current vector-shaped SQL subset:

- row-oriented vector search is expressed as SQL `SELECT` plus vector ordering
- the current public shape is:

```sql
SELECT id
FROM docs
WHERE topic = $topic
ORDER BY embedding <=> $query
LIMIT 5
```

- the supported vector operators are currently modeled after pgvector-style syntax:
    - `<->` for L2 distance
    - `<=>` for cosine distance
    - `<#>` for dot-product-style ordering
- the current vector query path requires the ordering expression to target an
  `embedding` column
- the current candidate-query subset expects one base table, not arbitrary joins

What HumemDB is **not** claiming here:

- full PostgreSQL grammar support
- full pgvector compatibility
- every PostgreSQL expression shape around vector ordering
- arbitrary SQL planner equivalence with PostgreSQL

## `HumemCypher v0`

Current Cypher direction:

- Neo4j-like graph query syntax
- handwritten narrow parser and relational lowering over SQLite-backed graph tables
- explicit subset rather than broad Cypher compatibility

Current statement subset:

- `CREATE` for one labeled node
- `CREATE` for one directed relationship between two labeled nodes
- `MATCH` for labeled nodes and single directed relationships
- `MATCH ... SET` for narrow node property updates

Current read-clause subset:

- simple property equality predicates in `WHERE`
- `AND` between those predicates
- `RETURN alias.field`
- `ORDER BY alias.field [ASC|DESC]`
- integer-literal `LIMIT`
- named parameters such as `$name`

Current vector-shaped Cypher subset:

- node-oriented vector search is expressed as `MATCH` plus `SEARCH`
- the current public shape is:

```cypher
MATCH (u:User {cohort: 'alpha'})
SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 3)
RETURN u.id
ORDER BY u.id
```

- the current `SEARCH` subset requires:
    - a `MATCH` clause first
    - a bound node alias from that `MATCH`
    - `VECTOR INDEX embedding`
    - `FOR $query`
    - literal or parameterized `LIMIT`
- the current candidate-query result still comes from the parsed `MATCH ... RETURN ...`
  subset that HumemCypher already supports

What HumemDB is **not** claiming here:

- full Neo4j Cypher compatibility
- full openCypher compatibility
- relationship-vector `SEARCH`
- `SCORE AS ...`
- the richer filtered `SEARCH` forms supported by newer Neo4j releases
- arbitrary Cypher planner equivalence with Neo4j

## Why This Matters

This distinction is important for both implementation and documentation.

- The syntax choices are grounded in familiar upstream ecosystems.
- The supported behavior is still only the narrow subset HumemDB parses, lowers,
  tests, and documents.
- Future phase-9 work should keep expanding that subset through parser/planner work,
  not by adding undocumented string-shaped special cases.
