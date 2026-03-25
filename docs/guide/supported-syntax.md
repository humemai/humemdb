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
- executed on SQLite or DuckDB depending on the inferred runtime route
- backend-specific SQL is not part of the public contract

Current statement subset:

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`
- `CREATE`

Currently defended broader read shapes:

- non-recursive `WITH` clauses and multi-CTE read queries
- `UNION ALL`
- window functions such as `ROW_NUMBER() OVER (...)`
- `CASE` expressions, including `CASE WHEN EXISTS (...)`
- correlated `EXISTS` predicates in admitted read queries

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
- generated parser-backed narrow frontend plus relational lowering over SQLite-backed graph tables
- explicit subset rather than broad Cypher compatibility
- public Cypher execution currently stays on SQLite; there is no public route override

Current statement subset:

- `CREATE` for one labeled node, with or without an explicit node alias
- `CREATE` for one directed relationship between two labeled nodes, in either arrow
  direction, and endpoint node aliases may be omitted
- `CREATE` may also form a single labeled self-loop when the same node alias is repeated
  consistently on both ends of the relationship pattern
- `CREATE` also admits one narrow multi-pattern form with two labeled node patterns
  followed by one relationship pattern that reuses exactly those two created aliases
- `MATCH` for labeled nodes and single directed relationships
- `MATCH ... SET` for narrow node or relationship property updates
- `MATCH ... DETACH DELETE` for one matched node alias
- `MATCH ... DELETE` for one matched relationship alias
- `MATCH` over one node pattern followed by `CREATE` of one directed relationship
  pattern when at least one created endpoint reuses the matched node alias

Current read-clause subset:

- simple scalar comparison predicates in `WHERE` using `=`, `<`, `<=`, `>`, or `>=`
- `AND` within one clause, plus narrow top-level `OR` across those comparison clauses
- `AND` binds within each `OR` branch, and parenthesized regrouping of admitted
  comparison clauses is supported
- string property predicates with `STARTS WITH`, `ENDS WITH`, and `CONTAINS`
- property null predicates with `IS NULL` and `IS NOT NULL`
- relationship `MATCH` patterns may omit endpoint node aliases when those nodes are only
  used structurally
- relationship match patterns may omit the relationship type entirely when broad
  matching is intended
- relationship match patterns may use narrow type alternation such as `:KNOWS|FOLLOWS`
- in the admitted `MATCH ... CREATE` subset, one matched alias may be reused to create
  an existing-node self-loop or to connect that matched node to one newly created
  labeled endpoint node
- the admitted `MATCH ... CREATE` subset also includes two disconnected matched node
  patterns followed by one relationship create that connects those two matched aliases
  directly
- richer boolean expressions such as `NOT`, path predicates, or function-style boolean
  filters are still outside the subset
- direct graph fields still stay narrow: node `label` and relationship `type` are
  equality-only, and unknown aliases are rejected
- `RETURN alias.field`
- `RETURN DISTINCT alias.field`
- `ORDER BY alias.field [ASC|DESC]`
- integer-literal `SKIP` or `OFFSET`
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
