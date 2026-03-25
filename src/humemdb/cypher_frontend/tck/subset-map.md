# HumemDB Cypher TCK Subset Map

This file records the first clause-by-clause mapping between HumemDB's admitted
generated Cypher frontend subset and the openCypher TCK materials under the local
reference checkout at `/mnt/ssd2/repos/openCypher/tck/features/clauses/`.

It is intentionally a subset map, not a compatibility claim.

## Current admitted surface

HumemDB currently admits only these generated-frontend Cypher shapes:

1. single `CREATE` of one node or one directed relationship pattern
2. single `MATCH ... RETURN` over one node pattern or one directed relationship
   pattern
3. single `MATCH ... SET` over one node pattern or one directed relationship pattern
   with simple assignments
4. single `MATCH ... DETACH DELETE` over one node alias, or single `MATCH ... DELETE`
   over one relationship alias
5. simple scalar `WHERE` comparison predicates joined by `AND`, plus narrow top-level
   `OR` across those comparison groups, with `AND` binding inside each group and
   parenthesized regrouping of those comparison clauses
6. string property predicates with `STARTS WITH`, `ENDS WITH`, and `CONTAINS` on
   stored node and relationship properties
7. property null predicates with `IS NULL` and `IS NOT NULL` on stored node and
   relationship properties
8. single-relationship match patterns may omit the relationship type or use narrow
   relationship type alternation such as `:KNOWS|FOLLOWS`
9. node aliases may be omitted in `CREATE` and in structural relationship-match
   endpoints when those nodes are not referenced by `RETURN`, `WHERE`, or `SET`
10. `DISTINCT`, `ORDER BY`, `SKIP` or `OFFSET`, and `LIMIT` on the admitted
   `MATCH ... RETURN` subset

## Mapping status

Legend:

- `covered`: directly within the current admitted subset
- `boundary`: close to the subset, but currently rejected clearly
- `out`: outside the current frontend and should remain rejected for now

| TCK area | Feature families | Status | Notes |
| --- | --- | --- | --- |
| `clauses/create` | `Create1` simple node create cases | covered | Single labeled node create and inline properties are in scope, including anonymous-node create shapes such as `CREATE (:Label)`. |
| `clauses/create` | `Create2` creating relationships | covered | One directed relationship create between two node patterns is in scope, including reverse-arrow create shapes, endpoint node aliases omitted, repeated-alias single-node self loops when both node patterns agree, a narrow three-pattern `CREATE (a:A), (b:B), (a)-[:R]->(b)` form, narrow single-node `MATCH ... CREATE` shapes where one endpoint reuses the matched alias, and narrow two-node `MATCH ... CREATE` shapes where two disconnected matched aliases are connected directly. |
| `clauses/create` | `Create1` multi-label node cases | boundary | HumemDB currently supports at most one node label in the admitted subset. |
| `clauses/create` | `Create1` create-and-return cases | out | Current `CREATE` subset is write-only and does not admit `RETURN`. |
| `clauses/create` | `Create3` interoperation with `MATCH`, `WITH`, `UNWIND`, `MERGE` | out | Multi-part queries are not yet admitted. |
| `clauses/delete` | narrow `MATCH ... DETACH DELETE node_alias` and `MATCH ... DELETE relationship_alias` | covered | The admitted write subset now supports deleting one matched node alias with detach semantics and deleting one matched relationship alias, with graph-table and graph-owned vector cleanup kept in sync. |
| `clauses/create` | `Create4`, `Create5`, `Create6` | out | Large queries, multi-hop create patterns, and clause-side-effect persistence are outside the current subset. |
| `clauses/match` | `Match1` simple node match cases | covered | Single node pattern plus `RETURN` is in scope. |
| `clauses/match` | `Match2` simple relationship match cases | covered | Single directed relationship match plus `RETURN` is in scope, including anonymous endpoint nodes, untyped relationship matches, and narrow relationship type alternation such as `:KNOWS\|HATES`. |
| `clauses/match` | Inline property predicates on one node or relationship | covered | Supported through inline properties and simple `WHERE alias.field OP value` comparisons. |
| `clauses/match` | Multiple labels, disconnected patterns, repeated `MATCH`, Cartesian products | boundary | These parse today but fall outside the admitted normalization/lowering subset and should stay rejected clearly. |
| `clauses/match` | `Match3` through `Match9` variable-length, named-path, optional, cyclic, deprecated scenarios | out | Current frontend does not admit path variables, variable-length patterns, `OPTIONAL MATCH`, or multi-hop pattern semantics. |
| `clauses/match-where` | `MatchWhere1` simple property equality and parameter equality | covered | Equality predicates over admitted node/relationship bindings are in scope, and the shipped subset also admits simple scalar inequalities, string `STARTS WITH` / `ENDS WITH` / `CONTAINS` predicates over stored properties, property `IS NULL` / `IS NOT NULL` predicates, narrow top-level `OR`, and parenthesized regrouping across admitted comparison clauses, with `AND` binding inside each branch. Direct graph fields such as node `label` and relationship `type` remain equality-only. |
| `clauses/match-where` | Label predicates in `WHERE` | boundary | TCK label-predicate coverage is broader than the current simple equality-only `WHERE` subset. |
| `clauses/match-where` | `MatchWhere2` to `MatchWhere6` multi-variable filters, non-equi joins, null logic, optional-match filters | out | These require richer expression support and broader pattern semantics than the current frontend admits. |

## Representative covered scenario classes

These TCK scenario classes are structurally close to or already represented by the
current HumemDB tests:

1. `Create1`: create one labeled node with inline properties
2. `Create2`: create one directed relationship with inline properties, including reverse-arrow create patterns such as `(:A)<-[:R]-(:B)`, repeated-alias self loops such as `(root:Root)-[:LINK]->(root:Root)`, a narrow three-pattern create form such as `CREATE (a:A), (b:B), (a)-[:R]->(b)`, narrow single-node `MATCH ... CREATE` shapes such as `MATCH (root:Root) CREATE (root)-[:LINK]->(root)` or `MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End)`, and narrow two-node `MATCH ... CREATE` shapes such as `MATCH (x:Begin), (y:End) CREATE (x)-[:TYPE]->(y)`
3. `Match1`: match one labeled node and return projected properties
4. `Match2`: match one directed relationship and return projected properties
5. `MatchWhere1`: filter one admitted binding by `alias.field = literal` or
   `alias.field = $param`, plus narrow scalar comparisons such as
   `alias.field >= 30`, and narrow top-level disjunctions such as
   `alias.field >= 30 AND alias.active = true OR alias.other = 'x'`, including
   parenthesized regrouping such as `(alias.field >= 30 OR alias.other = 'x') AND alias.active = true`, string predicates such as `alias.name STARTS WITH 'Al'` or `alias.note CONTAINS 'met'`, and null predicates such as `alias.nickname IS NULL` or `alias.note IS NOT NULL`
6. untyped relationship matches such as `MATCH (a)-[r]->(b)`
7. narrow relationship type alternation such as `MATCH (a)-[r:KNOWS|FOLLOWS]->(b)`
8. anonymous node endpoints in admitted relationship patterns such as `MATCH (:A)-[r]->(:B)`
9. narrow `MATCH ... SET`: update node properties or relationship properties on one
   admitted pattern, including reverse-direction single-relationship matches
10. narrow `MATCH ... DETACH DELETE`: delete one matched node alias, including
   graph-owned vector cleanup through the SQLite-backed storage path
11. narrow `MATCH ... DELETE`: delete one matched relationship alias

## Representative enforced boundaries

These scenario families should continue to raise clear rejections until the admitted
subset expands:

1. multiple labels on a node pattern such as `CREATE (:A:B)`
2. disconnected or Cartesian-product patterns such as `MATCH (n), (m) RETURN n`
3. richer boolean expressions such as
   `WHERE NOT (n.name = 'Bar' OR n.age = 1)`
4. longer chained patterns such as `MATCH (a)<--()<--(b)-->()-->(c) RETURN c`
5. clause families requiring `OPTIONAL MATCH`, named paths, variable-length
   relationships, `WITH`, or multi-part query semantics
6. non-equality comparisons on direct graph fields such as `u.label > 'A'` or
   `r.type > 'KNOWS'`

## Next adoption steps

1. Cherry-pick a very small executable subset from `Create1`, `Create2`, `Match1`,
   `Match2`, and `MatchWhere1` into HumemDB-owned tests.
2. Expand this map as generated normalization/lowering grows.
3. Do not mark broader TCK areas as covered until the generated frontend owns both
   the syntax and the semantics for those shapes.
