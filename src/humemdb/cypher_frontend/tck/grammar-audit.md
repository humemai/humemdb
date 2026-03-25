# Cypher Grammar Audit

This note records a narrow Phase 9 audit of HumemDB's vendored
`src/humemdb/cypher_frontend/grammar/Cypher.g4` against the local openCypher
reference checkout at `/mnt/ssd2/repos/openCypher/grammar/openCypher.bnf`.

Scope is intentionally narrow:

1. `CREATE`
2. `MATCH`
3. `WHERE`
4. `RETURN`
5. `SET`
6. relationship-pattern structure

This is not a full openCypher compatibility claim.

## Baseline

- The local openCypher checkout is the reference source for BNF and TCK intent.
- HumemDB's `Cypher.g4` is a vendored parser-ready ANTLR artifact, not a file copied
  from the local checkout itself.
- HumemDB support is narrower than grammar acceptance and is ultimately determined by
  `validate.py`, `normalize.py`, `lower.py`, and runtime tests.

## Confirmed Alignment On The Admitted Subset

These areas appear structurally aligned between the local openCypher BNF and the
vendored `Cypher.g4`.

### `MATCH`

- openCypher BNF has both simple and optional match statements.
- `Cypher.g4` also admits `MATCH` and `OPTIONAL MATCH` forms.
- HumemDB's admitted subset currently uses only simple `MATCH` shapes, which is a
  subset restriction rather than a grammar contradiction.

### `CREATE`

- openCypher BNF models `CREATE <create graph pattern>`.
- `Cypher.g4` models `CREATE SP? oC_Pattern`.
- HumemDB's admitted create subset is narrower than the grammar, but the broad clause
  shape aligns.

### `SET`

- openCypher BNF models `SET <set item list>` with property set, add-all, set-labels,
  and set-all-properties variants.
- `Cypher.g4` models the same major branches through `oC_SetItem`:
  - property set
  - variable set-all-properties
  - `+=`
  - label set
- HumemDB admits only a narrower executable subset, but the grammar shape aligns.

### `RETURN`

- openCypher BNF models `RETURN <return statement body> [ <order by and page clause> ]`.
- `Cypher.g4` models `RETURN oC_ProjectionBody`, and `oC_ProjectionBody` includes
  `DISTINCT`, projection items, `ORDER BY`, `SKIP`, and `LIMIT`.
- HumemDB currently uses a narrower runtime subset, but the clause structure aligns.

### `WHERE`

- openCypher BNF models `WHERE <search condition>`.
- `Cypher.g4` models `WHERE SP oC_Expression`.
- HumemDB admits only a narrow boolean-expression subset, but the top-level clause
  shape aligns.

### Relationship Pattern Structure

- openCypher BNF models left-pointing, right-pointing, bidirectional-marked, and
  undirected relationship patterns.
- `Cypher.g4` models the same four broad relationship-pattern direction families in
  `oC_RelationshipPattern`.
- HumemDB's admitted runtime subset only executes a much smaller portion of that
  surface, but the vendored grammar is not inventing new direction syntax here.

## Clear Drift Points To Keep In Mind

These are the most concrete grammar-level differences noticed during the narrow audit.

### `OFFSET` Synonym Now Matches The BNF Pagination Shape

- openCypher BNF defines `<offset synonym>` as `SKIP | OFFSET`.
- HumemDB's vendored `Cypher.g4` now accepts both `SKIP` and `OFFSET` through the
  same pagination production.
- The admitted runtime subset still keeps pagination narrow to integer-literal
  offsets, but the grammar-level synonym drift is no longer present.

### Inline Pattern `WHERE` Predicates Exist In The BNF But Not In HumemDB's G4 Node Pattern

- openCypher BNF allows an `<element pattern predicate>` that can be either
  inline `WHERE` or property specification inside node-pattern filler.
- HumemDB's `oC_NodePattern` currently allows variable, node labels, and properties,
  but not an inline pattern `WHERE` inside the node-pattern production.
- This is broader than the current admitted subset anyway, but it is another real
  grammar drift point.

## Practical Conclusion

- For the currently admitted HumemDB subset, the vendored `Cypher.g4` looks aligned
  enough with the local openCypher reference to keep using it as the parser input.
- The audit did not find evidence that HumemDB's vendored grammar is making up its own
  clause syntax for the admitted `CREATE`, `MATCH`, `WHERE`, `RETURN`, `SET`, and
  relationship-direction families.
- The audit did find a specific broader-surface drift point around inline pattern
  `WHERE`, which should be remembered if Phase 9 expands grammar breadth further.

## Follow-Up

If Phase 9 expands beyond today's admitted subset, the next useful checks are:

1. audit pattern predicates and path-pattern productions clause-by-clause
2. compare admitted behavior against local TCK scenarios, not just grammar shape
3. decide whether known drift points should be corrected in the vendored grammar or
   intentionally left outside HumemDB's supported subset for now
