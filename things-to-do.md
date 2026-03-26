# HumemDB Things To Do

Internal phase tracking and roadmap notes.

## Phase 1 - Done

Build the minimal router.

Status: complete.

- Keep the system embedded and in-process.
- Open SQLite and DuckDB connections.
- Support explicit routing: `sqlite` and `duckdb`.
- Send writes to SQLite.

Initial package shape:

```text
src/humemdb/
    __init__.py
    db.py
    engines.py
    types.py
tests/
    test_db.py
```

Initial classes:

- `HumemDB`: main in-process entry point.
- `SQLiteEngine`: wrapper around the Python `sqlite3` connection.
- `DuckDBEngine`: wrapper around the Python `duckdb` connection.
- `QueryResult`: normalized result object returned by queries.

Initial methods:

- `HumemDB.__init__(sqlite_path, duckdb_path=None)`
- `HumemDB.query(text, *, route, query_type="sql", params=None)`
- `HumemDB.executemany(text, params_seq, *, route, query_type="sql")`
- `HumemDB.begin(route=...)`
- `HumemDB.commit(route=...)`
- `HumemDB.rollback(route=...)`
- `HumemDB.transaction(route=...)`
- `HumemDB.close()`
- `SQLiteEngine.execute(text, params=None)`
- `DuckDBEngine.execute(text, params=None)`

Phase 1 transaction behavior:

- Writes auto-commit unless they are inside an explicit transaction block.
- `with db.transaction(route="sqlite"):` and `with db.transaction(route="duckdb"):` are
  supported.
- Public writes target SQLite; DuckDB is read-only from the `HumemDB` API.
- Small to moderate batch writes go through SQLite with `executemany(...)`.

Phase 1 bulk ingest behavior:

- Start with transactional SQLite batch writes for in-memory Python data.
- Keep larger file-based or workload-specific ingestion strategies for later phases.

## Phase 2 - Done

Define HumemSQL and the SQL translation layer.

Status: complete.

- Start with a small PostgreSQL-like portable SQL subset.
- Keep it close to common PostgreSQL-style SQL where practical.
- Avoid engine-specific syntax in the public surface.
- Parse SQL with `sqlglot` instead of string rewriting.
- Reject unsupported SQL clearly instead of guessing.
- HumemSQL v0 currently supports `SELECT`, `INSERT`, `UPDATE`, `DELETE`, and `CREATE`.
- HumemSQL v0 does not support recursive CTEs.

## Phase 3 - Done

Use DuckDB over SQLite first.

Status: complete.

- Let DuckDB read SQLite directly.
- Benchmark analytical queries on the direct path.
- Add materialization into DuckDB only if needed.
- Emit backend-specific SQL for SQLite and DuckDB from the HumemSQL layer.

Current benchmark utility:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py --rows 50000
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py \
    --rows 10000000 --warmup 1 --repetitions 5 --batch-size 50000
```

The benchmark now compares multiple relational workload families, including
OLTP-style event reads, analytical event rollups, selective document-tag joins,
and memory-style grouped rollups.

Current takeaway:

- SQLite stays better for point lookups and smaller filtered reads.
- DuckDB is already faster on broader grouped scans and analytical aggregates.
- Not every join is analytical; selective indexed joins can still favor SQLite.
- Direct DuckDB-over-SQLite reads are the default analytical path for now.
- Materialization is deferred until a future workload proves it is necessary.

## Phase 4 - Done

Add graph storage and Cypher support.

Status: complete for `HumemCypher v0`.

- Parse Cypher as its own frontend.
- Lower Cypher into graph and relational operations over graph tables.
- Store nodes and edges in SQLite.
- Use DuckDB for graph analytics when useful.
- HumemCypher v0 now supports narrow `CREATE` and `MATCH` flows for labeled
  nodes and single directed relationships.
- HumemCypher v0 supports relationship aliases, reverse-edge matches, and
  returning or filtering relationship `type`, `id`, and stored properties.
- HumemCypher v0 supports simple `WHERE alias.field = value` predicates joined
  by `AND`.
- HumemCypher v0 supports `ORDER BY` and `LIMIT` on `MATCH` queries.
- HumemCypher v0 supports named parameters such as `$name` through mapping-style
  query params.
- The current graph path uses SQLite-backed `graph_nodes`, `graph_node_properties`,
  `graph_edges`, and `graph_edge_properties` tables.
- HumemDB now creates a small default set of SQLite graph indexes around node labels,
  edge endpoints, and property equality lookups.
- The default indexes are meant to support common graph access paths now, not to cover
  every possible workload; users should be able to add workload-specific indexes later.
- Cypher reads can run on SQLite or DuckDB; Cypher writes still go to SQLite.
- Property values currently persist as typed scalar values over the graph property
  tables rather than as a broader document model.
- A broader shared IR is still deferred; Phase 4 only adds a thin Cypher-specific graph
  plan.

Current graph benchmark utility:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py --nodes 5000 --fanout 3
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py \
  --nodes 1000000 --fanout 4 --tag-fanout 2 --warmup 1 --repetitions 5 --batch-size 20000
```

The graph benchmark measures graph seed time, Cypher parse and bind/compile cost,
and raw SQL versus end-to-end Cypher execution for several graph query shapes on
SQLite and DuckDB across multiple labels and edge types.

Current takeaway:

- SQLite is already extremely strong for selective node lookup and selective graph
  traversals anchored by equality predicates.
- DuckDB only clearly pulls ahead once graph reads broaden into higher-fanout,
  graph-analytic traversal patterns.
- The benchmark evidence is now strong enough to keep `HumemCypher v0` closed and push
  broader graph work into later routing and planning phases instead of treating the
  current surface as unfinished.

## Phase 5 - Done

Implement `HumemVector v0`.

Status: complete for the exact baseline path.

- Add vector search as its own frontend, not as forced SQL syntax.
- Keep SQLite as the canonical vector store first.
- Store vectors in SQLite and execute the default search path as exact NumPy over cached
  vector-set matrices.
- Expose the shipped direct-vector surface through explicit `HumemDB` methods for
  insert and search. The earlier public `query_type="vector"` path has since been
  retired in favor of language-level SQL/Cypher vector forms plus those explicit
  direct vector methods.
- Benchmark the exact NumPy path against vector-set size, dimensionality, and `top_k`
  so routing can be based on measured crossover points instead of guesswork.
- Include quantization experiments as part of the Phase 5 benchmark work.
- Keep LanceDB as an optional accelerated backend only where the benchmark justifies the
  extra complexity; it is not the default path.

## Phase 6

Settle the simple public vector model, then harden docs/package.

Status: done.

- Decide one simple vector story that maps cleanly onto SQLite storage.
- Treat vectors as attached to rows, nodes, or minimal vector-only records.
- Keep `HumemDB.query(...)` as the main public center of gravity instead of growing a
  large separate vector API family.
- Make SQL vector search mean row-oriented vector search by default.
- Let SQL vector search candidate filtering come from SQL itself: table,
  vector-bearing column, and
  normal SQL filters should define the candidate set.
- Let SQL vector writes also live in SQL itself: vector-bearing row inserts and narrow
  updates should work through the SQL surface instead of helper-only methods.
- Make Cypher vector search mean node-oriented vector search by default.
- Let Cypher vector search candidate filtering come from Cypher itself: node labels,
  node properties,
  and graph patterns should define the candidate set.
- Let Cypher vector writes also live in Cypher itself: vector-bearing node creates and
  narrow `MATCH ... SET n.embedding = ...` flows should work through the Cypher
  surface instead of helper-only methods.
- Keep a minimal direct vector runtime for benchmarking and vector-only workflows, but
  do not treat it as the main public product surface.
- Let the explicit direct vector surface use distinct object-style APIs and metadata
  filters without forcing that model onto the main public `HumemDB` text-query story.
- Keep the first vector-only categorization step narrow: one canonical SQLite-backed
  vector table plus simple metadata/filter support.
- Make the canonical vector identity explicit before release: vectors should be keyed by
  `target`, `namespace`, and `target_id` instead of one shared bare integer id so direct
  vectors, SQL row-owned vectors, and graph node-owned vectors can safely coexist in
  one SQLite database.
- Keep the first SQL/Cypher vector write step narrow too: support the mainstream
  PostgreSQL-like and Neo4j-like ownership model as a subset without claiming full
  pgvector or full Neo4j Cypher compatibility.
- Start vector-only categorization with equality-style filtering and keep broader
  metadata/query semantics for later if real usage demands them.
- Do not design around edge vectors in this phase.
- Decide whether the direct vector object API should feel record-first or
  property-first, but keep it narrow and explicit.
- Keep SQLite as the canonical persisted vector store and exact NumPy as the shipped
  search baseline.
- Do not expose internal storage normalization choices as the main public abstraction.
- Keep `route`, `query_type`, and low-level param plumbing as internal scaffolding in
  this phase even if the current implementation still uses them under the hood.
- Make the top-level README fully consistent with the current SQL, Cypher, and vector
  runtime behavior.
- Add minimal public examples for `HumemSQL v0`, `HumemCypher v0`, and `HumemVector v0`.
- Add MkDocs and move the project toward a proper docs site instead of relying only on
  the single repository README.
- Make sure install, release, and public-surface wording is good enough that an early
  user can understand the project without reading source.
- Make the SQL, Cypher, and vector `v0` surfaces explicit enough that users can tell
  what is supported and what is intentionally out of scope.
- Make the vector wording explicit enough that users can tell the current path is an
  exact SQLite-plus-NumPy baseline rather than an indexed ANN runtime.
- Make the vector wording explicit enough that users can tell what is supported now:
  row-oriented SQL vector search and node-oriented Cypher vector search, with direct
  vector APIs treated as a separate explicit path rather than the main query
  narrative, and edge vectors deferred.
- Make the public API direction explicit enough that early users can understand that
  `HumemDB` is heading toward `db.query(...)` and later `db.ask(...)`, while the
  current query-type plumbing remains internal and `route` is a secondary override
  rather than the main product story.
- Make the benchmark scripts and benchmark README reproducible enough to justify the
  current routing story.
- Ensure package metadata, docs entry points, and dependency notices are ready for a
  public release.
- Keep this phase focused on choosing and documenting the simple vector model plus the
  docs/examples/package work needed for release.

## Phase 7

Simplify the public surface around `db.query(...)`.

Status: done.

- [x] Treat `db.query(...)` as the explicit public text-query surface for SQL or
  Cypher.
- [x] Separate the direct vector path from `db.query(...)` now instead of carrying
  that overlap forward: direct vectors should live behind their own explicit vector
  API, while `db.query(...)` stays focused on text-query surfaces.
- [x] Start pulling `route`, `query_type`, and low-level param plumbing out of the
  public mental model even if the implementation still uses them internally.
- [x] Make the public API direction clearer: `db.query(...)` is the explicit surface
  and `db.ask(...)` is the later NL surface.
- [x] Keep direct-vector runtime details off the main public path.
- [x] Keep the current runtime deterministic and testable while the public API shape
  is cleaned up.

## Phase 8

Do the internal planning / IR cleanup.

Status: done.

- Started with a thin internal plan layer for `db.query(...)` dispatch and
  candidate-filtered vector execution so the next cleanup steps have a clearer seam.
- Added explicit internal plans for SQL vector writes and direct-vector search.
- Added explicit resolved-candidate objects for candidate-filtered and direct vector
  search.
- Cached logical-vector lookup tables so candidate-filtered and filtered vector search
  no longer
  rebuilds candidate index mappings by re-scanning the full loaded vector set.
- Pulled Cypher node-property writes and graph-node vector synchronization behind
  clearer internal helper boundaries.
- Retired the remaining duplicate SQL vector-write analysis from `db.py`; the live
  SQL vector-write planning path now delegates to the extracted `_vector_runtime.py`
  module.
- Moved more of the `db.query(...)` and `executemany(...)` route/query-type branching
  behind thin internal plan-and-execute helpers so the public entrypoints mostly
  normalize inputs and delegate.
- Removed the remaining internal bounce back through the public `query(...)` surface:
  candidate-filtered vector resolution and the `cypher(...)` convenience path now
  execute through internal query plans directly.
- Added an explicit internal candidate-query result boundary so SQL/Cypher
  candidate resolution is no longer just a loose `QueryResult` hop before vector
  candidate resolution.
- Made broad-candidate candidate-filtered search behavior explicit with
  candidate-coverage metadata while keeping candidate-filtered vector search exact
  even when the candidate query keeps a large
  fraction of one namespace.
- Internalized candidate-query route/query-type/param plumbing behind one thin
  internal candidate-query plan object instead of carrying those fields around
  separately.

- [x] Replace the current narrow mix of SQL AST inspection and Cypher string parsing
  with a small explicit internal plan layer for the supported subset.
- [x] Use that cleanup to consolidate SQL vector writes, Cypher vector writes, and
  vector candidate-query planning behind clearer internal execution boundaries.
- [x] Make candidate-filtered vector execution more explicit and efficient: avoid
  repeated
  candidate-id remapping work, tighten the boundary between frontend filtering and
  vector ranking, and make the candidate-filtered path easier to optimize
  independently.
- [x] Revisit broad-candidate candidate-filtered search behavior once the execution
  boundaries are
  clearer, especially when SQL or Cypher filtering keeps a large fraction of the
  vector set.
- [x] Keep `route`, `query_type`, and low-level params internal to this layer.
- [x] Expect HumemDB to eventually need an internal plan or IR layer, but keep it thin
  and incremental rather than starting with a full compiler architecture.

## Phase 9

Broaden grammar coverage and harden parser/planner support.

Status: done.

Cypher Phase 9 is now explicitly on Path B: meaningful grammar expansion should move
away from growing the current handwritten parser into a broad frontend. The chosen
direction is to use openCypher grammar/TCK materials as reference input while building
a HumemDB-owned parser pipeline in-repo.

- [x] Expand `HumemSQL v0` beyond the initial statement subset toward a broader
  PostgreSQL-like portable grammar where the semantics are clear and testable.
- [x] Move HumemSQL vector support from the current narrow text-shape lowering toward
  proper SQL AST inspection and lowering, so PostgreSQL-like vector syntax is a real
  language feature rather than a regex-shaped special case, while keeping a narrow
  fallback only where parser support is still missing.
- [x] Expand `HumemCypher v0` beyond the initial narrow `CREATE` and `MATCH` subset
  toward a broader Cypher grammar where the relational lowering remains defensible.
- [x] Use the cloned openCypher repository as spec/grammar/TCK reference material,
  not as a runtime dependency or a separate maintained fork unless that later becomes
  necessary.
- [x] Decide the concrete Python-first parser-generation route for Cypher expansion:
  generated parser artifacts in-repo, plus HumemDB-owned normalization, validation,
  and lowering layers.
- [x] Start with the openCypher 9 published ANTLR grammar artifact as the first parser-
  ready input source for experimentation, while using the cloned openCypher main repo
  for current BNF and TCK reference; do not block early frontend work on building a
  full BNF-to-ANTLR conversion pipeline first.
- [x] Treat the current openCypher clone as giving two immediate assets: ISO WG3 BNF
  in `grammar/openCypher.bnf` and clause-organized Cucumber TCK scenarios in
  `tck/features/**`; do not assume the main repository itself is the direct runtime
  parser package.
- [x] Resolve the grammar-generation gap explicitly before implementation: the cloned
  repository workflow references generated `Cypher.g4` output and generator tooling,
  but this checkout does not currently ship those tools or generated artifacts, so
  HumemDB must either source ANTLR grammar artifacts separately or own a preprocessing
  step that turns reference grammar material into parser-ready inputs.
- [x] Create a clear in-repo Cypher frontend boundary for grammar files, generated
  parser artifacts, parse-tree normalization, subset validation, and internal plan
  lowering so the parser can evolve without spilling parser mechanics across `db.py`
  and the current handwritten frontend.
- [x] Use an initial in-repo layout roughly like `src/humemdb/cypher_frontend/`
  with `grammar/`, `generated/`, `parser.py`, `normalize.py`, `validate.py`,
  `lower.py`, and `tck/`, so parser generation, subset policy, and HumemDB-specific
  lowering stay separated from each other.
- [x] Make parser regeneration a documented, owned development workflow rather than an
  implicit local setup requirement: use a Docker-first wrapper so contributors do not
  need Java installed on the host.
- [x] Keep generated parser artifacts verifiable in CI so the checked-in ANTLR output
  cannot silently drift from the vendored grammar.
- [x] Stop treating further broad handwritten Cypher parsing as the main Phase 9
  strategy; keep the current parser only as the narrow shipped path until the new
  frontend replaces or subsumes it.
- [x] Prefer Python-first parser ownership over C or Rust for now; only revisit native
  parser implementations if correctness, packaging, and measured performance work show
  that Python-generated parsing is insufficient.
- [x] Avoid making HumemDB depend on a low-trust small third-party parser package as
  core runtime infrastructure; if external grammar or parser work helps, vendor or
  generate owned artifacts rather than coupling the public runtime to an unstable
  foreign AST/API shape.
- [x] Keep the parser work in-repo first; only split it into a separate repository if
  the parser boundary later stabilizes into a genuinely reusable standalone component.
- [x] Land the first raw generated-parser entrypoint in `src/humemdb/cypher_frontend/`
  and validate that it parses the current `CREATE` and `MATCH ... WHERE ... RETURN`
  shapes while reporting syntax errors through a stable HumemDB-facing result.
- [x] Land the first normalize and validate layers on top of the generated parser so
  the current admitted `CREATE` and `MATCH ... RETURN` subset can be converted into
  stable HumemDB-facing structures before lowering.
- [x] Land the first lowering bridge from generated-parser normalized statements into
  the existing handwritten `GraphPlan` types so admitted `CREATE` and
  `MATCH ... RETURN` queries can be compared for plan equivalence before broader
  runtime replacement.
- [x] Route the admitted Cypher subset through the generated frontend in real runtime
  planning first, while keeping a narrow handwritten fallback only for any
  already-supported shapes that the generated subset policy does not yet admit.
- [x] Keep syntax errors owned by the generated parser path instead of falling back
  to the handwritten parser, so malformed Cypher is rejected by one clear frontend
  boundary and fallback only covers admitted-subset policy gaps.
- [x] Adopt the openCypher TCK incrementally by supported clause family instead of as
  an all-or-nothing certification step; start by mapping HumemDB's current and near-
  term surface against `clauses/create`, `clauses/match`, and `clauses/match-where`
  scenarios before considering broader clause families such as `merge`, `call`, or
  richer expression coverage.
- [x] Use the TCK primarily as behavior validation after HumemDB subset selection,
  not as permission to claim broad compatibility; scenarios outside the admitted
  subset should remain rejected clearly.
- [x] Use existing grammar references such as
  [openCypher](https://github.com/opencypher/openCypher) where they help clarify the
  supported subset, without claiming full compatibility.
- [x] Move Cypher vector support from the current narrow text-shape lowering toward a
  parsed `SEARCH ... VECTOR INDEX ...` analysis path under the current Cypher parser
  boundary, while leaving a fuller first-class plan node for later work.
- [x] Carry language-level vector planning through explicit internal plan variants
  instead of one generic candidate-query blob: SQL-backed and Cypher-backed vector
  query plans now exist as separate internal plan shapes, and their candidate-query
  plans are also language-specific.
- [x] Keep rejecting unsupported constructs clearly instead of pretending to support
  full PostgreSQL or full Cypher compatibility before the implementation is actually
  there.
- [x] Expand the translation-overhead benchmark so parser, lowering, and planning cost
  stays visible as grammar coverage broadens.
- [x] Keep frontend benchmark evidence attached to parser/planner changes so grammar
  work does not silently become a latency bottleneck.
- [x] Reuse parsed plans where possible instead of reparsing at execution time, so the
  planner boundary becomes a real internal seam rather than duplicated frontend work.
- [x] Move SQL read-only classification and lightweight SQL/Cypher shape extraction
  onto parsed structure so later routing is driven by validated plan metadata rather
  than first-keyword heuristics.
- [x] Harden the SQLite-backed graph storage path as Cypher grammar breadth grows:
  make one logical Cypher write atomic by default, tighten graph-table integrity
  guarantees, and keep the SQL-backed graph representation defensible rather than
  assuming the current write path is already robust enough.

Current hardening landed so far:

- broader HumemSQL runtime regression coverage now explicitly locks in read-only
  `WITH ... UNION ALL`, `CASE WHEN EXISTS (...)`, multi-CTE join/aggregate, and
  window-ranked CTE query shapes, so those already-working PostgreSQL-like forms
  are defended in tests instead of living only as implicit runtime behavior
- public syntax docs now reflect that broadened `HumemSQL v0` read surface instead
  of underselling it as only the original statement-family list
- logical Cypher writes now execute atomically by default on SQLite
- SQLite-backed graph tables now enforce referential integrity between nodes, edges,
  and graph property tables through foreign-key constraints on new databases
- SQLite-backed graph node property storage now enforces at most one vector-valued
  property row per node, keeping graph-property state aligned with the canonical
  vector sidecar for graph-owned embeddings
- the translation-overhead benchmark now measures handwritten parse cost, generated-
  first runtime planning cost, and bind+compile cost separately for the admitted
  Cypher subset
- parser/planner benchmark coverage now includes parenthesized node boolean filters,
  untyped relationship reads, relationship-type alternation, and anonymous-endpoint
  relationship reads
- the admitted non-vector Cypher `WHERE` subset now also supports string property
  predicates with `STARTS WITH`, `ENDS WITH`, and `CONTAINS`, with generated-
  frontend, runtime, and translation-overhead regression coverage instead of leaving
  those grammar-admitted operators outside the executable subset
- the admitted non-vector Cypher `WHERE` subset now also supports `IS NULL` and
  `IS NOT NULL` over stored node and relationship properties, with the executable
  subset treating absent properties as null for `IS NULL` checks and keeping that
  behavior under generated-frontend, runtime, and translation-overhead regression
  coverage
- the admitted non-vector Cypher write subset now also supports narrow
  `MATCH ... DETACH DELETE node_alias` and `MATCH ... DELETE relationship_alias`
  statements, with generated-frontend and runtime regression coverage instead of
  leaving graph deletion semantics outside the generated-first planner path
- TCK-style subset regressions now also cover string predicates, null predicates,
  `DISTINCT` plus `OFFSET`, and narrow delete flows so the documented generated
  Cypher subset is defended as a language boundary rather than only by ad hoc
  unit tests
- generated-frontend regression coverage now locks in the remaining admitted
  MATCH ... CREATE endpoint-reuse variants, including new-start-node creation from a
  matched end node and reverse-direction connection between two matched nodes
- generated and handwritten Cypher pagination now also accept `OFFSET` as the same
  admitted integer-literal synonym as `SKIP`, keeping the vendored grammar and
  runtime subset aligned with the openCypher pagination shape
- ordinary non-vector Cypher planning now treats the generated frontend as the
  authoritative runtime boundary instead of silently falling back to the handwritten
  parser for `MATCH` and `CREATE` statements
- SQLite-backed graph and vector cleanup now stay aligned when graph-owned nodes are
  deleted: graph-node deletes remove graph-owned vector rows, vector-row deletes
  remove vector metadata rows, and Cypher writes invalidate the exact-search cache
  so graph-owned embedding updates and deletes stay visible immediately
- [x] Treat this as the phase where grammar breadth and internal language correctness
  are reconsidered seriously, not as part of the initial public snapshot.

## Phase 10

Add automatic routing and deeper query/workload classification.

Status: done.

- [x] Keep `route` out of the main public mental model as `db.query(...)` becomes the
  main explicit surface.
- [x] Do not require public `query_type`; `db.query(...)` now infers SQL, Cypher, and
  the current language-level vector forms from the query text.
- [x] Keep public batch execution aligned with that cleanup: `executemany(...)` now
  assumes SQL internally instead of exposing a separate public `query_type` switch.
- [x] Keep the internal `"vector"` label off the public query-type surface: public
  query typing now stays SQL-or-Cypher, while direct vector-method results report
  `None` instead of exposing a third public query kind.
- [x] Treat query-type inference as a convenience feature, not as part of the core
  vector model.
- [x] Keep `route` internal even after automatic routing is added.
- [x] Build on the current text-surface inference and parser/planner work instead of
  reintroducing public query-type controls.
- [x] Validate the supported portable SQL and Cypher subsets before routing a query.
- [x] Keep internal direct vector-object calls explicit instead of trying to infer
  vector intent from arbitrary free-form text.
- [x] Stop using `query_type == "vector"` as the main internal dispatcher switch once
  the explicit SQL-vector and Cypher-vector plan variants are strong enough to drive
  execution directly from plan shape.
- [x] Classify queries safely at first: read versus write, then simple OLTP-style
  versus OLAP-style read shapes.
- [x] Keep the first automatic-routing implementation in Python and make its behavior
  explainable in tests and logs.
- [x] Route writes, point reads, and explicit transactional work to SQLite.
- [x] Route broader scans, aggregates, and analytical reads to DuckDB.
- [x] Ship the first conservative omitted-route automatic-routing slice: broad
  analytical SQL may route to DuckDB, while writes, ambiguous SQL, and current
  Cypher reads remain SQLite-preferred by default.
- [x] Keep SQL OLAP-to-DuckDB recommendation conservative until the benchmark suite
  produces calibrated admission thresholds; do not treat every join or aggregate as
  enough evidence on its own.
- [x] Keep routing explainable and overridable internally, even if it is no longer a
  main public knob.
- [x] Extend the benchmark suite so routing decisions are justified by measured
  workload results, not architecture preferences.
- [x] Broaden the benchmark matrix beyond a few obvious DuckDB wins: keep expanding
  SQL and Cypher workload families so selective joins, anchored graph traversals,
  reverse-edge reads, broad fanout, ordered limits, and CTE-backed rollups are all
  measured before routing rules harden.
- [x] Make the SQL and Cypher benchmark scripts emit machine-readable JSON summaries so
  scale sweeps and threshold extraction can be automated instead of hand-read from
  terminal output.
- [x] Add a routing sweep helper plus a threshold-report helper so SQL and Cypher
  crossover summaries can be reproduced from code rather than ad hoc shell history.
- [x] Record the current graph-routing boundary explicitly in code and docs: present
  Cypher evidence is still not broad enough to harden automatic DuckDB routing beyond
  a narrow broad-fanout case, so graph reads should remain SQLite-preferred until the
  benchmark matrix gets stronger.
- [x] Re-run relational, graph, and candidate-filtered vector benchmarks when routing
  heuristics change so the multimodel story remains evidence-backed.
- [x] Define a small representative routing benchmark set that can catch
  misclassification regressions before they become product behavior.

Current regression guardrails:

- representative SQL and Cypher routing-crossover expectations now have dedicated
  tests around the routing sweep and threshold-report tooling, so scale-summary
  output shape and first-DuckDB-win reporting are covered by regression tests.
- SQL routing benchmark outputs now also carry the same lightweight plan-shape
  metadata the runtime classifier uses, and threshold reports emit an env-consumable
  SQL OLAP recommendation block so measured routing evidence can feed planner
  calibration without adding a new public API knob.
- SQL benchmark regression coverage now explicitly locks in `EXISTS`-filtered and
  `DISTINCT` join-projection workload families, and planning tests now defend the
  matching calibrated DuckDB-routing rules instead of leaving those classifier
  branches justified only by checked-in benchmark JSON.
- candidate-filtered vector routing evidence now also flows through the same sweep
  and threshold-report tooling, with regression coverage around representative
  vector crossover summaries instead of leaving ANN-versus-exact evidence in a
  separate ad hoc reporting path.
- the graph routing benchmark matrix now also covers anonymous-endpoint relationship
  reads, so admitted anonymous-node graph patterns remain part of the measured
  routing evidence instead of living only in unit tests.
- the graph routing benchmark matrix now also covers broader reverse-direction
  relationship traversal reads with ordering, so admitted reverse-edge traversal
  families are not represented only by selective property-anchor cases.
- the translation-overhead and graph-routing benchmark suites now also keep admitted
  `DISTINCT` plus paginated `OFFSET` read shapes under regression coverage, so new
  Cypher breadth does not land without parser, lowering, and compile evidence.
- explicit route overrides now stay confined to internal planning and execution
  tests rather than the public `HumemDB` surface, so omitted-route automatic
  routing remains the only public path while route-selection coverage is preserved
  at the internal seam where it belongs.
- the representative `routing_sweep_current` SQL, graph, and candidate-filtered
  vector artifacts have been regenerated from code and fed back through the
  threshold-report helper, so the checked-in routing story remains tied to fresh
  JSON evidence instead of stale benchmark outputs

## Phase 11

Re-evaluate the graph-table representation as its own architecture phase.

Status: completed.

- [x] Treat this as a distinct architecture decision after Phase 9 grammar growth and
  Phase 10 routing evidence, not as overflow work inside either of those phases.
- [x] Revisit the current graph-to-table representation with dedicated benchmark
  evidence when graph runtime behavior changes materially: measure whether the SQLite
  graph tables plus DuckDB-over-SQLite analytics path are still solid enough before
  changing routing claims or pursuing deeper graph-storage redesign.
- [x] Separate three questions clearly: whether the current write path is correct
  enough, whether the current storage model is performant enough, and whether a real
  structural redesign is justified.
- [x] Keep the default outcome conservative unless evidence says otherwise: prefer to
  keep the current representation plus targeted hardening if benchmarks still support
  it.
- [x] Measure the current graph-table path more directly on the access patterns that
  are most likely to become product constraints: multi-edge traversals, endpoint-plus-
  type filters, endpoint-plus-property filters, and broader ordered fanout reads.
- [x] Revisit whether the current default SQLite graph indexes are still the right
  minimal set once the broader benchmark matrix is rerun, and add narrowly targeted
  graph indexes before considering a larger storage redesign.
- [x] Check whether graph property-table joins, rather than the node/edge base tables
  themselves, have become the main graph-read bottleneck before changing the storage
  model.
- [x] Evaluate whether a more DuckDB-friendly graph-read shape or lightweight
  analytical projection is justified for broader graph scans, but keep fresh
  DuckDB-over-SQLite reads as the default analytical path unless measured evidence
  says otherwise.
- [x] Only pursue graph-storage redesign in this phase if benchmark evidence shows the
  current SQLite-backed graph tables have become a meaningful product constraint.
- [x] If redesign work is justified, compare it against the current graph-table path on
  correctness cost, routing implications, migration cost, and benchmark deltas rather
  than treating a new storage model as automatically better.

Current Phase 11 progress:

- current benchmark/report plumbing now captures graph workload families, lightweight
  plan-shape metadata, and SQLite plan summaries so storage and index questions are
  driven by current evidence rather than ad hoc inspection
- unordered `MATCH` no longer carries implicit stable ordering; explicit `ORDER BY`
  is the only ordering contract and the benchmark suite now measures ordered versus
  unordered cost separately
- ordered relationship reads now reuse property joins across projection and ordering,
  and descending single-key top-k relationship reads use a narrowed projection fast
  path when the measured plan shape justifies it
- disjunctive relationship `MATCH` queries now union matched graph identities before
  the outer projection and ordering step, which materially reduces the
  `social_mixed_boolean` workload without changing row semantics
- split Phase 11 index experiments show the node-property covering index is the
  better narrow candidate, but the broader `phase11-targeted` rollout is still not a
  default winner after the saved 100k and 1M routing sweeps; the current minimal
  graph index set still stands unless a narrower ordered-workload win emerges
- [x] ordinary app-owned SQLite `CREATE INDEX IF NOT EXISTS ...` DDL now works
  through `db.query(...)`, so public examples no longer need to reach into
  `db.sqlite` just to add workload-specific relational indexes
- the remaining active bottlenecks are still temp-B-tree-heavy ordered traversals and
  the question of whether narrowly targeted graph indexes are justified before any
  storage redesign work

Phase 11 conclusion:

- write-path question: this phase did not uncover evidence that the current SQLite
  graph write path is incorrect enough to force a storage-model change; the work here
  stayed focused on read-path planning and benchmark evidence
- storage-model question: the current SQLite-backed graph tables are still performant
  enough for the admitted public graph contract once the compiler avoids implicit
  stable ordering, reuses property joins, and narrows the highest-value ordered
  top-k relationship reads before full projection
- redesign question: a structural graph-storage redesign is not justified by the
  current benchmark matrix, so the redesign-comparison branch closes here as not
  triggered rather than rolling speculative architecture work into Phase 11

## Phase 12

Add larger ingestion strategies.

Status: in progress.

- [x] Keep SQLite as the canonical ingest target because it remains the source of truth.
- [ ] Add larger file-based and workload-specific ingestion paths only when the simple
  transactional SQLite path is no longer enough.
- [ ] Consider an optional Parquet materialization path only if later benchmarks show
  that repeated broad analytical reads need a snapshot or cache layer beyond the
  current live DuckDB-over-SQLite path.
- [x] Start with CSV-first bulk ingest for table data into SQLite tables.
- [x] Add graph CSV ingest into the SQLite-backed graph tables rather than treating the
  initial Cypher frontend as the bulk loader.
- [x] Prioritize graph CSV ingest ahead of broader Cypher write-surface growth when
  the goal is operational scale rather than query-language completeness.
- [ ] Add a real bulk Cypher ingest path later if usage justifies it; the current
  `HumemCypher v0` write surface is transactional but still statement-oriented rather
  than a true batched `CREATE` bulk loader.
- [ ] Allow staging-table and normalize-into-final-table flows where they make ingest
  simpler or safer.
- [ ] Keep DuckDB as the analytical read path after ingest, not as the canonical
  ingest destination.
- [ ] If Parquet is added later, keep it as an optional analytical snapshot/export
  layer, not as a replacement for fresh DuckDB reads over the SQLite source of truth.
- [ ] Re-evaluate append-heavy time-series workloads later to see whether they need
  time-aware partitioning, retention, rollups, or helper APIs beyond the current SQL
  surface.
- [ ] Choose ingest strategies based on data size, source format, and workload instead
  of assuming one bulk-load path fits everything.
- [ ] Keep this as an ingestion/runtime phase, not a change to the public query
  surfaces.
- [x] Add ingest benchmarks for transactional insert, CSV load, graph CSV ingest, and
  staging/normalize flows so larger ingest paths are admitted by evidence.
- [x] Measure ingest-to-query freshness and end-to-end load cost, not just raw rows per
  second.

Current Phase 12 progress:

- `HumemDB` now ships a first public ingestion family with `import_table(...)`,
  `import_nodes(...)`, and `import_edges(...)`, all backed by chunked CSV reads,
  SQLite transactions, and direct SQLite batch writes on the relational hot path
- relational CSV import currently supports header-based column inference, explicit
  column selection, headerless imports with explicit columns, and full rollback when
  later chunks fail
- graph CSV ingest now writes directly into the current SQLite-backed graph tables,
  preserving explicit imported node ids, assigning edge ids transactionally, and
  reusing the existing graph property encoding rules for string, integer, real, and
  boolean property values
- focused public-surface tests now cover relational CSV import, graph node import,
  graph edge import, and rollback behavior on relational uniqueness failures and graph
  foreign-key failures
- the dedicated `csv_ingest.py` comparison benchmark now measures relational ingest,
  graph node ingest, graph edge ingest, and post-ingest freshness queries against the
  realistic public baselines plus internal SQLite lower bounds
- the post-optimization sweep through `1M` rows now shows `import_table(...)` beating
  public `executemany(...)`, while `import_nodes(...)` and `import_edges(...)` stay
  close to the internal lower bound and far ahead of repeated public Cypher writes
- the benchmark README and public examples now cover the new ingestion family, so this
  phase has moved from “API exists” to “API is benchmark-backed and documented,” even
  though later staging/normalize and non-CSV ingest work still remains open

## Phase 13

Evaluate broader graph property values.

Status: planned.

- [ ] Decide whether HumemDB graph properties should remain scalar-only or expand toward
  lists, nested values, or more document-like payloads.
- [ ] Treat this as a data-model decision, not just a parser or grammar extension.
- [ ] Define the storage, indexing, filtering, ordering, and return semantics before
  claiming support for broader graph properties.
- [ ] Keep this explicitly out of the initial public snapshot.
- [ ] If broader graph property values are explored, benchmark their storage and query
  cost against the scalar baseline before widening the model.

## Phase 14

Add indexed vector runtime and vector index lifecycle once indexed search is real.

Status: planned.

- [ ] Keep this out of the current exact-baseline work.
- [ ] Add explicit vector index build, rebuild, refresh, inspect, and drop operations
  once HumemDB ships a real indexed vector path.
- [ ] Expose vector index lifecycle through the internal/advanced vector object API
  first.
- [ ] Add matching SQL and Cypher support later so vector indexing is not permanently
  object-API-only.
- [ ] Keep exact search free of mandatory index-build steps.
- [ ] Use vector benchmarks to define the admission bar for indexed search: build cost,
  refresh cost, latency, recall, and memory overhead must justify the added runtime.
- [ ] Extend the current vector sweep and tuning benchmarks so exact versus indexed
  crossover points are measured rather than guessed.
- [ ] Treat this as the phase where indexed ANN semantics and lifecycle are defined
  together, not separately.

## Phase 15

Harden the public surfaces for `v0.1.0`.

Status: in progress.

This phase is about coherence, correctness, and documentation quality, not preserving
backward compatibility with every earlier pre-`v0.1.0` API shape.

- [ ] Review whether `HumemSQL v0`, `HumemCypher v0`, and `HumemVector v0` are
  stable, coherent, and documented enough to ship as one explicit `v0.1.0`
  snapshot.
- [ ] Use surface maturity, benchmark evidence, and routing stability as the bar for
  release hardening, not raw feature count alone.
- [ ] Decide explicitly whether HumemCypher should take one more narrow operational
  clause-family expansion before `v0.1.0`, instead of letting that scope drift during
  release hardening.
- [ ] If one more graph-clause expansion lands before `v0.1.0`, prioritize a narrow
  `MERGE` subset first because it improves real graph write workflows more directly
  than broader path semantics.
- [ ] Consider a narrow `OPTIONAL MATCH` subset only after the `MERGE` decision and
  only if its null/return semantics can be documented and defended cleanly in tests.
- [ ] Keep variable-length paths, named paths, richer multi-part Cypher flows, and
  other broad graph-language features explicitly out of the `v0.1.0` admission bar
  unless benchmark-backed product evidence shows they are more urgent than storage,
  ingest, and release-coherence work.
- [ ] Tighten unsupported-case behavior and error messages across SQL, Cypher, and
  vector paths.
- [ ] Make result shapes and explicit query semantics stable and well documented.
- [ ] Re-run benchmark-backed routing checks when runtime behavior changes materially.
- [ ] Revisit the SQLite-to-NumPy vector load and exact-index materialization path if
  later benchmarks show it has become a real bottleneck, but keep the current simple
  loader unless measured evidence justifies lower-level optimization work.
- [ ] Add public-surface tests that defend the documented semantics instead of only
  the current happy paths.
- [ ] Turn the benchmark suite into a release-hardening tool with stable representative
  workloads and explicit regression thresholds.
- [ ] Make release decisions depend on benchmark regressions as well as correctness
  regressions.
- [ ] Use this phase to close the gap between a useful `v0` implementation and a
  release candidate that is stable enough to publish.

Current Phase 15 progress:

- public examples and docs now use the backend-neutral `HumemDB.open(...)` helper
  instead of teaching paired SQLite and DuckDB filenames directly
- public examples no longer label query results with backend-specific names such as
  `sqlite_result` or `duckdb_result` where the route is an internal concern
- ordinary app-owned SQLite `CREATE INDEX IF NOT EXISTS ...` DDL now works through
  `db.query(...)`, so the public examples no longer need to imply that users should
  reach into backend engine handles to manage workload-specific relational indexes
- `HumemDB` no longer exposes `sqlite` or `duckdb` as public attributes, and the test
  suite now locks that public-surface expectation in directly
- a narrow public convenience constructor and the first import-family public-surface
  tests are now in place
- the first public ingestion family is now benchmark-validated through `1M` rows and
  documented with a dedicated public example, which removes a major release-hardening
  gap for operational data loading
- broader release-hardening checks, benchmark gates, and final SQL/Cypher/vector
  coherence review still remain open

## Phase 16

Release `v0.1.0`.

Status: planned.

Pre-`v0.1.0` cleanup can still make clean breaking changes. The release bar is one
coherent shipped surface, not continuity with every earlier experimental shape.

- [ ] Cut the GitHub `v0.1.0` release once `db.query(...)`, the docs, and the
  explicit SQL/Cypher/vector baseline form one coherent public snapshot.
- [ ] Publish the same `v0.1.0` to PyPI once the README, MkDocs setup, package
  metadata, and examples match that public snapshot.
- [ ] Require the simple public vector model to be settled before release; do not
  ship a placeholder abstraction that is likely to be renamed immediately.
- [ ] Require the near-term vector story to be clear before release: SQL vector
  search is row-oriented and Cypher vector search is node-oriented; keep direct-vector
  runtime details out of the main public product narrative.
- [ ] Require the direct vector-only story to be clear before release: it is an
  explicit secondary surface, not the main public abstraction.
- [ ] Require the public `v0` paths to be green in tests before release.
- [ ] Require the benchmark suite to be green enough before release that the routing and
  multimodel claims remain defensible.
- [ ] Do not block `v0.1.0` on `db.ask(...)`, later model work, or future planner
  refinement.

## Phase 17

Add `db.ask(...)` as the final major public surface.

Status: planned.

- [ ] Keep `db.ask(...)` as the natural-language public surface built on top of the
  already-cleaned `db.query(...)` surface and internal plan layer.
- [ ] Do not expose `route`, `query_type`, or low-level param plumbing from
  `db.ask(...)`.
- [ ] Let `db.ask(...)` own intent understanding and planner selection instead of
  forcing users to think in terms of SQL versus Cypher versus vector execution.
- [ ] Start narrow: make `db.ask(...)` good at the supported SQL/Cypher/vector subset
  instead of pretending to solve arbitrary natural-language database questions.
- [ ] Start with an existing model or constrained NL-to-plan flow before considering
  any model training.
- [ ] Benchmark `db.ask(...)` separately from raw `db.query(...)` so NL latency,
  planner quality, and end-to-end overhead are visible instead of being conflated with
  core runtime performance.
- [ ] Keep this phase focused on the first coherent NL UX, not on perfect automation
  or a fully general planner.

## Phase 18

Revisit graph storage architecture if later evidence justifies it.

Status: planned.

This phase should only open after `v0.1.0` if later data-model or workload evidence
shows that the current SQLite-backed property-graph table design has become the real
constraint rather than the current implementation around it.

- [ ] Do not treat this as pre-`v0.1.0` scope creep; keep the current graph storage
  model through release hardening unless benchmark or correctness evidence forces a
  change earlier.
- [ ] Revisit the graph storage layout only if Phase 13 broadens graph property values,
  Phase 12 follow-on ingest work still leaves graph loading too expensive, or later
  workloads show that the current table model has become a product bottleneck.
- [ ] Start by checking whether staged bulk-load flows, normalize-into-final-table
  paths, or index-lifecycle changes solve the problem before replacing the logical
  graph storage model outright.
- [ ] If a deeper storage redesign is needed, compare it explicitly against the current
  `graph_nodes` plus `graph_edges` plus property-table layout on ingest cost, query
  cost, operational complexity, and migration risk.
- [ ] Keep SQLite as the canonical persisted store unless the broader HumemDB storage
  strategy itself changes; this phase is about graph layout and write/read strategy,
  not about replacing the embedded storage foundation casually.
- [ ] Treat broader property payloads, operational graph writes such as `MERGE`, and
  post-release ingest workloads as the main triggers for deciding whether the current
  EAV-style property-table model is still the right tradeoff.
