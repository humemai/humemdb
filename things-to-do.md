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
- Expose the first public vector surface through `query_type="vector"` on the SQLite
  route and convenience methods on `HumemDB` for insert and search.
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
- Keep a minimal direct vector runtime for internal testing, benchmarking, and narrow
  experimental use, but do not treat it as a main public product surface.
- Let the internal direct vector runtime use distinct object-style APIs and metadata
  filters without forcing that model onto the main public `HumemDB` story.
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
  vector APIs treated as internal/experimental and edge vectors deferred.
- Make the public API direction explicit enough that early users can understand that
  `HumemDB` is heading toward `db.query(...)` and later `db.ask(...)`, while the
  current low-level routing/query-type controls remain implementation details.
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

Status: in progress.

- [ ] Expand `HumemSQL v0` beyond the initial statement subset toward a broader
  PostgreSQL-like portable grammar where the semantics are clear and testable.
- [x] Move HumemSQL vector support from the current narrow text-shape lowering toward
  proper SQL AST inspection and lowering, so PostgreSQL-like vector syntax is a real
  language feature rather than a regex-shaped special case, while keeping a narrow
  fallback only where parser support is still missing.
- [ ] Expand `HumemCypher v0` beyond the initial narrow `CREATE` and `MATCH` subset
  toward a broader Cypher grammar where the relational lowering remains defensible.
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
- [ ] Keep rejecting unsupported constructs clearly instead of pretending to support
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
- [ ] Treat this as the phase where grammar breadth and internal language correctness
  are reconsidered seriously, not as part of the initial public snapshot.

## Phase 10

Add automatic routing and deeper query/workload classification.

Status: in progress.

- [x] Keep `route` out of the main public mental model as `db.query(...)` becomes the
  main explicit surface.
- [x] Do not require public `query_type`; `db.query(...)` now infers SQL, Cypher, and
  the current language-level vector forms from the query text.
- [x] Treat query-type inference as a convenience feature, not as part of the core
  vector model.
- [ ] Keep `route` internal even after automatic routing is added.
- [x] Build on the current text-surface inference and parser/planner work instead of
  reintroducing public query-type controls.
- [x] Validate the supported portable SQL and Cypher subsets before routing a query.
- [ ] Keep internal direct vector-object calls explicit instead of trying to infer
  vector intent from arbitrary free-form text.
- [ ] Stop using `query_type == "vector"` as the main internal dispatcher switch once
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
- [ ] Keep routing explainable and overridable internally, even if it is no longer a
  main public knob.
- [ ] Extend the benchmark suite so routing decisions are justified by measured
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
- [ ] Re-run relational, graph, and candidate-filtered vector benchmarks when routing
  heuristics change so the multimodel story remains evidence-backed.
- [ ] Define a small representative routing benchmark set that can catch
  misclassification regressions before they become product behavior.

## Phase 11

Add larger ingestion strategies.

Status: planned.

- [ ] Keep SQLite as the canonical ingest target because it remains the source of truth.
- [ ] Add larger file-based and workload-specific ingestion paths only when the simple
  transactional SQLite path is no longer enough.
- [ ] Consider an optional Parquet materialization path only if later benchmarks show
  that repeated broad analytical reads need a snapshot or cache layer beyond the
  current live DuckDB-over-SQLite path.
- [ ] Start with CSV-first bulk ingest for table data into SQLite tables.
- [ ] Add graph CSV ingest into the SQLite-backed graph tables rather than treating the
  initial Cypher frontend as the bulk loader.
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
- [ ] Add ingest benchmarks for transactional insert, CSV load, graph CSV ingest, and
  staging/normalize flows so larger ingest paths are admitted by evidence.
- [ ] Measure ingest-to-query freshness and end-to-end load cost, not just raw rows per
  second.

## Phase 12

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

## Phase 13

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

## Phase 14

Harden the public surfaces for `v0.1.0`.

Status: planned.

- [ ] Review whether `HumemSQL v0`, `HumemCypher v0`, and `HumemVector v0` are
  stable, coherent, and documented enough to ship as one explicit `v0.1.0`
  snapshot.
- [ ] Use surface maturity, benchmark evidence, and routing stability as the bar for
  release hardening, not raw feature count alone.
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

## Phase 15

Release `v0.1.0`.

Status: planned.

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
  internal/experimental runtime, not the main public abstraction.
- [ ] Require the public `v0` paths to be green in tests before release.
- [ ] Require the benchmark suite to be green enough before release that the routing and
  multimodel claims remain defensible.
- [ ] Do not block `v0.1.0` on `db.ask(...)`, later model work, or future planner
  refinement.

## Phase 16

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
