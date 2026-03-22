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

Status: next.

- Decide one simple vector story that maps cleanly onto SQLite storage.
- Treat vectors as attached to rows, nodes, or minimal vector-only records.
- Keep `HumemDB.query(...)` as the main public center of gravity instead of growing a
  large separate vector API family.
- Make SQL vector search mean row-oriented vector search by default.
- Let SQL vector search scope come from SQL itself: table, vector-bearing column, and
  normal SQL filters should define the candidate set.
- Let SQL vector writes also live in SQL itself: vector-bearing row inserts and narrow
  updates should work through the SQL surface instead of helper-only methods.
- Make Cypher vector search mean node-oriented vector search by default.
- Let Cypher vector search scope come from Cypher itself: node labels, node properties,
  and graph patterns should define the candidate set.
- Let Cypher vector writes also live in Cypher itself: vector-bearing node creates and
  narrow `MATCH ... SET n.embedding = ...` flows should work through the Cypher
  surface instead of helper-only methods.
- Keep a minimal direct object API for vector-only users, but treat it as convenience,
  not as the conceptual center of the vector model.
- Let vector-only users categorize vectors through metadata and filters instead of
  reintroducing `collection` as the main public abstraction.
- Keep the first vector-only categorization step narrow: one canonical SQLite-backed
  vector table plus simple metadata/filter support.
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
- Keep `query_type` explicit in this phase even if SQL-versus-Cypher inference is added
  later.
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
  row-oriented SQL vector search, node-oriented Cypher vector search, and a minimal
  vector-only object API, with edge vectors deferred.
- Make the vector wording explicit enough that users can tell vector search is intended
  to exist through SQL, Cypher, and a thin object API, even if implementation lands
  incrementally.
- Make the benchmark scripts and benchmark README reproducible enough to justify the
  current routing story.
- Ensure package metadata, docs entry points, and dependency notices are ready for a
  public release.
- Keep this phase focused on choosing and documenting the simple vector model plus the
  docs/examples/package work needed for release.

## Phase 7

Release `v0.1.0`.

Status: release immediately after Phase 6.

- Cut the GitHub `v0.1.0` release once the repo is a clean, reproducible public
  snapshot.
- Publish the same `v0.1.0` to PyPI once the README, MkDocs setup, package metadata, and
  examples are aligned with the shipped runtime.
- Treat this as the first coherent public preview of the current SQL, graph, and exact
  vector baseline.
- Require the simple public vector model to be settled before release; do not ship a
  placeholder abstraction that is likely to be renamed immediately.
- Require the near-term vector story to be clear before release: SQL vector search is
  row-oriented, Cypher vector search is node-oriented, and the direct object API is the
  thin vector-only convenience path.
- Require the direct vector-only story to be clear before release: categorization uses
  metadata/filtering, not named collections.
- Require the public `v0` paths to be green in tests before release.
- Do not block `v0.1.0` on indexed LanceDB runtime integration, automatic routing, or
  later planning work.

## Phase 8

Do the internal planning / IR cleanup after `v0.1.0`.

- Keep this post-release on purpose; do not let Phase 6 expand into a parser/compiler
  rewrite.
- Replace the current narrow mix of SQL AST inspection and Cypher string parsing with a
  small explicit internal plan layer for the supported subset.
- Use that cleanup to consolidate SQL vector writes, Cypher vector writes, and vector
  scope planning behind clearer internal execution boundaries.
- Make scoped vector execution more explicit and efficient: avoid repeated candidate-id
  remapping work, tighten the boundary between frontend filtering and vector ranking,
  and make the scoped path easier to optimize independently.
- Revisit broad-candidate scoped search behavior once the execution boundaries are
  clearer, especially when SQL or Cypher filtering keeps a large fraction of the vector
  set.
- Expect HumemDB to eventually need an internal plan or IR layer, but keep it thin and
  incremental rather than starting with a full compiler architecture.
- Design earlier seams so this plan layer can be inserted cleanly instead of forcing a
  later rewrite from scratch.

## Phase 9

Add indexed vector runtime and vector index lifecycle once indexed search is real.

- Keep this out of the current exact-baseline work.
- Add explicit vector index build, rebuild, refresh, inspect, and drop operations once
  HumemDB ships a real indexed vector path.
- Expose vector index lifecycle through the Python object API first.
- Add matching SQL and Cypher support later so vector indexing is not permanently
  object-API-only.
- Keep exact search free of mandatory index-build steps.
- Treat this as the phase where indexed ANN semantics and lifecycle are defined
  together, not separately.

## Phase 10

Add query classification, optional inference, and automatic routing.

- Keep `query_type` as an explicit override even if inference is added.
- Detect SQL versus Cypher with lightweight parsing or heuristics, not an LLM.
- Treat query-type inference as a convenience feature, not as part of the core vector
  model.
- Keep direct vector-object calls explicit instead of trying to infer vector intent from
  arbitrary free-form text.
- Classify read versus write queries safely.
- Detect simple OLTP versus OLAP query shapes.
- Validate the supported portable SQL subset.
- Keep the first implementation in Python.
- Point reads and transactional queries to SQLite.
- Send broader scans, aggregates, and analytics to DuckDB.
- Keep routing explainable and overridable.

## Phase 11

Add larger ingestion strategies.

- Keep SQLite as the canonical ingest target because it remains the source of truth.
- Add larger file-based and workload-specific ingestion paths only when the simple
  transactional SQLite path is no longer enough.
- Consider an optional Parquet materialization path only if later benchmarks show that
  repeated broad analytical reads need a snapshot or cache layer beyond the current
  live DuckDB-over-SQLite path.
- Start with CSV-first bulk ingest for table data into SQLite tables.
- Add graph CSV ingest into the SQLite-backed graph tables rather than treating the
  initial Cypher frontend as the bulk loader.
- Add a real bulk Cypher ingest path later if usage justifies it; the current
  `HumemCypher v0` write surface is transactional but still statement-oriented rather
  than a true batched `CREATE` bulk loader.
- Allow staging-table and normalize-into-final-table flows where they make ingest
  simpler or safer.
- Keep DuckDB as the analytical read path after ingest, not as the canonical ingest
  destination.
- If Parquet is added later, keep it as an optional analytical snapshot/export layer,
  not as a replacement for fresh DuckDB reads over the SQLite source of truth.
- Re-evaluate append-heavy time-series workloads later to see whether they need
  time-aware partitioning, retention, rollups, or helper APIs beyond the current SQL
  surface.
- Choose ingest strategies based on data size, source format, and workload instead of
  assuming one bulk-load path fits everything.
- Keep this as an ingestion/runtime phase, not a change to the public query surfaces.

## Phase 12

Broaden SQL and Cypher grammar coverage.

- Expand `HumemSQL v0` beyond the initial statement subset toward a broader
  PostgreSQL-like portable grammar where the semantics are clear and testable.
- Expand `HumemCypher v0` beyond the initial narrow `CREATE` and `MATCH` subset toward a
  broader Cypher grammar where the relational lowering remains defensible.
- Keep rejecting unsupported constructs clearly instead of pretending to support full
  PostgreSQL or full Cypher compatibility before the implementation is actually there.
- Treat this as the phase where grammar breadth is reconsidered seriously, not as part
  of the initial `v0.1.0` release bar.

## Phase 13

Evaluate broader graph property values.

- Decide whether HumemDB graph properties should remain scalar-only or expand toward
  lists, nested values, or more document-like payloads.
- Treat this as a data-model decision, not just a parser or grammar extension.
- Define the storage, indexing, filtering, ordering, and return semantics before
  claiming support for broader graph properties.
- Keep this explicitly out of the initial `v0.1.0` scope.

## Phase 14

Evaluate `v1` readiness and harden the public surfaces.

- Review whether `HumemSQL v0`, `HumemCypher v0`, and `HumemVector v0` are stable enough
  to promote to `v1`.
- Use surface maturity, benchmark evidence, and routing stability as the bar for
  promotion, not raw feature count alone.
- Promote a frontend to `v1` only when HumemDB is ready to preserve its semantics as a
  real compatibility commitment.
- Tighten unsupported-case behavior and error messages across SQL, Cypher, and vector
  paths.
- Make result shapes, parameter behavior, and route/query-type semantics explicit.
- Re-run benchmark-backed routing checks when runtime behavior changes materially.
- Revisit the SQLite-to-NumPy vector load and exact-index materialization path if later
  benchmarks show it has become a real bottleneck, but keep the current simple loader
  unless measured evidence justifies lower-level optimization work.
- Add public-surface tests that defend the documented semantics instead of only the
  current happy paths.
- Use this phase to close the gap between a useful `v0` and a frontend that is stable
  enough to promote to `v1`.

## Phase 15

Add natural language support later.

- Start with a small model or parser that maps natural language into structured HumemDB
  requests.
- Compile that structured request into SQL, Cypher, or vector operations.
- Do not make raw natural-language-to-backend-SQL the core interface.
