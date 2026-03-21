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
  collection matrices.
- Expose the first public vector surface through `query_type="vector"` on the SQLite
  route and convenience methods on `HumemDB` for insert and search.
- Support optional bucket filtering for the exact path.
- Benchmark the exact NumPy path against collection size, dimensionality, and `top_k`
  so routing can be based on measured crossover points instead of guesswork.
- Include quantization experiments as part of the Phase 5 benchmark work.
- Keep LanceDB as an optional accelerated backend only where the benchmark justifies the
  extra complexity; it is not the default path.

## Phase 6

Documentation and packaging hardening.

Status: next.

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
- Make the benchmark scripts and benchmark README reproducible enough to justify the
  current routing story.
- Ensure package metadata, docs entry points, and dependency notices are ready for a
  public release.
- Keep this phase focused on docs, examples, and packaging polish instead of adding new
  backend behavior.

## Phase 7

Release `v0.1.0`.

Status: release immediately after Phase 6.

- Cut the GitHub `v0.1.0` release once the repo is a clean, reproducible public
  snapshot.
- Publish the same `v0.1.0` to PyPI once the README, MkDocs setup, package metadata, and
  examples are aligned with the shipped runtime.
- Treat this as the first coherent public preview of the current SQL, graph, and exact
  vector baseline.
- Require the public `v0` paths to be green in tests before release.
- Do not block `v0.1.0` on indexed LanceDB runtime integration, automatic routing, or
  later planning work.

## Phase 8

Introduce a thin internal plan layer only when needed.

- Expect HumemDB to eventually need an internal plan or IR layer.
- Do not start with a full IR just because it sounds clean.
- Add a small internal plan layer when one user request needs multiple coordinated
  operations across SQL, graph, and vector execution.
- Design earlier phases with clean seams so that later IR work is an insertion, not a
  rewrite from scratch.

## Phase 9

Add automatic routing and lightweight planning.

- Point reads and transactional queries go to SQLite.
- Scans, aggregates, and analytics go to DuckDB.
- Keep routing explainable and overridable.

## Phase 10

Add SQL classification and validation.

- Classify read versus write queries safely.
- Detect simple OLTP versus OLAP query shapes.
- Validate the supported portable SQL subset.
- Keep the first implementation in Python.

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
- Allow staging-table and normalize-into-final-table flows where they make ingest
  simpler or safer.
- Keep DuckDB as the analytical read path after ingest, not as the canonical ingest
  destination.
- If Parquet is added later, keep it as an optional analytical snapshot/export layer,
  not as a replacement for fresh DuckDB reads over the SQLite source of truth.
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

Evaluate `v1` promotion.

- Review whether `HumemSQL v0`, `HumemCypher v0`, and `HumemVector v0` are stable enough
  to promote to `v1`.
- Use surface maturity, benchmark evidence, and routing stability as the bar for
  promotion, not raw feature count alone.
- Promote a frontend to `v1` only when HumemDB is ready to preserve its semantics as a
  real compatibility commitment.

## Phase 15

Add natural language support later.

- Start with a small model or parser that maps natural language into structured HumemDB
  requests.
- Compile that structured request into SQL, Cypher, or vector operations.
- Do not make raw natural-language-to-backend-SQL the core interface.

## Phase 16

Stabilization and `v1` hardening.

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
