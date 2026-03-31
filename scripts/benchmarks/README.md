# Benchmark Scripts

This directory contains benchmarking utilities for comparing query paths, storage
strategies, and execution backends.

The relational benchmark exercises `HumemSQL v0` query shapes. The graph benchmark
exercises `HumemCypher v0` query shapes. The vector benchmark exercises the current
`HumemVector v0` execution candidates.

Routing automation utilities:

- [`routing_sweep.py`](./routing_sweep.py) runs the SQL, Cypher, and real-data vector
  benchmark sweeps across scale ladders and writes merged JSON summaries.
- [`routing_threshold_report.py`](./routing_threshold_report.py) reads one merged sweep
  summary and prints a crossover report showing where DuckDB first wins, if it does.

## [`translation_overhead.py`](./translation_overhead.py)

Purpose:

- Measure frontend translation overhead separately from backend execution.
- Isolate cached versus uncached PostgreSQL-like SQL translation through `sqlglot`.
- Isolate Cypher parse and bind+compile cost through the current `HumemCypher v0`
  compiler.

Example command:

```bash
python scripts/benchmarks/translation_overhead.py --warmup 100 --repetitions 1000
```

What it reports:

- cached SQL translation cost for SQLite and DuckDB targets
- uncached SQL translation cost for SQLite and DuckDB targets
- Cypher parse cost
- Cypher runtime-planning cost through the generated-first `db.query(...)` planning path
- Cypher bind+compile cost

Large-run command used:

```bash
python scripts/benchmarks/translation_overhead.py --warmup 200 --repetitions 3000
```

Observed means on the current development machine:

SQL translation:

| Workload | Complexity | SQLite cached | SQLite uncached | DuckDB cached | DuckDB uncached | Takeaway |
| -------- | ---------- | ------------: | --------------: | ------------: | --------------: | -------- |
| `literal_projection` | simple | 0.0002 ms | 0.0876 ms | 0.0002 ms | 0.0769 ms | Tiny literal and cast rewrites stay well under 0.1 ms uncached. |
| `point_lookup` | simple | 0.0002 ms | 0.1385 ms | 0.0002 ms | 0.1284 ms | A selective lookup with `ILIKE`, cast, `ORDER BY`, and `LIMIT` still stays around 0.13 ms uncached. |
| `filtered_aggregate` | simple | 0.0002 ms | 0.1549 ms | 0.0002 ms | 0.1582 ms | Small aggregation adds a bit of cost but still stays around 0.15 ms uncached. |
| `join_aggregate` | medium | 0.0002 ms | 0.1945 ms | 0.0002 ms | 0.1954 ms | A straightforward join-and-group shape stays around 0.2 ms uncached. |
| `windowed_filter` | medium | 0.0002 ms | 0.1510 ms | 0.0002 ms | 0.1536 ms | A date-range filter plus ordering behaves similarly to other medium OLTP-shaped queries. |
| `case_and_exists` | medium | 0.0002 ms | 0.2264 ms | 0.0002 ms | 0.2234 ms | Correlated `EXISTS` plus `CASE` raises frontend cost, but still stays near 0.22 ms on average. |
| `union_rollup` | medium | 0.0002 ms | 0.1872 ms | 0.0002 ms | 0.1865 ms | A subquery-wrapped `UNION ALL` remains below 0.2 ms uncached. |
| `cte_multi_join` | complex | 0.0002 ms | 0.5092 ms | 0.0002 ms | 0.5256 ms | The heaviest current SQL shape, with multiple CTEs and joins, still stays around half a millisecond uncached. |
| `windowed_rank_cte` | complex | 0.0002 ms | 0.2436 ms | 0.0002 ms | 0.2416 ms | Window functions plus a CTE land in the mid-0.2 ms range uncached. |

Cypher translation:

| Workload | Complexity | Parse mean | Bind+compile mean | Takeaway |
| -------- | ---------- | ---------: | ----------------: | -------- |
| `node_anchor` | simple | 0.0112 ms | 0.0063 ms | Anchored node matches compile very cheaply. |
| `node_lookup` | simple | 0.0170 ms | 0.0073 ms | Adding `WHERE`, `ORDER BY`, and `LIMIT` only increases frontend cost slightly. |
| `relationship_expand` | medium | 0.0276 ms | 0.0114 ms | Basic edge expansion remains in the low-hundredths-of-a-millisecond range. |
| `relationship_reverse` | medium | 0.0193 ms | 0.0095 ms | Reverse edge matching is also very cheap in the current compiler. |
| `relationship_property_anchor` | complex | 0.0250 ms | 0.0126 ms | Extra property anchors on both sides increase compile work modestly. |
| `relationship_dense_return` | complex | 0.0349 ms | 0.0167 ms | The densest current return shape is still only a few hundredths of a millisecond. |

Result interpretation:

- Cached SQL translation is effectively free at this scale.
- Uncached SQL translation grows with query shape complexity, but the current `HumemSQL v0`
  workload mix still stays sub-millisecond.
- Current `HumemCypher v0` parse and compile costs remain much smaller than uncached SQL
  translation cost.
- These are machine-specific measurements from one longer run, so the exact values will
  move somewhat across hardware and runtime conditions even if the general pattern holds.

Important note:

- `HumemCypher v0` compilation is route-agnostic today. The compiled SQL shape is the
  same before it is sent to SQLite or DuckDB.

Set `HUMEMDB_THREADS` to cap the backend worker-thread budget used by HumemDB. Today
this affects DuckDB plus the vector runtime's NumPy/BLAS and Arrow-backed LanceDB paths.
SQLite still behaves like a selective, single-query OLTP engine.

## [`duckdb_direct_read.py`](./duckdb_direct_read.py)

Purpose:

- Compare SQLite and DuckDB across a broader relational workload mix over the SQLite
  source of truth.
- Cover OLTP-style event reads, analytical aggregates, document-tag joins, and
  memory-style rollups.
- Label each SQL workload by structural shape and selectivity so later routing
  thresholds can be benchmark-calibrated against parsed planner metadata instead of
  query names alone.

Current workload groups now include:

- point lookup, filtered range, and ordered top-k OLTP reads
- selective joins that should not be misclassified as OLAP
- broader join-and-group workloads
- CTE-backed analytical rollups
- windowed, `EXISTS`, and `DISTINCT` SQL shapes
- document and memory workloads with both selective and broad join shapes
- wider table schemas so query-shape tests are not all running on minimal column sets

Machine-readable output:

- pass `--output-json path/to/results.json` to persist one run as structured JSON for
  later threshold extraction and scale summaries
- use [`routing_sweep.py`](./routing_sweep.py) to automate multi-scale SQL and Cypher
  runs into one merged summary file

Thread-control example:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py --rows 1000000
```

Large-run command used:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py \
  --rows 1000000 \
  --repetitions 3 \
  --warmup 1 \
  --batch-size 20000 \
  --users 50000 \
  --tags 1024
```

Dataset:

- 50,000 users
- 1,000,000 event rows
- 50,000 documents
- 1,024 tags
- 150,000 document-tag rows
- 100,000 memory chunks
- 1 warmup iteration and 3 timed repetitions per query shape
- Initial load time: 3168.79 ms

Observed means:

| Query shape | SQLite mean | DuckDB mean | Takeaway |
| --- | ---: | ---: | --- |
| `event_point_lookup` | 0.02 ms | 108.32 ms | SQLite remains overwhelmingly better for indexed point reads. |
| `event_filtered_range` | 4.83 ms | 46.49 ms | SQLite still clearly wins on selective filtered reads. |
| `event_type_hot_window` | 113.46 ms | 55.70 ms | A filtered ordered top-k over a large event table now favors DuckDB by about `2.0x`. |
| `event_aggregate_topk` | 462.20 ms | 64.18 ms | DuckDB is about `7.2x` faster on broad scan-and-group aggregation. |
| `event_region_join` | 412.46 ms | 63.01 ms | DuckDB is about `6.5x` faster on low-selectivity join aggregation. |
| `event_active_user_join_lookup` | 5.08 ms | 62.54 ms | A selective join lookup is still firmly SQLite territory. |
| `event_active_user_rollup` | 707.02 ms | 66.20 ms | DuckDB is about `10.7x` faster once the join expands into a grouped rollup. |
| `event_cte_daily_rollup` | 2315.87 ms | 70.31 ms | A CTE-backed daily rollup is the strongest DuckDB win in this run, at about `32.9x`. |
| `document_tag_rollup` | 0.14 ms | 26.83 ms | A highly selective multi-join document read still strongly favors SQLite. |
| `document_owner_region_rollup` | 40.03 ms | 18.60 ms | A broader document-owner aggregation now favors DuckDB by about `2.2x`. |
| `memory_hot_rollup` | 22.15 ms | 18.08 ms | The filtered memory rollup now tilts modestly toward DuckDB at this scale. |
| `memory_owner_join_lookup` | 39.63 ms | 27.38 ms | Even a join lookup can flip once the result path is broad enough and the dataset is large enough. |

SQL findings:

- SQLite remains the clear default for point lookups, selective filters, and selective
  indexed joins, even at one million event rows.
- DuckDB now wins a broader set of SQL reads than before, including filtered ordered
  top-k on the event table, broad join-and-group workloads, CTE-backed rollups, and a
  broader document-owner aggregation.
- The new run still reinforces the core routing lesson: join presence alone is not
  enough. Selectivity, grouping breadth, and how much of the table has to be touched
  matter more than surface syntax.
- Some mixed workloads now sit near the crossover region instead of being clear wins for
  one engine, which is exactly why benchmark-calibrated thresholds are needed.

## [`cypher_graph_path.py`](./cypher_graph_path.py)

Purpose:

- Measure graph initial load time, Cypher parse and compile overhead, and raw SQL versus
  end-to-end Cypher execution on SQLite and DuckDB.
- Cover multiple node labels and edge types instead of a single graph shape.
- Recheck graph-path behavior after changes to graph indexes or Cypher SQL
  compilation, since those dominate performance more than Cypher parsing itself.
- Label each Cypher workload by structural shape and selectivity so graph routing can
  later distinguish anchored lookup, selective traversal, and broad fanout more
  defensibly.

Current workload groups now include:

- anchored node lookups
- selective and reverse relationship-property anchored traversals
- broader social fanout traversals
- reverse-direction relationship fanout traversals with ordering
- untyped relationship reads over admitted graph patterns
- narrow relationship-type alternation reads such as `:KNOWS|FOLLOWS`
- anonymous-endpoint relationship reads over admitted graph patterns
- ordered limited traversals
- topic-side fanout reads that are broader than the earlier selective TAGGED case
- additional `Team` nodes and `MEMBER_OF` edges so graph routing is not benchmarked on
  only one node/edge vocabulary

Machine-readable output:

- pass `--output-json path/to/results.json` to persist one run as structured JSON for
  later graph-routing analysis and scale summaries
- pass `--index-set baseline|node-prop-covering|edge-prop-covering|targeted-covering`
  to rerun the same graph workload set against one named SQLite graph-index experiment
- use [`routing_sweep.py`](./routing_sweep.py) to automate multi-scale SQL and Cypher
  runs into one merged summary file

Structured Cypher payloads now also include:

- per-workload `cypher_features` describing lightweight query shape such as property
  join count, direct type filters, and whether the admitted Cypher includes `ORDER BY`,
  `LIMIT`, `OFFSET`, or `DISTINCT`
- per-workload `sqlite_plan_summary` derived from `EXPLAIN QUERY PLAN`, including
  index mentions and whether SQLite used a temp B-tree

## [`csv_ingest.py`](./csv_ingest.py)

Purpose:

- Measure the first public ingestion family built on top of the canonical SQLite
  write path.
- Compare the new CSV-backed import APIs against the current manual alternatives the
  team would realistically use today.
- Keep a small post-ingest freshness query for each path so the benchmark validates
  that every comparison method produced the expected relational or graph rows.

Example command:

```bash
python scripts/benchmarks/csv_ingest.py \
  --table-rows 50000 \
  --node-rows 20000 \
  --edge-fanout 2 \
  --chunk-size 1000 \
  --warmup 1 \
  --repetitions 5
```

Large-scale sweep pattern:

- run all comparison methods at smaller scales where repeated public Cypher writes are
  still realistic
- drop `public_cypher_query` at larger graph scales and compare `import_api` against
  `internal_sqlite` instead
- use `--table-methods` and `--graph-methods` to select those subsets explicitly

What it reports:

- relational ingest timings for:
  - `import_table(...)`
  - `staging_normalize`, which loads CSV rows into one staging table first and then
    normalizes them into the final relational table with one set-based SQLite insert
  - manual CSV parsing plus public `db.executemany(...)`
  - manual CSV parsing plus internal SQLite `executemany(...)` as a lower bound
- graph node ingest timings for:
  - `import_nodes(...)`
  - repeated public Cypher `db.query(...)` writes inside one transaction
  - internal SQLite graph-table batch writes as a lower bound
- graph edge ingest timings for:
  - `import_edges(...)`
  - repeated public Cypher `db.query(...)` writes inside one transaction
  - internal SQLite graph-table batch writes as a lower bound
- post-ingest count-query timing summaries for each comparison path

Use this benchmark when ingest behavior changes materially, especially when:

- chunk-size defaults are adjusted
- graph import validation or property coercion rules change
- staging-table or normalize-into-final-table flows are added later
- the team needs evidence on whether the new import APIs are actually better than
  staying with `executemany(...)`, repeated `db.query(...)`, or internal benchmark-only
  write paths

Method-selection flags:

- `--table-methods import_table,staging_normalize,public_executemany,internal_sqlite`
- `--graph-methods import_api,public_cypher_query,internal_sqlite`
- for larger graph sweeps, a practical pattern is
  `--graph-methods import_api,internal_sqlite`

Current note on staged relational ingest:

- `staging_normalize` exists to measure one realistic follow-on path where
  CSV rows first land in a permissive staging table and then move into the final table
  through one set-based SQLite normalization step
- this staged path is intentionally table-first today; graph ingest is benchmarked
  directly through `import_nodes(...)` and `import_edges(...)`, and graph-specific
  staging flows should only be added later if a real graph-derivation workload
  justifies them
- a dedicated staged-relational comparison run has now been captured alongside the
  original direct-import snapshot

Current sweep snapshot:

- sweep date: `2026-03-26`
- `warmup=0`, `repetitions=1`
- relational staged-comparison runs used `node_rows=100` and `edge_fanout=1` so the
  script stayed focused on table ingest while still exercising the shared benchmark
  harness
- the relational table snapshot below now includes `10k`, `100k`, `1M`, and `10M`
  staged-comparison measurements
- the graph node and edge snapshots below remain the earlier post-optimization graph
  comparison runs, which are still the representative evidence for graph ingest

Relational table ingest means:

| Rows | `import_table(...)` | `staging_normalize` | `public_executemany` | `internal_sqlite` | Takeaway |
| ---: | ------------------: | ------------------: | -------------------: | ----------------: | -------- |
| `10k` | `55.94 ms` | `124.55 ms` | `52.12 ms` | `44.28 ms` | At this small scale, `staging_normalize` is clearly a convenience path rather than a fast path: it was about `2.23x` slower than `import_table(...)` and about `2.39x` slower than public `executemany(...)`. |
| `100k` | `256.68 ms` | `361.62 ms` | `283.00 ms` | `222.01 ms` | By `100k`, direct `import_table(...)` is still the right default; staged normalize remained about `1.41x` slower than `import_table(...)` and about `1.28x` slower than public `executemany(...)`. |
| `1M` | `1996.66 ms` | `2514.54 ms` | `2518.13 ms` | `1843.78 ms` | At `1M`, staged normalize roughly matched the realistic public baseline but still trailed `import_table(...)` by about `1.26x`, which means the staged flow looks operationally reasonable when normalization is needed but not like the new default fast path. |
| `10M` | `19737.82 ms` | `24939.36 ms` | `24203.80 ms` | `18456.09 ms` | The same pattern held at `10M`: `import_table(...)` remained the fastest public path, while `staging_normalize` stayed workable for schema-cleanup workflows but ended up slightly slower than public `executemany(...)` and about `1.35x` off the internal lower bound. |

Graph node ingest means:

| Node rows | `import_api` | `public_cypher_query` | `internal_sqlite` | Takeaway |
| --------: | -----------: | --------------------: | ----------------: | -------- |
| `10k` | `467.35 ms` | `403.24 ms` | `440.08 ms` | At this smaller scale, repeated public Cypher writes were still competitive enough to benchmark directly, while `import_nodes(...)` stayed in the same rough range. |
| `100k` | `1642.19 ms` | `3508.84 ms` | `1740.47 ms` | `import_nodes(...)` stayed effectively at the internal lower bound, while public Cypher writes were about `2.14x` slower. |
| `1M` | `15092.71 ms` | `34613.55 ms` | `14370.08 ms` | `import_nodes(...)` remained close to the internal lower bound at larger scale, about `1.05x`, while public Cypher writes were about `2.41x` slower. |

Graph edge ingest means:

| Edge rows | `import_api` | `public_cypher_query` | `internal_sqlite` | Takeaway |
| --------: | -----------: | --------------------: | ----------------: | -------- |
| `20k` | `264.64 ms` | `1543.90 ms` | `244.70 ms` | Repeated public Cypher edge creation was already about `6.31x` slower than the internal lower bound, while `import_edges(...)` stayed close to it. |
| `200k` | `2401.76 ms` | `15482.01 ms` | `2233.34 ms` | `import_edges(...)` stayed within about `1.08x` of the internal lower bound, while public Cypher edge writes were about `6.93x` slower. |
| `2M` | `25242.79 ms` | `149294.07 ms` | `23834.69 ms` | The larger edge run still kept `import_edges(...)` close to internal SQLite, about `1.06x`, while public Cypher edge writes were about `6.26x` slower. |

Current ingest takeaway:

- relational CSV ingest is now in the right place: `import_table(...)` remains the
  fastest public path and stays close to the internal SQLite lower bound from `100k`
  through `10M`
- `staging_normalize` is now benchmarked too, and the measured result is useful but
  clear: it is a workflow option for permissive load plus SQL normalization, not the
  new default performance path
- graph CSV ingest continues to look directionally right: `import_nodes(...)` and
  `import_edges(...)` stay close to the internal lower bound at `100k` and `1M`
- repeated public Cypher graph writes remain a valid baseline, but they scale much
  worse, especially for edges where they are still more than `6x` slower than the
  internal lower bound at `100k` and `1M`
- the completed direct-import plus staged-relational runs are strong enough to treat
  the first public ingestion family as benchmark-validated for the current release bar

## [`routing_sweep.py`](./routing_sweep.py)

Purpose:

- run scale ladders for the SQL and Cypher routing benchmarks plus the real-data
  vector benchmark sweep
- persist per-scale JSON outputs plus one merged summary document
- keep scale-sweep workflow reproducible instead of ad hoc terminal history
- allow Cypher sweeps to compare named graph-index experiments without hand-editing
  the benchmark script
- pass through the fixed real-data vector baseline inputs such as dataset, filter
  sources, sample mode, row scales, and `top_k` grid

Example command:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/routing_sweep.py \
  --benchmark all \
  --sql-scales 10000,100000,1000000 \
  --cypher-scales 100000,1000000 \
  --vector-dataset msmarco-10m \
  --vector-scales 100000,1000000 \
  --vector-top-k-grid 10,50 \
  --cypher-index-set baseline \
  --warmup 1 \
  --repetitions 3
```

Default output directory:

- `scripts/benchmarks/results/routing_sweep/`

Main outputs:

- `sql_summary.json`
- `cypher_summary.json`
- `vector_summary.json`
- `routing_sweep_summary.json`
- when the Cypher benchmark is enabled, the merged summary records the selected graph
  `index_set`
- when the vector benchmark is enabled, the merged summary records the selected real
  dataset sweep and its scenario summaries

## [`routing_threshold_report.py`](./routing_threshold_report.py)

Purpose:

- summarize the merged routing sweep JSON into a workload-by-workload crossover report
- show the first scale where DuckDB wins for each SQL or Cypher workload, if any
- summarize the real-data vector baseline by dataset, filter, and `top_k`, using the
  scenario-level recall policy that was already baked into the vector sweep
- emit `recommended_runtime.cypher_graph_index_diagnostics` for graph-specific follow-up
  work such as temp-B-tree pressure, property-join-heavy workloads, direct type-filter
  workloads, and ordered-versus-unordered sort overhead pairs

Example command:

```bash
python scripts/benchmarks/routing_threshold_report.py \
  --input scripts/benchmarks/results/routing_sweep/routing_sweep_summary.json \
  --output-json scripts/benchmarks/results/routing_sweep/routing_thresholds.json
```

Targeted graph-index comparison example:

```bash
python scripts/benchmarks/routing_sweep.py \
  --benchmark cypher \
  --cypher-scales 100000,1000000 \
  --cypher-index-set targeted-covering \
  --warmup 0 \
  --repetitions 1 \
  --output-dir /tmp/humemdb-graph-index-sweep-targeted

python scripts/benchmarks/routing_threshold_report.py \
  --input /tmp/humemdb-graph-index-sweep-targeted/routing_sweep_summary.json \
  --output-json /tmp/humemdb-graph-index-sweep-targeted/report.json
```

Current note:

- treat the threshold report as a derived summary of whatever merged sweep JSON you
  point it at, not as a permanent benchmark snapshot document
- the SQL and Cypher sections still summarize first-crossing behavior
- the vector section now summarizes the retained real-data baseline only, grouped by
  dataset, filter family, and `top_k`, and uses the per-scenario recall admission
  result from `vector_search_real_sweep.py` instead of the old synthetic crossover
  interpretation

Thread-control example:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py --nodes 100000
```

Large-run command used:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py \
  --nodes 100000 \
  --fanout 4 \
  --tag-fanout 2 \
  --repetitions 3 \
  --warmup 1 \
  --batch-size 5000
```

Dataset:

- 100,000 total nodes
  - 50,000 `User` nodes
  - 35,000 `Document` nodes
  - 15,000 `Topic` nodes
- 305,000 total edges
  - 200,000 `KNOWS` edges
  - 35,000 `AUTHORED` edges
  - 70,000 `TAGGED` edges
- Approximately 1,095,000 total rows across graph tables and graph property tables
- 1 warmup iteration and 3 timed repetitions per stage
- Initial load time: 3317.85 ms

Observed means:

| Workload | SQLite raw SQL | DuckDB raw SQL | SQLite Cypher | DuckDB Cypher | Takeaway |
| --- | ---: | ---: | ---: | ---: | --- |
| `user_lookup` | 0.02 ms | 158.48 ms | 0.07 ms | 161.53 ms | Anchored user lookup remains overwhelmingly SQLite-favored. |
| `document_lookup` | 0.02 ms | 159.24 ms | 0.08 ms | 159.31 ms | Anchored document lookup also strongly favors SQLite. |
| `topic_lookup` | 0.02 ms | 137.80 ms | 0.06 ms | 129.24 ms | Anchored topic lookup remains strongly SQLite-favored. |
| `social_expand` | 155.14 ms | 180.88 ms | 165.25 ms | 188.36 ms | On this 100k-node graph, even the broader `KNOWS` traversal still favors SQLite. |
| `social_expand_unfiltered` | 13.60 ms | 117.46 ms | 14.06 ms | 120.11 ms | Full fanout with only a `LIMIT` is still far from a DuckDB win here. |
| `social_reverse_since_anchor` | 125.44 ms | 191.43 ms | 132.82 ms | 203.11 ms | Reverse-edge traversal with a relationship-property anchor remains SQLite-favored. |
| `author_expand` | 48.09 ms | 170.93 ms | 50.37 ms | 176.73 ms | Selective authored traversal still clearly favors SQLite. |
| `author_expand_ordered` | 82.25 ms | 193.70 ms | 83.48 ms | 192.80 ms | Adding ordering and `LIMIT` does not flip this authored traversal to DuckDB. |
| `tagged_expand` | 10.14 ms | 169.26 ms | 10.20 ms | 168.10 ms | Selective document-to-topic traversal remains very strongly SQLite-favored. |
| `tagged_topic_fanout` | 18.90 ms | 137.21 ms | 19.98 ms | 138.99 ms | A broader topic-side fanout still does not justify DuckDB on this graph size. |

Compiler overhead:

- Cypher parse cost stayed around 0.02 to 0.03 ms.
- Cypher bind+compile cost stayed around 0.03 to 0.04 ms.
- End-to-end Cypher timings tracked raw SQL closely, which confirms that execution plan
  shape and backend behavior dominate total latency.

Graph findings:

- The multi-label graph benchmark makes the routing boundary clearer than the earlier
  single-label version did.
- The 100k-node table above still shows SQLite winning the current matrix, but the full
  routing sweep now shows limited raw-backend Cypher crossovers rather than none.
- The earliest current Cypher crossover is `social_mixed_boolean`, which first flips to
  DuckDB at `100k` total nodes.
- Broader `KNOWS` traversal first flips to DuckDB at `1M` total nodes.
- Reverse-edge traversal with a relationship-property anchor also flips, but only at
  `1M` total nodes.
- Most other current Cypher workloads still stay on SQLite even at `1M`, including
  anchored lookups, selective traversals, ordered traversals, topic fanout, and the
  added `Team`/`MEMBER_OF` graph family.
- The current evidence therefore still says Cypher routing should be more conservative
  than SQL routing: broad graph fanout may cross, but the portable `HumemCypher v0`
  surface has a much narrower DuckDB-friendly region today.

## [`vector_search_real.py`](./vector_search_real.py)

Purpose:

- Benchmark shipped vector datasets.
- Hold the operational threshold steady: NumPy exact below the ANN snapshot cutoff and
  LanceDB `IVF_PQ` snapshot builds above it.
- Measure dataset load cost, LanceDB table/index build cost, indexed query latency,
  and recall versus NumPy exact where the exact baseline is still enabled.
- Reuse one SQLite, NumPy, and LanceDB build across multiple `top_k` values when a
  `top_k` grid is requested.

Representative commands:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search_real.py \
  --dataset msmarco-10m \
  --rows 100000 \
  --top-k-grid 10,50 \
  --queries 100 \
  --warmup 1 \
  --repetitions 3 \
  --metric cosine \
  --sample-mode auto \
  --lancedb-index-type IVF_PQ \
  --output json
```

Use the same shape for `stackoverflow-xlarge`, typically with `--sample-mode stratified`
and whichever `--rows` / `--top-k` combination you want to test.

Current implementation note:

- Full NumPy exact now stops above `100k` rows by default so the benchmark mirrors the
  default ANN snapshot threshold.
- The LanceDB side is intentionally narrowed to `IVF_PQ`; this script is no longer a
  multi-family comparison harness.
- Exact-enabled runs still stage sampled real vectors into SQLite first, then stream
  them through `DuckDB -> Arrow batches -> LanceDB` before index build.
- Snapshot-only runs above the `100k` cutoff now bypass SQLite and DuckDB, ingesting
  selected shard memmaps straight into `Arrow batches -> LanceDB` before index build.
- That direct snapshot ingest path materially reduced peak RSS in the `1M`
  `msmarco-10m` profile, from about `9.5 GiB` to about `3.7 GiB`, so the dominant
  memory spike is no longer the export path itself.
- When `--top-k-grid` is used, the benchmark builds once per dataset and scale, then
  reuses that build for all requested `top_k` values.

## [`vector_search_real_sweep.py`](./vector_search_real_sweep.py)

Purpose:

- Sweep real dataset scales over `top_k` and sampling choices.
- Persist rolling summaries and per-scenario JSON files while long runs are still in
  progress.
- Produce a real-data baseline for the fixed `100k` ANN snapshot threshold.
- Score each scenario against the current IVF_PQ recall admission bar instead of using
  one flat cutoff for every scale.
- Reuse one real-data build per dataset, scale, and filter before expanding results
  back out across the requested `top_k` grid.

Representative commands:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search_real_sweep.py \
  --dataset msmarco-10m \
  --rows-grid 100000,1000000 \
  --top-k-grid 10,50 \
  --queries 100 \
  --warmup 1 \
  --repetitions 3 \
  --sample-mode auto \
  --filter-sources auto \
  --lancedb-index-type IVF_PQ \
  --output-json scripts/benchmarks/results/routing_sweep_msmarco_10m/vector_summary.json \
  --intermediate-dir scripts/benchmarks/results/routing_sweep_msmarco_10m/vector_intermediate \
  --output json
```

For `stackoverflow-xlarge`, use the same command shape with `--dataset stackoverflow-xlarge`
plus the appropriate `--sample-mode`, `--filter-sources`, and output paths.

Current real-data indexed baseline:

- This sweep is no longer trying to discover the routing threshold. The ANN snapshot
  policy is fixed at `100k`: below that cut the benchmark keeps the NumPy exact
  baseline, and above it the benchmark measures LanceDB `IVF_PQ` snapshot builds and
  search.
- The current questions are narrower:
  - what do exact-search latency and memory look like at the `100k` cut;
  - what do snapshot `IVF_PQ` build/query costs look like above that cut; and
  - how much operational pressure does LanceDB ingest add as snapshot history grows.
- The current snapshot-only benchmark path uses direct shard-memmap ingest into LanceDB;
  that change removed the earlier SQLite -> DuckDB -> Arrow blow-up seen in the `1M`
  `msmarco-10m` profile.
- The sweep now reports pass/fail against these recall targets:

| top_k | 100K | 1M | 10M | 25M | 100M |
| ----- | ---- | -- | --- | --- | ---- |
| 10 | 0.95 | 0.93 | 0.90 | 0.89 | 0.88 |
| 50 | 0.98 | 0.96 | 0.95 | 0.94 | 0.93 |

### Tuned References

This table keeps the current measured `100K` and `1M` references together and leaves
explicit placeholders for the next larger `10M` and `25M` runs. At `100K`, the exact
NumPy baseline remains enabled at the threshold; at `1M`, recall now comes from
packaged dataset ground-truth filtered back down to the sampled subset, so effective
query counts can land below the requested `100` when the subset contains fewer
GT-covered queries.

| Scale | Dataset | IVF_PQ settings | LanceDB k=10 recall / ms | LanceDB k=50 recall / ms | NumPy k=10 ms | NumPy k=50 ms | SQLite seed | SQLite->NumPy load | NumPy build | Peak RSS | Lance export | Lance index build | Status | Notes |
| ----- | ------- | --------------- | ------------------------: | ------------------------: | ------------: | ------------: | ----------: | -----------------: | ----------: | -------: | -----------: | ----------------: | ------ | ----- |
| `100K` | `msmarco-10m` | `partitions=128`, `sub_vectors=128`, `nprobes=32`, `refine_factor=4` | `0.9970` / `2.27` | `0.9844` / `3.07` | `10.23` | `10.11` | `1206.73 ms` | `1164.94 ms` | `1647.96 ms` | `2.94 GiB` | `932.52 ms` | `62.79 s` | measured | `batch_count=1`; JSON/time RSS matched closely; files: `real_ivf_pq_refresh_100k/msmarco-10m_rows100000_topk10_50_memory_refresh.*` |
| `100K` | `stackoverflow-xlarge` | `partitions=64`, `sub_vectors=128`, `nprobes=32`, `refine_factor=4` | `0.9940` / `2.02` | `0.9864` / `2.91` | `3.18` | `3.24` | `1023.53 ms` | `710.22 ms` | `418.97 ms` | `1.18 GiB` | `491.01 ms` | `52.28 s` | measured | `batch_count=1`; JSON/time RSS matched closely; files: `real_ivf_pq_refresh_100k/stackoverflow-xlarge_rows100000_topk10_50_memory_refresh.*` |
| `1M` | `msmarco-10m` | `partitions=128`, `sub_vectors=128`, `nprobes=32`, `refine_factor=4` | `0.9978` / `3.31` | `0.9974` / `4.36` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `3.27 GiB` | `3076.65 ms` | `74.24 s` | measured | `ground_truth=packaged_gt_subset_filtered`; `queries=94/100 requested`; `batch_count=10`; files: `real_ivf_pq_refresh_1m/msmarco-10m_rows1000000_topk10_50_refresh.*` |
| `1M` | `stackoverflow-xlarge` | `partitions=64`, `sub_vectors=128`, `nprobes=32`, `refine_factor=4` | `1.0000` / `5.47` | `1.0000` / `10.19` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `1.20 GiB` | `1869.69 ms` | `63.03 s` | measured | `ground_truth=packaged_gt_subset_filtered`; `queries=38/100 requested`; `batch_count=252`; files: `real_ivf_pq_refresh_1m/stackoverflow-xlarge_rows1000000_topk10_50_refresh.*` |
| `10M` | `msmarco-10m` | `partitions=128`, `sub_vectors=128`, `nprobes=32`, `refine_factor=4` | `0.9800` / `13.12` | `0.9904` / `14.98` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `4.95 GiB` | `53.60 s` | `217.89 s` | measured | `ground_truth=packaged_gt_subset_filtered`; `queries=100/100 requested`; `batch_count=100`; files: `real_ivf_pq_refresh_10m/msmarco-10m_rows10000000_topk10_50_refresh.*` |
| `10M` | `stackoverflow-xlarge` | `partitions=64`, `sub_vectors=128`, `nprobes=32`, `refine_factor=4` | `1.0000` / `25.42` | `0.9985` / `30.25` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `4.16 GiB` | `22.35 s` | `181.33 s` | measured | `ground_truth=packaged_gt_subset_filtered`; `queries=100/100 requested`; `batch_count=201`; files: `real_ivf_pq_refresh_10m/stackoverflow-xlarge_rows10000000_topk10_50_refresh.*` |
| `25M` | `stackoverflow-xlarge` | `partitions=64`, `sub_vectors=128`, `nprobes=32`, `refine_factor=4` | `0.9940` / `58.36` | `0.9959` / `63.27` | `n/a` | `n/a` | `n/a` | `n/a` | `n/a` | `6.39 GiB` | `40.73 s` | `386.58 s` | measured | `ground_truth=packaged_gt_subset_filtered`; `queries=100/100 requested`; `batch_count=252`; files: `real_ivf_pq_refresh_25m/stackoverflow-xlarge_rows25000000_topk10_50_refresh.*` |

## [`vector_surface_runtime_attribution.py`](./vector_surface_runtime_attribution.py)

Purpose:

- Measure the live HumemDB ANN snapshot plus exact-delta runtime rather than the
  real-data build pipeline.
- Break out snapshot build, snapshot load, candidate resolution, ANN search, exact
  rerank, and surface-dispatch time through the real runtime seams.
- Compare direct, SQL, and Cypher surfaces over the same snapshot-runtime shape.
- Capture both process RSS deltas and Python-only heap peaks so orchestration overhead
  can be separated from backend-heavy work more clearly.

Representative command:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_surface_runtime_attribution.py \
  --dataset msmarco-10m \
  --rows 100000 \
  --queries 100 \
  --top-k 10 \
  --ann-min-vectors 100000 \
  --warmup 1 \
  --repetitions 3
```

What it measures:

- direct ANN snapshot build
- SQL ANN snapshot build
- Cypher ANN snapshot build
- direct search over a ready snapshot plus exact-delta rerank runtime
- SQL search over a ready snapshot plus exact-delta rerank runtime
- Cypher search over a ready snapshot plus exact-delta rerank runtime

Current stage names:

- `build_snapshot_ms`: synchronous snapshot build path
- `snapshot_materialization_ms`: full-corpus matrix materialization used for snapshot
  builds
- `snapshot_index_load_ms`: live or persisted snapshot acquisition before a search
- `snapshot_persisted_load_ms`: persisted snapshot reopen path
- `vector_dispatch_ms`: top-level vector query dispatch inside the runtime
- `candidate_resolution_ms`: SQL or Cypher candidate-query resolution before vector
  ranking
- `snapshot_rerank_path_ms`: end-to-end snapshot search plus exact rerank path
- `snapshot_ann_search_ms`: LanceDB ANN search call over the snapshot
- `numpy_exact_search_ms`: exact NumPy rerank or fallback search call

Important note:

- This benchmark is intentionally synthetic and runtime-focused. Keep using
  [`vector_search_real.py`](./vector_search_real.py) and
  [`vector_search_real_sweep.py`](./vector_search_real_sweep.py) for real-dataset
  recall, build-cost, and routing evidence.

Current attribution takeaway:

- The fresh tuned `1M` MSMARCO rerun still says the expensive part of offline
  real-data indexing is the LanceDB build itself, while the expensive part of
  online vector search is mostly the surrounding Python-side orchestration,
  candidate-resolution, and snapshot/rerank coordination work rather than the raw
  native ANN or exact-search calls by themselves.
- Treat the percentages this benchmark produces as stage-attribution proxies, not as
  profiler-precise CPU accounting: `orchestration_ms` means time outside the wrapped
  native-heavy search calls, while the backend side is mostly `snapshot_ann_search_ms`
  plus `numpy_exact_search_ms`.

Practical interpretation:

- To reduce offline snapshot build and refresh cost, optimize or amortize LanceDB index build;
  the fresh tuned `1M` rerun still spends about `95%` of build time there.
- To reduce online direct-vector latency, optimize the ANN snapshot threshold and surrounding
  runtime bookkeeping first.
- To reduce online SQL/Cypher vector latency, optimize candidate-set generation and
  candidate-resolution overhead before focusing on rerank, because merge/rerank is
  already effectively free in the current measurements.
