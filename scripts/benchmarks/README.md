# Benchmark Scripts

This directory contains benchmarking utilities for comparing query paths, storage
strategies, and execution backends.

The relational benchmark exercises `HumemSQL v0` query shapes. The graph benchmark
exercises `HumemCypher v0` query shapes. The vector benchmark exercises the current
`HumemVector v0` execution candidates.

Routing automation utilities:

- [`routing_sweep.py`](./routing_sweep.py) runs the SQL and Cypher routing benchmarks
  across scale ladders and writes merged JSON summaries.
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
- pass `--index-set baseline|phase11-node-prop-covering|phase11-edge-prop-covering|phase11-targeted`
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

- Measure the first Phase 12 ingestion family built on top of the canonical SQLite
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

Use this benchmark when Phase 12 ingest behavior changes materially, especially when:

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

- `staging_normalize` exists to measure one realistic Phase 12 follow-on path where
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
  the first public ingestion family as benchmark-validated for this phase

## [`routing_sweep.py`](./routing_sweep.py)

Purpose:

- run scale ladders for the SQL and Cypher routing benchmarks
- persist per-scale JSON outputs plus one merged summary document
- keep scale-sweep workflow reproducible instead of ad hoc terminal history
- allow Cypher sweeps to compare named graph-index experiments without hand-editing
  the benchmark script

Example command:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/routing_sweep.py \
  --benchmark all \
  --sql-scales 10000,100000,1000000 \
  --cypher-scales 100000,1000000 \
  --cypher-index-set baseline \
  --warmup 1 \
  --repetitions 3
```

Default output directory:

- `scripts/benchmarks/results/routing_sweep/`

Main outputs:

- `sql_summary.json`
- `cypher_summary.json`
- `routing_sweep_summary.json`
- when the Cypher benchmark is enabled, the merged summary also records the selected
  graph `index_set`

## [`routing_threshold_report.py`](./routing_threshold_report.py)

Purpose:

- summarize the merged routing sweep JSON into a workload-by-workload crossover report
- show the first scale where DuckDB wins for each SQL or Cypher workload, if any
- show the first scale where indexed vector search wins with acceptable recall, if any
- emit `recommended_runtime.cypher_phase11_diagnostics` for graph-specific follow-up
  work such as temp-B-tree pressure, property-join-heavy workloads, direct type-filter
  workloads, and ordered-versus-unordered sort overhead pairs

Example command:

```bash
python scripts/benchmarks/routing_threshold_report.py \
  --input scripts/benchmarks/results/routing_sweep/routing_sweep_summary.json \
  --output-json scripts/benchmarks/results/routing_sweep/routing_thresholds.json
```

Targeted Phase 11 comparison example:

```bash
python scripts/benchmarks/routing_sweep.py \
  --benchmark cypher \
  --cypher-scales 100000,1000000 \
  --cypher-index-set phase11-targeted \
  --warmup 0 \
  --repetitions 1 \
  --output-dir /tmp/humemdb-phase11-sweep-targeted

python scripts/benchmarks/routing_threshold_report.py \
  --input /tmp/humemdb-phase11-sweep-targeted/routing_sweep_summary.json \
  --output-json /tmp/humemdb-phase11-sweep-targeted/report.json
```

Current full-sweep inputs:

- SQL scales: `10k`, `100k`, `1M` event rows
- Cypher scales: `100k`, `1M` total nodes
- Vector scales: `2k`, `10k`, `50k` rows at `256` dims and `top_k=10`

Current SQL crossover summary:

| Workload | Family | Shape | First DuckDB win | Takeaway |
| --- | --- | --- | ---: | --- |
| `event_point_lookup` | `oltp` | `point_lookup` | none | Indexed point reads stay on SQLite across the current sweep. |
| `event_filtered_range` | `oltp` | `filtered_range` | none | Selective filtered reads still stay on SQLite. |
| `event_type_hot_window` | `oltp` | `filtered_ordered_limit` | none | The current top-k event filter is still not a stable DuckDB crossover in the sweep. |
| `event_aggregate_topk` | `analytics` | `scan_group_limit` | `10k` | Broad grouped scan work crosses to DuckDB immediately. |
| `event_region_join` | `analytics` | `join_group_order` | `10k` | Low-selectivity join-group work also crosses early. |
| `event_active_user_join_lookup` | `oltp_join` | `selective_join_lookup` | none | A selective join lookup remains SQLite territory. |
| `event_active_user_rollup` | `analytics` | `filtered_join_group` | `10k` | Grouped join rollups cross early even when filtered. |
| `event_cte_daily_rollup` | `analytics` | `cte_group_order` | `10k` | CTE-backed broad aggregation is an early DuckDB win. |
| `event_window_rank` | `analytics` | `window_partition_order` | `10k` | Windowed ranking also crosses early in the current dataset family. |
| `event_exists_region_filter` | `mixed` | `exists_filter` | `10k` | Even correlated `EXISTS` can flip once the work is broad enough. |
| `document_tag_rollup` | `document` | `selective_multi_join_group` | none | A highly selective document-tag join still strongly favors SQLite. |
| `document_owner_region_rollup` | `document` | `broad_multi_join_group` | `100k` | Broader document-owner aggregation crosses later, around the mid-scale tier. |
| `document_distinct_owner_regions` | `document` | `distinct_join_projection` | `1M` | `DISTINCT` join projection does not flip until the larger scales. |
| `memory_hot_rollup` | `memory` | `filtered_group_limit` | `1M` | Memory rollups sit in the crossover region and only move at larger scales. |
| `memory_owner_exists_projection` | `memory` | `exists_projection` | `1M` | `EXISTS` over the memory table also crosses later. |
| `memory_owner_join_lookup` | `memory` | `selective_join_lookup` | `1M` | Even lookup-like memory joins can flip, but only much later than event OLTP joins. |

Current Cypher crossover summary:

| Workload | Family | Shape | First DuckDB win | Takeaway |
| --- | --- | --- | ---: | --- |
| `user_lookup` | `node` | `anchored_node_lookup` | none | Anchored node lookups remain firmly SQLite-favored. |
| `document_lookup` | `node` | `anchored_node_lookup` | none | Anchored document lookup stays on SQLite. |
| `topic_lookup` | `node` | `anchored_node_lookup` | none | Anchored topic lookup stays on SQLite. |
| `team_lookup` | `node` | `anchored_node_lookup` | none | The added `Team` node family also stays on SQLite. |
| `social_expand` | `edge` | `broad_relationship_expand` | `1M` | Broad `KNOWS` traversal crosses, but only at the larger current graph scale. |
| `social_mixed_boolean` | `edge` | `mixed_boolean_expand` | `100k` | Mixed boolean relationship filtering is the earliest current raw-backend graph crossover. |
| `social_expand_ordered` | `edge` | `ordered_relationship_expand` | none | Ordering plus `LIMIT` still does not make this traversal a DuckDB win. |
| `social_expand_untyped` | `edge` | `untyped_relationship_expand` | none | Untyped relationship expansion still stays on SQLite. |
| `social_expand_type_alternation` | `edge` | `relationship_type_alternation` | none | Narrow type alternation does not justify DuckDB in the current sweep. |
| `social_expand_anonymous_endpoints` | `edge` | `anonymous_endpoint_expand` | none | Anonymous-endpoint relationship reads still stay on SQLite. |
| `social_expand_unfiltered` | `edge` | `full_relationship_expand` | none | Full fanout with a `LIMIT` still stays on SQLite in the current sweep. |
| `social_reverse_since_anchor` | `edge` | `relationship_property_anchor` | `1M` | Reverse-edge traversal with relationship-property anchoring only crosses at the larger scale. |
| `social_reverse_expand_ordered` | `edge` | `reverse_relationship_expand` | none | Reverse ordered expansion remains SQLite-favored. |
| `author_expand` | `edge` | `selective_relationship_expand` | none | Selective authored traversal stays on SQLite. |
| `author_expand_ordered` | `edge` | `ordered_relationship_expand` | none | Ordered authored traversal still stays on SQLite. |
| `tagged_expand` | `edge` | `selective_relationship_expand` | none | Selective tagged traversal stays on SQLite. |
| `tagged_topic_fanout` | `edge` | `topic_fanout_expand` | none | Topic-side fanout still does not justify DuckDB. |
| `team_membership_expand` | `edge` | `membership_expand` | none | The new `MEMBER_OF` traversal family stays on SQLite. |
| `team_membership_ordered` | `edge` | `ordered_membership_expand` | none | Ordered team-membership traversal also stays on SQLite. |

Current vector crossover summary:

| Workload | Family | Shape | First indexed win | Takeaway |
| --- | --- | --- | ---: | --- |
| `vector_dims256_topk10` | `vector` | `candidate_filtered_ann` | none | The current default indexed LanceDB path never beats the exact SQLite/NumPy path with acceptable recall in this sweep. |

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

## [`vector_search.py`](./vector_search.py)

Purpose:

- Compare the first realistic `HumemVector v0` execution candidates.
- Establish a benchmarked routing baseline for exact NumPy search, scalar-int8 quantized
  search, and LanceDB flat versus indexed vector search.
- Measure both global nearest-neighbor search and a simple metadata-prefiltered search
  shape.
- Treat LanceDB as a black-box indexed backend for now: the benchmark uses LanceDB's
  default index algorithm and default search hyperparameters instead of hand-tuned
  settings.

Thread-control note:

- `HUMEMDB_THREADS` is the top-level thread cap for the vector benchmark.
- The vector runtime applies it to common NumPy/BLAS/OpenMP env vars and also uses
  `threadpoolctl` as a best-effort runtime cap for loaded numeric pools.
- The vector benchmark applies the same cap to Arrow's global CPU pool with
  `pyarrow.set_cpu_count()`.
- `LANCEDB_THREADS` remains available as a vector-only fallback when `HUMEMDB_THREADS`
  is unset.
- This is a best-effort local-thread cap for LanceDB's Arrow-backed execution path, not
  a documented LanceDB-specific hard limit across every internal pool.
- LanceDB still uses its own default index/search settings unless the library changes
  them.

Tuning note:

- If your recall target is stricter, for example `>= 0.95`, benchmark tuned LanceDB
  settings instead of relying on the library defaults.
- The current benchmark supports explicit LanceDB index/search knobs such as
  `--lancedb-index-type`, `--lancedb-num-partitions`, `--lancedb-num-sub-vectors`,
  `--lancedb-nprobes`, `--lancedb-refine-factor`, and `--lancedb-ef`.

Shared command:

```bash
python scripts/benchmarks/vector_search.py \
  --rows 100000 \
  --dimensions 384 \
  --queries 64 \
  --top-k 10 \
  --warmup 1 \
  --repetitions 5
```

Use it in one of these ways:

## [`vector_query_steps.py`](./vector_query_steps.py)

Purpose:

- Break one exact-vector workload into step timings instead of only comparing complete
  end-to-end paths.
- Measure ingest cost for direct vectors, SQL-owned vectors, and Cypher-owned vectors.
- Measure frontend overhead for SQL translation and Cypher parse/bind+compile.
- Measure candidate-query execution, candidate-id mapping, pure NumPy vector search, and
  end-to-end candidate-filtered vector query latency.

Representative command used for the current intermediate result:

```bash
python scripts/benchmarks/vector_query_steps.py \
  --rows 100000 \
  --dimensions 768 \
  --queries 50 \
  --warmup 1 \
  --repetitions 3 \
  --output json
```

Current status:

- These are intermediate measurements, not a final routing policy.
- The current runtime is still expected to improve, especially around candidate-filtered-path
  execution and future ingest work.
- Cypher ingest is currently transactional but still statement-oriented, not a true
  batched bulk-ingest path, so its ingest cost is not yet a fair lower bound.

Scenario:

| Metric | Value |
| ------ | ----: |
| Rows | 100,000 |
| Dimensions | 768 |
| Queries | 50 |
| `top_k` | 10 |
| Candidate-filtered count | 50,000 |

One-time stage timings:

| Stage | Time |
| ----- | ---: |
| Direct ingest | 1999.15 ms |
| SQL-owned ingest | 9096.09 ms |
| Cypher-owned ingest | 18029.06 ms |
| Direct preload | 1707.34 ms |
| SQL-owned preload | 1197.45 ms |
| Cypher-owned preload | 1805.85 ms |

Per-query timing means:

| Stage | Mean |
| ----- | ---: |
| Direct vector query end-to-end | 8.38 ms |
| Direct vector search only | 7.76 ms |
| SQL cached translation | 0.0007 ms |
| SQL uncached translation | 0.0982 ms |
| SQL candidate query only | 109.10 ms |
| SQL candidate mapping only | 5.11 ms |
| SQL vector search only | 19.20 ms |
| SQL vector query end-to-end | 150.56 ms |
| Cypher parse only | 0.0149 ms |
| Cypher bind+compile | 0.0077 ms |
| Cypher candidate query only | 28.55 ms |
| Cypher candidate mapping only | 5.12 ms |
| Cypher vector search only | 349.70 ms |
| Cypher vector query end-to-end | 432.36 ms |

Interim interpretation:

| Question | Current answer |
| -------- | -------------- |
| Is frontend translation/planning the bottleneck? | No. SQL uncached translation stayed around `0.10 ms`, and Cypher parse plus bind+compile stayed around `0.02 ms` combined. |
| What dominates candidate-filtered vector latency today? | For SQL candidate-filtered search, the candidate query dominates first. For Cypher candidate-filtered search in this run, the vector search over the large candidate subset dominated heavily. |
| Did candidate filtering help in this run? | No. The candidate filter kept 50,000 of 100,000 vectors, so the filter was still too broad to pay for the extra frontend and candidate-mapping work. |
| Why is Cypher-owned ingest much slower? | The current Cypher write path is transactional but still one `CREATE` per node, not a true batched bulk-ingest surface. |
| What should be optimized next? | Candidate-filtered path execution, candidate mapping, selectivity-sensitive vector search, and later bulk graph ingest rather than parser/compiler micro-optimizations. |

Use this benchmark when you want to answer where time is spent inside the current exact
vector path rather than only whether one whole end-to-end backend path wins.

## [`vector_search_sweep.py`](./vector_search_sweep.py)

Purpose:

- Sweep the vector benchmark across multiple row counts, dimensions, and `top_k` values.
- Capture setup costs, steady-state query latency, and recall together so HumemDB can
  estimate when NumPy exact, NumPy scalar-int8, or LanceDB indexed should be the
  practical route.
- Print a first-pass break-even estimate for LanceDB indexed versus NumPy exact.

Example command:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search_sweep.py \
  --rows-grid 2000,10000,50000 \
  --dimensions-grid 64,256,768 \
  --top-k-grid 10 \
  --queries 16 \
  --repetitions 2
```

What it reports:

- Per-scenario setup totals for NumPy and LanceDB paths.
- Per-scenario mean query latencies and recall.
- Break-even query estimates for LanceDB indexed versus NumPy exact.
- A preliminary routing recommendation for each scenario plus an overall summary.

Current use:

- Use this script to build the current vector rule of thumb instead of relying on a single
  benchmark point.
- Re-run it when LanceDB versions change, when thread budgets change, or when the
  expected production dimensions and query volumes change.
- For most current LanceDB-versus-NumPy sweeps, pass `--skip-numpy-sq8`. In this
  implementation, NumPy SQ8 is usually slower than NumPy FP32 and is only worth
  keeping in the benchmark when memory-saving tradeoffs are the specific question.

Recommended current sweep style:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search_sweep.py \
  --rows-grid 100000,250000,500000,1000000 \
  --dimensions-grid 256,384 \
  --top-k-grid 10 \
  --queries 100 \
  --warmup 1 \
  --repetitions 2 \
  --lancedb-mode tuned \
  --lancedb-tuned-family ivf_flat \
  --skip-numpy-sq8
```

Representative tuned sweep:

Key result artifacts:

| Artifact                                                                                                                                                                                     | Grid                                    | High-level finding                                                                                                                                                                       |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`results/vector_search_sweep_tuned_threads4_queries100_rows2k-100k_dims256-1024_topk10.json`](./results/vector_search_sweep_tuned_threads4_queries100_rows2k-100k_dims256-1024_topk10.json) | `2k` to `100k`, dims `256,384,768,1024` | Early tuned reference sweep. NumPy exact won `15 / 16`; LanceDB won only `100k x 256`, with break-even about `61,551` queries.                                                           |
| [`results/ivfpq_100k_384.json`](./results/ivfpq_100k_384.json)                                                                                                                               | `100k x 384`                            | `IVF_PQ` gate failed badly for high recall. Best tested recall was only `0.482`.                                                                                                         |
| [`results/ivfhnswsq_crossover_100k_1m_dims256_384.json`](./results/ivfhnswsq_crossover_100k_1m_dims256_384.json)                                                                             | `100k` to `1M`, dims `256,384`          | `IVF_HNSW_SQ` met the `0.95` recall bar in only `2 / 8` scenarios and had zero acceptable latency wins.                                                                                  |
| [`results/ivfflat_crossover_100k_1m_dims256_384.json`](./results/ivfflat_crossover_100k_1m_dims256_384.json)                                                                                 | `100k` to `1M`, dims `256,384`          | `IVF_FLAT` met the `0.95` recall bar in `8 / 8` scenarios and produced the first real high-recall crossover.                                                                             |
| [`results/ivfflat_boundary_150k_400k_dims256_384.json`](./results/ivfflat_boundary_150k_400k_dims256_384.json)                                                                               | `150k` to `400k`, dims `256,384`        | Refined the `IVF_FLAT` boundary: crossover starts around `300k` for `384` dims and around `400k` for `256` dims.                                                                         |
| [`results/ivfflat_crossover_100k_1m_dims768_1024.json`](./results/ivfflat_crossover_100k_1m_dims768_1024.json)                                                                               | `100k` to `1M`, dims `768,1024`         | Higher-dimension validation. `IVF_FLAT` again met the recall bar in `8 / 8` scenarios and won in `5 / 8`, with crossover around `250k` for `768` dims and around `500k` for `1024` dims. |

Column legend:

- `SQLite->NumPy ms` = source-to-NumPy load cost.
- `LanceDB table ms` = source-to-LanceDB materialization cost.
- `LanceDB index ms` = extra indexing cost after the LanceDB table exists.

`IVF_FLAT` crossover decision table:

|      Rows | Dims | Tuned LanceDB candidate | Recall | LanceDB indexed ms | NumPy FP32 ms | SQLite->NumPy ms | NumPy build ms | LanceDB table ms | LanceDB index ms | Break-even queries | Verdict                                 |
| --------: | ---: | ----------------------- | -----: | -----------------: | ------------: | ---------------: | -------------: | ---------------: | ---------------: | -----------------: | --------------------------------------- |
|   100,000 |  256 | `ivf_flat_probe256`     |  1.000 |               3.67 |          2.52 |           505.76 |          26.29 |          1396.26 |          7634.81 |                  — | NumPy exact                             |
|   100,000 |  384 | `ivf_flat_probe256`     |  1.000 |               4.70 |          3.56 |           570.25 |          34.85 |          2075.78 |         12065.13 |                  — | NumPy exact                             |
|   250,000 |  256 | `ivf_flat_probe256`     |  1.000 |               6.98 |          7.03 |          1250.24 |          63.39 |          3548.75 |          8256.99 |            107,144 | LanceDB only for very high reuse        |
|   250,000 |  384 | `ivf_flat_probe256`     |  1.000 |              10.27 |         10.04 |          1516.16 |          84.96 |          5163.05 |         12589.26 |                  — | NumPy exact                             |
|   500,000 |  256 | `ivf_flat_probe256`     |  1.000 |              13.18 |         15.27 |          2948.89 |         126.18 |          7209.81 |          9101.06 |              3,055 | LanceDB indexed if vector set is reused |
|   500,000 |  384 | `ivf_flat_probe256`     |  1.000 |              19.07 |         23.07 |          3424.51 |         175.05 |         10266.06 |         13700.80 |              2,656 | LanceDB indexed if vector set is reused |
| 1,000,000 |  256 | `ivf_flat_probe256`     |  1.000 |              26.23 |         36.27 |          6379.64 |         292.40 |         14989.44 |         10818.55 |              1,096 | Strong LanceDB indexed case             |
| 1,000,000 |  384 | `ivf_flat_probe512`     |  1.000 |              37.77 |         41.22 |          7520.92 |         361.06 |         20651.90 |         15224.71 |              6,378 | LanceDB indexed if vector set is reused |

Current LanceDB family takeaway:

Family verdict:

| Family        | Evidence                                                                                                                                                                                         | Verdict                                    |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------ |
| `IVF_PQ`      | Best tested recall in [`results/ivfpq_100k_384.json`](./results/ivfpq_100k_384.json) was `0.482`.                                                                                                | Out for current `>= 0.95` recall target.   |
| `IVF_HNSW_SQ` | Met recall bar in only `2 / 8` scenarios in [`results/ivfhnswsq_crossover_100k_1m_dims256_384.json`](./results/ivfhnswsq_crossover_100k_1m_dims256_384.json), with zero acceptable latency wins. | Out as default high-recall indexed family. |
| `IVF_FLAT`    | Met recall bar in `8 / 8` scenarios in the crossover and boundary sweeps and produced the only real high-recall crossover.                                                                       | In. First serious indexed candidate.       |

Current routing threshold:

| Dims | Use NumPy exact through | Start considering tuned `IVF_FLAT` at | Notes                                                                                 |
| ---: | ----------------------: | ------------------------------------: | ------------------------------------------------------------------------------------- |
|  256 |       about `300k` rows |                     about `400k` rows | `300k` still loses to NumPy exact; `400k` wins with break-even about `6,481` queries. |
|  384 |       about `200k` rows |                     about `300k` rows | `300k` wins with break-even about `3,642` queries.                                    |
|  768 |       about `100k` rows |                     about `250k` rows | `250k` wins with break-even about `9,421` queries.                                    |
| 1024 |       about `250k` rows |                     about `500k` rows | `250k` still loses; `500k` wins with break-even about `16,185` queries.               |

Current takeaway:

- Prefer NumPy exact as the baseline below the crossover region.
- Use tuned `IVF_FLAT` for larger reused vector sets.
- Treat NumPy SQ8 as a memory tradeoff, not a speed path.

SQ8 tradeoffs:

- Good: cuts the stored vector matrix from 4 bytes/value (`float32`) to about 1
  byte/value plus small per-dimension scale metadata.
- Bad: does not currently improve end-to-end latency here. FP32 exact uses an efficient
  dense matrix-vector multiply, while the current SQ8 path pays extra dequantization-like
  overhead without a specialized low-bit kernel.
- Bad: recall is lower than FP32 exact by design, so it adds approximation error without
  delivering a speed win on the tested grid.
- Practical takeaway: SQ8 is only interesting when memory pressure matters more than raw
  latency or exactness. If memory is not the bottleneck, prefer NumPy FP32 exact.

## [`vector_search_tune_lancedb.py`](./vector_search_tune_lancedb.py)

Purpose:

- Search a curated set of LanceDB index/search configurations.
- Find the lowest-latency configuration that still meets a recall target such as `0.95`.
- Cover a few progressively more aggressive families, including `IVF_PQ`, `IVF_FLAT`,
  and `IVF_HNSW_SQ`, so the search is useful for both default-like and high-recall
  scenarios.

Example command:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search_tune_lancedb.py \
  --rows 10000 \
  --dimensions 384 \
  --queries 16 \
  --top-k 10 \
  --target-recall 0.95
```

Initial observations from one representative sweep:

- Sweep used `HUMEMDB_THREADS=4`, row counts `2000,10000,50000`, dimensions `64,384`,
  `top_k=10`, `queries=16`, and LanceDB defaults.
- SQLite canonical insert cost ranged from about `94 ms` to `705 ms`.
- SQLite-to-NumPy load cost ranged from about `5 ms` to `307 ms`.
- LanceDB table creation ranged from about `14 ms` to `1139 ms`.
- LanceDB index build ranged from about `44 ms` to `5495 ms`.
- NumPy exact global query latency stayed between about `0.03 ms` and `6.53 ms` in these
  scenarios.
- LanceDB indexed global latency stayed between about `0.88 ms` and `1.22 ms`, but
  recall@k only ranged from about `0.15` to `0.32`, so it did not meet a `0.95` recall
  acceptance bar in any tested scenario.
- NumPy scalar-int8 recall stayed around `0.98` to `0.99`, which makes it a plausible
  in-memory compromise when exact float32 memory cost becomes painful.

Follow-up tuning observations:

- Default LanceDB indexed settings are still not acceptable when the recall bar is `>=
0.95`.
- Early small-scale tuning runs showed that `IVF_HNSW_SQ` could meet the bar in some
  representative cases such as `10000 x 384` and `50000 x 384`, which made it a
  plausible family to investigate further.
- The later crossover sweeps changed that conclusion: on the broader
  [`results/ivfhnswsq_crossover_100k_1m_dims256_384.json`](./results/ivfhnswsq_crossover_100k_1m_dims256_384.json)
  grid, tuned `IVF_HNSW_SQ` met the `0.95` recall target in only 2 of 8 scenarios and
  produced zero latency wins with acceptable recall.
- By contrast, the broader
  [`results/ivfflat_crossover_100k_1m_dims256_384.json`](./results/ivfflat_crossover_100k_1m_dims256_384.json)
  sweep showed that tuned `IVF_FLAT` met the recall bar in all 8 scenarios and became
  the only tested indexed family that delivered a real high-recall crossover on this
  workload.

Current rule of thumb:

- Default to NumPy exact for the currently explored range.
- Treat scalar-int8 as the first in-memory optimization to evaluate before routing to
  LanceDB indexed.
- Do not treat default LanceDB indexed search as the public default yet.
- If the workload needs indexed search and recall must stay near `0.95` or above,
  benchmark tuned `IVF_FLAT` first. On the currently tested grid, `IVF_PQ` and
  `IVF_HNSW_SQ` both failed as high-recall defaults, while `IVF_FLAT` was the only
  family that produced a real crossover against NumPy exact.
