# Benchmarks

Benchmarks are part of the routing story, not an afterthought.

The benchmark scripts live in [`scripts/benchmarks/`]({{ config.repo_url }}/tree/{{ config.extra.version_tag }}/scripts/benchmarks).
The MkDocs guide mirrors that directory with one page per Python benchmark script so the
reported tables and current numbers are visible in the docs site instead of only in the
repository README.

## Pages

- Translation overhead: [`translation_overhead.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/translation_overhead.py)
- CSV ingest benchmark: [`csv_ingest.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/csv_ingest.py)
- Relational direct-read benchmark: [`duckdb_direct_read.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/duckdb_direct_read.py)
- Cypher graph-path benchmark: [`cypher_graph_path.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/cypher_graph_path.py)
- Vector single-run benchmark: [`vector_search.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search.py)
- Vector step-timing benchmark: [`vector_query_steps.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_query_steps.py)
- Vector sweep benchmark: [`vector_search_sweep.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_sweep.py)
- LanceDB tuning benchmark: [`vector_search_tune_lancedb.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_tune_lancedb.py)

## Current benchmark map

The routing benchmarks now also have automation helpers:

- [`routing_sweep.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag
  }}/scripts/benchmarks/routing_sweep.py) runs SQL and Cypher scale ladders and writes
  merged JSON summaries.
- [`routing_threshold_report.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag
  }}/scripts/benchmarks/routing_threshold_report.py) summarizes those JSON files into
  workload crossover reports.

### Translation overhead benchmark

```bash
python scripts/benchmarks/translation_overhead.py --warmup 100 --repetitions 1000
```

Current purpose:

- isolate cached and uncached PostgreSQL-like SQL translation overhead
- isolate Cypher parse, generated-first runtime planning, and bind+compile overhead
- keep frontend cost separate from backend execution cost when routing decisions are
    being discussed

### CSV ingest benchmark

```bash
python scripts/benchmarks/csv_ingest.py --table-rows 50000 --node-rows 20000 --edge-fanout 2
```

Current takeaway:

- the Phase 12 ingest benchmark covers both relational CSV ingest and graph CSV ingest
- `staging_normalize` is intentionally a table-first benchmark path for permissive
  staging-table load plus normalize-into-final-table flow
- measured staged-relational runs now show that this path is operationally reasonable
  when normalization is needed, but still slower than direct `import_table(...)`
  across the current `10k` to `10M` sweep
- graph ingest is benchmarked directly through `import_nodes(...)` and
  `import_edges(...)`; graph-specific staging should wait until a real graph-derivation
  workload justifies it
- the current public ingest APIs now have benchmark evidence against both realistic
  public baselines and internal SQLite lower bounds

### Relational benchmark

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py --rows 50000
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py \
    --rows 10000000 --warmup 1 --repetitions 5 --batch-size 50000
```

Current takeaway:

- SQLite stays stronger for point lookups and smaller filtered reads.
- DuckDB wins broader grouped scans and analytical aggregates.
- The SQL benchmark now carries workload-shape and selectivity labels so future routing
  thresholds can be tied to parsed plan metadata instead of hand-picked query names.
- The SQL benchmark also now covers wider table schemas plus windowed, `EXISTS`, and
  `DISTINCT` query shapes, and can emit JSON summaries with `--output-json`.

Current SQL crossover summary from the full routing sweep:

| Workload | Shape | First DuckDB win | Takeaway |
| --- | --- | ---: | --- |
| `event_point_lookup` | `point_lookup` | none | Indexed point reads stay on SQLite across the current sweep. |
| `event_filtered_range` | `filtered_range` | none | Selective filtered reads still stay on SQLite. |
| `event_aggregate_topk` | `scan_group_limit` | `10k` | Broad grouped scan work crosses to DuckDB immediately. |
| `event_region_join` | `join_group_order` | `10k` | Low-selectivity join-group work also crosses early. |
| `event_active_user_join_lookup` | `selective_join_lookup` | none | A selective join lookup remains SQLite territory. |
| `event_active_user_rollup` | `filtered_join_group` | `10k` | Grouped join rollups cross early even when filtered. |
| `event_cte_daily_rollup` | `cte_group_order` | `10k` | CTE-backed broad aggregation is an early DuckDB win. |
| `event_window_rank` | `window_partition_order` | `10k` | Windowed ranking also crosses early in the current dataset family. |
| `document_owner_region_rollup` | `broad_multi_join_group` | `100k` | Broader document-owner aggregation crosses later, around the mid-scale tier. |
| `document_distinct_owner_regions` | `distinct_join_projection` | `1M` | `DISTINCT` join projection does not flip until the larger scales. |
| `memory_hot_rollup` | `filtered_group_limit` | `1M` | Memory rollups sit in the crossover region and only move at larger scales. |

Routing takeaway from the current SQL sweep:

- broad grouped, windowed, or CTE-backed SQL reads have enough evidence for
  conservative DuckDB admission
- selective point reads and selective join lookups should still stay on SQLite
- join presence alone is not enough; selectivity and breadth still matter more than
  surface syntax

### Graph benchmark

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py --nodes 5000 --fanout 3
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py \
    --nodes 1000000 --fanout 4 --tag-fanout 2 --warmup 1 --repetitions 5 --batch-size 20000
```

Current takeaway:

- SQLite is very strong for selective graph traversal.
- DuckDB becomes compelling only once the read broadens into graph-analytic shapes.
- The current Cypher sweep is still not broad enough to justify hard automatic graph
  routing in the product: only one current broad-fanout family crosses, so SQLite should
  remain the default graph recommendation for now.
- The Cypher benchmark now carries workload-shape and selectivity labels so later graph
  routing work can reason about anchored lookup versus broader traversal families.
- The Cypher benchmark also now includes untyped relationship reads, narrow
  relationship-type alternation reads, anonymous-endpoint relationship reads,
  reverse-direction relationship fanout reads, `Team` nodes, and `MEMBER_OF` edges,
  and can
  emit JSON summaries with `--output-json`.

Current Cypher crossover summary from the full routing sweep:

| Workload | Shape | First DuckDB win | Takeaway |
| --- | --- | ---: | --- |
| `user_lookup` | `anchored_node_lookup` | none | Anchored node lookups remain firmly SQLite-favored. |
| `document_lookup` | `anchored_node_lookup` | none | Anchored document lookup stays on SQLite. |
| `topic_lookup` | `anchored_node_lookup` | none | Anchored topic lookup stays on SQLite. |
| `team_lookup` | `anchored_node_lookup` | none | The added `Team` node family also stays on SQLite. |
| `social_expand` | `broad_relationship_expand` | `1M` | Broad `KNOWS` traversal is the first current graph workload to cross to DuckDB. |
| `social_expand_ordered` | `ordered_relationship_expand` | none | Ordering plus `LIMIT` still does not make this traversal a DuckDB win. |
| `social_reverse_since_anchor` | `relationship_property_anchor` | none | Reverse-edge traversal with property anchoring stays SQLite-favored. |
| `author_expand` | `selective_relationship_expand` | none | Selective authored traversal stays on SQLite. |
| `tagged_topic_fanout` | `topic_fanout_expand` | none | Topic-side fanout still does not justify DuckDB. |
| `team_membership_expand` | `membership_expand` | none | The new `MEMBER_OF` traversal family stays on SQLite. |

Routing takeaway from the current Cypher sweep:

- current graph evidence is still too narrow for broad automatic DuckDB routing
- SQLite should remain the default recommendation for omitted-route Cypher reads
- only one current broad-fanout family crosses, and only at `1M`, so graph routing
  should stay more conservative than SQL routing

### Routing sweep helpers

Use the routing sweep helper when you want to compare the same workload families across
multiple scales without manually running each command yourself.

Use the threshold report helper when you want a workload-by-workload summary of where
DuckDB first wins, if it wins at all.

### Vector benchmark

The vector benchmark scripts measure exact NumPy search and quantized variants so later
routing choices can be based on observed crossover points instead of guesswork.

The single-run, sweep, and LanceDB-tuning vector pages are split out because they answer
different questions:

- [`vector_search.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search.py) measures one concrete scenario in depth.
- [`vector_query_steps.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_query_steps.py) breaks one exact-vector flow into ingest, frontend, candidate-query, candidate-mapping, and search stages.
- [`vector_search_sweep.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_sweep.py) finds crossover regions across row-count and dimension grids.
- [`vector_search_tune_lancedb.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_tune_lancedb.py) searches candidate indexed profiles that can still hit
    a recall target.
