# Benchmarks

Benchmarks are part of the routing story, not an afterthought.

The benchmark scripts live in [`scripts/benchmarks/`]({{ config.repo_url }}/tree/{{ config.extra.version_tag }}/scripts/benchmarks).
The MkDocs guide mirrors that directory with one page per Python benchmark script so the
reported tables and current numbers are visible in the docs site instead of only in the
repository README.

## Pages

- Translation overhead: [`translation_overhead.py`]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/translation_overhead.py)
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
- isolate Cypher parse and bind+compile overhead
- keep frontend cost separate from backend execution cost when routing decisions are
    being discussed

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
- The Cypher benchmark also now includes `Team` nodes plus `MEMBER_OF` edges and can
  emit JSON summaries with `--output-json`.

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
