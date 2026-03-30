# Real Vector Sweep Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_real_sweep.py){ .md-button }

Purpose:

- sweep real dataset scales over `top_k` and sampling choices
- persist rolling summaries and per-scenario JSON files while long runs are still in progress
- produce the real-data routing baseline for the fixed `100k` hot / `>100k` cold split
- score each scenario against the current `IVF_PQ` recall admission bar

Representative command:

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

Current baseline:

- the routing policy is fixed at `100k`: hot stays in NumPy exact, cold goes to LanceDB `IVF_PQ`
- the sweep focuses on hot-tier exact latency and memory at the cut, cold-tier build and query cost above the cut, and the operational pressure of LanceDB ingest as cold history grows
- cold-only runs above `100k` now ingest selected shard memmaps directly into LanceDB rather than routing through SQLite and DuckDB first; in the `1M` `msmarco-10m` profile that cut peak RSS from about `9.5 GiB` to about `3.7 GiB`
- the sweep reuses one build per dataset, scale, and filter before expanding results back out across the requested `top_k` grid
