# Real Vector Search Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_real.py){ .md-button }

Purpose:

- benchmark shipped vector datasets in one concrete shared-build scenario
- hold the current threshold steady: NumPy exact below the ANN snapshot cutoff and
  LanceDB `IVF_PQ` snapshot builds above it
- measure load cost, LanceDB table and index build cost, indexed query latency, and
  recall versus NumPy exact where the exact baseline is still enabled

Representative command:

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

Current implementation note:

- full NumPy exact stops above `100k` rows by default so the benchmark matches the ANN snapshot threshold
- the LanceDB side is intentionally narrowed to `IVF_PQ`
- snapshot-only runs above the `100k` cutoff ingest selected shard memmaps directly into `Arrow batches -> LanceDB` before index build
- when `--top-k-grid` is used, the benchmark builds once per dataset and scale, then reuses that build for all requested `top_k` values
