# Vector Search Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search.py){ .md-button }

Purpose:

- compare the current `HumemVector v0` execution candidates in one concrete scenario
- measure exact NumPy search, scalar-int8 NumPy search, LanceDB flat search, and LanceDB
  indexed search
- report both setup costs and steady-state query latency plus recall

Representative command:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search.py \
  --rows 100000 \
  --dimensions 384 \
  --queries 64 \
  --top-k 10 \
  --warmup 1 \
  --repetitions 3
```

Scenario:

- 100,000 vectors
- 384 dimensions
- cosine metric
- 64 queries
- `top_k=10`
- filtered bucket candidate count: 733
- LanceDB index type: `IVF_PQ` with library-default partition settings

Stage timings:

| Stage | Time |
| ----- | ---: |
| SQLite seed | 1243.06 ms |
| SQLite to NumPy load | 662.98 ms |
| NumPy FP32 build | 41.28 ms |
| NumPy SQ8 build | 131.77 ms |
| LanceDB table create | 2292.47 ms |
| LanceDB index build | 12553.09 ms |

Per-query latency and recall:

| Path | Global mean | Filtered mean | Recall@k global | Recall@k filtered | Takeaway |
| ---- | ----------: | ------------: | --------------: | ----------------: | -------- |
| NumPy FP32 exact | 4.05 ms | 0.07 ms | 1.0000 | 1.0000 | Current exact baseline. |
| NumPy SQ8 | 14.44 ms | 0.07 ms | 0.9859 | 0.9891 | Saves memory, but this run did not turn that into a latency win. |
| LanceDB flat | 38.77 ms | 37.64 ms | 1.0000 | 1.0000 | Exact but much slower than in-memory NumPy here. |
| LanceDB indexed default | 1.27 ms | 2.18 ms | 0.1594 | 0.2734 | Lowest latency, but recall is far below a high-recall target. |

Artifact sizes:

| Artifact | Size |
| -------- | ---: |
| NumPy FP32 matrix | 153,600,000 bytes |
| NumPy SQ8 quantized data | 38,400,000 bytes |
| NumPy SQ8 scales | 1,536 bytes |
| Query batch FP32 | 98,304 bytes |

Current interpretation:

- NumPy FP32 exact remains the strongest baseline in this representative mid-size case
- NumPy SQ8 reduces memory substantially, but the current implementation does not turn
  that into a speed win
- default LanceDB indexed search can beat NumPy exact on latency, but its recall is far
  below an acceptable high-recall default for this query shape
- the sweep and tuning benchmarks are the better place to decide routing thresholds,
  because one single-run point is not enough to define a crossover policy
