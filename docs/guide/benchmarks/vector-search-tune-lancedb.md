# Vector Search Tune LanceDB Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_tune_lancedb.py){ .md-button }

Purpose:

- search a curated set of LanceDB index and search configurations
- find the lowest-latency configuration that still meets a recall target such as `0.95`
- compare candidate families including `IVF_PQ`, `IVF_FLAT`, and `IVF_HNSW_SQ`

Representative command:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search_tune_lancedb.py \
  --rows 10000 \
  --dimensions 384 \
  --queries 16 \
  --top-k 10 \
  --target-recall 0.95
```

Initial observations from one representative sweep:

- sweep used `HUMEMDB_THREADS=4`, row counts `2000,10000,50000`, dimensions `64,384`,
  `top_k=10`, `queries=16`, and LanceDB defaults
- SQLite canonical insert cost ranged from about `94 ms` to `705 ms`
- SQLite to NumPy load cost ranged from about `5 ms` to `307 ms`
- LanceDB table creation ranged from about `14 ms` to `1139 ms`
- LanceDB index build ranged from about `44 ms` to `5495 ms`
- NumPy exact global query latency stayed between about `0.03 ms` and `6.53 ms`
- LanceDB indexed global latency stayed between about `0.88 ms` and `1.22 ms`, but
  recall@k only ranged from about `0.15` to `0.32`, so it did not meet a `0.95` recall
  acceptance bar in any tested scenario
- NumPy scalar-int8 recall stayed around `0.98` to `0.99`, which makes it a plausible
  in-memory compromise when exact float32 memory cost becomes painful

Current LanceDB family verdict:

| Family | Evidence | Verdict |
| ------ | -------- | ------- |
| `IVF_PQ` | Best tested recall in `results/ivfpq_100k_384.json` was `0.482`. | Out for the current `>= 0.95` recall target. |
| `IVF_HNSW_SQ` | Met the recall bar in only `2 / 8` scenarios in `results/ivfhnswsq_crossover_100k_1m_dims256_384.json`, with zero acceptable latency wins. | Out as the default high-recall indexed family. |
| `IVF_FLAT` | Met the recall bar in `8 / 8` scenarios in the crossover and boundary sweeps and produced the only real high-recall crossover. | In. First serious indexed candidate. |

Current interpretation:

- default LanceDB indexed settings are not acceptable when the recall bar is `>= 0.95`
- tuned `IVF_FLAT` is the first indexed family that consistently clears that bar on the
  currently tested grid
- `IVF_PQ` and `IVF_HNSW_SQ` are not good current defaults for high-recall routing
