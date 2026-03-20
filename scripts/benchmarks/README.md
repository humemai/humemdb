# Benchmark Scripts

This directory contains benchmarking utilities for comparing query paths, storage
strategies, and execution backends.

The relational benchmark exercises `HumemSQL v0` query shapes. The graph benchmark
exercises `HumemCypher v0` query shapes. The vector benchmark exercises the current
`HumemVector v0` execution candidates.

Set `HUMEMDB_THREADS` to cap the backend worker-thread budget used by HumemDB. Today
this affects DuckDB plus the vector runtime's NumPy/BLAS and Arrow-backed LanceDB paths.
SQLite still behaves like a selective, single-query OLTP engine.

## [`duckdb_direct_read.py`](./duckdb_direct_read.py)

Purpose:

- Compare SQLite and DuckDB across a broader relational workload mix over the SQLite
  source of truth.
- Cover OLTP-style event reads, analytical aggregates, document-tag joins, and
  memory-style rollups.

Thread-control example:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py --rows 1000000
```

Large-run command used:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py \
  --rows 10000000 \
  --repetitions 5 \
  --warmup 1 \
  --batch-size 50000 \
  --users 200000 \
  --tags 2048
```

Dataset:

- 200,000 users
- 10,000,000 event rows
- 500,000 documents
- 2,048 tags
- 1,500,000 document-tag rows
- 1,000,000 memory chunks
- 1 warmup iteration and 5 timed repetitions per query shape
- Initial load time: 22266.28 ms

Observed means:

| Query shape            | SQLite mean | DuckDB mean | Takeaway                                                                            |
| ---------------------- | ----------: | ----------: | ----------------------------------------------------------------------------------- |
| `event_point_lookup`   |     0.01 ms |   973.49 ms | SQLite is vastly better for indexed point reads against the canonical store.        |
| `event_filtered_range` |     8.37 ms |   439.16 ms | SQLite stays much better for selective OLTP-style filters.                          |
| `event_aggregate_topk` |  4646.88 ms |   523.44 ms | DuckDB is about 8.9x faster on broad scan-and-group workloads.                      |
| `event_region_join`    |  4668.97 ms |   528.00 ms | DuckDB is about 8.8x faster on analytical join aggregation.                         |
| `document_tag_rollup`  |     1.29 ms |   107.13 ms | A selective indexed document join still strongly favors SQLite.                     |
| `memory_hot_rollup`    |   311.48 ms |    60.78 ms | DuckDB is about 5.1x faster on broader grouped rollups over the memory-style table. |

SQL findings:

- SQLite remains the clear default for point lookups, selective filters, and selective
  indexed joins, even when the overall dataset is large.
- DuckDB wins once the workload becomes genuinely analytical: broad scans, grouping, and
  large aggregation over many rows.
- The richer SQL suite makes an important point that the older benchmark could not: not
  every join is analytical. Join shape and selectivity matter more than the mere
  presence of joins.

## [`cypher_graph_path.py`](./cypher_graph_path.py)

Purpose:

- Measure graph initial load time, Cypher parse and compile overhead, and raw SQL versus
  end-to-end Cypher execution on SQLite and DuckDB.
- Cover multiple node labels and edge types instead of a single graph shape.
- Recheck graph-path behavior after changes to graph indexes or Cypher SQL
  compilation, since those dominate performance more than Cypher parsing itself.

Thread-control example:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py --nodes 100000
```

Large-run command used:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py \
  --nodes 1000000 \
  --fanout 4 \
  --tag-fanout 2 \
  --repetitions 5 \
  --warmup 1 \
  --batch-size 20000
```

Dataset:

- 1,000,000 total nodes
  - 500,000 `User` nodes
  - 350,000 `Document` nodes
  - 150,000 `Topic` nodes
- 3,050,000 total edges
  - 2,000,000 `KNOWS` edges
  - 350,000 `AUTHORED` edges
  - 700,000 `TAGGED` edges
- Approximately 10,950,000 total rows across graph tables and graph property tables
- 1 warmup iteration and 5 timed repetitions per stage
- Initial load time: 33052.16 ms

Observed means:

| Workload          | SQLite raw SQL | DuckDB raw SQL | SQLite Cypher | DuckDB Cypher | Takeaway                                                                  |
| ----------------- | -------------: | -------------: | ------------: | ------------: | ------------------------------------------------------------------------- |
| `user_lookup`     |        0.02 ms |     1157.15 ms |       0.05 ms |    1167.23 ms | SQLite is overwhelmingly better for anchored user-node lookup.            |
| `document_lookup` |        0.02 ms |     1179.71 ms |       0.07 ms |    1166.71 ms | SQLite is also overwhelmingly better for selective document lookup.       |
| `topic_lookup`    |        0.02 ms |      939.52 ms |       0.07 ms |     941.15 ms | Selective topic lookup strongly favors SQLite.                            |
| `social_expand`   |     1648.92 ms |     1221.28 ms |    1620.89 ms |    1220.32 ms | DuckDB wins once traversal broadens into the high-fanout social edge set. |
| `author_expand`   |      492.37 ms |     1160.95 ms |     500.58 ms |    1163.97 ms | A selective author-to-document expansion still favors SQLite.             |
| `tagged_expand`   |      100.08 ms |     1137.34 ms |      97.61 ms |    1125.02 ms | A selective document-to-topic expansion also still favors SQLite.         |

Compiler overhead:

- Cypher parse cost stayed around 0.02 to 0.03 ms.
- Cypher bind+compile cost stayed around 0.03 to 0.04 ms.
- End-to-end Cypher timings tracked raw SQL closely, which confirms that execution plan
  shape and backend behavior dominate total latency.

Graph findings:

- The multi-label graph benchmark makes the routing boundary clearer than the earlier
  single-label version did.
- SQLite remains the better route for selective node lookup and for selective traversals
  over the `AUTHORED` and `TAGGED` edges.
- DuckDB only pulled ahead on the broad `KNOWS` expansion workload, which is exactly the
  sort of graph-analytic traversal where parallel scan capacity starts to matter.

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

- Recommended shared-budget form:

  ```bash
  HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search.py ...
  ```

- Vector-only fallback form:

  Use this only when `HUMEMDB_THREADS` is unset.

  ```bash
  LANCEDB_THREADS=8 python scripts/benchmarks/vector_search.py ...
  ```

- Default thread behavior:

  Use no thread-cap env var when you want each backend's default behavior.

  ```bash
  python scripts/benchmarks/vector_search.py ...
  ```

What it reports:

- Stage timings for:
  - initial SQLite insert into the canonical vector store
  - SQLite-to-NumPy load time
  - NumPy exact-index materialization
  - NumPy scalar-int8 materialization
  - LanceDB table creation
  - LanceDB index build
- Artifact sizes for the in-memory NumPy exact and scalar-int8 representations.
- Mean, stdev, min, and max per-query latency for:
  - NumPy float32 exact search
  - NumPy scalar-int8 search
  - LanceDB flat search
  - LanceDB indexed search with library-default settings
- Recall@k versus the NumPy float32 exact baseline for the approximate paths.

Current use:

- This script is the starting point for the current vector routing rule of thumb.
- The benchmark should be run across multiple collection sizes, dimensions, and `top_k`
  settings before HumemDB freezes the first public vector-routing policy.
- If LanceDB changes its default algorithm or hyperparameters in a later release, the
  indexed benchmark results will change with it by design.

High-recall example:

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/vector_search.py \
  --rows 10000 \
  --dimensions 384 \
  --queries 16 \
  --top-k 10 \
  --repetitions 2 \
  --lancedb-index-type IVF_FLAT \
  --lancedb-num-partitions 64 \
  --lancedb-nprobes 128
```

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
|   500,000 |  256 | `ivf_flat_probe256`     |  1.000 |              13.18 |         15.27 |          2948.89 |         126.18 |          7209.81 |          9101.06 |              3,055 | LanceDB indexed if collection is reused |
|   500,000 |  384 | `ivf_flat_probe256`     |  1.000 |              19.07 |         23.07 |          3424.51 |         175.05 |         10266.06 |         13700.80 |              2,656 | LanceDB indexed if collection is reused |
| 1,000,000 |  256 | `ivf_flat_probe256`     |  1.000 |              26.23 |         36.27 |          6379.64 |         292.40 |         14989.44 |         10818.55 |              1,096 | Strong LanceDB indexed case             |
| 1,000,000 |  384 | `ivf_flat_probe512`     |  1.000 |              37.77 |         41.22 |          7520.92 |         361.06 |         20651.90 |         15224.71 |              6,378 | LanceDB indexed if collection is reused |

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
- Use tuned `IVF_FLAT` for larger reused collections.
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
