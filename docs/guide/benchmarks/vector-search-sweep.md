# Vector Search Sweep Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_search_sweep.py){ .md-button }

Purpose:

- sweep the vector benchmark across multiple row counts, dimensions, and `top_k` values
- capture setup costs, steady-state query latency, and recall together
- estimate break-even reuse points for LanceDB indexed versus NumPy exact

Representative command:

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

Key result artifacts:

| Artifact | Grid | High-level finding |
| -------- | ---- | ------------------ |
| `results/vector_search_sweep_tuned_threads4_queries100_rows2k-100k_dims256-1024_topk10.json` | `2k` to `100k`, dims `256,384,768,1024` | Early tuned reference sweep. NumPy exact won `15 / 16`; LanceDB won only `100k x 256`, with break-even about `61,551` queries. |
| `results/ivfpq_100k_384.json` | `100k x 384` | `IVF_PQ` failed badly for high recall. Best tested recall was only `0.482`. |
| `results/ivfhnswsq_crossover_100k_1m_dims256_384.json` | `100k` to `1M`, dims `256,384` | `IVF_HNSW_SQ` met the `0.95` recall bar in only `2 / 8` scenarios and had zero acceptable latency wins. |
| `results/ivfflat_crossover_100k_1m_dims256_384.json` | `100k` to `1M`, dims `256,384` | `IVF_FLAT` met the `0.95` recall bar in `8 / 8` scenarios and produced the first real high-recall crossover. |
| `results/ivfflat_boundary_150k_400k_dims256_384.json` | `150k` to `400k`, dims `256,384` | Refined the `IVF_FLAT` boundary: crossover starts around `300k` for `384` dims and around `400k` for `256` dims. |
| `results/ivfflat_crossover_100k_1m_dims768_1024.json` | `100k` to `1M`, dims `768,1024` | Higher-dimension validation. `IVF_FLAT` met the recall bar in `8 / 8` scenarios and won in `5 / 8`. |

`IVF_FLAT` crossover decision table:

| Rows | Dims | Tuned LanceDB candidate | Recall | LanceDB indexed ms | NumPy FP32 ms | SQLite to NumPy ms | NumPy build ms | LanceDB table ms | LanceDB index ms | Break-even queries | Verdict |
| ---: | ---: | ----------------------- | -----: | -----------------: | ------------: | -----------------: | -------------: | ---------------: | ---------------: | -----------------: | ------- |
| 100,000 | 256 | `ivf_flat_probe256` | 1.000 | 3.67 | 2.52 | 505.76 | 26.29 | 1396.26 | 7634.81 | — | NumPy exact |
| 100,000 | 384 | `ivf_flat_probe256` | 1.000 | 4.70 | 3.56 | 570.25 | 34.85 | 2075.78 | 12065.13 | — | NumPy exact |
| 250,000 | 256 | `ivf_flat_probe256` | 1.000 | 6.98 | 7.03 | 1250.24 | 63.39 | 3548.75 | 8256.99 | 107,144 | LanceDB only for very high reuse |
| 250,000 | 384 | `ivf_flat_probe256` | 1.000 | 10.27 | 10.04 | 1516.16 | 84.96 | 5163.05 | 12589.26 | — | NumPy exact |
| 500,000 | 256 | `ivf_flat_probe256` | 1.000 | 13.18 | 15.27 | 2948.89 | 126.18 | 7209.81 | 9101.06 | 3,055 | LanceDB indexed if collection is reused |
| 500,000 | 384 | `ivf_flat_probe256` | 1.000 | 19.07 | 23.07 | 3424.51 | 175.05 | 10266.06 | 13700.80 | 2,656 | LanceDB indexed if collection is reused |
| 1,000,000 | 256 | `ivf_flat_probe256` | 1.000 | 26.23 | 36.27 | 6379.64 | 292.40 | 14989.44 | 10818.55 | 1,096 | Strong LanceDB indexed case |
| 1,000,000 | 384 | `ivf_flat_probe512` | 1.000 | 37.77 | 41.22 | 7520.92 | 361.06 | 20651.90 | 15224.71 | 6,378 | LanceDB indexed if collection is reused |

Current routing threshold:

| Dims | Use NumPy exact through | Start considering tuned `IVF_FLAT` at | Notes |
| ---: | ----------------------: | ------------------------------------: | ----- |
| 256 | about `300k` rows | about `400k` rows | `300k` still loses to NumPy exact; `400k` wins with break-even about `6,481` queries. |
| 384 | about `200k` rows | about `300k` rows | `300k` wins with break-even about `3,642` queries. |
| 768 | about `100k` rows | about `250k` rows | `250k` wins with break-even about `9,421` queries. |
| 1024 | about `250k` rows | about `500k` rows | `250k` still loses; `500k` wins with break-even about `16,185` queries. |

Current interpretation:

- prefer NumPy exact as the baseline below the crossover region
- use tuned `IVF_FLAT` for larger reused collections
- treat NumPy SQ8 as a memory tradeoff, not a speed path
