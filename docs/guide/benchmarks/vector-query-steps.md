# Vector Query Steps Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/vector_query_steps.py){ .md-button }

Purpose:

- break one exact-vector workload into step timings instead of only comparing complete
  end-to-end paths
- measure ingest cost for direct vectors, SQL-owned vectors, and Cypher-owned vectors
- measure frontend overhead for SQL translation and Cypher parse/bind+compile
- measure candidate-query execution, candidate-id mapping, pure NumPy vector search, and
  end-to-end candidate-filtered vector query latency

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

- these are intermediate measurements, not a final routing policy
- the candidate-filtered path is still expected to improve with later optimization work
- Cypher ingest is currently transactional but still statement-oriented rather than a
  true batched bulk-ingest path

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

What this benchmark is useful for:

- understanding where time is actually going inside the current exact vector path
- checking whether parser/compiler cleanup is likely to matter to latency
- comparing direct, SQL-owned, and Cypher-owned vector flows without collapsing them
  into one blended number

What this benchmark is not:

- it is not the best tool for deciding ANN crossover thresholds
- it is not a substitute for the larger exact-versus-LanceDB benchmark suite
- it should be revisited after future optimization work lands