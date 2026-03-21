# Relational Direct-Read Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/duckdb_direct_read.py){ .md-button }

Purpose:

- compare SQLite and DuckDB across a broader relational workload mix over the SQLite
  source of truth
- cover OLTP-style event reads, analytical aggregates, document-tag joins, and
  memory-style rollups

Representative command:

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
- initial load time: 22266.28 ms

Observed means:

| Query shape | SQLite mean | DuckDB mean | Takeaway |
| ----------- | ----------: | ----------: | -------- |
| `event_point_lookup` | 0.01 ms | 973.49 ms | SQLite is vastly better for indexed point reads against the canonical store. |
| `event_filtered_range` | 8.37 ms | 439.16 ms | SQLite stays much better for selective OLTP-style filters. |
| `event_aggregate_topk` | 4646.88 ms | 523.44 ms | DuckDB is about 8.9x faster on broad scan-and-group workloads. |
| `event_region_join` | 4668.97 ms | 528.00 ms | DuckDB is about 8.8x faster on analytical join aggregation. |
| `document_tag_rollup` | 1.29 ms | 107.13 ms | A selective indexed document join still strongly favors SQLite. |
| `memory_hot_rollup` | 311.48 ms | 60.78 ms | DuckDB is about 5.1x faster on broader grouped rollups over the memory-style table. |

Current interpretation:

- SQLite remains the clear default for point lookups, selective filters, and selective
  indexed joins, even when the overall dataset is large
- DuckDB wins once the workload becomes genuinely analytical: broad scans, grouping, and
  large aggregation over many rows
- the richer SQL suite makes an important point that the older benchmark could not: not
  every join is analytical, and join shape plus selectivity matter more than the mere
  presence of joins
