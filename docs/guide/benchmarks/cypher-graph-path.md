# Cypher Graph-Path Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/cypher_graph_path.py){ .md-button }

Purpose:

- measure graph initial load time, Cypher parse and compile overhead, and raw SQL versus
  end-to-end Cypher execution on SQLite and DuckDB
- cover multiple node labels and edge types instead of a single graph shape
- recheck graph-path behavior after changes to graph indexes or Cypher SQL compilation

Representative command:

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
- 3,050,000 total edges
- approximately 10,950,000 total rows across graph tables and graph property tables
- 1 warmup iteration and 5 timed repetitions per stage
- initial load time: 33052.16 ms

Observed means:

| Workload | SQLite raw SQL | DuckDB raw SQL | SQLite Cypher | DuckDB Cypher | Takeaway |
| -------- | -------------: | -------------: | ------------: | ------------: | -------- |
| `user_lookup` | 0.02 ms | 1157.15 ms | 0.05 ms | 1167.23 ms | SQLite is overwhelmingly better for anchored user-node lookup. |
| `document_lookup` | 0.02 ms | 1179.71 ms | 0.07 ms | 1166.71 ms | SQLite is also overwhelmingly better for selective document lookup. |
| `topic_lookup` | 0.02 ms | 939.52 ms | 0.07 ms | 941.15 ms | Selective topic lookup strongly favors SQLite. |
| `social_expand` | 1648.92 ms | 1221.28 ms | 1620.89 ms | 1220.32 ms | DuckDB wins once traversal broadens into the high-fanout social edge set. |
| `author_expand` | 492.37 ms | 1160.95 ms | 500.58 ms | 1163.97 ms | A selective author-to-document expansion still favors SQLite. |
| `tagged_expand` | 100.08 ms | 1137.34 ms | 97.61 ms | 1125.02 ms | A selective document-to-topic expansion also still favors SQLite. |

Compiler overhead:

- Cypher parse cost stayed around 0.02 to 0.03 ms
- Cypher bind+compile cost stayed around 0.03 to 0.04 ms
- end-to-end Cypher timings tracked raw SQL closely, which confirms that execution plan
  shape and backend behavior dominate total latency

Current interpretation:

- SQLite remains the better route for selective node lookup and for selective traversals
  over the `AUTHORED` and `TAGGED` edges
- DuckDB only pulled ahead on the broad `KNOWS` expansion workload, which is exactly the
  sort of graph-analytic traversal where parallel scan capacity starts to matter
