# Translation Overhead Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/translation_overhead.py){ .md-button }

Purpose:

- measure frontend translation overhead separately from backend execution
- isolate cached versus uncached PostgreSQL-like SQL translation through `sqlglot`
- isolate `HumemCypher v0` parse and bind+compile cost through the current compiler

Representative command:

```bash
python scripts/benchmarks/translation_overhead.py --warmup 200 --repetitions 3000
```

Observed means on the current development machine:

## SQL translation

| Workload | Complexity | SQLite cached | SQLite uncached | DuckDB cached | DuckDB uncached | Takeaway |
| -------- | ---------- | ------------: | --------------: | ------------: | --------------: | -------- |
| `literal_projection` | simple | 0.0002 ms | 0.0876 ms | 0.0002 ms | 0.0769 ms | Tiny literal and cast rewrites stay well under 0.1 ms uncached. |
| `point_lookup` | simple | 0.0002 ms | 0.1385 ms | 0.0002 ms | 0.1284 ms | A selective lookup with `ILIKE`, cast, `ORDER BY`, and `LIMIT` still stays around 0.13 ms uncached. |
| `filtered_aggregate` | simple | 0.0002 ms | 0.1549 ms | 0.0002 ms | 0.1582 ms | Small aggregation adds a bit of cost but still stays around 0.15 ms uncached. |
| `join_aggregate` | medium | 0.0002 ms | 0.1945 ms | 0.0002 ms | 0.1954 ms | A straightforward join-and-group shape stays around 0.2 ms uncached. |
| `windowed_filter` | medium | 0.0002 ms | 0.1510 ms | 0.0002 ms | 0.1536 ms | A date-range filter plus ordering behaves similarly to other medium OLTP-shaped queries. |
| `case_and_exists` | medium | 0.0002 ms | 0.2264 ms | 0.0002 ms | 0.2234 ms | Correlated `EXISTS` plus `CASE` raises frontend cost, but still stays near 0.22 ms on average. |
| `union_rollup` | medium | 0.0002 ms | 0.1872 ms | 0.0002 ms | 0.1865 ms | A subquery-wrapped `UNION ALL` remains below 0.2 ms uncached. |
| `cte_multi_join` | complex | 0.0002 ms | 0.5092 ms | 0.0002 ms | 0.5256 ms | The heaviest current SQL shape, with multiple CTEs and joins, still stays around half a millisecond uncached. |
| `windowed_rank_cte` | complex | 0.0002 ms | 0.2436 ms | 0.0002 ms | 0.2416 ms | Window functions plus a CTE land in the mid-0.2 ms range uncached. |

## Cypher translation

| Workload | Complexity | Parse mean | Bind+compile mean | Takeaway |
| -------- | ---------- | ---------: | ----------------: | -------- |
| `node_anchor` | simple | 0.0112 ms | 0.0063 ms | Anchored node matches compile very cheaply. |
| `node_lookup` | simple | 0.0170 ms | 0.0073 ms | Adding `WHERE`, `ORDER BY`, and `LIMIT` only increases frontend cost slightly. |
| `relationship_expand` | medium | 0.0276 ms | 0.0114 ms | Basic edge expansion remains in the low-hundredths-of-a-millisecond range. |
| `relationship_reverse` | medium | 0.0193 ms | 0.0095 ms | Reverse edge matching is also very cheap in the current compiler. |
| `relationship_property_anchor` | complex | 0.0250 ms | 0.0126 ms | Extra property anchors on both sides increase compile work modestly. |
| `relationship_dense_return` | complex | 0.0349 ms | 0.0167 ms | The densest current return shape is still only a few hundredths of a millisecond. |

Current interpretation:

- cached SQL translation is effectively free at this scale
- uncached SQL translation grows with query shape complexity, but the current `HumemSQL v0`
  workload mix still stays sub-millisecond
- current `HumemCypher v0` parse and compile costs remain much smaller than uncached SQL
  translation cost
- these are machine-specific measurements from one longer run, so exact values will move
  somewhat across hardware and runtime conditions even if the pattern holds

Important note:

- `HumemCypher v0` compilation is route-agnostic today, so the compiled SQL shape is the
  same before it is sent to SQLite or DuckDB
