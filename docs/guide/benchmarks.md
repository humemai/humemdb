# Benchmarks

Benchmarks are part of the routing story, not an afterthought.

## Relational benchmark

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py --rows 50000
HUMEMDB_THREADS=8 python scripts/benchmarks/duckdb_direct_read.py \
    --rows 10000000 --warmup 1 --repetitions 5 --batch-size 50000
```

Current takeaway:

- SQLite stays stronger for point lookups and smaller filtered reads.
- DuckDB wins broader grouped scans and analytical aggregates.

## Graph benchmark

```bash
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py --nodes 5000 --fanout 3
HUMEMDB_THREADS=8 python scripts/benchmarks/cypher_graph_path.py \
    --nodes 1000000 --fanout 4 --tag-fanout 2 --warmup 1 --repetitions 5 --batch-size 20000
```

Current takeaway:

- SQLite is very strong for selective graph traversal.
- DuckDB becomes compelling only once the read broadens into graph-analytic shapes.

## Vector benchmark

The vector benchmark scripts measure exact NumPy search and quantized variants so later
routing choices can be based on observed crossover points instead of guesswork.