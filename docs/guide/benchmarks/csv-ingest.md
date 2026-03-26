# CSV Ingest Benchmark

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/scripts/benchmarks/csv_ingest.py){ .md-button }

Purpose:

- measure the first Phase 12 ingestion family built on top of the canonical SQLite
  write path
- compare the public CSV import APIs against the realistic manual alternatives the
  team would otherwise use today
- keep a small post-ingest freshness query for each path so the benchmark validates
  that every comparison method produced the expected relational or graph rows

Representative command:

```bash
python scripts/benchmarks/csv_ingest.py \
  --table-rows 50000 \
  --node-rows 20000 \
  --edge-fanout 2 \
  --chunk-size 1000 \
  --warmup 1 \
  --repetitions 5
```

What it reports:

- relational ingest timings for:
  - `import_table(...)`
  - `staging_normalize`, which loads CSV rows into a permissive staging table first
    and then normalizes them into the final relational table with one set-based SQLite
    insert
  - manual CSV parsing plus public `db.executemany(...)`
  - manual CSV parsing plus internal SQLite `executemany(...)` as a lower bound
- graph node ingest timings for:
  - `import_nodes(...)`
  - repeated public Cypher `db.query(...)` writes inside one transaction
  - internal SQLite graph-table batch writes as a lower bound
- graph edge ingest timings for:
  - `import_edges(...)`
  - repeated public Cypher `db.query(...)` writes inside one transaction
  - internal SQLite graph-table batch writes as a lower bound
- post-ingest count-query timing summaries for each comparison path

Current design note on `staging_normalize`:

- `staging_normalize` is intentionally table-first today
- it exists to measure one realistic Phase 12 follow-on path where raw CSV lands in a
  permissive staging table and then moves into the final relational schema through one
  set-based SQLite normalization step
- graph ingest is benchmarked directly through `import_nodes(...)` and
  `import_edges(...)` instead of through a graph-specific staging flow
- graph staging should only be added later if a real graph-derivation workload
  justifies it, such as deriving nodes and edges from staged relational exports or
  resolving graph endpoints from business keys before materialization

Method-selection flags:

- `--table-methods import_table,staging_normalize,public_executemany,internal_sqlite`
- `--graph-methods import_api,public_cypher_query,internal_sqlite`
- for larger graph sweeps, a practical pattern is
  `--graph-methods import_api,internal_sqlite`

Current sweep snapshot:

- sweep date: `2026-03-26`
- `warmup=0`, `repetitions=1`
- relational staged-comparison runs used `node_rows=100` and `edge_fanout=1` so the
  script stayed focused on table ingest while still exercising the shared benchmark
  harness
- the relational table snapshot below now includes `10k`, `100k`, `1M`, and `10M`
  staged-comparison measurements
- the graph node and edge snapshots below remain the earlier post-optimization graph
  comparison runs, which are still the representative evidence for graph ingest

Relational table ingest means:

| Rows | `import_table(...)` | `staging_normalize` | `public_executemany` | `internal_sqlite` | Takeaway |
| ---: | ------------------: | ------------------: | -------------------: | ----------------: | -------- |
| `10k` | `55.94 ms` | `124.55 ms` | `52.12 ms` | `44.28 ms` | At this small scale, `staging_normalize` is clearly a convenience path rather than a fast path: it was about `2.23x` slower than `import_table(...)` and about `2.39x` slower than public `executemany(...)`. |
| `100k` | `256.68 ms` | `361.62 ms` | `283.00 ms` | `222.01 ms` | By `100k`, direct `import_table(...)` is still the right default; staged normalize remained about `1.41x` slower than `import_table(...)` and about `1.28x` slower than public `executemany(...)`. |
| `1M` | `1996.66 ms` | `2514.54 ms` | `2518.13 ms` | `1843.78 ms` | At `1M`, staged normalize roughly matched the realistic public baseline but still trailed `import_table(...)` by about `1.26x`, which means the staged flow looks operationally reasonable when normalization is needed but not like the new default fast path. |
| `10M` | `19737.82 ms` | `24939.36 ms` | `24203.80 ms` | `18456.09 ms` | The same pattern held at `10M`: `import_table(...)` remained the fastest public path, while `staging_normalize` stayed workable for schema-cleanup workflows but ended up slightly slower than public `executemany(...)` and about `1.35x` off the internal lower bound. |

Graph node ingest means:

| Node rows | `import_api` | `public_cypher_query` | `internal_sqlite` | Takeaway |
| --------: | -----------: | --------------------: | ----------------: | -------- |
| `10k` | `467.35 ms` | `403.24 ms` | `440.08 ms` | At this smaller scale, repeated public Cypher writes were still competitive enough to benchmark directly, while `import_nodes(...)` stayed in the same rough range. |
| `100k` | `1642.19 ms` | `3508.84 ms` | `1740.47 ms` | `import_nodes(...)` stayed effectively at the internal lower bound, while public Cypher writes were about `2.14x` slower. |
| `1M` | `15092.71 ms` | `34613.55 ms` | `14370.08 ms` | `import_nodes(...)` remained close to the internal lower bound at larger scale, about `1.05x`, while public Cypher writes were about `2.41x` slower. |

Graph edge ingest means:

| Edge rows | `import_api` | `public_cypher_query` | `internal_sqlite` | Takeaway |
| --------: | -----------: | --------------------: | ----------------: | -------- |
| `20k` | `264.64 ms` | `1543.90 ms` | `244.70 ms` | Repeated public Cypher edge creation was already about `6.31x` slower than the internal lower bound, while `import_edges(...)` stayed close to it. |
| `200k` | `2401.76 ms` | `15482.01 ms` | `2233.34 ms` | `import_edges(...)` stayed within about `1.08x` of the internal lower bound, while public Cypher edge writes were about `6.93x` slower. |
| `2M` | `25242.79 ms` | `149294.07 ms` | `23834.69 ms` | The larger edge run still kept `import_edges(...)` close to internal SQLite, about `1.06x`, while public Cypher edge writes were about `6.26x` slower. |

Current interpretation:

- relational CSV ingest is now in the right place: `import_table(...)` remains the
  fastest public path and stays close to the internal SQLite lower bound from `100k`
  through `10M`
- `staging_normalize` is now benchmarked too, and the measured result is useful but
  clear: it is a workflow option for permissive load plus SQL normalization, not the
  new default performance path
- graph CSV ingest continues to look directionally right: `import_nodes(...)` and
  `import_edges(...)` stay close to the internal lower bound at `100k` and `1M`
- repeated public Cypher graph writes remain a valid baseline, but they scale much
  worse, especially for edges where they are still more than `6x` slower than the
  internal lower bound at `100k` and `1M`
- the completed direct-import plus staged-relational runs are strong enough to treat
  the first public ingestion family as benchmark-validated for this phase
