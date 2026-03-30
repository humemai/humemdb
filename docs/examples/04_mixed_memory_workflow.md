# 04 - Mixed Memory Workflow

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/04_mixed_memory_workflow.py){ .md-button }

## What the Python example does

The script exercises HumemDB as one mixed workflow instead of treating SQL, graph,
and vectors as isolated demos.

- one shared SQLite plus DuckDB-backed database
- twelve relational tables with 143 columns across the schema and 151,056 rows total
- a multi-label graph with 105,832 nodes and 253,524 edges
- 128-dimensional embeddings used consistently across SQL rows, graph nodes, and direct vectors
- 100,000 direct vectors alongside SQL-owned and graph-owned vector data
- direct vectors for standalone playbook-style recall
- SQL-owned vector search over notes, incidents, and eval runs
- graph-owned vector search over both profiles and services
- ordinary SQL joins and multiple Cypher traversals over the same project context
- application-owned SQLite indexes for the recurring relational join and filter paths
- embedding updates on SQL rows plus graph nodes to show cache-safe reruns
- step-by-step timing printed from the script itself

## Why this example exists

The first three examples each isolate one public surface. This example shows the
product story after those basics: one application can keep relational work tracking,
release state, incident history, ownership links, service topology, evaluation runs,
and vector recall in one place without switching APIs or inventing parallel storage
layers.

## Main operations covered

- `CREATE TABLE` and batched `executemany(...)` for twelve relational tables
- `CREATE INDEX` on the application-owned SQL tables for the repeated relational paths
- graph `CREATE` and relationship creation through public Cypher across six labels
- direct vector inserts through `insert_vectors(...)`
- SQL `ORDER BY embedding <=> $query LIMIT k` on multiple large tables
- Cypher `CALL db.index.vector.queryNodes(...) YIELD node, score` on multiple node families
- exact direct-vector `search_vectors(...)`
- SQL `UPDATE` and Cypher `SET` that refresh vector-backed lookup state
- per-step elapsed timing output

The example also creates a small set of application-owned SQLite indexes for the
repeated relational join and filter paths. Those are realistic app-side hygiene,
but they are not the main reason the direct and graph-owned vector lookups stay fast.

## Representative flow

```python
with HumemDB("memory") as db:
    create_relational_tables(db)
    populate_relational_rows(db)
    populate_graph(db)
    populate_direct_vectors(db)

    note_matches = db.query(...)
    graph_matches = db.query(...)
    direct_matches = db.search_vectors(...)
```

## What this example demonstrates

- SQL rows, graph nodes, and direct vectors can coexist in one application database.
- Vector recall is ownership-aware: direct vectors use `search_vectors(...)`, while
  SQL-row and graph-node vector search stay inside `db.query(...)` text.
- The current public API is enough for one pragmatic "memory layer" workflow without
  requiring a separate graph server or ANN-only vector subsystem.
- A more realistic mixed workflow can stay within the admitted Cypher surface by
  composing a few single-edge traversals in Python instead of depending on multi-hop
  Cypher support.
- The same public surfaces still work when the synthetic dataset is large enough to
  look like a large staging environment instead of a toy demo.
