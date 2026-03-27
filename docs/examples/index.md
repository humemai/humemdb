# Examples

HumemDB ships real Python examples in the repository `examples/` directory,
and the docs pages in this section describe those exact files.

Like the ArcadeDB docs, each page is intended to be a companion to the real Python
script, not a disconnected hand-written snippet.

Current shipped examples:

- [01 - SQL Basics](01_sql_basics.md): builds a four-table commerce-style schema with more than 200,000 relational rows and timed SQL steps.
- [02 - Cypher Social Graph](02_cypher_social_graph.md): builds a multi-label collaboration graph with more than 50,000 nodes, more than 100,000 edges, and timed Cypher steps.
- [03 - Vector Search](03_vector_search.md): combines more than 60,000 direct vectors with SQL-owned vectors and graph-owned vectors, with timed vector workflow steps.
- [04 - Mixed Memory Workflow](04_mixed_memory_workflow.md): combines 12 relational tables, 151,056 SQL rows, a 105,832-node and 253,524-edge graph, 100,000 direct vectors, and SQL-owned plus graph-owned vector recall in one shared application workflow.
- [05 - CSV Ingest](05_csv_ingest.md): generates CSV fixtures, imports relational rows plus graph nodes and edges through the public ingestion family, and runs representative SQL and Cypher reads over the imported state.
- [Download Data](download_data.md): downloads or generates the pinned benchmark and example datasets used by the larger repository workflows.

Run them locally from the repository root:

```bash
python scripts/release/run_examples.py
```

Or run individual files directly:

```bash
python examples/01_sql_basics.py
python examples/02_cypher_social_graph.py
python examples/03_vector_search.py
python examples/04_mixed_memory_workflow.py
python examples/05_csv_ingest.py
python examples/download_data.py movielens-small
```
