# 05 - CSV Ingest

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/05_csv_ingest.py){ .md-button }

## What the Python example does

The script generates representative CSV fixtures and then imports them through the
public ingestion family instead of manually parsing files into `executemany(...)` or
repeated Cypher writes.

- 20,000 relational `accounts`
- 80,000 relational `account_events`
- 10,000 graph `Service` nodes
- 20,000 `DEPENDS_ON` graph edges
- CSV-backed relational ingest through `import_table(...)`
- CSV-backed graph ingest through `import_nodes(...)` and `import_edges(...)`
- chunked import execution with per-step timing
- post-ingest SQL and Cypher reads over the imported state

## Why this example exists

Phase 12 added a first public ingestion family because real data loading should not
force users to choose between internal engine handles, hand-written CSV loops, or
statement-by-statement Cypher writes. This example shows the intended public path:
generate or receive CSV files, import them into the canonical SQLite-backed store, and
query the fresh relational and graph state immediately.

## Main operations covered

- writing CSV fixture files with Python's standard `csv` module
- `CREATE TABLE` for the relational target schema
- `import_table(...)` for relational row ingest
- `import_nodes(...)` with explicit `id_column` and typed graph properties
- `import_edges(...)` with endpoint columns and typed edge properties
- SQL post-ingest grouped reads over imported relational rows
- Cypher post-ingest traversals over imported graph rows
- per-step elapsed timing output

Schema note:

- relational imports still require the destination table schema to be defined first
- graph imports do not require manual creation of the internal graph storage tables;
  `import_nodes(...)` and `import_edges(...)` ensure that internal graph schema
  themselves before writing rows

## Representative flow

```python
with HumemDB.open("ingest") as db:
    db.query("CREATE TABLE accounts (...)")
    db.query("CREATE TABLE account_events (...)")

    db.import_table("accounts", accounts_csv)
    db.import_table("account_events", events_csv)
    db.import_nodes("Service", services_csv, id_column="id")
    db.import_edges(
        "DEPENDS_ON",
        dependencies_csv,
        source_id_column="source_id",
        target_id_column="target_id",
    )

    relational_rows = db.query(...)
    graph_rows = db.query(...)
```

## What this example demonstrates

- the public ingestion family is data-model-first: table rows, graph nodes, and graph
  edges each have an explicit import path
- relational and graph imports stay inside the ordinary `HumemDB` surface rather than
  requiring backend-engine access
- relational import remains schema-first, while graph import owns its internal graph
  storage setup automatically
- imported graph properties can be typed explicitly for integer and boolean coercion
- imported data is queryable immediately through the same SQL and Cypher surfaces used
  elsewhere in the library
