# test_vector.py

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/tests/test_vector.py){ .md-button }

## What this test file covers

`test_vector.py` is the focused vector contract suite.

It tests the lower-level vector helpers directly and also verifies that vector behavior
stays coherent when vectors are owned by standalone records, SQL rows, or Cypher nodes.

## Main areas covered

- float32 vector blob encode/decode round-trips
- exact cosine search ordering
- candidate filtering inside the exact vector index
- scalar-quantized index sanity checks
- loading SQLite-backed vector matrices into NumPy
- SQL-owned vector writes through `INSERT` and `UPDATE`
- Cypher-owned vector writes through `CREATE` and `MATCH ... SET`
- vector queries scoped by SQL and Cypher surfaces

## Why this test file exists

This file keeps the vector semantics tight without forcing every vector regression to be
discovered indirectly through the broader runtime suite.

It is the place to add tests when the vector contract changes.

## Representative themes

```python
with HumemDB("humem.sqlite3") as db:
    db.query(
        "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT NOT NULL, topic TEXT NOT NULL, embedding BLOB)",
        route="sqlite",
    )
    db.executemany(
        "INSERT INTO docs (id, title, topic, embedding) VALUES (?, ?, ?, ?)",
        [
            (1, "Alpha one", "alpha", [1.0, 0.0]),
            (2, "Alpha two", "alpha", [0.8, 0.2]),
        ],
        route="sqlite",
    )
```

## Current emphasis

- exact SQLite-plus-NumPy baseline behavior
- correctness before indexed ANN work
- one coherent vector story across direct, SQL, and Cypher usage