# Vector Example

`HumemVector v0` is the exact baseline path: vectors live in SQLite and search runs as
exact NumPy over cached collection matrices.

```python
from humemdb import HumemDB

with HumemDB("vectors.sqlite3") as db:
    db.insert_vectors(
        [
            (101, "docs", 1, [1.0, 0.0, 0.0]),
            (102, "docs", 1, [0.8, 0.2, 0.0]),
            (103, "docs", 2, [0.0, 1.0, 0.0]),
        ]
    )

    result = db.search_vectors(
        "docs",
        [1.0, 0.0, 0.0],
        top_k=2,
        metric="cosine",
        bucket=1,
    )

    print(result.columns)
    print(result.rows)
```

## What this means in practice

- SQLite is the canonical vector store.
- The public path is exact, not ANN.
- Bucket filters can narrow the candidate set before scoring.
- Collection caches are invalidated automatically after SQL writes that touch vector
  storage.

## Metrics

- `cosine`
- `dot`
- `l2`