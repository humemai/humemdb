# Cypher Example

`HumemCypher v0` is a graph-specific frontend lowered into SQLite-backed graph tables.

```python
from humemdb import HumemDB

with HumemDB("graph.sqlite3", "graph.duckdb") as db:
    db.query(
        (
            "CREATE (a:User {name: 'Alice'})"
            "-[r:KNOWS {since: 2020}]->"
            "(b:User {name: 'Bob'})"
        ),
        route="sqlite",
        query_type="cypher",
    )

    result = db.query(
        (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.since = 2020 "
            "RETURN a.name, r.type, b.name"
        ),
        route="sqlite",
        query_type="cypher",
    )

    print(result.columns)
    print(result.rows)
```

## Supported today

- labeled node creation
- single directed relationship creation
- narrow `MATCH` flows
- relationship aliases
- reverse-edge matching
- `WHERE` equality predicates joined by `AND`
- `ORDER BY` and `LIMIT`
- named parameters such as `$name`

## Not promised yet

HumemDB does not claim broad Cypher compatibility today. The current surface is the
tested subset described above, and unsupported constructs should fail clearly instead of
being guessed at.