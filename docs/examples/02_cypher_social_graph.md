# 02 - Cypher Social Graph

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/02_cypher_social_graph.py){ .md-button }

## What the Python example does

The script builds a generated social/work graph rather than a tiny three-node toy.

- 5,000 repeated graph patterns
- thousands of nodes
- thousands of directed `KNOWS` edges
- richer node properties: `name`, `age`, `active`, `cohort`, `city`
- richer relationship properties: `since`, `strength`, and implicit `type`
- SQLite and DuckDB graph reads over the same stored graph state

## Main operations covered

- repeated `CREATE` graph writes on the SQLite route
- named parameters in Cypher `CREATE`
- relationship alias returns
- reverse-edge matching
- `WHERE ... AND ...` filters
- `ORDER BY` and `LIMIT`

## Representative flow

```python
db.query(
    (
        "CREATE (a:User {name: $a_name, age: $a_age, active: $a_active, cohort: $cohort, city: $city})"
        "-[r:KNOWS {since: $since_one, strength: $strength_one}]->"
        "(b:User {name: $b_name, age: $b_age, active: $b_active, cohort: $cohort, city: $city})"
    ),
    params={...},
)
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
