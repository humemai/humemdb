# 02 - Cypher Social Graph

[View source code]({{ config.repo_url }}/blob/{{ config.extra.version_tag }}/examples/02_cypher_social_graph.py){ .md-button }

## What the Python example does

The script builds a generated collaboration graph rather than a tiny three-node toy.

- 30,000 `User` nodes
- 20,000 `Document` nodes
- 64 `Team` nodes
- 256 `Topic` nodes
- more than 50,000 total nodes
- more than 100,000 total edges across `KNOWS`, `MENTORS`, `MEMBER_OF`, `AUTHORED`, and `TAGGED`
- richer node properties including string, numeric, boolean, and nullable fields
- relationship mutation and deletion flows after the initial load
- step-by-step timing printed from the script itself

## Main operations covered

- repeated `CREATE` graph writes on the SQLite route
- named parameters in Cypher `CREATE`
- `MATCH ... SET` for node and relationship updates
- narrow `MATCH ... DELETE` for one relationship alias
- narrow `MATCH ... DETACH DELETE` for one node alias
- relationship-type alternation with `[:KNOWS|MENTORS]`
- string predicates such as `STARTS WITH`, `ENDS WITH`, and `CONTAINS`
- null predicates such as `IS NULL` and `IS NOT NULL`
- `DISTINCT`, `OFFSET`, `ORDER BY`, and `LIMIT`
- per-step elapsed timing output

## Representative flow

```python
db.query(
    (
        "MATCH (u:User {name: $user_name}), (t:Team {slug: $team_slug}) "
        "CREATE (u)-[:MEMBER_OF {since: $since, role: $role}]->(t)"
    ),
    params={...},
)
```

## Supported today

- labeled node creation
- multiple directed relationship creation flows
- narrow `MATCH` flows
- relationship aliases
- reverse-edge matching
- `WHERE` equality predicates joined by `AND`
- string and null predicates in `WHERE`
- `MATCH ... SET`
- narrow `MATCH ... DELETE` and `MATCH ... DETACH DELETE`
- `DISTINCT`, `OFFSET`, `ORDER BY`, and `LIMIT`
- named parameters such as `$name`

## Not promised yet

HumemDB does not claim broad Cypher compatibility today. The current surface is the
tested subset described above, and unsupported constructs should fail clearly instead of
being guessed at.
