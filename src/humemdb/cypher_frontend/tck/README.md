# TCK Notes

This directory is reserved for mapping HumemDB's admitted Cypher subset against the
openCypher TCK.

Initial adoption should be incremental:

1. `clauses/create`
2. `clauses/match`
3. `clauses/match-where`

Scenarios outside the admitted subset should continue to fail clearly rather than
being treated as implied support.

See `subset-map.md` for the current HumemDB-specific mapping of covered, boundary,
and out-of-subset scenario families.
