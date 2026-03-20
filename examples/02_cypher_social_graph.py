from __future__ import annotations

import tempfile
from pathlib import Path

from humemdb import HumemDB


PAIR_COUNT = 120


def populate_graph(db: HumemDB) -> None:
    for index in range(PAIR_COUNT):
        cohort = f"batch-{index % 6}"
        city = ("Berlin", "Paris", "Lisbon", "Seoul")[index % 4]
        db.query(
            (
                "CREATE (a:User {name: $a_name, age: $a_age, active: $a_active, cohort: $cohort, city: $city})"
                "-[r:KNOWS {since: $since_one, strength: $strength_one}]->"
                "(b:User {name: $b_name, age: $b_age, active: $b_active, cohort: $cohort, city: $city})"
            ),
            route="sqlite",
            query_type="cypher",
            params={
                "a_name": f"Analyst {index:03d}",
                "a_age": 26 + (index % 9),
                "a_active": True,
                "b_name": f"Lead {index:03d}",
                "b_age": 31 + (index % 11),
                "b_active": index % 5 != 0,
                "cohort": cohort,
                "city": city,
                "since_one": 2019 + (index % 5),
                "strength_one": 5 + (index % 5),
            },
        )
        db.query(
            (
                "CREATE (b:User {name: $b_name, age: $b_age, active: $b_active, cohort: $cohort, city: $city})"
                "-[r:KNOWS {since: $since_two, strength: $strength_two}]->"
                "(c:User {name: $c_name, age: $c_age, active: $c_active, cohort: $cohort, city: $city})"
            ),
            route="sqlite",
            query_type="cypher",
            params={
                "b_name": f"Lead {index:03d}",
                "b_age": 31 + (index % 11),
                "b_active": index % 5 != 0,
                "c_name": f"Mentor {index:03d}",
                "c_age": 38 + (index % 7),
                "c_active": True,
                "cohort": cohort,
                "city": city,
                "since_two": 2021 + (index % 3),
                "strength_two": 6 + (index % 4),
            },
        )


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        sqlite_path = root / "graph.sqlite3"
        duckdb_path = root / "graph.duckdb"

        with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
            populate_graph(db)

            sqlite_result = db.query(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "WHERE r.type = 'KNOWS' AND a.active = true AND a.cohort = 'batch-0' "
                    "RETURN a.name, r.since, r.strength, b.name "
                    "ORDER BY r.since DESC, a.name LIMIT 5"
                ),
                route="sqlite",
                query_type="cypher",
            )
            duckdb_result = db.query(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "WHERE r.type = 'KNOWS' AND a.active = true AND a.cohort = 'batch-0' "
                    "RETURN a.name, r.since, r.strength, b.name "
                    "ORDER BY r.since DESC, a.name LIMIT 5"
                ),
                route="duckdb",
                query_type="cypher",
            )
            reverse_result = db.query(
                (
                    "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                    "WHERE b.name = 'Lead 000' AND r.since = 2019 "
                    "RETURN a.name, b.name, r.since"
                ),
                route="sqlite",
                query_type="cypher",
            )

        expected_rows = (
            ("Analyst 024", 2023, 9, "Lead 024"),
            ("Analyst 054", 2023, 9, "Lead 054"),
            ("Analyst 084", 2023, 9, "Lead 084"),
            ("Analyst 114", 2023, 9, "Lead 114"),
            ("Lead 006", 2021, 8, "Mentor 006"),
        )
        assert sqlite_result.rows == expected_rows
        assert duckdb_result.rows == expected_rows
        assert reverse_result.rows == (("Analyst 000", "Lead 000", 2019),)

        print("Cypher relationship rows:", sqlite_result.rows)
        print("Cypher reverse edge rows:", reverse_result.rows)


if __name__ == "__main__":
    main()
