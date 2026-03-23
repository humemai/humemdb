from __future__ import annotations

import tempfile
from pathlib import Path

from humemdb import HumemDB


PAIR_COUNT = 5_000


def populate_graph(db: HumemDB) -> None:
    for index in range(PAIR_COUNT):
        cohort = f"batch-{index % 6}"
        city = ("Berlin", "Paris", "Lisbon", "Seoul")[index % 4]
        db.query(
            (
                "CREATE ("
                "a:User {"
                "name: $a_name, "
                "age: $a_age, "
                "active: $a_active, "
                "cohort: $cohort, "
                "city: $city"
                "}"
                ")"
                "-[r:KNOWS {since: $since_one, strength: $strength_one}]->"
                "("
                "b:User {"
                "name: $b_name, "
                "age: $b_age, "
                "active: $b_active, "
                "cohort: $cohort, "
                "city: $city"
                "}"
                ")"
            ),
            params={
                "a_name": f"Analyst {index:04d}",
                "a_age": 26 + (index % 9),
                "a_active": True,
                "b_name": f"Lead {index:04d}",
                "b_age": 31 + (index % 11),
                "b_active": index % 5 != 0,
                "cohort": cohort,
                "city": city,
                "since_one": 2019 + (index % 5),
                "strength_one": 5 + (index % 5),
            },
        )


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        sqlite_path = root / "graph.sqlite3"
        duckdb_path = root / "graph.duckdb"

        with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
            with db.transaction():
                populate_graph(db)

            sqlite_result = db.query(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "WHERE r.type = 'KNOWS' AND a.active = true "
                    "AND a.cohort = 'batch-0' "
                    "RETURN a.name, r.since, r.strength, b.name "
                    "ORDER BY r.since DESC, a.name LIMIT 5"
                )
            )
            duckdb_result = db.query(
                (
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "WHERE r.type = 'KNOWS' AND a.active = true "
                    "AND a.cohort = 'batch-0' "
                    "RETURN a.name, r.since, r.strength, b.name "
                    "ORDER BY r.since DESC, a.name LIMIT 5"
                ),
                route="duckdb",
            )
            reverse_result = db.query(
                (
                    "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                    "WHERE b.name = 'Lead 0000' AND r.since = 2019 "
                    "RETURN a.name, b.name, r.since"
                )
            )

        expected_rows = (
            ("Analyst 0024", 2023, 9, "Lead 0024"),
            ("Analyst 0054", 2023, 9, "Lead 0054"),
            ("Analyst 0084", 2023, 9, "Lead 0084"),
            ("Analyst 0114", 2023, 9, "Lead 0114"),
            ("Analyst 0144", 2023, 9, "Lead 0144"),
        )
        assert sqlite_result.rows == expected_rows
        assert duckdb_result.rows == expected_rows
        assert reverse_result.rows == (("Analyst 0000", "Lead 0000", 2019),)

        print("Cypher relationship rows:", sqlite_result.rows)
        print("Cypher reverse edge rows:", reverse_result.rows)


if __name__ == "__main__":
    main()
