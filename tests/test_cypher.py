from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.support import humemdb_class


class TestCypher(unittest.TestCase):
    def test_cypher_create_and_match_node_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (u:User {name: 'Alice', age: 30})")

                self.assertEqual(created.columns, ("node_id",))
                self.assertEqual(created.rows[0][0], 1)

                result = db.query("MATCH (u:User {name: 'Alice'}) RETURN u.name, u.age")

                self.assertEqual(result.columns, ("u.name", "u.age"))
                self.assertEqual(result.rows, (("Alice", 30),))

    def test_cypher_supports_named_params_in_create_and_match(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: $name, active: $active, note: $note})",
                    params={"name": "Alice", "active": True, "note": None},
                )

                result = db.query(
                    (
                        "MATCH (u:User) "
                        "WHERE u.name = $name AND u.active = $active "
                        "RETURN u.name, u.active, u.note"
                    ),
                    params={"name": "Alice", "active": True},
                )

                self.assertEqual(result.rows, (("Alice", True, None),))

    def test_cypher_create_relationship_and_match_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[:KNOWS {since: 2020}]->"
                        "(b:User {name: 'Bob'})"
                    ),
                )

                result = db.query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
                )

                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_cypher_supports_relationship_alias_returns_and_filters(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2020}]->"
                        "(b:User {name: 'Bob'})"
                    ),
                )

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE r.since = 2020 AND r.type = 'KNOWS' "
                        "RETURN a.name, r.type, r.since, b.name"
                    ),
                )

                self.assertEqual(
                    result.columns,
                    ("a.name", "r.type", "r.since", "b.name"),
                )
                self.assertEqual(result.rows, (("Alice", "KNOWS", 2020, "Bob"),))

    def test_cypher_supports_reverse_relationship_match(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[:KNOWS]->"
                        "(b:User {name: 'Bob'})"
                    ),
                )

                sqlite_result = db.query(
                    "MATCH (b:User)<-[:KNOWS]-(a:User) RETURN a.name, b.name",
                )
                duckdb_result = db.query(
                    "MATCH (b:User)<-[:KNOWS]-(a:User) RETURN a.name, b.name",
                    route="duckdb",
                )

                self.assertEqual(sqlite_result.rows, (("Alice", "Bob"),))
                self.assertEqual(duckdb_result.rows, (("Alice", "Bob"),))

    def test_cypher_match_where_filters_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 30})")
                db.query("CREATE (u:User {name: 'Bob', age: 40})")

                result = db.query(
                    "MATCH (u:User) WHERE u.age = 40 RETURN u.name, u.age"
                )

                self.assertEqual(result.rows, (("Bob", 40),))

    def test_cypher_match_supports_order_by_and_limit_on_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                for name, age in (("Alice", 30), ("Bob", 40), ("Carol", 20)):
                    db.query(f"CREATE (u:User {{name: '{name}', age: {age}}})")

                sqlite_result = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 2",
                    route="sqlite",
                )
                duckdb_result = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 2",
                    route="duckdb",
                )

                self.assertEqual(sqlite_result.rows, (("Bob",), ("Alice",)))
                self.assertEqual(duckdb_result.rows, (("Bob",), ("Alice",)))

    def test_cypher_match_supports_order_by_and_limit_on_relationships(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                for query in (
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2020}]->"
                        "(b:User {name: 'Bob'})"
                    ),
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2018}]->"
                        "(b:User {name: 'Carol'})"
                    ),
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022}]->"
                        "(b:User {name: 'Dave'})"
                    ),
                ):
                    db.query(query)

                cypher = (
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "RETURN b.name, r.since ORDER BY r.since DESC LIMIT 2"
                )
                sqlite_result = db.query(cypher, route="sqlite")
                duckdb_result = db.query(cypher, route="duckdb")

                expected = (("Dave", 2022), ("Bob", 2020))
                self.assertEqual(sqlite_result.rows, expected)
                self.assertEqual(duckdb_result.rows, expected)

    def test_cypher_match_can_run_on_duckdb(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[:KNOWS]->"
                        "(b:User {name: 'Bob'})"
                    ),
                )

                result = db.query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
                    route="duckdb",
                )

                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_public_api_rejects_direct_duckdb_cypher_writes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaisesRegex(ValueError, "Cypher writes to DuckDB"):
                    db.query("CREATE (u:User {name: 'Alice'})", route="duckdb")

    def test_cypher_persists_graph_data_across_reopen(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', active: true})")

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "MATCH (u:User) WHERE u.active = true RETURN u.name, u.active",
                )

                self.assertEqual(result.rows, (("Alice", True),))

    def test_cypher_rejects_unsupported_where_expression(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "WHERE items"):
                    db.query("MATCH (u:User) WHERE u.age > 30 RETURN u.name")

    def test_cypher_rejects_positional_params(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(NotImplementedError, "named parameter"):
                    db.query("CREATE (u:User {name: $name})", params=("Alice",))

    def test_query_infers_cypher_create_and_match_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (u:User {name: 'Alice', age: 30})")

                self.assertEqual(created.query_type, "cypher")
                self.assertEqual(created.rows[0][0], 1)

                result = db.query("MATCH (u:User {name: 'Alice'}) RETURN u.name, u.age")

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.rows, (("Alice", 30),))

    def test_query_infers_cypher_for_uppercase_multiline_starters(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("  CREATE\n(u:User {name: 'Alice', age: 30})")

                self.assertEqual(created.query_type, "cypher")
                self.assertEqual(created.rows[0][0], 1)

                updated = db.query("\tMATCH\n(u:User {name: 'Alice'}) SET u.age = 31")

                self.assertEqual(updated.query_type, "cypher")

                result = db.query("\nMATCH\t(u:User {name: 'Alice'}) RETURN u.age")

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.rows, ((31,),))

    def test_query_does_not_infer_mixed_case_cypher(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaises(ValueError):
                    db.query("cReAtE (u:User {name: 'Alice'})")

                with self.assertRaisesRegex(
                    ValueError,
                    "could not parse the SQL as PostgreSQL-like HumemSQL",
                ):
                    db.query("mAtCh (u:User {name: 'Alice'}) RETURN u.name")
