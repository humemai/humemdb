from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import importlib


def _humemdb_class():
    # Import lazily so the test can work with the src/ layout.
    return importlib.import_module("humemdb").HumemDB


def _translate_sql():
    # Import lazily so tests exercise the installed package surface.
    return importlib.import_module("humemdb").translate_sql


class HumemDBTest(unittest.TestCase):
    def test_translate_sql_rewrites_postgres_cast_for_sqlite(self) -> None:
        translate_sql = _translate_sql()

        translated = translate_sql("SELECT 1::INTEGER AS value", target="sqlite")

        self.assertEqual(translated, "SELECT CAST(1 AS INTEGER) AS value")

    def test_translate_sql_rewrites_ilike_for_sqlite(self) -> None:
        translate_sql = _translate_sql()

        translated = translate_sql(
            "SELECT 'Alice' ILIKE 'aLiCe' AS matched",
            target="sqlite",
        )

        self.assertEqual(
            translated,
            "SELECT LOWER('Alice') LIKE LOWER('aLiCe') AS matched",
        )

    def test_translate_sql_rejects_invalid_postgres_like_sql(self) -> None:
        translate_sql = _translate_sql()

        with self.assertRaises(ValueError):
            translate_sql("SELECT FROM", target="sqlite")

    def test_translate_sql_rejects_unsupported_statement_kind(self) -> None:
        translate_sql = _translate_sql()

        with self.assertRaisesRegex(ValueError, "HumemSQL v0 only supports"):
            translate_sql("DROP TABLE users", target="sqlite")

    def test_translate_sql_rejects_recursive_cte(self) -> None:
        translate_sql = _translate_sql()

        with self.assertRaisesRegex(ValueError, "recursive CTEs"):
            translate_sql(
                "WITH RECURSIVE t(n) AS (SELECT 1) SELECT * FROM t",
                target="sqlite",
            )

    def test_explicit_sqlite_and_duckdb_routing(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Each test gets fresh on-disk database files inside a temporary folder.
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            # Entering HumemDB opens both embedded database connections.
            # Exiting this block closes both connections.
            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                # This table is created in SQLite because the route is explicit.
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    route="sqlite",
                )
                # This write also goes to SQLite.
                db.query(
                    "INSERT INTO users (name) VALUES (?)",
                    route="sqlite",
                    params=("Alice",),
                )

                # Read the row back from SQLite and assert the normalized result shape.
                sqlite_result = db.query(
                    "SELECT id, name FROM users",
                    route="sqlite",
                )

                self.assertEqual(sqlite_result.columns, ("id", "name"))
                self.assertEqual(sqlite_result.rows, ((1, "Alice"),))

                # DuckDB should be able to read the SQLite source-of-truth tables
                # directly through the Phase 3 direct-read path.
                duckdb_result = db.query(
                    "SELECT id, name FROM users",
                    route="duckdb",
                )

                self.assertEqual(duckdb_result.columns, ("id", "name"))
                self.assertEqual(duckdb_result.rows, ((1, "Alice"),))

    def test_duckdb_reads_sqlite_source_of_truth_directly(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    route="sqlite",
                )
                db.executemany(
                    "INSERT INTO users (name) VALUES (?)",
                    [("Alice",), ("Bob",)],
                    route="sqlite",
                )

                result = db.query(
                    "SELECT name FROM users ORDER BY id",
                    route="duckdb",
                )

                self.assertEqual(result.rows, (("Alice",), ("Bob",)))

    def test_public_api_rejects_direct_duckdb_writes(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaises(ValueError):
                    db.query(
                        "CREATE TABLE metrics (name VARCHAR, value INTEGER)",
                        route="duckdb",
                    )

    def test_invalid_route_raises_value_error(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaises(ValueError):
                    db.query("SELECT 1", route="postgres")

    def test_sqlite_transaction_context_commits_on_success(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    route="sqlite",
                )

                # A successful transaction block commits when the block exits.
                with db.transaction(route="sqlite"):
                    db.query(
                        "INSERT INTO users (name) VALUES (?)",
                        route="sqlite",
                        params=("Alice",),
                    )

            # Re-open SQLite to prove the committed row was persisted to disk.
            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "SELECT name FROM users",
                    route="sqlite",
                )

                self.assertEqual(result.rows, (("Alice",),))

    def test_sqlite_transaction_context_rolls_back_on_error(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    route="sqlite",
                )

                with self.assertRaises(RuntimeError):
                    # Any exception inside the block causes a rollback.
                    with db.transaction(route="sqlite"):
                        db.query(
                            "INSERT INTO users (name) VALUES (?)",
                            route="sqlite",
                            params=("Alice",),
                        )
                        raise RuntimeError("force rollback")

                result = db.query(
                    "SELECT COUNT(*) FROM users",
                    route="sqlite",
                )

                self.assertEqual(result.rows, ((0,),))

    def test_sqlite_executemany_commits_small_batch(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    route="sqlite",
                )
                db.executemany(
                    "INSERT INTO users (name) VALUES (?)",
                    [("Alice",), ("Bob",)],
                    route="sqlite",
                )

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "SELECT name FROM users ORDER BY id",
                    route="sqlite",
                )

                self.assertEqual(result.rows, (("Alice",), ("Bob",)))

    def test_sqlite_query_accepts_postgres_cast_syntax(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "SELECT 1::INTEGER AS value",
                    route="sqlite",
                )

                self.assertEqual(result.columns, ("value",))
                self.assertEqual(result.rows, ((1,),))

    def test_duckdb_query_accepts_postgres_cast_syntax(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                result = db.query(
                    "SELECT 1::INTEGER AS value",
                    route="duckdb",
                )

                self.assertEqual(result.columns, ("value",))
                self.assertEqual(result.rows, ((1,),))

    def test_sqlite_query_accepts_postgres_ilike_syntax(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "SELECT 'Alice' ILIKE 'aLiCe' AS matched",
                    route="sqlite",
                )

                self.assertEqual(result.columns, ("matched",))
                self.assertTrue(bool(result.rows[0][0]))

    def test_sqlite_query_rejects_unsupported_humemsql_statement(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "HumemSQL v0 only supports"):
                    db.query("DROP TABLE users", route="sqlite")

    def test_sqlite_executemany_rolls_back_inside_transaction(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                    route="sqlite",
                )

                with self.assertRaises(RuntimeError):
                    with db.transaction(route="sqlite"):
                        db.executemany(
                            "INSERT INTO users (name) VALUES (?)",
                            [("Alice",), ("Bob",)],
                            route="sqlite",
                        )
                        raise RuntimeError("force rollback")

                result = db.query(
                    "SELECT COUNT(*) FROM users",
                    route="sqlite",
                )

                self.assertEqual(result.rows, ((0,),))

    def test_public_api_rejects_batched_duckdb_writes(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaises(ValueError):
                    db.executemany(
                        "INSERT INTO metrics VALUES (?, ?)",
                        [("queries", 1)],
                        route="duckdb",
                    )

    def test_duckdb_transaction_context_commits_on_success(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaises(ValueError):
                    with db.transaction(route="duckdb"):
                        db.query(
                            "CREATE TABLE metrics (name VARCHAR, value INTEGER)",
                            route="duckdb",
                        )

    def test_non_sql_query_types_are_not_implemented_in_phase_1(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                # Only SQL is implemented right now, so Cypher should fail clearly.
                with self.assertRaises(NotImplementedError):
                    db.query("MATCH (n) RETURN n", route="sqlite", query_type="cypher")

    def test_duckdb_allows_read_only_cte_queries(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.duckdb.execute("CREATE TABLE metrics (name VARCHAR, value INTEGER)")
                db.duckdb.execute(
                    "INSERT INTO metrics VALUES (?, ?)",
                    params=("queries", 1),
                )

                result = db.query(
                    (
                        "WITH m AS (SELECT name, value FROM metrics) "
                        "SELECT name, value FROM m"
                    ),
                    route="duckdb",
                )

                self.assertEqual(result.rows, (("queries", 1),))


if __name__ == "__main__":
    unittest.main()
