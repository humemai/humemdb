from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.support import humemdb_class, translate_sql


class TestSQL(unittest.TestCase):
    def test_translate_sql_rewrites_postgres_cast_for_sqlite(self) -> None:
        translated = translate_sql()("SELECT 1::INTEGER AS value", target="sqlite")

        self.assertEqual(translated, "SELECT CAST(1 AS INTEGER) AS value")

    def test_translate_sql_rewrites_ilike_for_sqlite(self) -> None:
        translated = translate_sql()(
            "SELECT 'Alice' ILIKE 'aLiCe' AS matched",
            target="sqlite",
        )

        self.assertEqual(
            translated,
            "SELECT LOWER('Alice') LIKE LOWER('aLiCe') AS matched",
        )

    def test_translate_sql_rejects_invalid_postgres_like_sql(self) -> None:
        with self.assertRaises(ValueError):
            translate_sql()("SELECT FROM", target="sqlite")

    def test_translate_sql_rejects_unsupported_statement_kind(self) -> None:
        with self.assertRaisesRegex(ValueError, "HumemSQL v0 only supports"):
            translate_sql()("DROP TABLE users", target="sqlite")

    def test_translate_sql_rejects_recursive_cte(self) -> None:
        with self.assertRaisesRegex(ValueError, "recursive CTEs"):
            translate_sql()(
                "WITH RECURSIVE t(n) AS (SELECT 1) SELECT * FROM t",
                target="sqlite",
            )

    def test_query_infers_sql_by_default_for_create_table(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                self.assertEqual(created.query_type, "sql")

                inserted = db.query(
                    "INSERT INTO users (name) VALUES ($name)",
                    params={"name": "Alice"},
                )
                self.assertEqual(inserted.query_type, "sql")

                result = db.query("SELECT name FROM users")
                self.assertEqual(result.query_type, "sql")
                self.assertEqual(result.rows, (("Alice",),))

    def test_query_keeps_multiline_sql_create_table_on_sql_path(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "create\n table users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                self.assertEqual(created.query_type, "sql")

    def test_sqlite_query_accepts_postgres_cast_syntax(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                result = db.query("SELECT 1::INTEGER AS value")

                self.assertEqual(result.columns, ("value",))
                self.assertEqual(result.rows, ((1,),))

    def test_duckdb_query_accepts_postgres_cast_syntax(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                result = db.query("SELECT 1::INTEGER AS value", route="duckdb")

                self.assertEqual(result.columns, ("value",))
                self.assertEqual(result.rows, ((1,),))

    def test_sqlite_query_accepts_postgres_ilike_syntax(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                result = db.query("SELECT 'Alice' ILIKE 'aLiCe' AS matched")

                self.assertEqual(result.columns, ("matched",))
                self.assertTrue(bool(result.rows[0][0]))

    def test_sql_query_supports_named_params_with_dollar_placeholders(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO users (id, name) VALUES ($id, $name)",
                    [
                        {"id": 1, "name": "Alice"},
                        {"id": 2, "name": "Bob"},
                    ],
                )

                result = db.query(
                    "SELECT id, name FROM users WHERE name = $name",
                    params={"name": "Alice"},
                )

                self.assertEqual(result.rows, ((1, "Alice"),))

    def test_sqlite_query_rejects_unsupported_humemsql_statement(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "HumemSQL v0 only supports"):
                    db.query("DROP TABLE users")

    def test_duckdb_allows_read_only_cte_queries(self) -> None:
        HumemDB = humemdb_class()

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

    def test_duckdb_rejects_data_modifying_cte_queries(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with self.assertRaisesRegex(
                    ValueError,
                    "does not allow direct writes to DuckDB",
                ):
                    db.query(
                        (
                            "WITH inserted AS ("
                            "INSERT INTO users (name) VALUES ('Alice')"
                            ") SELECT name FROM users"
                        ),
                        route="duckdb",
                    )

    def test_sql_rejects_positional_params(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with self.assertRaisesRegex(ValueError, "named mapping params"):
                    db.query(
                        "INSERT INTO users (name) VALUES ($name)",
                        params=("Alice",),
                    )

    def test_sql_batch_rejects_positional_params(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with self.assertRaisesRegex(ValueError, "mapping params"):
                    db.executemany(
                        "INSERT INTO users (name) VALUES ($name)",
                        [("Alice",), ("Bob",)],
                    )
