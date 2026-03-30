from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from humemdb import HumemDB, translate_sql
from humemdb.db import _plan_query


class TestSQL(unittest.TestCase):
    def test_translate_sql_rewrites_postgres_cast_for_sqlite(self) -> None:
        translated = translate_sql("SELECT 1::INTEGER AS value", target="sqlite")

        self.assertEqual(translated, "SELECT CAST(1 AS INTEGER) AS value")

    def test_translate_sql_rewrites_ilike_for_sqlite(self) -> None:
        translated = translate_sql(
            "SELECT 'Alice' ILIKE 'aLiCe' AS matched",
            target="sqlite",
        )

        self.assertEqual(
            translated,
            "SELECT LOWER('Alice') LIKE LOWER('aLiCe') AS matched",
        )

    def test_translate_sql_rejects_invalid_postgres_like_sql(self) -> None:
        with self.assertRaises(ValueError):
            translate_sql("SELECT FROM", target="sqlite")

    def test_translate_sql_rejects_unsupported_statement_kind(self) -> None:
        with self.assertRaisesRegex(ValueError, "HumemSQL v0 only supports"):
            translate_sql("DROP TABLE users", target="sqlite")

    def test_translate_sql_rejects_recursive_cte(self) -> None:
        with self.assertRaisesRegex(ValueError, "recursive CTEs"):
            translate_sql(
                "WITH RECURSIVE t(n) AS (SELECT 1) SELECT * FROM t",
                target="sqlite",
            )

    def test_query_infers_sql_by_default_for_create_table(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                created = db.query(
                    "create\n table users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                self.assertEqual(created.query_type, "sql")

    def test_sqlite_query_accepts_postgres_cast_syntax(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                result = db.query("SELECT 1::INTEGER AS value")

                self.assertEqual(result.columns, ("value",))
                self.assertEqual(result.rows, ((1,),))

    def test_duckdb_query_accepts_postgres_cast_syntax(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO events (id, kind) VALUES ($id, $kind)",
                    [
                        {"id": 1, "kind": "click"},
                        {"id": 2, "kind": "click"},
                        {"id": 3, "kind": "view"},
                    ],
                )
                result = db.query(
                    (
                        "SELECT kind, COUNT(*)::INTEGER AS value "
                        "FROM events GROUP BY kind ORDER BY value DESC"
                    )
                )

                self.assertEqual(result.route, "duckdb")
                self.assertEqual(result.columns, ("kind", "value"))
                self.assertEqual(result.rows, (("click", 2), ("view", 1)))

    def test_sqlite_query_accepts_postgres_ilike_syntax(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                result = db.query("SELECT 'Alice' ILIKE 'aLiCe' AS matched")

                self.assertEqual(result.columns, ("matched",))
                self.assertTrue(bool(result.rows[0][0]))

    def test_sql_query_supports_named_params_with_dollar_placeholders(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                with self.assertRaisesRegex(ValueError, "HumemSQL v0 only supports"):
                    db.query("DROP TABLE users")

    def test_duckdb_allows_read_only_cte_queries(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE metrics (name TEXT NOT NULL, value INTEGER NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO metrics (name, value) VALUES ($name, $value)",
                    [
                        {"name": "queries", "value": 1},
                        {"name": "queries", "value": 2},
                        {"name": "writes", "value": 1},
                    ],
                )

                result = db.query(
                    (
                        "WITH m AS (SELECT name, value FROM metrics) "
                        "SELECT name, SUM(value) AS total "
                        "FROM m GROUP BY name ORDER BY total DESC"
                    )
                )

                self.assertEqual(result.route, "duckdb")
                self.assertEqual(result.rows, (("queries", 3), ("writes", 1)))

    def test_sql_query_supports_case_when_exists_runtime_shape(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, deleted_at TEXT)"
                )
                db.query(
                    "CREATE TABLE orders ("
                    "id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT)"
                )
                db.executemany(
                    "INSERT INTO users (id, deleted_at) VALUES ($id, $deleted_at)",
                    [
                        {"id": 1, "deleted_at": None},
                        {"id": 2, "deleted_at": None},
                    ],
                )
                db.executemany(
                    (
                        "INSERT INTO orders (id, user_id, status) "
                        "VALUES ($id, $user_id, $status)"
                    ),
                    [{"id": 1, "user_id": 1, "status": "paid"}],
                )

                result = db.query(
                    (
                        "SELECT u.id, "
                        "CASE WHEN EXISTS ("
                        "SELECT 1 FROM orders o "
                        "WHERE o.user_id = u.id AND o.status = 'paid'"
                        ") THEN 'buyer' ELSE 'prospect' END AS cohort "
                        "FROM users u "
                        "WHERE u.deleted_at IS NULL "
                        "ORDER BY u.id LIMIT 10"
                    )
                )

                self.assertEqual(result.route, "duckdb")
                self.assertEqual(result.rows, ((1, "buyer"), (2, "prospect")))

    def test_sql_query_supports_cte_and_union_runtime_shape(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                result = db.query(
                    (
                        "WITH regional_totals AS ("
                        "SELECT 'east' AS region, 2 AS total_orders "
                        "UNION ALL "
                        "SELECT 'west' AS region, 3 AS total_orders"
                        ") "
                        "SELECT region, total_orders "
                        "FROM regional_totals ORDER BY total_orders DESC"
                    )
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("west", 3), ("east", 2)))

    def test_sql_query_supports_cte_multi_join_runtime_shape(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE users ("
                    "id INTEGER PRIMARY KEY, segment TEXT, country_id INTEGER)"
                )
                db.query(
                    "CREATE TABLE orders ("
                    "id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT, "
                    "total_cents INTEGER, created_at DATE)"
                )
                db.query(
                    "CREATE TABLE countries (id INTEGER PRIMARY KEY, name TEXT)"
                )
                db.executemany(
                    "INSERT INTO countries (id, name) VALUES ($id, $name)",
                    [{"id": 1, "name": "US"}, {"id": 2, "name": "CA"}],
                )
                db.executemany(
                    (
                        "INSERT INTO users (id, segment, country_id) "
                        "VALUES ($id, $segment, $country_id)"
                    ),
                    [
                        {"id": 1, "segment": "pro", "country_id": 1},
                        {"id": 2, "segment": "free", "country_id": 2},
                        {"id": 3, "segment": "pro", "country_id": None},
                    ],
                )
                db.executemany(
                    (
                        "INSERT INTO orders ("
                        "id, user_id, status, total_cents, created_at"
                        ") VALUES ($id, $user_id, $status, $total_cents, $created_at)"
                    ),
                    [
                        {
                            "id": 1,
                            "user_id": 1,
                            "status": "paid",
                            "total_cents": 1000,
                            "created_at": "2026-01-05",
                        },
                        {
                            "id": 2,
                            "user_id": 1,
                            "status": "paid",
                            "total_cents": 3000,
                            "created_at": "2026-01-06",
                        },
                        {
                            "id": 3,
                            "user_id": 2,
                            "status": "paid",
                            "total_cents": 2000,
                            "created_at": "2026-01-07",
                        },
                        {
                            "id": 4,
                            "user_id": 3,
                            "status": "refunded",
                            "total_cents": 500,
                            "created_at": "2026-01-08",
                        },
                    ],
                )

                result = db.query(
                    (
                        "WITH recent_paid AS ("
                        "SELECT user_id, total_cents, created_at "
                        "FROM orders "
                        "WHERE status = 'paid' AND created_at >= DATE '2026-01-01'"
                        "), top_users AS ("
                        "SELECT user_id, SUM(total_cents) AS spent_cents "
                        "FROM recent_paid GROUP BY user_id"
                        ") "
                        "SELECT u.segment, c.name AS country, "
                        "AVG(t.spent_cents) AS avg_spend "
                        "FROM top_users t "
                        "JOIN users u ON u.id = t.user_id "
                        "LEFT JOIN countries c ON c.id = u.country_id "
                        "GROUP BY u.segment, c.name "
                        "ORDER BY avg_spend DESC, u.segment"
                    )
                )

                self.assertEqual(result.route, "duckdb")
                self.assertEqual(
                    result.rows,
                    (("pro", "US", 4000.0), ("free", "CA", 2000.0)),
                )

    def test_sql_query_supports_windowed_rank_cte_runtime_shape(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE events ("
                        "id INTEGER PRIMARY KEY, user_id INTEGER, "
                        "event_type TEXT, created_at TEXT)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO events (id, user_id, event_type, created_at) "
                        "VALUES ($id, $user_id, $event_type, $created_at)"
                    ),
                    [
                        {
                            "id": 1,
                            "user_id": 1,
                            "event_type": "login",
                            "created_at": "2026-01-03",
                        },
                        {
                            "id": 2,
                            "user_id": 1,
                            "event_type": "click",
                            "created_at": "2026-01-05",
                        },
                        {
                            "id": 3,
                            "user_id": 1,
                            "event_type": "purchase",
                            "created_at": "2026-01-06",
                        },
                        {
                            "id": 4,
                            "user_id": 1,
                            "event_type": "share",
                            "created_at": "2026-01-07",
                        },
                        {
                            "id": 5,
                            "user_id": 2,
                            "event_type": "login",
                            "created_at": "2026-01-02",
                        },
                        {
                            "id": 6,
                            "user_id": 2,
                            "event_type": "click",
                            "created_at": "2026-01-04",
                        },
                        {
                            "id": 7,
                            "user_id": 2,
                            "event_type": "logout",
                            "created_at": "2026-01-08",
                        },
                    ],
                )

                result = db.query(
                    (
                        "WITH ranked AS ("
                        "SELECT e.user_id, e.event_type, e.created_at, "
                        "ROW_NUMBER() OVER ("
                        "PARTITION BY e.user_id ORDER BY e.created_at DESC"
                        ") AS rn "
                        "FROM events e"
                        ") "
                        "SELECT user_id, event_type, created_at "
                        "FROM ranked WHERE rn <= 3 "
                        "ORDER BY user_id, created_at DESC"
                    )
                )

                self.assertEqual(result.route, "duckdb")
                self.assertEqual(
                    result.rows,
                    (
                        (1, "share", "2026-01-07"),
                        (1, "purchase", "2026-01-06"),
                        (1, "click", "2026-01-05"),
                        (2, "logout", "2026-01-08"),
                        (2, "click", "2026-01-04"),
                        (2, "login", "2026-01-02"),
                    ),
                )

    def test_internal_duckdb_sql_write_guard_still_rejects_non_read_only_plans(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )
                plan = _plan_query(
                    "INSERT INTO users (name) VALUES ($name)",
                    route="duckdb",
                    params={"name": "Alice"},
                )

                with self.assertRaisesRegex(
                    ValueError,
                    "does not allow direct writes to DuckDB",
                ):
                    db._execute_sql_query_plan(plan)

    def test_sql_rejects_positional_params(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with self.assertRaisesRegex(ValueError, "named mapping params"):
                    db.query(
                        "INSERT INTO users (name) VALUES ($name)",
                        params=("Alice",),
                    )

    def test_sql_batch_rejects_positional_params(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with self.assertRaisesRegex(ValueError, "mapping params"):
                    db.executemany(
                        "INSERT INTO users (name) VALUES ($name)",
                        [("Alice",), ("Bob",)],
                    )
