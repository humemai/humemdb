from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import importlib
from unittest import mock


def _humemdb_class():
    # Import lazily so the test can work with the src/ layout.
    return importlib.import_module("humemdb").HumemDB


def _translate_sql():
    # Import lazily so tests exercise the installed package surface.
    return importlib.import_module("humemdb").translate_sql


def _runtime_module():
    return importlib.import_module("humemdb.runtime")


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

    def test_cypher_create_and_match_node_on_sqlite(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "CREATE (u:User {name: 'Alice', age: 30})",
                    route="sqlite",
                    query_type="cypher",
                )

                self.assertEqual(created.columns, ("node_id",))
                self.assertEqual(created.rows[0][0], 1)

                result = db.query(
                    "MATCH (u:User {name: 'Alice'}) RETURN u.name, u.age",
                    route="sqlite",
                    query_type="cypher",
                )

                self.assertEqual(result.columns, ("u.name", "u.age"))
                self.assertEqual(result.rows, (("Alice", 30),))

    def test_cypher_supports_named_params_in_create_and_match(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: $name, active: $active, note: $note})",
                    route="sqlite",
                    query_type="cypher",
                    params={"name": "Alice", "active": True, "note": None},
                )

                result = db.query(
                    (
                        "MATCH (u:User) "
                        "WHERE u.name = $name AND u.active = $active "
                        "RETURN u.name, u.active, u.note"
                    ),
                    route="sqlite",
                    query_type="cypher",
                    params={"name": "Alice", "active": True},
                )

                self.assertEqual(
                    result.rows,
                    (("Alice", True, None),),
                )

    def test_cypher_create_relationship_and_match_on_sqlite(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[:KNOWS {since: 2020}]->"
                        "(b:User {name: 'Bob'})"
                    ),
                    route="sqlite",
                    query_type="cypher",
                )

                result = db.query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
                    route="sqlite",
                    query_type="cypher",
                )

                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_cypher_supports_relationship_alias_returns_and_filters(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "WHERE r.since = 2020 AND r.type = 'KNOWS' "
                        "RETURN a.name, r.type, r.since, b.name"
                    ),
                    route="sqlite",
                    query_type="cypher",
                )

                self.assertEqual(
                    result.columns,
                    ("a.name", "r.type", "r.since", "b.name"),
                )
                self.assertEqual(result.rows, (("Alice", "KNOWS", 2020, "Bob"),))

    def test_cypher_supports_reverse_relationship_match(self) -> None:
        HumemDB = _humemdb_class()

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
                    route="sqlite",
                    query_type="cypher",
                )

                sqlite_result = db.query(
                    "MATCH (b:User)<-[:KNOWS]-(a:User) RETURN a.name, b.name",
                    route="sqlite",
                    query_type="cypher",
                )
                duckdb_result = db.query(
                    "MATCH (b:User)<-[:KNOWS]-(a:User) RETURN a.name, b.name",
                    route="duckdb",
                    query_type="cypher",
                )

                self.assertEqual(sqlite_result.rows, (("Alice", "Bob"),))
                self.assertEqual(duckdb_result.rows, (("Alice", "Bob"),))

    def test_cypher_match_where_filters_nodes(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: 'Alice', age: 30})",
                    route="sqlite",
                    query_type="cypher",
                )
                db.query(
                    "CREATE (u:User {name: 'Bob', age: 40})",
                    route="sqlite",
                    query_type="cypher",
                )

                result = db.query(
                    "MATCH (u:User) WHERE u.age = 40 RETURN u.name, u.age",
                    route="sqlite",
                    query_type="cypher",
                )

                self.assertEqual(result.rows, (("Bob", 40),))

    def test_cypher_match_supports_order_by_and_limit_on_nodes(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                for name, age in (("Alice", 30), ("Bob", 40), ("Carol", 20)):
                    db.query(
                        f"CREATE (u:User {{name: '{name}', age: {age}}})",
                        route="sqlite",
                        query_type="cypher",
                    )

                sqlite_result = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 2",
                    route="sqlite",
                    query_type="cypher",
                )
                duckdb_result = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 2",
                    route="duckdb",
                    query_type="cypher",
                )

                self.assertEqual(sqlite_result.rows, (("Bob",), ("Alice",)))
                self.assertEqual(duckdb_result.rows, (("Bob",), ("Alice",)))

    def test_cypher_match_supports_order_by_and_limit_on_relationships(self) -> None:
        HumemDB = _humemdb_class()

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
                    db.query(query, route="sqlite", query_type="cypher")

                cypher = (
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "RETURN b.name, r.since ORDER BY r.since DESC LIMIT 2"
                )
                sqlite_result = db.query(
                    cypher,
                    route="sqlite",
                    query_type="cypher",
                )
                duckdb_result = db.query(
                    cypher,
                    route="duckdb",
                    query_type="cypher",
                )

                expected = (("Dave", 2022), ("Bob", 2020))
                self.assertEqual(sqlite_result.rows, expected)
                self.assertEqual(duckdb_result.rows, expected)

    def test_cypher_match_can_run_on_duckdb(self) -> None:
        HumemDB = _humemdb_class()

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
                    route="sqlite",
                    query_type="cypher",
                )

                result = db.query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
                    route="duckdb",
                    query_type="cypher",
                )

                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_public_api_rejects_direct_duckdb_cypher_writes(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaisesRegex(ValueError, "Cypher writes to DuckDB"):
                    db.query(
                        "CREATE (u:User {name: 'Alice'})",
                        route="duckdb",
                        query_type="cypher",
                    )

    def test_cypher_persists_graph_data_across_reopen(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: 'Alice', active: true})",
                    route="sqlite",
                    query_type="cypher",
                )

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "MATCH (u:User) WHERE u.active = true RETURN u.name, u.active",
                    route="sqlite",
                    query_type="cypher",
                )

                self.assertEqual(result.rows, (("Alice", True),))

    def test_cypher_rejects_unsupported_where_expression(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "WHERE items"):
                    db.query(
                        "MATCH (u:User) WHERE u.age > 30 RETURN u.name",
                        route="sqlite",
                        query_type="cypher",
                    )

    def test_cypher_rejects_positional_params(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(NotImplementedError, "named parameter"):
                    db.query(
                        "CREATE (u:User {name: $name})",
                        route="sqlite",
                        query_type="cypher",
                        params=("Alice",),
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
                # directly through the current direct-read path.
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

    def test_duckdb_threads_can_be_overridden_from_humemdb_env(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with mock.patch.dict(os.environ, {"HUMEMDB_THREADS": "8"}):
                with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                    threads = db.duckdb.connection.execute(
                        "SELECT current_setting('threads')"
                    ).fetchone()[0]

            self.assertEqual(threads, 8)

    def test_runtime_threads_cap_arrow_and_numpy_from_humemdb_env(self) -> None:
        runtime = _runtime_module()
        limiter = mock.Mock()
        threadpool_state = getattr(runtime, "_THREADPOOL_STATE")

        with mock.patch.dict(os.environ, {"HUMEMDB_THREADS": "6"}, clear=False):
            with mock.patch.dict(
                threadpool_state,
                {"limiter": None, "limit": None},
                clear=True,
            ):
                with mock.patch("pyarrow.set_cpu_count") as set_cpu_count:
                    with mock.patch(
                        "pyarrow.set_io_thread_count"
                    ) as set_io_thread_count:
                        with mock.patch("pyarrow.cpu_count", return_value=6):
                            with mock.patch("pyarrow.io_thread_count", return_value=6):
                                with mock.patch(
                                    "threadpoolctl.threadpool_limits",
                                    return_value=limiter,
                                ) as threadpool_limits:
                                    budget = (
                                        runtime.configure_runtime_threads_from_env()
                                    )

                                    self.assertEqual(budget.thread_count, 6)
                                    self.assertEqual(budget.arrow_cpu_count, 6)
                                    self.assertEqual(budget.arrow_io_thread_count, 6)
                                    self.assertEqual(budget.numpy_thread_limit, 6)
                                    self.assertEqual(
                                        budget.source_env,
                                        runtime.HUMEMDB_THREADS_ENV,
                                    )
                                    self.assertEqual(
                                        os.environ["OMP_THREAD_LIMIT"],
                                        "6",
                                    )
                                    self.assertEqual(os.environ["OMP_NUM_THREADS"], "6")
                                    self.assertEqual(
                                        os.environ["OPENBLAS_NUM_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["MKL_NUM_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["BLIS_NUM_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["VECLIB_MAXIMUM_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["NUMEXPR_NUM_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["RAYON_NUM_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["TOKIO_WORKER_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["POLARS_MAX_THREADS"],
                                        "6",
                                    )
                                    self.assertEqual(
                                        os.environ["ARROW_NUM_THREADS"],
                                        "6",
                                    )

        set_cpu_count.assert_called_once_with(6)
        set_io_thread_count.assert_called_once_with(6)
        threadpool_limits.assert_called_once_with(limits=6)

    def test_runtime_threads_support_vector_only_fallback_env(self) -> None:
        runtime = _runtime_module()

        with mock.patch.dict(os.environ, {"LANCEDB_THREADS": "5"}, clear=True):
            source_env, thread_count = runtime.resolve_thread_budget_from_env(
                fallback_env_names=(runtime.LANCEDB_THREADS_ENV,),
            )

        self.assertEqual(source_env, runtime.LANCEDB_THREADS_ENV)
        self.assertEqual(thread_count, 5)

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

    def test_vector_query_type_searches_sqlite_collection(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.insert_vectors(
                    [
                        (1, "default", 7, [1.0, 0.0]),
                        (2, "default", 7, [0.8, 0.2]),
                        (3, "default", 9, [0.0, 1.0]),
                    ]
                )

                result = db.query(
                    "default",
                    route="sqlite",
                    query_type="vector",
                    params={"query": [1.0, 0.0], "top_k": 2},
                )

                self.assertEqual(result.columns, ("item_id", "score"))
                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.query_type, "vector")
                self.assertEqual(tuple(row[0] for row in result.rows), (1, 2))

    def test_search_vectors_supports_bucket_filter(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.insert_vectors(
                    [
                        (1, "default", 7, [1.0, 0.0]),
                        (2, "default", 8, [0.0, 1.0]),
                    ]
                )

                result = db.search_vectors(
                    "default",
                    [1.0, 0.0],
                    top_k=2,
                    bucket=8,
                )

                self.assertEqual(result.rows, ((2, result.rows[0][1]),))

    def test_insert_vectors_invalidates_cached_index(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.insert_vectors(
                    [
                        (1, "default", 7, [0.8, 0.2]),
                        (2, "default", 7, [0.0, 1.0]),
                    ]
                )

                first_result = db.search_vectors("default", [1.0, 0.0], top_k=1)
                self.assertEqual(first_result.rows[0][0], 1)

                db.insert_vectors([(3, "default", 7, [1.0, 0.0])])

                second_result = db.search_vectors("default", [1.0, 0.0], top_k=1)
                self.assertEqual(second_result.rows[0][0], 3)

    def test_raw_sql_vector_write_invalidates_cached_index(self) -> None:
        HumemDB = _humemdb_class()
        vector = importlib.import_module("humemdb.vector")

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.insert_vectors(
                    [
                        (1, "default", 7, [0.8, 0.2]),
                        (2, "default", 7, [0.0, 1.0]),
                    ]
                )

                first_result = db.search_vectors("default", [1.0, 0.0], top_k=1)
                self.assertEqual(first_result.rows[0][0], 1)

                db.query(
                    (
                        "INSERT INTO vector_entries "
                        "(item_id, collection, bucket, dimensions, embedding) "
                        "VALUES (?, ?, ?, ?, ?)"
                    ),
                    route="sqlite",
                    params=(
                        3,
                        "default",
                        7,
                        2,
                        vector.encode_vector_blob([1.0, 0.0]),
                    ),
                )

                second_result = db.search_vectors("default", [1.0, 0.0], top_k=1)
                self.assertEqual(second_result.rows[0][0], 3)

    def test_preload_vector_collections_warms_existing_collection(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.insert_vectors(
                    [
                        (1, "default", 7, [1.0, 0.0]),
                        (2, "archive", 8, [0.0, 1.0]),
                    ]
                )

            with HumemDB(
                str(sqlite_path),
                preload_vector_collections=("default",),
            ) as db:
                self.assertEqual(db.cached_vector_collections(), ("default",))

                result = db.search_vectors("default", [1.0, 0.0], top_k=1)
                self.assertEqual(result.rows[0][0], 1)

    def test_preload_vector_collections_true_loads_all_existing_collections(
        self,
    ) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.insert_vectors(
                    [
                        (1, "default", 7, [1.0, 0.0]),
                        (2, "archive", 8, [0.0, 1.0]),
                    ]
                )

            with HumemDB(str(sqlite_path), preload_vector_collections=True) as db:
                self.assertEqual(
                    db.cached_vector_collections(),
                    ("archive", "default"),
                )

    def test_preload_vector_collections_ignores_missing_vector_table(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path), preload_vector_collections=True) as db:
                self.assertEqual(db.cached_vector_collections(), ())

    def test_vector_query_type_rejects_duckdb_route(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaisesRegex(ValueError, "route='sqlite'"):
                    db.query(
                        "default",
                        route="duckdb",
                        query_type="vector",
                        params={"query": [1.0, 0.0]},
                    )

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
