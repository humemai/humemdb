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
                )

                self.assertEqual(created.columns, ("node_id",))
                self.assertEqual(created.rows[0][0], 1)

                result = db.query(
                    "MATCH (u:User {name: 'Alice'}) RETURN u.name, u.age",
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
                )

                result = db.query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
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
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: 'Alice', age: 30})",
                )
                db.query(
                    "CREATE (u:User {name: 'Bob', age: 40})",
                )

                result = db.query(
                    "MATCH (u:User) WHERE u.age = 40 RETURN u.name, u.age",
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
                    )

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
                    db.query(query)

                cypher = (
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "RETURN b.name, r.since ORDER BY r.since DESC LIMIT 2"
                )
                sqlite_result = db.query(
                    cypher,
                    route="sqlite",
                )
                duckdb_result = db.query(
                    cypher,
                    route="duckdb",
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
                )

                result = db.query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
                    route="duckdb",
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
                    )

    def test_cypher_persists_graph_data_across_reopen(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: 'Alice', active: true})",
                )

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "MATCH (u:User) WHERE u.active = true RETURN u.name, u.active",
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
                    )

    def test_cypher_rejects_positional_params(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(NotImplementedError, "named parameter"):
                    db.query(
                        "CREATE (u:User {name: $name})",
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
                    "INSERT INTO users (name) VALUES ($name)",
                    route="sqlite",
                    params={"name": "Alice"},
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
                    "INSERT INTO users (name) VALUES ($name)",
                    [{"name": "Alice"}, {"name": "Bob"}],
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

    def test_query_defaults_to_sqlite_route(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO users (id, name) VALUES ($id, $name)",
                    [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
                )

                result = db.query("SELECT id, name FROM users ORDER BY id")

                self.assertEqual(result.rows, ((1, "Alice"), (2, "Bob")))

    def test_sqlite_transaction_context_commits_on_success(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                )

                # A successful transaction block commits when the block exits.
                with db.transaction():
                    db.query(
                        "INSERT INTO users (name) VALUES ($name)",
                        params={"name": "Alice"},
                    )

            # Re-open SQLite to prove the committed row was persisted to disk.
            with HumemDB(str(sqlite_path)) as db:
                result = db.query("SELECT name FROM users")

                self.assertEqual(result.rows, (("Alice",),))

    def test_query_infers_cypher_create_and_match_on_sqlite(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (u:User {name: 'Alice', age: 30})")

                self.assertEqual(created.query_type, "cypher")
                self.assertEqual(created.rows[0][0], 1)

                result = db.query(
                    "MATCH (u:User {name: 'Alice'}) RETURN u.name, u.age"
                )

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.rows, (("Alice", 30),))

    def test_query_infers_cypher_for_uppercase_multiline_starters(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "  CREATE\n(u:User {name: 'Alice', age: 30})"
                )

                self.assertEqual(created.query_type, "cypher")
                self.assertEqual(created.rows[0][0], 1)

                updated = db.query(
                    "\tMATCH\n(u:User {name: 'Alice'}) SET u.age = 31"
                )

                self.assertEqual(updated.query_type, "cypher")

                result = db.query(
                    "\nMATCH\t(u:User {name: 'Alice'}) RETURN u.age"
                )

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.rows, ((31,),))

    def test_query_does_not_infer_mixed_case_cypher(self) -> None:
        HumemDB = _humemdb_class()

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

    def test_query_infers_sql_by_default_for_create_table(self) -> None:
        HumemDB = _humemdb_class()

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
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "create\n table users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                self.assertEqual(created.query_type, "sql")

    def test_sqlite_transaction_context_rolls_back_on_error(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                )

                with self.assertRaises(RuntimeError):
                    # Any exception inside the block causes a rollback.
                    with db.transaction():
                        db.query(
                            "INSERT INTO users (name) VALUES ($name)",
                            params={"name": "Alice"},
                        )
                        raise RuntimeError("force rollback")

                result = db.query("SELECT COUNT(*) FROM users")

                self.assertEqual(result.rows, ((0,),))

    def test_sqlite_executemany_commits_small_batch(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                )
                db.executemany(
                    "INSERT INTO users (name) VALUES ($name)",
                    [{"name": "Alice"}, {"name": "Bob"}],
                )

            with HumemDB(str(sqlite_path)) as db:
                result = db.query("SELECT name FROM users ORDER BY id")

                self.assertEqual(result.rows, (("Alice",), ("Bob",)))

    def test_sqlite_query_accepts_postgres_cast_syntax(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                result = db.query("SELECT 1::INTEGER AS value")

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
                result = db.query("SELECT 'Alice' ILIKE 'aLiCe' AS matched")

                self.assertEqual(result.columns, ("matched",))
                self.assertTrue(bool(result.rows[0][0]))

    def test_sql_query_supports_named_params_with_dollar_placeholders(self) -> None:
        HumemDB = _humemdb_class()

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
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "HumemSQL v0 only supports"):
                    db.query("DROP TABLE users")

    def test_sqlite_executemany_rolls_back_inside_transaction(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
                )

                with self.assertRaises(RuntimeError):
                    with db.transaction():
                        db.executemany(
                            "INSERT INTO users (name) VALUES ($name)",
                            [{"name": "Alice"}, {"name": "Bob"}],
                        )
                        raise RuntimeError("force rollback")

                result = db.query("SELECT COUNT(*) FROM users")

                self.assertEqual(result.rows, ((0,),))

    def test_public_api_rejects_batched_duckdb_writes(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaises(ValueError):
                    db.executemany(
                        "INSERT INTO metrics VALUES ($name, $value)",
                        [{"name": "queries", "value": 1}],
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

    def test_search_vectors_returns_expected_matches(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors(
                    [
                        [1.0, 0.0],
                        [0.8, 0.2],
                        [0.0, 1.0],
                    ]
                )
                self.assertEqual(inserted_ids, (1, 2, 3))

                result = db.search_vectors([1.0, 0.0], top_k=2)

                self.assertEqual(tuple(row[2] for row in result.rows), (1, 2))

    def test_insert_vectors_invalidates_cached_index(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors(
                    [
                        [0.8, 0.2],
                        [0.0, 1.0],
                    ]
                )
                self.assertEqual(inserted_ids, (1, 2))

                first_result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(first_result.rows[0][2], 1)

                inserted_ids = db.insert_vectors([[1.0, 0.0]])
                self.assertEqual(inserted_ids, (3,))

                second_result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(second_result.rows[0][2], 3)

    def test_insert_vectors_can_use_explicit_direct_ids(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors(
                    [
                        (11, [1.0, 0.0]),
                        (14, [0.8, 0.2]),
                    ]
                )

                self.assertEqual(inserted_ids, (11, 14))

                result = db.search_vectors([1.0, 0.0], top_k=2)
                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (("direct", "", 11), ("direct", "", 14)),
                )

    def test_search_vectors_supports_direct_metadata_filters(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors(
                    [
                        [1.0, 0.0],
                        [0.9, 0.1],
                        [0.0, 1.0],
                    ]
                )
                self.assertEqual(inserted_ids, (1, 2, 3))
                db.set_vector_metadata(
                    [
                        (inserted_ids[0], {"group": "alpha", "active": True}),
                        (inserted_ids[1], {"group": "alpha", "active": False}),
                        (inserted_ids[2], {"group": "beta", "active": True}),
                    ]
                )

                result = db.search_vectors(
                    [1.0, 0.0],
                    top_k=3,
                    filters={"group": "alpha", "active": True},
                )

                self.assertEqual(result.rows, (("direct", "", 1, 1.0),))

    def test_insert_vectors_accepts_record_rows_with_inline_metadata(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors(
                    [
                        {
                            "embedding": [1.0, 0.0],
                            "metadata": {"group": "alpha", "active": True},
                        },
                        {
                            "embedding": [0.9, 0.1],
                            "metadata": {"group": "alpha", "active": False},
                        },
                        {
                            "embedding": [0.0, 1.0],
                            "metadata": {"group": "beta", "active": True},
                        },
                    ]
                )

                self.assertEqual(inserted_ids, (1, 2, 3))

                result = db.search_vectors(
                    [1.0, 0.0],
                    top_k=3,
                    filters={"group": "alpha", "active": True},
                )

                self.assertEqual(result.rows, (("direct", "", 1, 1.0),))

    def test_vector_targets_can_reuse_same_numeric_id_in_one_database(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors([[1.0, 0.0]])
                self.assertEqual(inserted_ids, (1,))

                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.query(
                    (
                        "INSERT INTO docs (id, topic, embedding) "
                        "VALUES ($id, $topic, $embedding)"
                    ),
                    params={"id": 1, "topic": "alpha", "embedding": [0.0, 1.0]},
                )

                created = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Alice', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [0.8, 0.2]},
                )
                node_id = created.rows[0][0]
                self.assertEqual(node_id, 1)

                direct = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(direct.rows[0][:3], ("direct", "", 1))

                sql_candidate_filtered = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 1"
                    ),
                    params={
                        "query": [0.0, 1.0],
                        "topic": "alpha",
                    },
                )
                self.assertEqual(
                    sql_candidate_filtered.rows[0][:3],
                    ("sql_row", "docs", 1),
                )

                cypher_candidate_filtered = db.query(
                    (
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 1) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={
                        "query": [0.8, 0.2],
                    },
                )
                self.assertEqual(
                    cypher_candidate_filtered.rows[0][:3],
                    ("graph_node", "", 1),
                )

    def test_sql_insert_with_embedding_updates_row_and_vector_store(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, "
                        "title TEXT NOT NULL, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )

                inserted = db.executemany(
                    (
                        "INSERT INTO docs (id, title, topic, embedding) "
                        "VALUES ($id, $title, $topic, $embedding)"
                    ),
                    [
                        {
                            "id": 1,
                            "title": "Alpha one",
                            "topic": "alpha",
                            "embedding": [1.0, 0.0],
                        },
                        {
                            "id": 2,
                            "title": "Alpha two",
                            "topic": "alpha",
                            "embedding": [0.8, 0.2],
                        },
                        {
                            "id": 3,
                            "title": "Beta one",
                            "topic": "beta",
                            "embedding": [0.0, 1.0],
                        },
                    ],
                )

                self.assertEqual(inserted.rowcount, 3)

                relational = db.query(
                    "SELECT id, title, topic FROM docs ORDER BY id"
                )
                self.assertEqual(
                    relational.rows,
                    (
                        (1, "Alpha one", "alpha"),
                        (2, "Alpha two", "alpha"),
                        (3, "Beta one", "beta"),
                    ),
                )

                vector_result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={
                        "query": [1.0, 0.0],
                        "topic": "alpha",
                    },
                )
                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )

    def test_sql_insert_with_auto_ids_updates_vector_store(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, "
                        "title TEXT NOT NULL, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )

                inserted = db.executemany(
                    (
                        "INSERT INTO docs (title, topic, embedding) "
                        "VALUES ($title, $topic, $embedding)"
                    ),
                    [
                        {
                            "title": "Alpha one",
                            "topic": "alpha",
                            "embedding": [1.0, 0.0],
                        },
                        {
                            "title": "Alpha two",
                            "topic": "alpha",
                            "embedding": [0.8, 0.2],
                        },
                        {
                            "title": "Beta one",
                            "topic": "beta",
                            "embedding": [0.0, 1.0],
                        },
                    ],
                )

                self.assertEqual(inserted.rowcount, 3)

                relational = db.query(
                    "SELECT id, title, topic FROM docs ORDER BY id"
                )
                self.assertEqual(
                    relational.rows,
                    (
                        (1, "Alpha one", "alpha"),
                        (2, "Alpha two", "alpha"),
                        (3, "Beta one", "beta"),
                    ),
                )

                vector_result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={
                        "query": [1.0, 0.0],
                        "topic": "alpha",
                    },
                )
                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )

    def test_sql_single_insert_with_auto_id_updates_vector_store(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )

                db.query(
                    "INSERT INTO docs (title, embedding) VALUES ($title, $embedding)",
                    params={"title": "Alpha", "embedding": [1.0, 0.0]},
                )

                relational = db.query("SELECT id, title FROM docs ORDER BY id")
                self.assertEqual(relational.rows, ((1, "Alpha"),))

                vector_result = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={
                        "query": [1.0, 0.0],
                    },
                )
                self.assertEqual(vector_result.rows[0][:3], ("sql_row", "docs", 1))

    def test_sql_update_with_embedding_updates_vector_store(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.query(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    params={"id": 1, "title": "Alpha", "embedding": [0.0, 1.0]},
                )

                first = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={
                        "query": [1.0, 0.0],
                    },
                )
                self.assertEqual(first.rows[0][:3], ("sql_row", "docs", 1))

                db.query(
                    "UPDATE docs SET embedding = $embedding WHERE id = $id",
                    params={"embedding": [1.0, 0.0], "id": 1},
                )

                second = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={
                        "query": [1.0, 0.0],
                    },
                )
                self.assertEqual(second.rows[0][:3], ("sql_row", "docs", 1))
                self.assertAlmostEqual(second.rows[0][3], 1.0, places=6)

    def test_cypher_create_with_embedding_keeps_node_and_vector_write_together(
        self,
    ) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                node_ids = []
                for name, cohort, embedding in (
                    ("Alice", "alpha", [1.0, 0.0]),
                    ("Bob", "alpha", [0.85, 0.15]),
                    ("Carol", "beta", [0.0, 1.0]),
                ):
                    created = db.query(
                        (
                            "CREATE (u:User {"
                            "name: $name, cohort: $cohort, embedding: $embedding})"
                        ),
                        params={
                            "name": name,
                            "cohort": cohort,
                            "embedding": embedding,
                        },
                    )
                    node_ids.append(created.rows[0][0])
                node_ids = tuple(node_ids)

                self.assertEqual(len(node_ids), 3)

                graph_result = db.query(
                    "MATCH (u:User) RETURN u.id, u.name ORDER BY u.id",
                )
                self.assertEqual(
                    graph_result.rows,
                    (
                        (node_ids[0], "Alice"),
                        (node_ids[1], "Bob"),
                        (node_ids[2], "Carol"),
                    ),
                )

                vector_result = db.query(
                    (
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={
                        "query": [1.0, 0.0],
                    },
                )
                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (
                        ("graph_node", "", node_ids[0]),
                        ("graph_node", "", node_ids[1]),
                    ),
                )

    def test_cypher_match_set_embedding_updates_vector_store(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    (
                        "CREATE (u:User {"
                        "name: $name, cohort: $cohort, embedding: $embedding})"
                    ),
                    params={
                        "name": "Alice",
                        "cohort": "alpha",
                        "embedding": [0.0, 1.0],
                    },
                )
                node_id = created.rows[0][0]

                db.query(
                    "MATCH (u:User {name: 'Alice'}) SET u.embedding = $embedding",
                    params={"embedding": [1.0, 0.0]},
                )

                result = db.query(
                    (
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 1) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={
                        "query": [1.0, 0.0],
                    },
                )
                self.assertEqual(result.rows[0][2], node_id)
                self.assertAlmostEqual(result.rows[0][3], 1.0, places=6)

    def test_sql_vector_syntax_supports_candidate_scope(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, topic, embedding) "
                        "VALUES ($id, $topic, $embedding)"
                    ),
                    [
                        {"id": 1, "topic": "alpha", "embedding": [1.0, 0.0]},
                        {"id": 2, "topic": "alpha", "embedding": [0.8, 0.2]},
                        {"id": 3, "topic": "beta", "embedding": [1.0, 0.0]},
                    ],
                )

                result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={
                        "query": [1.0, 0.0],
                        "topic": "alpha",
                    },
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )

    def test_sql_vector_syntax_keeps_large_fraction_scope_exact(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, topic, embedding) "
                        "VALUES ($id, $topic, $embedding)"
                    ),
                    [
                        {"id": 1, "topic": "alpha", "embedding": [0.8, 0.2]},
                        {"id": 2, "topic": "alpha", "embedding": [0.75, 0.25]},
                        {"id": 3, "topic": "alpha", "embedding": [0.7, 0.3]},
                        {"id": 4, "topic": "alpha", "embedding": [0.65, 0.35]},
                        {"id": 5, "topic": "beta", "embedding": [1.0, 0.0]},
                    ],
                )

                result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 5"
                    ),
                    params={
                        "query": [1.0, 0.0],
                        "topic": "alpha",
                    },
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("sql_row", "docs", 1),
                        ("sql_row", "docs", 2),
                        ("sql_row", "docs", 3),
                        ("sql_row", "docs", 4),
                    ),
                )

    def test_cypher_vector_syntax_supports_candidate_scope(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                alice = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Alice', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [1.0, 0.0]},
                )
                bob = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Bob', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [0.85, 0.15]},
                )
                db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Carol', cohort: 'beta', embedding: $embedding})"
                    ),
                    params={"embedding": [1.0, 0.0]},
                )

                result = db.query(
                    (
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={
                        "query": [1.0, 0.0],
                    },
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("graph_node", "", alice.rows[0][0]),
                        ("graph_node", "", bob.rows[0][0]),
                    ),
                )

    def test_raw_sql_vector_write_invalidates_cached_index(self) -> None:
        HumemDB = _humemdb_class()
        vector = importlib.import_module("humemdb.vector")

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors(
                    [
                        [0.8, 0.2],
                        [0.0, 1.0],
                    ]
                )
                self.assertEqual(inserted_ids, (1, 2))

                first_result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(first_result.rows[0][2], 1)

                db.query(
                    (
                        "INSERT INTO vector_entries "
                        "(target, namespace, target_id, dimensions, embedding) "
                        "VALUES ("
                        "$target, $namespace, $target_id, $dimensions, $embedding"
                        ")"
                    ),
                    params={
                        "target": "direct",
                        "namespace": "",
                        "target_id": 3,
                        "dimensions": 2,
                        "embedding": vector.encode_vector_blob([1.0, 0.0]),
                    },
                )

                second_result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(second_result.rows[0][2], 3)

    def test_preload_vectors_warms_existing_vector_set(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                inserted_ids = db.insert_vectors(
                    [
                        [1.0, 0.0],
                        [0.0, 1.0],
                    ]
                )
                self.assertEqual(inserted_ids, (1, 2))

            with HumemDB(str(sqlite_path), preload_vectors=True) as db:
                self.assertTrue(db.vectors_cached())

                result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(result.rows[0][2], 1)

    def test_preload_vectors_ignores_missing_vector_table(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path), preload_vectors=True) as db:
                self.assertFalse(db.vectors_cached())

    def test_vector_queries_reject_duckdb_route(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                with self.assertRaisesRegex(ValueError, "route='sqlite'"):
                    db.query(
                        "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                        route="duckdb",
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

    def test_sql_rejects_positional_params(self) -> None:
        HumemDB = _humemdb_class()

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
        HumemDB = _humemdb_class()

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


if __name__ == "__main__":
    unittest.main()
