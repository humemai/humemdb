from __future__ import annotations

import inspect
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.support import humemdb_class, runtime_module


def _duckdb_engine(db):
    return getattr(db, "_duckdb")


class TestRoutingRuntime(unittest.TestCase):
    def test_public_api_no_longer_exposes_route_selection(self) -> None:
        HumemDB = humemdb_class()

        query_signature = inspect.signature(HumemDB.query)
        executemany_signature = inspect.signature(HumemDB.executemany)
        transaction_signature = inspect.signature(HumemDB.transaction)
        begin_signature = inspect.signature(HumemDB.begin)
        commit_signature = inspect.signature(HumemDB.commit)
        rollback_signature = inspect.signature(HumemDB.rollback)

        self.assertNotIn("route", query_signature.parameters)
        self.assertNotIn("query_type", executemany_signature.parameters)
        self.assertNotIn("route", executemany_signature.parameters)
        self.assertNotIn("route", transaction_signature.parameters)
        self.assertNotIn("route", begin_signature.parameters)
        self.assertNotIn("route", commit_signature.parameters)
        self.assertNotIn("route", rollback_signature.parameters)

    def test_duckdb_reads_sqlite_source_of_truth_directly(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO users (name) VALUES ($name)",
                    [{"name": "Alice"}, {"name": "Alice"}, {"name": "Bob"}],
                )

                result = db.query(
                    (
                        "SELECT name, COUNT(*) AS total "
                        "FROM users GROUP BY name ORDER BY total DESC, name"
                    )
                )
                self.assertEqual(result.route, "duckdb")
                self.assertEqual(result.rows, (("Alice", 2), ("Bob", 1)))

    def test_duckdb_threads_can_be_overridden_from_humemdb_env(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with mock.patch.dict(os.environ, {"HUMEMDB_THREADS": "8"}):
                with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                    threads = _duckdb_engine(db).connection.execute(
                        "SELECT current_setting('threads')"
                    ).fetchone()[0]

            self.assertEqual(threads, 8)

    def test_runtime_threads_cap_arrow_and_numpy_from_humemdb_env(self) -> None:
        runtime = runtime_module()
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
                                    self.assertEqual(os.environ["MKL_NUM_THREADS"], "6")
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
        runtime = runtime_module()

        with mock.patch.dict(os.environ, {"LANCEDB_THREADS": "5"}, clear=True):
            source_env, thread_count = runtime.resolve_thread_budget_from_env(
                fallback_env_names=(runtime.LANCEDB_THREADS_ENV,),
            )

        self.assertEqual(source_env, runtime.LANCEDB_THREADS_ENV)
        self.assertEqual(thread_count, 5)

    def test_query_defaults_to_sqlite_route(self) -> None:
        HumemDB = humemdb_class()

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
                self.assertEqual(result.route, "sqlite")

    def test_query_auto_routes_broad_sql_read_to_duckdb(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    (
                        "CREATE TABLE events ("
                        "id INTEGER PRIMARY KEY, "
                        "kind TEXT NOT NULL, "
                        "value INTEGER NOT NULL)"
                    )
                )
                db.executemany(
                    "INSERT INTO events (id, kind, value) VALUES ($id, $kind, $value)",
                    [
                        {"id": 1, "kind": "click", "value": 10},
                        {"id": 2, "kind": "click", "value": 20},
                        {"id": 3, "kind": "view", "value": 5},
                    ],
                )

                result = db.query(
                    (
                        "SELECT kind, COUNT(*) AS total "
                        "FROM events GROUP BY kind ORDER BY total DESC"
                    )
                )

                self.assertEqual(result.route, "duckdb")
                self.assertEqual(result.rows, (("click", 2), ("view", 1)))

    def test_query_keeps_selective_sql_read_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    (
                        "CREATE TABLE events ("
                        "id INTEGER PRIMARY KEY, "
                        "kind TEXT NOT NULL, "
                        "value INTEGER NOT NULL)"
                    )
                )
                db.executemany(
                    "INSERT INTO events (id, kind, value) VALUES ($id, $kind, $value)",
                    [
                        {"id": 1, "kind": "click", "value": 10},
                        {"id": 2, "kind": "click", "value": 20},
                        {"id": 3, "kind": "view", "value": 5},
                    ],
                )

                result = db.query(
                    "SELECT value FROM events WHERE id = $id",
                    params={"id": 1},
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, ((10,),))

    def test_query_auto_routes_read_only_cypher_to_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query("CREATE (u:User {name: 'Alice'})")

                result = db.query("MATCH (u:User) RETURN u.name")

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Alice",),))

    def test_sqlite_transaction_context_commits_on_success(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with db.transaction():
                    db.query(
                        "INSERT INTO users (name) VALUES ($name)",
                        params={"name": "Alice"},
                    )

            with HumemDB(str(sqlite_path)) as db:
                result = db.query("SELECT name FROM users")
                self.assertEqual(result.rows, (("Alice",),))

    def test_sqlite_transaction_context_rolls_back_on_error(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with self.assertRaises(RuntimeError):
                    with db.transaction():
                        db.query(
                            "INSERT INTO users (name) VALUES ($name)",
                            params={"name": "Alice"},
                        )
                        raise RuntimeError("force rollback")

                result = db.query("SELECT COUNT(*) FROM users")
                self.assertEqual(result.rows, ((0,),))

    def test_sqlite_executemany_commits_small_batch(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )
                db.executemany(
                    "INSERT INTO users (name) VALUES ($name)",
                    [{"name": "Alice"}, {"name": "Bob"}],
                )

            with HumemDB(str(sqlite_path)) as db:
                result = db.query("SELECT name FROM users ORDER BY id")
                self.assertEqual(result.rows, (("Alice",), ("Bob",)))

    def test_sqlite_executemany_rolls_back_inside_transaction(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
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
