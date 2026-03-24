from __future__ import annotations

from dataclasses import replace
import importlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.support import humemdb_class


class TestPlanning(unittest.TestCase):
    def test_query_reuses_parsed_cypher_plan_during_execution(self) -> None:
        HumemDB = humemdb_class()
        db_module = importlib.import_module("humemdb.db")
        cypher_module = importlib.import_module("humemdb.cypher")

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with mock.patch.object(
                db_module,
                "parse_cypher",
                wraps=db_module.parse_cypher,
            ) as plan_parse:
                with mock.patch.object(
                    cypher_module,
                    "parse_cypher",
                    wraps=cypher_module.parse_cypher,
                ) as execute_parse:
                    with HumemDB(str(sqlite_path)) as db:
                        result = db.query("CREATE (u:User {name: 'Alice', age: 30})")

                    self.assertEqual(result.query_type, "cypher")
                    self.assertEqual(plan_parse.call_count, 1)
                    self.assertEqual(execute_parse.call_count, 0)

    def test_sql_translation_plan_exposes_shape_metadata(self) -> None:
        sql_module = importlib.import_module("humemdb.sql")

        plan = sql_module.translate_sql_plan(
            (
                "WITH recent AS (SELECT customer_id, amount FROM payments) "
                "SELECT c.name, COUNT(*) AS total "
                "FROM recent AS r "
                "JOIN customers AS c ON c.id = r.customer_id "
                "GROUP BY c.name "
                "ORDER BY total DESC "
                "LIMIT 5"
            ),
            target="sqlite",
        )

        self.assertEqual(plan.statement_name, "Select")
        self.assertTrue(plan.is_read_only)
        self.assertEqual(plan.cte_count, 1)
        self.assertEqual(plan.join_count, 1)
        self.assertEqual(plan.aggregate_count, 1)
        self.assertEqual(plan.window_count, 0)
        self.assertTrue(plan.has_order_by)
        self.assertTrue(plan.has_limit)
        self.assertTrue(plan.has_group_by)
        self.assertFalse(plan.has_distinct)

    def test_query_plan_carries_sql_shape_metadata(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            (
                "SELECT kind, COUNT(*) AS total FROM events "
                "GROUP BY kind ORDER BY total DESC LIMIT 3"
            ),
            route="duckdb",
            params=None,
        )

        self.assertEqual(plan.query_type, "sql")
        self.assertTrue(plan.sql_is_read_only)
        self.assertIsNotNone(plan.sql_plan)
        assert plan.sql_plan is not None
        self.assertEqual(plan.sql_plan.statement_name, "Select")
        self.assertEqual(plan.sql_plan.aggregate_count, 1)
        self.assertEqual(plan.sql_plan.window_count, 0)
        self.assertTrue(plan.sql_plan.has_order_by)
        self.assertTrue(plan.sql_plan.has_limit)
        self.assertTrue(plan.sql_plan.has_group_by)
        self.assertEqual(plan.workload.kind, "analytical_read")
        self.assertEqual(plan.workload.preferred_route, "duckdb")
        self.assertIn("benchmark-calibrated", plan.workload.reason)

    def test_query_plan_carries_cypher_shape_metadata(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            (
                "MATCH (a:User)-[:KNOWS]->(b:User) "
                "WHERE b.name = $name "
                "RETURN a.name "
                "ORDER BY a.name "
                "LIMIT 1"
            ),
            route="sqlite",
            params={"name": "Bob"},
        )

        self.assertEqual(plan.query_type, "cypher")
        self.assertIsNotNone(plan.cypher_plan)
        self.assertIsNotNone(plan.cypher_shape)
        assert plan.cypher_shape is not None
        self.assertEqual(plan.cypher_shape.plan_name, "MatchRelationshipPlan")
        self.assertTrue(plan.cypher_shape.is_read_only)
        self.assertEqual(plan.cypher_shape.pattern_kind, "relationship")
        self.assertEqual(plan.cypher_shape.predicate_count, 1)
        self.assertTrue(plan.cypher_shape.has_order_by)
        self.assertTrue(plan.cypher_shape.has_limit)
        self.assertEqual(plan.workload.kind, "graph_read")
        self.assertEqual(plan.workload.preferred_route, "sqlite")
        self.assertIn("not broad enough", plan.workload.reason)

    def test_query_plan_carries_sql_vector_plan_metadata(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT $limit",
            route=None,
            params={"query": [1.0, 0.0], "limit": 2},
        )

        self.assertEqual(plan.query_type, "vector")
        self.assertEqual(plan.route, "sqlite")
        self.assertIsNotNone(plan.vector_plan)
        assert plan.vector_plan is not None
        self.assertEqual(type(plan.vector_plan).__name__, "SQLVectorQueryPlan")
        self.assertEqual(plan.vector_plan.metric, "cosine")
        self.assertEqual(plan.vector_plan.top_k, 2)
        self.assertEqual(
            type(plan.vector_plan.candidate_query).__name__,
            "SQLCandidateQueryPlan",
        )
        self.assertEqual(plan.vector_plan.candidate_query.target, "sql_row")
        self.assertEqual(plan.vector_plan.candidate_query.namespace, "docs")

    def test_query_plan_carries_cypher_vector_plan_metadata(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            (
                "MATCH (u:User) "
                "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT $limit) "
                "RETURN u.id"
            ),
            route=None,
            params={"query": [1.0, 0.0], "limit": 1},
        )

        self.assertEqual(plan.query_type, "vector")
        self.assertEqual(plan.route, "sqlite")
        self.assertIsNotNone(plan.vector_plan)
        assert plan.vector_plan is not None
        self.assertEqual(type(plan.vector_plan).__name__, "CypherVectorQueryPlan")
        self.assertEqual(plan.vector_plan.metric, "cosine")
        self.assertEqual(plan.vector_plan.top_k, 1)
        self.assertEqual(
            type(plan.vector_plan.candidate_query).__name__,
            "CypherCandidateQueryPlan",
        )
        self.assertEqual(plan.vector_plan.candidate_query.target, "graph_node")
        self.assertEqual(plan.vector_plan.candidate_query.namespace, "")

    def test_execute_query_plan_prefers_explicit_vector_plan_shape(self) -> None:
        HumemDB = humemdb_class()
        db_module = importlib.import_module("humemdb.db")
        humemdb_module = importlib.import_module("humemdb")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT $limit",
            route=None,
            params={"query": [1.0, 0.0], "limit": 2},
        )
        inconsistent_plan = replace(plan, query_type="sql")
        expected = humemdb_module.QueryResult(
            rows=(),
            columns=("target", "namespace", "target_id", "score"),
            route="sqlite",
            query_type="sql",
            rowcount=0,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with mock.patch.object(
                    db,
                    "_execute_vector_query",
                    return_value=expected,
                ) as execute_vector:
                    with mock.patch.object(
                        db,
                        "_execute_sql_query_plan",
                    ) as execute_sql:
                        with mock.patch.object(
                            db,
                            "_execute_cypher_query_plan",
                        ) as execute_cypher:
                            result = getattr(
                                db,
                                "_execute_query_plan",
                            )(inconsistent_plan)

        self.assertIs(result, expected)
        execute_vector.assert_called_once_with(inconsistent_plan)
        execute_sql.assert_not_called()
        execute_cypher.assert_not_called()

    def test_query_plan_classifies_simple_sql_as_transactional_read(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            "SELECT id, name FROM users WHERE id = $id",
            route="sqlite",
            params={"id": 1},
        )

        self.assertEqual(plan.workload.kind, "transactional_read")
        self.assertTrue(plan.workload.is_read_only)
        self.assertEqual(plan.workload.preferred_route, "sqlite")

    def test_sql_analytical_read_only_prefers_duckdb_after_calibration(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        sql_module = importlib.import_module("humemdb.sql")
        classify_workload = getattr(db_module, "_classify_workload")
        threshold_type = getattr(db_module, "_OlapRoutingThresholds")

        sql_plan = sql_module.translate_sql_plan(
            (
                "WITH recent AS (SELECT customer_id, amount FROM payments) "
                "SELECT c.name, COUNT(*) AS total "
                "FROM recent AS r "
                "JOIN customers AS c ON c.id = r.customer_id "
                "GROUP BY c.name ORDER BY total DESC LIMIT 5"
            ),
            target="duckdb",
        )
        workload = classify_workload(
            "sql",
            sql_plan=sql_plan,
            cypher_shape=None,
            olap_thresholds=threshold_type(
                benchmark_calibrated=True,
                min_join_count=1,
                min_aggregate_count=1,
                min_cte_count=1,
                min_window_count=0,
                require_order_by_or_limit=True,
            ),
        )

        self.assertEqual(workload.kind, "analytical_read")
        self.assertTrue(workload.is_read_only)
        self.assertEqual(workload.preferred_route, "duckdb")
        self.assertIn("benchmark-calibrated OLAP thresholds", workload.reason)

    def test_selective_sql_join_lookup_stays_sqlite_by_default(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            (
                "SELECT e.id, u.name FROM events AS e "
                "JOIN users AS u ON u.id = e.user_id "
                "WHERE e.id = $event_id"
            ),
            route="sqlite",
            params={"event_id": 1},
        )

        self.assertEqual(plan.workload.kind, "analytical_read")
        self.assertEqual(plan.workload.preferred_route, "sqlite")
        self.assertIn("admission stays disabled", plan.workload.reason)

    def test_windowed_sql_read_prefers_duckdb_by_default(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            (
                "SELECT user_id, "
                "ROW_NUMBER() OVER (PARTITION BY kind ORDER BY ts DESC) AS rank "
                "FROM events"
            ),
            route="duckdb",
            params=None,
        )

        assert plan.sql_plan is not None
        self.assertEqual(plan.sql_plan.window_count, 1)
        self.assertEqual(plan.workload.kind, "analytical_read")
        self.assertEqual(plan.workload.preferred_route, "duckdb")

    def test_read_only_cypher_stays_sqlite_by_default(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            "MATCH (u:User) RETURN u.name LIMIT 5",
            route="sqlite",
            params=None,
        )

        self.assertEqual(plan.workload.kind, "graph_read")
        self.assertEqual(plan.workload.preferred_route, "sqlite")
        self.assertIn("not broad enough", plan.workload.reason)

    def test_query_plan_classifies_cypher_create_as_graph_write(self) -> None:
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

        plan = plan_query(
            "CREATE (u:User {name: 'Alice'})",
            route="sqlite",
            params=None,
        )

        self.assertEqual(plan.workload.kind, "graph_write")
        self.assertFalse(plan.workload.is_read_only)
        self.assertEqual(plan.workload.preferred_route, "sqlite")

    def test_duckdb_rejects_cypher_writes_before_execution(self) -> None:
        HumemDB = humemdb_class()
        db_module = importlib.import_module("humemdb.db")

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with mock.patch.object(
                db_module,
                "execute_cypher",
                wraps=db_module.execute_cypher,
            ) as execute_cypher:
                with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                    with self.assertRaisesRegex(
                        ValueError,
                        "does not allow direct Cypher writes to DuckDB",
                    ):
                        db.query("CREATE (u:User {name: 'Alice'})", route="duckdb")

                self.assertEqual(execute_cypher.call_count, 0)
