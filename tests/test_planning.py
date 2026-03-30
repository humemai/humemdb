from __future__ import annotations

from dataclasses import replace
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from humemdb import HumemDB, QueryResult
import humemdb.cypher as humemdb_cypher
import humemdb.db
from humemdb.sql import translate_sql_plan


class TestPlanning(unittest.TestCase):
    def test_query_reuses_parsed_cypher_plan_during_execution(self) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        generated_lower = db_module._lower_generated_cypher_text

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with mock.patch.object(
                db_module,
                "_lower_generated_cypher_text",
                wraps=generated_lower,
            ) as generated_parse:
                with mock.patch.object(
                    cypher_module,
                    "parse_cypher",
                    wraps=cypher_module.parse_cypher,
                ) as plan_parse:
                    with mock.patch.object(
                        cypher_module,
                        "parse_cypher",
                        wraps=cypher_module.parse_cypher,
                    ) as execute_parse:
                        with HumemDB(base_path) as db:
                            result = db.query(
                                "CREATE (u:User {name: 'Alice', age: 30})"
                            )

                        self.assertEqual(result.query_type, "cypher")
                        self.assertEqual(generated_parse.call_count, 1)
                        self.assertEqual(plan_parse.call_count, 0)
                        self.assertEqual(execute_parse.call_count, 0)

    def test_query_planning_uses_generated_frontend_for_reverse_rel_create(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "CREATE (a:User)<-[r:KNOWS {since: 2020}]-(b:User)"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "CreateRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_self_loop_create(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "CREATE (root:Root)-[:LINK]->(root:Root)"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "CreateRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_separate_pattern_create(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "CREATE (a:A {name: 'Alice'}), (b:B {name: 'Bob'}), (a)-[:R]->(b)"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(
            type(plan.cypher_plan).__name__,
            "CreateRelationshipFromSeparatePatternsPlan",
        )
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_match_create_self_loop(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (root:Root) CREATE (root)-[:LINK]->(root)"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(
            type(plan.cypher_plan).__name__,
            "MatchCreateRelationshipPlan",
        )
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_match_create_new_endpoint(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End)"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(
            type(plan.cypher_plan).__name__,
            "MatchCreateRelationshipPlan",
        )
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_match_create_new_start_node(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (x:End) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(
            type(plan.cypher_plan).__name__,
            "MatchCreateRelationshipPlan",
        )
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_two_node_match_create(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (x:Begin), (y:End) "
            "WHERE y.name = 'finish' CREATE (x)-[:TYPE]->(y)"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(
            type(plan.cypher_plan).__name__,
            "MatchCreateRelationshipBetweenNodesPlan",
        )
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_reverse_two_node_match_create(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (x:Begin), (y:End) "
            "WHERE x.name = 'start' CREATE (x)<-[:TYPE]-(y)"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(
            type(plan.cypher_plan).__name__,
            "MatchCreateRelationshipBetweenNodesPlan",
        )
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_match_set(self) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (u:User {name: 'Alice'}) SET u.age = 31"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetNodePlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_multi_assignment_match_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (u:User {name: 'Alice'}) "
            "SET u.age = 31, u.active = true"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetNodePlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_reverse_relationship_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (b:User)<-[r:KNOWS]-(a:User) "
            "WHERE r.since = $since "
            "RETURN a.name, r.since, b.name ORDER BY r.since DESC LIMIT 1"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params={"since": 2020})

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_inequality_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (u:User) "
            "WHERE u.age >= $age "
            "RETURN u.name ORDER BY u.age LIMIT 1"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params={"age": 30})

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchNodePlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_top_level_or_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (u:User) "
            "WHERE u.age >= $age OR u.name = $name "
            "RETURN u.name ORDER BY u.name"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(
                    query,
                    route="sqlite",
                    params={"age": 40, "name": "Alice"},
                )

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchNodePlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_parenthesized_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (u:User) "
            "WHERE (u.age >= $age OR u.name = $name) AND u.active = $active "
            "RETURN u.name ORDER BY u.name"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(
                    query,
                    route="sqlite",
                    params={"age": 40, "name": "Alice", "active": True},
                )

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchNodePlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_multi_type_relationship_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) "
            "RETURN b.name, r.type ORDER BY b.name"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_untyped_relationship_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (a:User)-[r]->(b:User) RETURN b.name, r.type ORDER BY b.name"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_anonymous_node_create(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "CREATE (:User {name: 'Alice'})"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "CreateNodePlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_anonymous_rel_endpoints(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (:User {name: 'Alice'})-[r]->(:User) "
            "RETURN r.type ORDER BY r.type"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_multi_type_rel_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) SET r.strength = 5"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_untyped_relationship_match_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (a:User)-[r]->(b:User) SET r.strength = 5"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_anon_endpoint_rel_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (:User {name: 'Alice'})-[r]->(:User) "
            "SET r.strength = 7"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params=None)

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_mixed_and_or_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (u:User) "
            "WHERE u.age >= $age AND u.active = $active OR u.name = $name "
            "RETURN u.name ORDER BY u.name"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(
                    query,
                    route="sqlite",
                    params={"age": 40, "active": True, "name": "Alice"},
                )

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchNodePlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_mixed_and_or_relationship_match(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.since >= $since AND r.strength >= $strength OR b.name = $name "
            "RETURN b.name ORDER BY b.name"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(
                    query,
                    route="sqlite",
                    params={"since": 2022, "strength": 2, "name": "Bob"},
                )

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "MatchRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_relationship_match_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.name = $name SET r.since = 2021, r.strength = 2"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params={"name": "Alice"})

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_reverse_relationship_match_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (b:User)<-[r:KNOWS]-(a:User) "
            "WHERE a.name = $name SET r.since = 2022, r.strength = 3"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(query, route="sqlite", params={"name": "Alice"})

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_rel_mixed_bool_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.since >= $since AND r.strength >= $strength OR b.name = $name "
            "SET r.since = 2030, r.strength = 9"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(
                    query,
                    route="sqlite",
                    params={"since": 2022, "strength": 2, "name": "Bob"},
                )

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_uses_generated_frontend_for_reverse_rel_mixed_bool_set(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = (
            "MATCH (b:User)<-[r:KNOWS]-(a:User) "
            "WHERE r.since >= $since AND r.strength >= $strength OR b.name = $name "
            "SET r.since = 2040, r.strength = 7"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                plan = plan_query(
                    query,
                    route="sqlite",
                    params={"since": 2022, "strength": 2, "name": "Bob"},
                )

        self.assertEqual(plan.query_type, "cypher")
        self.assertEqual(type(plan.cypher_plan).__name__, "SetRelationshipPlan")
        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

        handwritten_plan = cypher_module.parse_cypher(query)
        self.assertEqual(plan.cypher_plan, handwritten_plan)

    def test_query_planning_rejects_relationship_set_alias_mismatch_without_fallback(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (a:User)-[r:KNOWS]->(b:User) SET a.name = 'Bob'"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                with self.assertRaisesRegex(ValueError, "matched relationship alias"):
                    plan_query(query, route="sqlite", params=None)

        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

    def test_query_planning_rejects_unknown_where_alias_without_fallback(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (u:User {name: 'Alice'}) WHERE v.age = 31 RETURN u.name"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                with self.assertRaisesRegex(ValueError, "unknown alias"):
                    plan_query(query, route="sqlite", params=None)

        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

    def test_query_planning_rejects_multi_part_match_without_fallback(
        self,
    ) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (u:User) WITH u RETURN u.name"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                with self.assertRaisesRegex(
                    ValueError,
                    "Generated Cypher frontend currently validates only single-part",
                ):
                    plan_query(query, route="sqlite", params=None)

        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

    def test_query_planning_caches_identical_cypher_text(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text
        cached_plan = db_module._plan_cypher_query

        cached_plan.cache_clear()
        query = (
            "CREATE ("
            "a:User {name: $a_name, age: $a_age, active: $a_active}"
            ")-[r:KNOWS {since: $since_one, strength: $strength_one}]->("
            "b:User {name: $b_name, age: $b_age, active: $b_active}"
            ")"
        )

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            first = plan_query(
                query,
                route="sqlite",
                params={
                    "a_name": "Alice",
                    "a_age": 30,
                    "a_active": True,
                    "b_name": "Bob",
                    "b_age": 31,
                    "b_active": True,
                    "since_one": 2020,
                    "strength_one": 5,
                },
            )
            second = plan_query(
                query,
                route="sqlite",
                params={
                    "a_name": "Carol",
                    "a_age": 32,
                    "a_active": True,
                    "b_name": "Dave",
                    "b_age": 33,
                    "b_active": False,
                    "since_one": 2021,
                    "strength_one": 6,
                },
            )

        self.assertEqual(generated_parse.call_count, 1)
        self.assertIs(first.cypher_plan, second.cypher_plan)
        self.assertIs(first.cypher_shape, second.cypher_shape)

    def test_query_planning_keeps_generated_syntax_errors(self) -> None:
        db_module = humemdb.db
        cypher_module = humemdb_cypher
        plan_query = db_module._plan_query
        generated_lower = db_module._lower_generated_cypher_text

        query = "MATCH (u RETURN u"

        with mock.patch.object(
            db_module,
            "_lower_generated_cypher_text",
            wraps=generated_lower,
        ) as generated_parse:
            with mock.patch.object(
                cypher_module,
                "parse_cypher",
                wraps=cypher_module.parse_cypher,
            ) as handwritten_parse:
                with self.assertRaisesRegex(
                    ValueError,
                    "Generated Cypher frontend reported syntax errors",
                ):
                    plan_query(query, route="sqlite", params=None)

        self.assertEqual(generated_parse.call_count, 1)
        self.assertEqual(handwritten_parse.call_count, 0)

    def test_sql_translation_plan_exposes_shape_metadata(self) -> None:
        plan = translate_sql_plan(
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
        db_module = humemdb.db
        plan_query = db_module._plan_query

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
        self.assertEqual(plan.sql_plan.exists_count, 0)
        self.assertTrue(plan.sql_plan.has_order_by)
        self.assertTrue(plan.sql_plan.has_limit)
        self.assertTrue(plan.sql_plan.has_group_by)
        self.assertEqual(plan.workload.kind, "analytical_read")
        self.assertEqual(plan.workload.preferred_route, "duckdb")
        self.assertIn("benchmark-calibrated", plan.workload.reason)
        self.assertEqual(plan.route_decision.source, "explicit")
        self.assertIn("matches the workload preference", plan.route_decision.reason)

    def test_query_plan_carries_cypher_shape_metadata(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

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
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT $limit",
            route=None,
            params={"query": [1.0, 0.0], "limit": 2},
        )

        self.assertEqual(plan.query_type, "vector")
        self.assertEqual(plan.route, "sqlite")
        self.assertEqual(plan.route_decision.source, "automatic")
        self.assertIn("Auto-selected 'sqlite'", plan.route_decision.reason)
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
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            (
                "CALL db.index.vector.queryNodes("
                "'user_embedding_idx', $limit, $query) "
                "YIELD node, score MATCH (node:User) "
                "RETURN node.id, score"
            ),
            route=None,
            params={"query": [1.0, 0.0], "limit": 1},
        )

        self.assertEqual(plan.query_type, "vector")
        self.assertEqual(plan.route, "sqlite")
        self.assertEqual(plan.route_decision.source, "automatic")
        self.assertIsNotNone(plan.vector_plan)
        assert plan.vector_plan is not None
        self.assertEqual(type(plan.vector_plan).__name__, "CypherVectorQueryPlan")
        self.assertEqual(plan.vector_plan.index_name, "user_embedding_idx")
        self.assertEqual(plan.vector_plan.top_k, 1)
        self.assertEqual(
            type(plan.vector_plan.candidate_query).__name__,
            "CypherCandidateQueryPlan",
        )
        self.assertEqual(plan.vector_plan.candidate_query.target, "graph_node")
        self.assertEqual(plan.vector_plan.candidate_query.namespace, "")

    def test_query_plan_carries_named_cypher_vector_plan_metadata(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            (
                "CALL db.index.vector.queryNodes("
                "'user_embedding_idx', $limit, $query) "
                "YIELD node, score MATCH (node:User) "
                "RETURN node.id, score"
            ),
            route=None,
            params={"query": [1.0, 0.0], "limit": 1},
        )

        self.assertEqual(plan.query_type, "vector")
        self.assertEqual(plan.route, "sqlite")
        self.assertEqual(plan.route_decision.source, "automatic")
        self.assertIsNotNone(plan.vector_plan)
        assert plan.vector_plan is not None
        self.assertEqual(type(plan.vector_plan).__name__, "CypherVectorQueryPlan")
        self.assertEqual(plan.vector_plan.index_name, "user_embedding_idx")
        self.assertEqual(plan.vector_plan.top_k, 1)

    def test_query_plan_carries_neo4j_like_query_nodes_metadata(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            (
                "CALL db.index.vector.queryNodes('user_embedding_idx', $limit, $query) "
                "YIELD node, score RETURN node.id, score"
            ),
            route=None,
            params={"query": [1.0, 0.0], "limit": 1},
        )

        self.assertEqual(plan.query_type, "vector")
        self.assertEqual(plan.route, "sqlite")
        self.assertEqual(plan.route_decision.source, "automatic")
        self.assertIsNotNone(plan.vector_plan)
        assert plan.vector_plan is not None
        self.assertEqual(type(plan.vector_plan).__name__, "CypherVectorQueryPlan")
        self.assertEqual(plan.vector_plan.index_name, "user_embedding_idx")
        self.assertEqual(plan.vector_plan.top_k, 1)
        self.assertEqual(plan.vector_plan.result_mode, "queryNodes")
        self.assertEqual(plan.vector_plan.return_items, ("node.id", "score"))

    def test_execute_query_plan_prefers_explicit_vector_plan_shape(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT $limit",
            route=None,
            params={"query": [1.0, 0.0], "limit": 2},
        )
        inconsistent_plan = replace(plan, query_type="sql")
        expected = QueryResult(
            rows=(),
            columns=("target", "namespace", "target_id", "score"),
            route="sqlite",
            query_type="sql",
            rowcount=0,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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
                            result = db._execute_query_plan(inconsistent_plan)

        self.assertIs(result, expected)
        execute_vector.assert_called_once_with(inconsistent_plan)
        execute_sql.assert_not_called()
        execute_cypher.assert_not_called()

    def test_query_plan_classifies_simple_sql_as_transactional_read(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            "SELECT id, name FROM users WHERE id = $id",
            route="sqlite",
            params={"id": 1},
        )

        self.assertEqual(plan.workload.kind, "transactional_read")
        self.assertTrue(plan.workload.is_read_only)
        self.assertEqual(plan.workload.preferred_route, "sqlite")

    def test_query_type_inference_recognizes_broader_cypher_prefixes(self) -> None:
        db_module = humemdb.db
        infer_query_type = db_module._infer_query_type

        self.assertEqual(
            infer_query_type("OPTIONAL MATCH (u:User) RETURN u.name"),
            "cypher",
        )
        self.assertEqual(
            infer_query_type("MERGE (u:User {name: 'Alice'})"),
            "cypher",
        )
        self.assertEqual(
            infer_query_type("UNWIND [1, 2, 3] AS x RETURN x"),
            "cypher",
        )
        self.assertEqual(
            infer_query_type("CALL db.labels()"),
            "cypher",
        )
        self.assertEqual(
            infer_query_type("RETURN 1 AS value"),
            "cypher",
        )

    def test_query_type_inference_keeps_sql_cte_as_sql(self) -> None:
        db_module = humemdb.db
        infer_query_type = db_module._infer_query_type

        self.assertEqual(
            infer_query_type(
                "WITH recent AS (SELECT id FROM events) SELECT id FROM recent"
            ),
            "sql",
        )

    def test_query_plan_records_automatic_route_selection(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            (
                "SELECT kind, COUNT(*) AS total FROM events "
                "GROUP BY kind ORDER BY total DESC LIMIT 3"
            ),
            route=None,
            params=None,
        )

        self.assertEqual(plan.route, "duckdb")
        self.assertEqual(plan.route_decision.source, "automatic")
        self.assertIn("Auto-selected 'duckdb'", plan.route_decision.reason)

    def test_query_plan_records_explicit_route_override(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            (
                "SELECT kind, COUNT(*) AS total FROM events "
                "GROUP BY kind ORDER BY total DESC LIMIT 3"
            ),
            route="sqlite",
            params=None,
        )

        self.assertEqual(plan.route, "sqlite")
        self.assertEqual(plan.workload.preferred_route, "duckdb")
        self.assertEqual(plan.route_decision.source, "explicit")
        self.assertIn(
            "overrides the workload preference 'duckdb'",
            plan.route_decision.reason,
        )

    def test_sql_analytical_read_only_prefers_duckdb_after_calibration(self) -> None:
        db_module = humemdb.db
        classify_workload = db_module._classify_workload
        threshold_type = db_module._OlapRoutingThresholds
        rule_type = db_module._OlapRoutingRule

        sql_plan = translate_sql_plan(
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
                rules=(
                    rule_type(
                        min_join_count=1,
                        min_aggregate_count=1,
                        min_cte_count=1,
                        require_group_by=True,
                        require_order_by_or_limit=True,
                    ),
                ),
            ),
        )

        self.assertEqual(workload.kind, "analytical_read")
        self.assertTrue(workload.is_read_only)
        self.assertEqual(workload.preferred_route, "duckdb")
        self.assertIn("benchmark-calibrated OLAP thresholds", workload.reason)

    def test_selective_sql_join_lookup_stays_transactional_sqlite_by_default(
        self,
    ) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            (
                "SELECT e.id, u.name FROM events AS e "
                "JOIN users AS u ON u.id = e.user_id "
                "WHERE e.id = $event_id"
            ),
            route="sqlite",
            params={"event_id": 1},
        )

        self.assertEqual(plan.workload.kind, "transactional_read")
        self.assertEqual(plan.workload.preferred_route, "sqlite")
        self.assertEqual(
            plan.workload.reason,
            "Simple read-only SQL stays classified as transactional.",
        )

    def test_join_heavy_ordered_sql_read_prefers_duckdb_when_broad_enough(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            (
                "SELECT e.id, u.name, t.tag FROM events AS e "
                "JOIN users AS u ON u.id = e.user_id "
                "JOIN event_tags AS t ON t.event_id = e.id "
                "ORDER BY e.ts DESC LIMIT 100"
            ),
            route=None,
            params=None,
        )

        self.assertEqual(plan.workload.kind, "analytical_read")
        self.assertEqual(plan.workload.preferred_route, "duckdb")
        self.assertEqual(plan.route, "duckdb")

    def test_windowed_sql_read_prefers_duckdb_by_default(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

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

    def test_exists_filtered_sql_read_prefers_duckdb_with_matching_rule(self) -> None:
        db_module = humemdb.db
        classify_workload = db_module._classify_workload
        threshold_type = db_module._OlapRoutingThresholds
        rule_type = db_module._OlapRoutingRule

        sql_plan = translate_sql_plan(
            (
                "SELECT topic, token_count FROM memory_chunks m "
                "WHERE EXISTS ("
                "SELECT 1 FROM users u "
                "WHERE u.id = m.owner_user_id AND u.tier = 'pro'"
                ") ORDER BY importance DESC LIMIT 100"
            ),
            target="duckdb",
        )

        self.assertEqual(sql_plan.exists_count, 1)

        workload = classify_workload(
            "sql",
            sql_plan=sql_plan,
            cypher_shape=None,
            olap_thresholds=threshold_type(
                benchmark_calibrated=True,
                rules=(
                    rule_type(
                        min_exists_count=1,
                        require_order_by_or_limit=True,
                    ),
                ),
            ),
        )

        self.assertEqual(workload.kind, "analytical_read")
        self.assertEqual(workload.preferred_route, "duckdb")

    def test_distinct_join_sql_read_prefers_duckdb_with_matching_rule(self) -> None:
        db_module = humemdb.db
        classify_workload = db_module._classify_workload
        threshold_type = db_module._OlapRoutingThresholds
        rule_type = db_module._OlapRoutingRule

        sql_plan = translate_sql_plan(
            (
                "SELECT DISTINCT users.region, documents.language "
                "FROM documents "
                "JOIN users ON users.id = documents.owner_user_id "
                "WHERE documents.status = 'published'"
            ),
            target="duckdb",
        )

        self.assertEqual(sql_plan.join_count, 1)
        self.assertTrue(sql_plan.has_distinct)

        workload = classify_workload(
            "sql",
            sql_plan=sql_plan,
            cypher_shape=None,
            olap_thresholds=threshold_type(
                benchmark_calibrated=True,
                rules=(
                    rule_type(
                        min_join_count=1,
                        require_distinct=True,
                    ),
                ),
            ),
        )

        self.assertEqual(workload.kind, "analytical_read")
        self.assertEqual(workload.preferred_route, "duckdb")

    def test_read_only_cypher_stays_sqlite_by_default(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            "MATCH (u:User) RETURN u.name LIMIT 5",
            route="sqlite",
            params=None,
        )

        self.assertEqual(plan.workload.kind, "graph_read")
        self.assertEqual(plan.workload.preferred_route, "sqlite")
        self.assertIn("not broad enough", plan.workload.reason)

    def test_query_plan_classifies_cypher_create_as_graph_write(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            "CREATE (u:User {name: 'Alice'})",
            route="sqlite",
            params=None,
        )

        self.assertEqual(plan.workload.kind, "graph_write")
        self.assertFalse(plan.workload.is_read_only)
        self.assertEqual(plan.workload.preferred_route, "sqlite")

    def test_query_plan_classifies_two_node_match_create_as_graph_write(
        self,
    ) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        plan = plan_query(
            "MATCH (x:Begin), (y:End) CREATE (x)-[:TYPE]->(y)",
            route="sqlite",
            params=None,
        )

        self.assertEqual(plan.workload.kind, "graph_write")
        self.assertFalse(plan.workload.is_read_only)
        self.assertEqual(plan.workload.preferred_route, "sqlite")
        self.assertEqual(
            type(plan.cypher_plan).__name__,
            "MatchCreateRelationshipBetweenNodesPlan",
        )

    def test_duckdb_rejects_cypher_writes_before_execution(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with mock.patch.object(
                db_module,
                "_execute_cypher",
                wraps=db_module.__dict__["_execute_cypher"],
            ) as execute_cypher:
                with HumemDB(base_path) as db:
                    plan = plan_query(
                        "CREATE (u:User {name: 'Alice'})",
                        route="duckdb",
                        params=None,
                    )
                    with self.assertRaisesRegex(
                        ValueError,
                        "does not allow direct Cypher writes to DuckDB",
                    ):
                        db._execute_cypher_query_plan(plan)

                self.assertEqual(execute_cypher.call_count, 0)

    def test_query_plan_can_load_sql_olap_thresholds_from_report_env(self) -> None:
        db_module = humemdb.db
        plan_query = db_module._plan_query
        load_thresholds = db_module._load_sql_olap_thresholds_from_path
        load_thresholds.cache_clear()

        report_payload = {
            "recommended_runtime": {
                "sql_olap_thresholds": {
                    "benchmark_calibrated": False,
                    "min_join_count": 2,
                    "min_aggregate_count": 1,
                    "min_cte_count": 0,
                    "min_window_count": 0,
                    "require_order_by_or_limit": True,
                    "rules": [
                        {
                            "min_exists_count": 1,
                            "require_order_by_or_limit": True,
                        }
                    ],
                }
            }
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "routing-thresholds.json"
            report_path.write_text(json.dumps(report_payload), encoding="utf-8")

            with mock.patch.dict(
                os.environ,
                {"HUMEMDB_SQL_OLAP_THRESHOLDS_PATH": str(report_path)},
                clear=False,
            ):
                plan = plan_query(
                    (
                        "SELECT kind, COUNT(*) AS total FROM events "
                        "GROUP BY kind ORDER BY total DESC LIMIT 3"
                    ),
                    route=None,
                    params=None,
                )

        self.assertEqual(plan.workload.kind, "analytical_read")
        self.assertEqual(plan.workload.preferred_route, "sqlite")
        self.assertIn("admission stays disabled", plan.workload.reason)
        load_thresholds.cache_clear()
