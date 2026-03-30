from __future__ import annotations

import unittest

import humemdb.cypher as humemdb_cypher
import humemdb.cypher_frontend as humemdb_cypher_frontend


class TestCypherFrontend(unittest.TestCase):
    def test_generated_frontend_parses_current_create_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        result = frontend.parse_cypher_text(
            "CREATE (u:User {name: 'Alice', age: 30})"
        )

        self.assertFalse(result.has_errors)
        self.assertEqual(type(result.tree).__name__, "OC_CypherContext")
        self.assertEqual(
            result.source_text,
            "CREATE (u:User {name: 'Alice', age: 30})",
        )
        self.assertIsNotNone(result.token_stream)

    def test_generated_frontend_parses_current_match_where_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        result = frontend.parse_cypher_text(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertFalse(result.has_errors)
        self.assertEqual(type(result.tree).__name__, "OC_CypherContext")

    def test_generated_frontend_reports_syntax_errors(self) -> None:
        frontend = humemdb_cypher_frontend

        result = frontend.parse_cypher_text("MATCH (u RETURN u")

        self.assertTrue(result.has_errors)
        self.assertGreaterEqual(len(result.syntax_errors), 1)

    def test_generated_frontend_normalizes_current_create_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "CREATE (u:User {name: 'Alice', age: 30})"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedCreateNode")
        self.assertEqual(normalized.kind, "create")
        self.assertEqual(normalized.pattern_kind, "node")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(
            normalized.node.properties,
            (("name", "Alice"), ("age", 30)),
        )

    def test_generated_frontend_normalizes_reverse_relationship_create_shape(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "CREATE (a:User)<-[r:KNOWS {since: 2020}]-(b:User)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedCreateRelationship")
        self.assertEqual(normalized.left.alias, "a")
        self.assertEqual(normalized.right.alias, "b")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertEqual(normalized.relationship.type_name, "KNOWS")
        self.assertEqual(normalized.relationship.direction, "in")
        self.assertEqual(normalized.relationship.properties, (("since", 2020),))

    def test_generated_frontend_normalizes_separate_pattern_relationship_create(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "CREATE (a:A {name: 'Alice'}), (b:B {name: 'Bob'}), (a)-[:R]->(b)"
        )

        self.assertEqual(
            type(normalized).__name__,
            "NormalizedCreateRelationshipFromSeparatePatterns",
        )
        self.assertEqual(normalized.first_node.alias, "a")
        self.assertEqual(normalized.first_node.label, "A")
        self.assertEqual(normalized.second_node.alias, "b")
        self.assertEqual(normalized.second_node.label, "B")
        self.assertEqual(normalized.left.alias, "a")
        self.assertEqual(normalized.right.alias, "b")
        self.assertEqual(normalized.relationship.type_name, "R")

    def test_generated_frontend_normalizes_self_loop_create_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "CREATE (root:Root)-[:LINK]->(root:Root)"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedCreateRelationship")
        self.assertEqual(normalized.left.alias, "root")
        self.assertEqual(normalized.right.alias, "root")
        self.assertEqual(normalized.left.label, "Root")
        self.assertEqual(normalized.right.label, "Root")
        self.assertEqual(normalized.relationship.type_name, "LINK")
        self.assertEqual(normalized.relationship.direction, "out")

    def test_generated_frontend_normalizes_match_create_self_loop_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (root:Root) CREATE (root)-[:LINK]->(root)"
        )

        self.assertEqual(
            type(normalized).__name__,
            "NormalizedMatchCreateRelationship",
        )
        self.assertEqual(normalized.match_node.alias, "root")
        self.assertEqual(normalized.left.alias, "root")
        self.assertEqual(normalized.right.alias, "root")
        self.assertEqual(normalized.relationship.type_name, "LINK")

    def test_generated_frontend_normalizes_match_create_with_new_endpoint(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End)"
        )

        self.assertEqual(
            type(normalized).__name__,
            "NormalizedMatchCreateRelationship",
        )
        self.assertEqual(normalized.match_node.alias, "x")
        self.assertEqual(normalized.left.alias, "x")
        self.assertEqual(normalized.right.label, "End")
        self.assertEqual(normalized.right.alias, "__humem_match_create_right_node")
        self.assertEqual(normalized.relationship.type_name, "TYPE")

    def test_generated_frontend_normalizes_match_create_with_new_start_node(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (x:End) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)"
        )

        self.assertEqual(
            type(normalized).__name__,
            "NormalizedMatchCreateRelationship",
        )
        self.assertEqual(normalized.match_node.alias, "x")
        self.assertEqual(normalized.left.label, "Begin")
        self.assertEqual(normalized.left.alias, "__humem_match_create_left_node")
        self.assertEqual(normalized.left.properties, (("name", "start"),))
        self.assertEqual(normalized.right.alias, "x")
        self.assertEqual(normalized.relationship.type_name, "TYPE")

    def test_generated_frontend_normalizes_two_node_match_create_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (x:Begin), (y:End) WHERE y.name = 'finish' CREATE (x)-[:TYPE]->(y)"
        )

        self.assertEqual(
            type(normalized).__name__,
            "NormalizedMatchCreateRelationshipBetweenNodes",
        )
        self.assertEqual(normalized.left_match.alias, "x")
        self.assertEqual(normalized.right_match.alias, "y")
        self.assertEqual(len(normalized.predicates), 1)
        self.assertEqual(normalized.predicates[0].alias, "y")
        self.assertEqual(normalized.left.alias, "x")
        self.assertEqual(normalized.right.alias, "y")
        self.assertEqual(normalized.relationship.type_name, "TYPE")

    def test_generated_frontend_normalizes_two_node_reverse_match_create_shape(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (x:Begin), (y:End) WHERE x.name = 'start' CREATE (x)<-[:TYPE]-(y)"
        )

        self.assertEqual(
            type(normalized).__name__,
            "NormalizedMatchCreateRelationshipBetweenNodes",
        )
        self.assertEqual(normalized.left_match.alias, "x")
        self.assertEqual(normalized.right_match.alias, "y")
        self.assertEqual(len(normalized.predicates), 1)
        self.assertEqual(normalized.predicates[0].alias, "x")
        self.assertEqual(normalized.left.alias, "x")
        self.assertEqual(normalized.right.alias, "y")
        self.assertEqual(normalized.relationship.type_name, "TYPE")
        self.assertEqual(normalized.relationship.direction, "in")

    def test_generated_frontend_normalizes_current_match_where_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (u:User) WHERE u.name = $name RETURN u.name ORDER BY u.name LIMIT 1"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.kind, "match")
        self.assertEqual(normalized.pattern_kind, "node")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(len(normalized.predicates), 1)
        self.assertEqual(normalized.predicates[0].alias, "u")
        self.assertEqual(normalized.predicates[0].field, "name")
        self.assertEqual(normalized.predicates[0].operator, "=")
        self.assertEqual(normalized.returns[0].column_name, "u.name")
        self.assertEqual(normalized.order_by[0].alias, "u")
        self.assertEqual(normalized.order_by[0].field, "name")
        self.assertEqual(normalized.limit, 1)

    def test_generated_frontend_normalizes_distinct_skip_match_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (u:User) RETURN DISTINCT u.name ORDER BY u.name SKIP 1 LIMIT 2"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertTrue(normalized.distinct)
        self.assertEqual(normalized.skip, 1)
        self.assertEqual(normalized.limit, 2)
        self.assertEqual(normalized.returns[0].column_name, "u.name")
        self.assertEqual(normalized.order_by[0].alias, "u")
        self.assertEqual(normalized.order_by[0].field, "name")

    def test_generated_frontend_normalizes_offset_match_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (u:User) RETURN u.name ORDER BY u.name OFFSET 2 LIMIT 3"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertFalse(normalized.distinct)
        self.assertEqual(normalized.skip, 2)
        self.assertEqual(normalized.limit, 3)
        self.assertEqual(normalized.returns[0].column_name, "u.name")

    def test_generated_frontend_normalizes_match_where_inequality_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (u:User) WHERE u.age >= 30 RETURN u.name ORDER BY u.age LIMIT 1"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(normalized.predicates[0].field, "age")
        self.assertEqual(normalized.predicates[0].operator, ">=")
        self.assertEqual(normalized.predicates[0].value, 30)

    def test_generated_frontend_normalizes_match_where_string_predicate_shape(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (u:User) "
                "WHERE u.name STARTS WITH 'Al' AND u.region CONTAINS 'east' "
                "RETURN u.name ORDER BY u.name LIMIT 5"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(len(normalized.predicates), 2)
        self.assertEqual(normalized.predicates[0].field, "name")
        self.assertEqual(normalized.predicates[0].operator, "STARTS WITH")
        self.assertEqual(normalized.predicates[0].value, "Al")
        self.assertEqual(normalized.predicates[1].field, "region")
        self.assertEqual(normalized.predicates[1].operator, "CONTAINS")
        self.assertEqual(normalized.predicates[1].value, "east")

    def test_generated_frontend_normalizes_match_where_null_predicate_shape(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (u:User) "
                "WHERE u.nickname IS NULL AND u.region IS NOT NULL "
                "RETURN u.name ORDER BY u.name LIMIT 5"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(len(normalized.predicates), 2)
        self.assertEqual(normalized.predicates[0].field, "nickname")
        self.assertEqual(normalized.predicates[0].operator, "IS NULL")
        self.assertIsNone(normalized.predicates[0].value)
        self.assertEqual(normalized.predicates[1].field, "region")
        self.assertEqual(normalized.predicates[1].operator, "IS NOT NULL")
        self.assertIsNone(normalized.predicates[1].value)

    def test_generated_frontend_normalizes_top_level_or_where_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (u:User) "
                "WHERE u.age >= 40 OR u.name = 'Alice' "
                "RETURN u.name ORDER BY u.name"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(len(normalized.predicates), 2)
        self.assertEqual(normalized.predicates[0].field, "age")
        self.assertEqual(normalized.predicates[0].operator, ">=")
        self.assertEqual(normalized.predicates[0].disjunct_index, 0)
        self.assertEqual(normalized.predicates[1].field, "name")
        self.assertEqual(normalized.predicates[1].operator, "=")
        self.assertEqual(normalized.predicates[1].disjunct_index, 1)
        self.assertEqual(normalized.predicates[1].value, "Alice")

    def test_generated_frontend_normalizes_and_within_or_groups(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (u:User) "
                "WHERE u.age >= 40 AND u.active = true OR u.name = 'Alice' "
                "RETURN u.name ORDER BY u.name"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(len(normalized.predicates), 3)
        self.assertEqual(normalized.predicates[0].field, "age")
        self.assertEqual(normalized.predicates[0].disjunct_index, 0)
        self.assertEqual(normalized.predicates[1].field, "active")
        self.assertEqual(normalized.predicates[1].disjunct_index, 0)
        self.assertEqual(normalized.predicates[2].field, "name")
        self.assertEqual(normalized.predicates[2].disjunct_index, 1)

    def test_generated_frontend_normalizes_current_match_set_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (u:User) WHERE u.name = $name SET u.age = 31"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetNode")
        self.assertEqual(normalized.kind, "set")
        self.assertEqual(normalized.pattern_kind, "node")
        self.assertEqual(normalized.node.alias, "u")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(len(normalized.predicates), 1)
        self.assertEqual(normalized.predicates[0].field, "name")
        self.assertEqual(len(normalized.assignments), 1)
        self.assertEqual(normalized.assignments[0].alias, "u")
        self.assertEqual(normalized.assignments[0].field, "age")
        self.assertEqual(normalized.assignments[0].value, 31)

    def test_generated_frontend_normalizes_multi_assignment_match_set(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (u:User) WHERE u.name = $name SET u.age = 31, u.active = true"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetNode")
        self.assertEqual(len(normalized.assignments), 2)
        self.assertEqual(normalized.assignments[0].field, "age")
        self.assertEqual(normalized.assignments[0].value, 31)
        self.assertEqual(normalized.assignments[1].field, "active")
        self.assertEqual(normalized.assignments[1].value, True)

    def test_generated_frontend_normalizes_relationship_match_set(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE a.name = $name SET r.since = 2021, r.strength = 2"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetRelationship")
        self.assertEqual(normalized.pattern_kind, "relationship")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertEqual(len(normalized.assignments), 2)
        self.assertEqual(normalized.assignments[0].field, "since")
        self.assertEqual(normalized.assignments[1].field, "strength")

    def test_generated_frontend_normalizes_match_detach_delete_node(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (u:User) WHERE u.name = $name DETACH DELETE u"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedDeleteNode")
        self.assertEqual(normalized.kind, "delete")
        self.assertEqual(normalized.pattern_kind, "node")
        self.assertEqual(normalized.node.alias, "u")
        self.assertTrue(normalized.detach)
        self.assertEqual(len(normalized.predicates), 1)
        self.assertEqual(normalized.predicates[0].field, "name")

    def test_generated_frontend_normalizes_match_delete_relationship(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE a.name = $name DELETE r"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedDeleteRelationship")
        self.assertEqual(normalized.kind, "delete")
        self.assertEqual(normalized.pattern_kind, "relationship")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertEqual(len(normalized.predicates), 1)
        self.assertEqual(normalized.predicates[0].field, "name")

    def test_generated_frontend_normalizes_multi_type_relationship_match(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) "
                "RETURN b.name, r.type ORDER BY b.name"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertEqual(normalized.relationship.type_name, "KNOWS|FOLLOWS")
        self.assertEqual(normalized.returns[0].column_name, "b.name")
        self.assertEqual(normalized.returns[1].column_name, "r.type")

    def test_generated_frontend_normalizes_untyped_relationship_match(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (a:User)-[r]->(b:User) RETURN b.name, r.type ORDER BY b.name"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertIsNone(normalized.relationship.type_name)
        self.assertEqual(normalized.returns[0].column_name, "b.name")
        self.assertEqual(normalized.returns[1].column_name, "r.type")

    def test_generated_frontend_normalizes_anonymous_node_create(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text("CREATE (:User {name: 'Alice'})")

        self.assertEqual(type(normalized).__name__, "NormalizedCreateNode")
        self.assertEqual(normalized.node.label, "User")
        self.assertEqual(normalized.node.alias, "__humem_create_node")

    def test_generated_frontend_normalizes_anonymous_relationship_endpoints(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (:User {name: 'Alice'})-[r]->(:User) "
                "RETURN r.type ORDER BY r.type"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchRelationship")
        self.assertEqual(normalized.left.alias, "__humem_match_left_node")
        self.assertEqual(normalized.right.alias, "__humem_match_right_node")
        self.assertEqual(normalized.relationship.alias, "r")

    def test_generated_frontend_normalizes_reverse_relationship_match_set(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                "WHERE a.name = $name SET r.since = 2022, r.strength = 3"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetRelationship")
        self.assertEqual(normalized.pattern_kind, "relationship")
        self.assertEqual(normalized.left.alias, "b")
        self.assertEqual(normalized.right.alias, "a")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertEqual(len(normalized.assignments), 2)
        self.assertEqual(normalized.assignments[0].field, "since")
        self.assertEqual(normalized.assignments[1].field, "strength")

    def test_generated_frontend_normalizes_multi_type_relationship_match_set(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) SET r.strength = 5"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetRelationship")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertEqual(normalized.relationship.type_name, "KNOWS|FOLLOWS")
        self.assertEqual(len(normalized.assignments), 1)
        self.assertEqual(normalized.assignments[0].field, "strength")

    def test_generated_frontend_normalizes_untyped_relationship_match_set(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            "MATCH (a:User)-[r]->(b:User) SET r.strength = 5"
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetRelationship")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertIsNone(normalized.relationship.type_name)
        self.assertEqual(len(normalized.assignments), 1)
        self.assertEqual(normalized.assignments[0].field, "strength")

    def test_generated_frontend_normalizes_anonymous_endpoint_match_set(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (:User {name: 'Alice'})-[r]->(:User) "
                "SET r.strength = 7"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetRelationship")
        self.assertEqual(normalized.left.alias, "__humem_set_left_node")
        self.assertEqual(normalized.right.alias, "__humem_set_right_node")
        self.assertEqual(normalized.relationship.alias, "r")
        self.assertEqual(normalized.assignments[0].field, "strength")

    def test_generated_frontend_normalizes_mixed_and_or_relationship_match_set(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
                "SET r.since = 2030, r.strength = 9"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetRelationship")
        self.assertEqual(len(normalized.predicates), 3)
        self.assertEqual(normalized.predicates[0].field, "since")
        self.assertEqual(normalized.predicates[0].disjunct_index, 0)
        self.assertEqual(normalized.predicates[1].field, "strength")
        self.assertEqual(normalized.predicates[1].disjunct_index, 0)
        self.assertEqual(normalized.predicates[2].field, "name")
        self.assertEqual(normalized.predicates[2].disjunct_index, 1)
        self.assertEqual(normalized.assignments[0].field, "since")
        self.assertEqual(normalized.assignments[1].field, "strength")

    def test_generated_frontend_normalizes_mixed_and_or_reverse_relationship_match_set(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
                "SET r.since = 2040, r.strength = 7"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedSetRelationship")
        self.assertEqual(normalized.left.alias, "b")
        self.assertEqual(normalized.right.alias, "a")
        self.assertEqual(len(normalized.predicates), 3)
        self.assertEqual(normalized.predicates[0].disjunct_index, 0)
        self.assertEqual(normalized.predicates[1].disjunct_index, 0)
        self.assertEqual(normalized.predicates[2].disjunct_index, 1)
        self.assertEqual(normalized.assignments[0].field, "since")
        self.assertEqual(normalized.assignments[1].field, "strength")

    def test_generated_frontend_rejects_node_match_set_alias_mismatch(self) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(ValueError, "matched node alias"):
            frontend.normalize_cypher_text(
                "MATCH (u:User {name: 'Alice'}) SET v.age = 31"
            )

    def test_generated_frontend_rejects_relationship_match_set_alias_mismatch(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(ValueError, "matched relationship alias"):
            frontend.normalize_cypher_text(
                "MATCH (a:User)-[r:KNOWS]->(b:User) SET a.name = 'Bob'"
            )

    def test_generated_frontend_rejects_unknown_where_alias(self) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(ValueError, "unknown alias"):
            frontend.normalize_cypher_text(
                "MATCH (u:User {name: 'Alice'}) WHERE v.age = 31 RETURN u.name"
            )

    def test_generated_frontend_rejects_non_equality_relationship_type_comparison(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(
            ValueError,
            "equality predicates for relationship field 'type'",
        ):
            frontend.normalize_cypher_text(
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE r.type > 'FOLLOWS' RETURN b.name"
            )

    def test_generated_frontend_validation_rejects_merge_for_now(self) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(
            ValueError,
            "currently validates only CREATE statements in the write subset",
        ):
            frontend.validate_cypher_text("MERGE (u:User {name: 'Alice'})")

    def test_generated_frontend_rejects_multi_label_create_boundary(self) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(ValueError, "HumemCypher v0"):
            frontend.lower_cypher_text("CREATE (:A:B)")

    def test_generated_frontend_rejects_cartesian_match_boundary(self) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(ValueError, "HumemCypher v0"):
            frontend.lower_cypher_text("MATCH (n), (m) RETURN n")

    def test_generated_frontend_normalizes_parenthesized_where_shape(self) -> None:
        frontend = humemdb_cypher_frontend

        normalized = frontend.normalize_cypher_text(
            (
                "MATCH (u:User) "
                "WHERE (u.age >= 40 OR u.name = 'Alice') AND u.active = true "
                "RETURN u.name ORDER BY u.name"
            )
        )

        self.assertEqual(type(normalized).__name__, "NormalizedMatchNode")
        self.assertEqual(len(normalized.predicates), 4)
        self.assertEqual(normalized.predicates[0].disjunct_index, 0)
        self.assertEqual(normalized.predicates[0].field, "age")
        self.assertEqual(normalized.predicates[1].disjunct_index, 0)
        self.assertEqual(normalized.predicates[1].field, "active")
        self.assertEqual(normalized.predicates[2].disjunct_index, 1)
        self.assertEqual(normalized.predicates[2].field, "name")
        self.assertEqual(normalized.predicates[3].disjunct_index, 1)
        self.assertEqual(normalized.predicates[3].field, "active")

    def test_generated_frontend_rejects_not_where_boundary(self) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(ValueError, "HumemCypher v0"):
            frontend.lower_cypher_text(
                "MATCH (n) WHERE NOT (n.name = 'Bar' OR n.age = 1) RETURN n"
            )

    def test_generated_frontend_rejects_long_pattern_boundary(self) -> None:
        frontend = humemdb_cypher_frontend

        with self.assertRaisesRegex(ValueError, "HumemCypher v0"):
            frontend.lower_cypher_text(
                "MATCH (a)<--()<--(b)-->()-->(c) WHERE a:A RETURN c"
            )

    def test_lowering_matches_handwritten_create_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = "CREATE (u:User {name: 'Alice', age: 30})"

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_create_relationship_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "CREATE (a:User {name: 'Alice'})"
            "-[r:KNOWS {since: 2020}]->"
            "(b:User {name: 'Bob'})"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_separate_pattern_create_relationship_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "CREATE (a:A {name: 'Alice'}), (b:B {name: 'Bob'}), "
            "(a)-[:R]->(b)"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_match_create_relationship_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_match_create_new_start_node_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = "MATCH (x:End) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)"

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_two_node_match_create_relationship_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (x:Begin), (y:End) "
            "WHERE y.name = 'finish' CREATE (x)-[:TYPE]->(y)"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_two_node_reverse_match_create_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (x:Begin), (y:End) "
            "WHERE x.name = 'start' CREATE (x)<-[:TYPE]-(y)"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_parameterized_create_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = "CREATE (u:User {name: $name, active: $active, note: $note})"

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_match_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "WHERE u.name = $name "
            "RETURN u.name ORDER BY u.name LIMIT 1"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_distinct_skip_match_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "RETURN DISTINCT u.name ORDER BY u.name SKIP 1 LIMIT 2"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_offset_match_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = "MATCH (u:User) RETURN u.name ORDER BY u.name OFFSET 2 LIMIT 3"

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_inequality_match_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "WHERE u.age >= 30 "
            "RETURN u.name ORDER BY u.age LIMIT 1"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_string_predicate_match_node_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "WHERE u.name STARTS WITH 'Al' AND u.region CONTAINS 'east' "
            "RETURN u.name ORDER BY u.name LIMIT 5"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_null_predicate_match_node_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "WHERE u.nickname IS NULL AND u.region IS NOT NULL "
            "RETURN u.name ORDER BY u.name LIMIT 5"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_top_level_or_match_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "WHERE u.age >= 40 OR u.name = 'Alice' "
            "RETURN u.name ORDER BY u.name"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_parenthesized_match_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "WHERE (u.age >= 40 OR u.name = 'Alice') AND u.active = true "
            "RETURN u.name ORDER BY u.name"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_mixed_and_or_match_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User) "
            "WHERE u.age >= 40 AND u.active = true OR u.name = 'Alice' "
            "RETURN u.name ORDER BY u.name"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_mixed_and_or_relationship_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
            "RETURN b.name ORDER BY b.name"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_match_relationship_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.since = 2020 "
            "RETURN a.name, b.name ORDER BY a.name LIMIT 1"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_reverse_relationship_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (b:User)<-[r:KNOWS]-(a:User) "
            "WHERE r.since = $since "
            "RETURN a.name, r.since, b.name ORDER BY r.since DESC LIMIT 1"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_match_set_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = "MATCH (u:User {name: 'Alice'}) SET u.age = 31"

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_multi_assignment_match_set_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User {name: 'Alice'}) "
            "SET u.age = 31, u.active = true"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_parameterized_multi_assignment_set_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (u:User {name: $name}) "
            "SET u.embedding = $embedding, u.cohort = $cohort"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_relationship_match_set_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.name = $name SET r.since = 2021, r.strength = 2"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_match_detach_delete_node_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = "MATCH (u:User {name: 'Alice'}) DETACH DELETE u"

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_match_delete_relationship_plan(self) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.name = 'Alice' DELETE r"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_reverse_relationship_match_set_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (b:User)<-[r:KNOWS]-(a:User) "
            "WHERE a.name = $name SET r.since = 2022, r.strength = 3"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_mixed_and_or_relationship_match_set_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
            "SET r.since = 2030, r.strength = 9"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)

    def test_lowering_matches_handwritten_reverse_rel_mixed_bool_set_plan(
        self,
    ) -> None:
        frontend = humemdb_cypher_frontend
        cypher = humemdb_cypher

        query = (
            "MATCH (b:User)<-[r:KNOWS]-(a:User) "
            "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
            "SET r.since = 2040, r.strength = 7"
        )

        lowered = frontend.lower_cypher_text(query)
        handwritten = cypher.parse_cypher(query)

        self.assertEqual(lowered, handwritten)
