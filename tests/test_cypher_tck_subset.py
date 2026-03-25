from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.support import humemdb_class


class TestCypherTCKSubset(unittest.TestCase):
    def test_create1_single_labeled_node_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (n:Label)")

                self.assertEqual(created.columns, ("node_id",))
                self.assertEqual(created.rows, ((1,),))

                result = db.query(
                    "MATCH (n:Label) RETURN n.label ORDER BY n.id LIMIT 1"
                )

                self.assertEqual(result.rows, (("Label",),))

    def test_create1_single_node_with_two_properties_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (n:Thing {uid: 12, name: 'foo'})")

                result = db.query(
                    (
                        "MATCH (n:Thing) "
                        "RETURN n.uid, n.name ORDER BY n.uid LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, ((12, "foo"),))

    def test_create1_null_property_omission_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (n:Thing {uid: 12, name: null})")

                result = db.query(
                    (
                        "MATCH (n:Thing) "
                        "RETURN n.uid, n.name ORDER BY n.uid LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, ((12, None),))

    def test_create1_large_integer_precision_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (p:TheLabel {bigint: 4611686018427387905})")

                result = db.query(
                    (
                        "MATCH (p:TheLabel) "
                        "RETURN p.bigint ORDER BY p.bigint LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, ((4611686018427387905,),))

    def test_create2_single_relationship_pattern_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "CREATE (a:A)-[r:R {since: 2020}]->(b:B {name: 'Bob'})"
                )

                self.assertEqual(created.columns, ("from_id", "edge_id", "to_id"))
                self.assertEqual(created.rows, ((1, 1, 2),))

                result = db.query(
                    (
                        "MATCH (a:A)-[r:R]->(b:B) "
                        "RETURN a.label, r.type, r.since, b.name "
                        "ORDER BY r.since LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("A", "R", 2020, "Bob"),))

    def test_create2_reverse_relationship_pattern_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "CREATE (:A)<-[r:R {since: 2020}]-(:B {name: 'Bee'})"
                )

                self.assertEqual(created.columns, ("from_id", "edge_id", "to_id"))
                self.assertEqual(created.rows, ((2, 1, 1),))

                result = db.query(
                    (
                        "MATCH (a:A)<-[r:R]-(b:B) "
                        "RETURN a.label, r.type, r.since, b.label "
                        "ORDER BY r.since LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("A", "R", 2020, "B"),))

    def test_create2_separate_patterns_relationship_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (a:A), (b:B), (a)-[:R]->(b)")

                self.assertEqual(created.columns, ("from_id", "edge_id", "to_id"))
                self.assertEqual(created.rows, ((1, 1, 2),))

                result = db.query(
                    (
                        "MATCH (a:A)-[r:R]->(b:B) "
                        "RETURN a.label, r.type, b.label ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("A", "R", "B"),))

    def test_create2_single_node_self_loop_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (root:Root)-[:LINK]->(root:Root)")

                self.assertEqual(created.columns, ("from_id", "edge_id", "to_id"))
                self.assertEqual(created.rows, ((1, 1, 1),))

                result = db.query(
                    (
                        "MATCH (a:Root)-[r:LINK]->(b:Root) "
                        "RETURN a.id, b.id, r.type ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, ((1, 1, "LINK"),))

    def test_create2_single_self_loop_on_existing_node_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Root)")

                created = db.query(
                    "MATCH (root:Root) CREATE (root)-[:LINK]->(root)"
                )

                self.assertEqual(created.rows, ())
                self.assertEqual(created.rowcount, 1)

                result = db.query(
                    (
                        "MATCH (a:Root)-[r:LINK]->(b:Root) "
                        "RETURN a.id, b.id, r.type ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, ((1, 1, "LINK"),))

    def test_create2_existing_start_node_to_new_end_node_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Begin)")

                created = db.query(
                    "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End)"
                )

                self.assertEqual(created.rows, ())
                self.assertEqual(created.rowcount, 1)

                result = db.query(
                    (
                        "MATCH (x:Begin)-[r:TYPE]->(y:End) "
                        "RETURN x.label, r.type, y.label ORDER BY y.id LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("Begin", "TYPE", "End"),))

    def test_create2_new_start_node_to_existing_end_node_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:End)")

                created = db.query(
                    "MATCH (x:End) CREATE (:Begin)-[:TYPE]->(x)"
                )

                self.assertEqual(created.rows, ())
                self.assertEqual(created.rowcount, 1)

                result = db.query(
                    (
                        "MATCH (x:Begin)-[r:TYPE]->(y:End) "
                        "RETURN x.label, r.type, y.label ORDER BY x.id LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("Begin", "TYPE", "End"),))

    def test_create2_connect_two_existing_nodes_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Begin {name: 'start'})")
                db.query("CREATE (:End {name: 'finish'})")

                created = db.query(
                    (
                        "MATCH (x:Begin), (y:End) "
                        "WHERE y.name = 'finish' CREATE (x)-[:TYPE]->(y)"
                    )
                )

                self.assertEqual(created.rows, ())
                self.assertEqual(created.rowcount, 1)

                result = db.query(
                    (
                        "MATCH (x:Begin)-[r:TYPE]->(y:End) "
                        "RETURN x.label, r.type, y.label ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("Begin", "TYPE", "End"),))

    def test_create2_connect_two_existing_nodes_reverse_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Begin {name: 'start'})")
                db.query("CREATE (:End {name: 'finish'})")

                created = db.query(
                    (
                        "MATCH (x:Begin), (y:End) "
                        "WHERE x.name = 'start' CREATE (x)<-[:TYPE]-(y)"
                    )
                )

                self.assertEqual(created.rows, ())
                self.assertEqual(created.rowcount, 1)

                result = db.query(
                    (
                        "MATCH (x:Begin)<-[r:TYPE]-(y:End) "
                        "RETURN x.label, r.type, y.label ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("Begin", "TYPE", "End"),))

    def test_match2_reverse_relationship_pattern_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (a:A)-[r:R {since: 2021}]->(b:B {name: 'Bee'})")

                result = db.query(
                    (
                        "MATCH (b:B)<-[r:R]-(a:A) "
                        "RETURN b.label, r.type, r.since, a.label "
                        "ORDER BY r.since LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("B", "R", 2021, "A"),))

    def test_match1_nonexistent_nodes_return_empty_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "MATCH (n:Missing) RETURN n.id ORDER BY n.id LIMIT 1"
                )

                self.assertEqual(result.rows, ())

    def test_match2_nonexistent_relationships_return_empty_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    (
                        "MATCH (a:A)-[r:R]->(b:B) "
                        "RETURN r.type ORDER BY r.type LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, ())

    def test_match2_relationship_pattern_with_labels_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (a:A)-[:T1]->(b:B)")
                db.query("CREATE (a:B)-[:T2]->(b:A)")
                db.query("CREATE (a:B)-[:T3]->(b:B)")
                db.query("CREATE (a:A)-[:T4]->(b:A)")

                result = db.query(
                    (
                        "MATCH (a:A)-[r:T1]->(b:B) "
                        "RETURN r.type ORDER BY r.type LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("T1",),))

    def test_match2_relationship_pattern_with_type_alternation_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (a:A)-[:KNOWS]->(b:B {name: 'Bee'})")
                db.query("CREATE (a:A)-[:HATES]->(b:B {name: 'Hex'})")
                db.query("CREATE (a:A)-[:LIKES]->(b:B {name: 'Lux'})")

                result = db.query(
                    (
                        "MATCH (a:A)-[r:KNOWS|HATES]->(b:B) "
                        "RETURN b.name, r.type ORDER BY b.name"
                    )
                )

                self.assertEqual(
                    result.rows,
                    (("Bee", "KNOWS"), ("Hex", "HATES")),
                )

    def test_match2_untyped_relationship_pattern_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (a:A)-[:KNOWS]->(b:B {name: 'Bee'})")
                db.query("CREATE (a:A)-[:HATES]->(b:B {name: 'Hex'})")

                result = db.query(
                    (
                        "MATCH (a:A)-[r]->(b:B) "
                        "RETURN b.name, r.type ORDER BY b.name"
                    )
                )

                self.assertEqual(
                    result.rows,
                    (("Bee", "KNOWS"), ("Hex", "HATES")),
                )

    def test_match2_anonymous_endpoint_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:A {name: 'left'})-[:KNOWS]->(:B {name: 'Bee'})")
                db.query("CREATE (:A {name: 'left'})-[:HATES]->(:B {name: 'Hex'})")

                result = db.query(
                    (
                        "MATCH (:A {name: 'left'})-[r]->(:B) "
                        "RETURN r.type ORDER BY r.type"
                    )
                )

                self.assertEqual(result.rows, (("HATES",), ("KNOWS",)))

    def test_match2_relationship_inline_property_predicate_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (m:Mid {name: 'mid'})"
                        "-[r:KNOWS {name: 'monkey'}]->"
                        "(a:Left {name: 'left'})"
                    )
                )
                db.query(
                    (
                        "CREATE (m:Mid {name: 'mid-two'})"
                        "-[r:KNOWS {name: 'woot'}]->"
                        "(b:Right {name: 'right'})"
                    )
                )

                result = db.query(
                    (
                        "MATCH (node)-[r:KNOWS {name: 'monkey'}]->(a:Left) "
                        "RETURN a.name ORDER BY a.name LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("left",),))

    def test_match1_inline_property_predicate_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (n:Item {name: 'bar'})")
                db.query("CREATE (n:Item {name: 'monkey'})")
                db.query("CREATE (n:Item {firstname: 'bar'})")

                result = db.query(
                    (
                        "MATCH (n:Item {name: 'bar'}) "
                        "RETURN n.name ORDER BY n.name LIMIT 1"
                    )
                )

                self.assertEqual(result.rows, (("bar",),))

    def test_match_where1_node_property_filter_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (n:Item {name: 'Bar'})")
                db.query("CREATE (n:Item {name: 'Baz'})")
                db.query("CREATE (n:Item)")

                result = db.query(
                    (
                        "MATCH (n:Item) "
                        "WHERE n.name = 'Bar' "
                        "RETURN n.name ORDER BY n.name"
                    )
                )

                self.assertEqual(result.rows, (("Bar",),))

    def test_match_where1_relationship_property_param_filter_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:A {name: 'left'})"
                        "-[r:T {name: 'bar'}]->"
                        "(b:B {name: 'me'})"
                    )
                )

                result = db.query(
                    (
                        "MATCH (a:A)-[r:T]->(b:B) "
                        "WHERE r.name = $param "
                        "RETURN b.name"
                    ),
                    params={"param": "bar"},
                )

                self.assertEqual(result.rows, (("me",),))

    def test_match_where1_node_disjunction_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (n:A {name: 'one', p1: 12})")
                db.query("CREATE (n:B {name: 'two', p2: 13})")
                db.query("CREATE (n:C {name: 'three'})")

                result = db.query(
                    (
                        "MATCH (n) "
                        "WHERE n.p1 = 12 OR n.p2 = 13 "
                        "RETURN n.name ORDER BY n.name"
                    )
                )

                self.assertEqual(result.rows, (("one",), ("two",)))

    def test_match_where1_relationship_property_disjunction_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:Person {name: 'A'})"
                        "-[r:KNOWS {name: 'first'}]->"
                        "(b:Person {name: 'B'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:Person {name: 'A'})"
                        "-[r:KNOWS {name: 'second'}]->"
                        "(b:Person {name: 'C'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:Person {name: 'A'})"
                        "-[r:KNOWS {name: 'third'}]->"
                        "(b:Person {name: 'D'})"
                    )
                )

                result = db.query(
                    (
                        "MATCH (n:Person)-[r:KNOWS]->(x:Person) "
                        "WHERE n.name = 'A' AND r.name = 'first' OR r.name = 'second' "
                        "RETURN x.name ORDER BY x.name"
                    )
                )

                self.assertEqual(result.rows, (("B",), ("C",)))

    def test_match_where1_node_string_predicates_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (n:User {name: 'Alice', region: 'west-coast'})")
                db.query("CREATE (n:User {name: 'Alicia', region: 'east-coast'})")
                db.query("CREATE (n:User {name: 'Bob', region: 'west-inland'})")

                result = db.query(
                    (
                        "MATCH (n:User) "
                        "WHERE n.name STARTS WITH 'Ali' "
                        "AND n.region CONTAINS 'coast' "
                        "RETURN n.name ORDER BY n.name"
                    )
                )

                self.assertEqual(result.rows, (("Alice",), ("Alicia",)))

    def test_match_where1_relationship_string_predicates_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {note: 'met at meetup'}]->"
                        "(b:User {name: 'Grace'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {note: 'met remotely'}]->"
                        "(b:User {name: 'Bri'})"
                    )
                )

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE r.note CONTAINS 'met' "
                        "AND b.name ENDS WITH 'ce' "
                        "RETURN b.name ORDER BY b.name"
                    )
                )

                self.assertEqual(result.rows, (("Grace",),))

    def test_match_where1_node_null_predicates_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (n:User {name: 'Alice', region: 'west'})")
                db.query("CREATE (n:User {name: 'Bob', nickname: 'Bobby'})")
                db.query("CREATE (n:User {name: 'Cara', region: 'east'})")

                result = db.query(
                    (
                        "MATCH (n:User) "
                        "WHERE n.nickname IS NULL AND n.region IS NOT NULL "
                        "RETURN n.name ORDER BY n.name"
                    )
                )

                self.assertEqual(result.rows, (("Alice",), ("Cara",)))

    def test_match_return_distinct_offset_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:User {region: 'east'})")
                db.query("CREATE (:User {region: 'north'})")
                db.query("CREATE (:User {region: 'north'})")
                db.query("CREATE (:User {region: 'south'})")
                db.query("CREATE (:User {region: 'west'})")

                result = db.query(
                    (
                        "MATCH (u:User) "
                        "RETURN DISTINCT u.region ORDER BY u.region OFFSET 1 LIMIT 2"
                    )
                )

                self.assertEqual(result.rows, (("north",), ("south",)))

    def test_match_detach_delete_node_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (:User {name: 'Alice'})-[:KNOWS]->(:User {name: 'Bob'})"
                )

                deleted = db.query(
                    "MATCH (u:User) WHERE u.name = 'Alice' DETACH DELETE u"
                )

                self.assertEqual(deleted.rows, ())
                self.assertEqual(deleted.rowcount, 1)

                remaining = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.name"
                )
                relationships = db.query(
                    "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.type"
                )

                self.assertEqual(remaining.rows, (("Bob",),))
                self.assertEqual(relationships.rows, ())

    def test_match_delete_relationship_subset(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (:User {name: 'Alice'})-[:KNOWS]->(:User {name: 'Bob'})"
                )

                deleted = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE b.name = 'Bob' DELETE r"
                    )
                )

                self.assertEqual(deleted.rows, ())
                self.assertEqual(deleted.rowcount, 1)

                remaining_nodes = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.name"
                )
                relationships = db.query(
                    "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.type"
                )

                self.assertEqual(remaining_nodes.rows, (("Alice",), ("Bob",)))
                self.assertEqual(relationships.rows, ())
