from __future__ import annotations

import importlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.support import humemdb_class


def sqlite_engine(db):
    return getattr(db, "_sqlite")


class TestCypher(unittest.TestCase):
    def test_graph_storage_enables_sqlite_foreign_key_enforcement(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                pragma_result = sqlite_engine(db).execute("PRAGMA foreign_keys")

                self.assertEqual(pragma_result.rows, ((1,),))

    def test_cypher_create_and_match_node_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (u:User {name: 'Alice', age: 30})")

                self.assertEqual(created.columns, ("node_id",))
                self.assertEqual(created.rows[0][0], 1)

                result = db.query("MATCH (u:User {name: 'Alice'}) RETURN u.name, u.age")

                self.assertEqual(result.columns, ("u.name", "u.age"))
                self.assertEqual(result.rows, (("Alice", 30),))

    def test_cypher_supports_named_params_in_create_and_match(self) -> None:
        HumemDB = humemdb_class()

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

                self.assertEqual(result.rows, (("Alice", True, None),))

    def test_cypher_create_relationship_and_match_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

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

    def test_cypher_create_relationship_from_separate_patterns_on_sqlite(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'}), "
                        "(b:User {name: 'Bob'}), "
                        "(a)-[:KNOWS {since: 2020}]->(b)"
                    ),
                )

                result = db.query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
                )

                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_cypher_create_reverse_relationship_and_match_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "<-[r:KNOWS {since: 2020}]-"
                        "(b:User {name: 'Bob'})"
                    ),
                )

                result = db.query(
                    "MATCH (a:User)<-[r:KNOWS]-(b:User) RETURN a.name, b.name",
                )

                self.assertEqual(created.columns, ("from_id", "edge_id", "to_id"))
                self.assertEqual(created.rows, ((2, 1, 1),))
                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_cypher_create_self_loop_with_repeated_alias(self) -> None:
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

    def test_cypher_create_self_loop_rejects_conflicting_reused_alias(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "same label"):
                    db.query("CREATE (root:A)-[:LINK]->(root:B)")

                with self.assertRaisesRegex(ValueError, "same inline properties"):
                    db.query(
                        (
                            "CREATE (root:Root {name: 'left'})"
                            "-[:LINK]->"
                            "(root:Root {name: 'right'})"
                        )
                    )

    def test_cypher_match_create_self_loop_on_existing_node(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Root {name: 'root'})")

                created = db.query(
                    "MATCH (root:Root) CREATE (root)-[:LINK]->(root)"
                )

                result = db.query(
                    (
                        "MATCH (a:Root)-[r:LINK]->(b:Root) "
                        "RETURN a.id, b.id, r.type ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(created.rows, ())
                self.assertEqual(created.columns, ())
                self.assertEqual(created.rowcount, 1)
                self.assertEqual(result.rows, ((1, 1, "LINK"),))

    def test_cypher_match_create_end_node_from_existing_start_node(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Begin {name: 'start'})")

                created = db.query(
                    "MATCH (x:Begin) CREATE (x)-[:TYPE]->(:End {name: 'finish'})"
                )

                result = db.query(
                    (
                        "MATCH (x:Begin)-[r:TYPE]->(y:End) "
                        "RETURN x.name, r.type, y.name ORDER BY y.id LIMIT 1"
                    )
                )

                self.assertEqual(created.rowcount, 1)
                self.assertEqual(result.rows, (("start", "TYPE", "finish"),))

    def test_cypher_match_create_start_node_to_existing_end_node(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:End {name: 'finish'})")

                created = db.query(
                    "MATCH (x:End) CREATE (:Begin {name: 'start'})-[:TYPE]->(x)"
                )

                result = db.query(
                    (
                        "MATCH (x:Begin)-[r:TYPE]->(y:End) "
                        "RETURN x.name, r.type, y.name ORDER BY x.id LIMIT 1"
                    )
                )

                self.assertEqual(created.rowcount, 1)
                self.assertEqual(result.rows, (("start", "TYPE", "finish"),))

    def test_cypher_match_create_requires_reusing_matched_alias(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Begin)")

                with self.assertRaisesRegex(ValueError, "reuse the matched node alias"):
                    db.query(
                        "MATCH (x:Begin) CREATE (:Left)-[:TYPE]->(:Right)"
                    )

    def test_cypher_match_create_between_two_existing_nodes(self) -> None:
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

                result = db.query(
                    (
                        "MATCH (x:Begin)-[r:TYPE]->(y:End) "
                        "RETURN x.name, r.type, y.name ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(created.rowcount, 1)
                self.assertEqual(result.rows, (("start", "TYPE", "finish"),))

    def test_cypher_match_create_between_two_existing_nodes_reverse_direction(
        self,
    ) -> None:
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

                result = db.query(
                    (
                        "MATCH (x:Begin)<-[r:TYPE]-(y:End) "
                        "RETURN x.name, r.type, y.name ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(created.rowcount, 1)
                self.assertEqual(result.rows, (("start", "TYPE", "finish"),))

    def test_cypher_create_relationship_rolls_back_partial_write_on_failure(
        self,
    ) -> None:
        HumemDB = humemdb_class()
        cypher_module = importlib.import_module("humemdb.cypher")

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with mock.patch.object(
                    cypher_module,
                    "_insert_edge",
                    side_effect=RuntimeError("boom"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "boom"):
                        db.query("CREATE (:A)-[:R]->(:B)")

                node_result = db.query(
                    "MATCH (n:A) RETURN n.id ORDER BY n.id LIMIT 1"
                )
                edge_result = db.query(
                    "MATCH (:A)-[r:R]->(:B) RETURN r.id ORDER BY r.id LIMIT 1"
                )

                self.assertEqual(node_result.rows, ())
                self.assertEqual(edge_result.rows, ())

    def test_cypher_match_create_rolls_back_new_endpoint_on_failure(self) -> None:
        HumemDB = humemdb_class()
        cypher_module = importlib.import_module("humemdb.cypher")

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:Begin {name: 'start'})")

                with mock.patch.object(
                    cypher_module,
                    "_insert_edge",
                    side_effect=RuntimeError("boom"),
                ):
                    with self.assertRaisesRegex(RuntimeError, "boom"):
                        db.query(
                            (
                                "MATCH (x:Begin) CREATE "
                                "(x)-[:TYPE]->(:End {name: 'finish'})"
                            )
                        )

                begin_result = db.query(
                    "MATCH (x:Begin) RETURN x.name ORDER BY x.id LIMIT 1"
                )
                end_result = db.query(
                    "MATCH (y:End) RETURN y.name ORDER BY y.id LIMIT 1"
                )
                edge_result = db.query(
                    (
                        "MATCH (x:Begin)-[r:TYPE]->(y:End) "
                        "RETURN r.id ORDER BY r.id LIMIT 1"
                    )
                )

                self.assertEqual(begin_result.rows, (("start",),))
                self.assertEqual(end_result.rows, ())
                self.assertEqual(edge_result.rows, ())

    def test_graph_storage_rejects_orphan_edge_and_property_writes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (:User {name: 'Alice'})")

                with self.assertRaises(sqlite3.IntegrityError):
                    sqlite_engine(db).execute(
                        (
                            "INSERT INTO graph_edges (type, from_node_id, to_node_id) "
                            "VALUES (?, ?, ?)"
                        ),
                        ("KNOWS", 1, 999),
                    )

                with self.assertRaises(sqlite3.IntegrityError):
                    sqlite_engine(db).execute(
                        (
                            "INSERT INTO graph_node_properties "
                            "(node_id, key, value, value_type) VALUES (?, ?, ?, ?)"
                        ),
                        (999, "name", "ghost", "string"),
                    )

                with self.assertRaises(sqlite3.IntegrityError):
                    sqlite_engine(db).execute(
                        (
                            "INSERT INTO graph_edge_properties "
                            "(edge_id, key, value, value_type) VALUES (?, ?, ?, ?)"
                        ),
                        (999, "since", "2020", "integer"),
                    )

    def test_graph_storage_rejects_second_vector_property_for_same_node(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (:User {name: 'Alice', embedding: $embedding})",
                    params={"embedding": [0.0, 1.0]},
                )

                with self.assertRaises(sqlite3.IntegrityError):
                    sqlite_engine(db).execute(
                        (
                            "INSERT INTO graph_node_properties "
                            "(node_id, key, value, value_type) VALUES (?, ?, ?, ?)"
                        ),
                        (1, "profile", b"another-vector", "vector"),
                    )

    def test_graph_storage_delete_cascades_node_and_edge_dependents(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2020}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )

                before_delete = sqlite_engine(db).execute(
                    (
                        "SELECT "
                        "(SELECT COUNT(*) FROM graph_nodes), "
                        "(SELECT COUNT(*) FROM graph_node_properties), "
                        "(SELECT COUNT(*) FROM graph_edges), "
                        "(SELECT COUNT(*) FROM graph_edge_properties)"
                    )
                )
                self.assertEqual(before_delete.rows, ((2, 2, 1, 1),))

                sqlite_engine(db).execute("DELETE FROM graph_nodes WHERE id = ?", (1,))

                after_delete = sqlite_engine(db).execute(
                    (
                        "SELECT "
                        "(SELECT COUNT(*) FROM graph_nodes), "
                        "(SELECT COUNT(*) FROM graph_node_properties), "
                        "(SELECT COUNT(*) FROM graph_edges), "
                        "(SELECT COUNT(*) FROM graph_edge_properties)"
                    )
                )
                self.assertEqual(after_delete.rows, ((1, 1, 0, 0),))

    def test_graph_storage_delete_cascades_graph_node_vectors(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query(
                    "CREATE (:User {name: 'Alice', embedding: $embedding})",
                    params={"embedding": [1.0, 0.0]},
                )
                node_id = created.rows[0][0]
                vector_id = sqlite_engine(db).execute(
                    (
                        "SELECT vector_id FROM vector_entries "
                        "WHERE target = 'graph_node' AND namespace = '' "
                        "AND target_id = ?"
                    ),
                    (node_id,),
                ).rows[0][0]
                sqlite_engine(db).execute(
                    (
                        "INSERT INTO vector_entry_metadata "
                        "(vector_id, key, value, value_type) VALUES (?, ?, ?, ?)"
                    ),
                    (vector_id, "tag", "alpha", "string"),
                )

                sqlite_engine(db).execute(
                    "DELETE FROM graph_nodes WHERE id = ?",
                    (node_id,),
                )

                remaining = sqlite_engine(db).execute(
                    (
                        "SELECT "
                        "(SELECT COUNT(*) FROM vector_entries), "
                        "(SELECT COUNT(*) FROM vector_entry_metadata)"
                    )
                )
                self.assertEqual(remaining.rows, ((0, 0),))

    def test_cypher_match_detach_delete_node_removes_graph_state(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2020}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )

                deleted = db.query(
                    "MATCH (u:User {name: 'Alice'}) DETACH DELETE u"
                )

                self.assertEqual(deleted.rowcount, 1)
                graph_counts = sqlite_engine(db).execute(
                    (
                        "SELECT "
                        "(SELECT COUNT(*) FROM graph_nodes), "
                        "(SELECT COUNT(*) FROM graph_edges)"
                    )
                )
                self.assertEqual(graph_counts.rows, ((1, 0),))

    def test_cypher_match_delete_relationship_removes_edge_only(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2020}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )

                deleted = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE a.name = 'Alice' DELETE r"
                    )
                )

                self.assertEqual(deleted.rowcount, 1)
                graph_counts = sqlite_engine(db).execute(
                    (
                        "SELECT "
                        "(SELECT COUNT(*) FROM graph_nodes), "
                        "(SELECT COUNT(*) FROM graph_edges), "
                        "(SELECT COUNT(*) FROM graph_edge_properties)"
                    )
                )
                self.assertEqual(graph_counts.rows, ((2, 0, 0),))

    def test_cypher_supports_relationship_alias_returns_and_filters(self) -> None:
        HumemDB = humemdb_class()

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

    def test_cypher_match_supports_multiple_relationship_types(self) -> None:
        HumemDB = humemdb_class()

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
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:FOLLOWS {since: 2021}]->"
                        "(b:User {name: 'Carol'})"
                    ),
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:BLOCKS {since: 2022}]->"
                        "(b:User {name: 'Dave'})"
                    ),
                )

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) "
                        "RETURN b.name, r.type ORDER BY b.name"
                    ),
                )

                self.assertEqual(
                    result.rows,
                    (("Bob", "KNOWS"), ("Carol", "FOLLOWS")),
                )

    def test_cypher_match_supports_untyped_relationship_patterns(self) -> None:
        HumemDB = humemdb_class()

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
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:FOLLOWS {since: 2021}]->"
                        "(b:User {name: 'Carol'})"
                    ),
                )

                result = db.query(
                    (
                        "MATCH (a:User)-[r]->(b:User) "
                        "RETURN b.name, r.type ORDER BY b.name"
                    ),
                )

                self.assertEqual(
                    result.rows,
                    (("Bob", "KNOWS"), ("Carol", "FOLLOWS")),
                )

    def test_cypher_create_supports_anonymous_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (:User {name: 'Alice'})")

                self.assertEqual(created.columns, ("node_id",))
                self.assertEqual(created.rows, ((1,),))

                result = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.id LIMIT 1"
                )

                self.assertEqual(result.rows, (("Alice",),))

    def test_cypher_match_supports_anonymous_relationship_endpoints(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (:User {name: 'Alice'})-[:KNOWS]->(:User {name: 'Bob'})"
                )
                db.query(
                    "CREATE (:User {name: 'Alice'})-[:FOLLOWS]->(:User {name: 'Carol'})"
                )

                result = db.query(
                    (
                        "MATCH (:User {name: 'Alice'})-[r]->(:User) "
                        "RETURN r.type ORDER BY r.type"
                    )
                )

                self.assertEqual(result.rows, (("FOLLOWS",), ("KNOWS",)))

    def test_cypher_create_rejects_multiple_relationship_types(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(
                    ValueError,
                    "exactly one relationship type",
                ):
                    db.query("CREATE (a:User)-[:KNOWS|FOLLOWS]->(b:User)")

                with self.assertRaisesRegex(
                    ValueError,
                    "exactly one relationship type",
                ):
                    db.query("CREATE (a:User)-[r]->(b:User)")

    def test_cypher_match_set_updates_relationship_properties(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2020, strength: 1}]->"
                        "(b:User {name: 'Bob'})"
                    ),
                )

                updated = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
                        "SET r.since = 2021, r.strength = 2"
                    ),
                )

                self.assertEqual(updated.rowcount, 1)

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
                        "RETURN r.since, r.strength"
                    ),
                )

                self.assertEqual(result.rows, ((2021, 2),))

    def test_cypher_match_set_updates_reverse_relationship_properties(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2020, strength: 1}]->"
                        "(b:User {name: 'Bob'})"
                    ),
                )

                updated = db.query(
                    (
                        "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
                        "SET r.since = 2022, r.strength = 3"
                    ),
                )

                self.assertEqual(updated.rowcount, 1)

                result = db.query(
                    (
                        "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                        "WHERE a.name = 'Alice' AND b.name = 'Bob' "
                        "RETURN r.since, r.strength"
                    ),
                )

                self.assertEqual(result.rows, ((2022, 3),))

    def test_cypher_match_set_supports_multiple_relationship_types(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {strength: 1}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:FOLLOWS {strength: 1}]->"
                        "(b:User {name: 'Carol'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:BLOCKS {strength: 1}]->"
                        "(b:User {name: 'Dave'})"
                    )
                )

                updated = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) "
                        "SET r.strength = 5"
                    )
                )

                self.assertEqual(updated.rowcount, 2)

                result = db.query(
                    (
                        "MATCH (a:User)-[r]->(b:User) "
                        "RETURN b.name, r.type, r.strength ORDER BY b.name"
                    )
                )

                self.assertEqual(
                    result.rows,
                    (
                        ("Bob", "KNOWS", 5),
                        ("Carol", "FOLLOWS", 5),
                        ("Dave", "BLOCKS", 1),
                    ),
                )

    def test_cypher_match_set_supports_untyped_relationship_patterns(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {strength: 1}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:FOLLOWS {strength: 2}]->"
                        "(b:User {name: 'Carol'})"
                    )
                )

                updated = db.query(
                    (
                        "MATCH (a:User)-[r]->(b:User) "
                        "WHERE a.name = 'Alice' SET r.strength = 9"
                    )
                )

                self.assertEqual(updated.rowcount, 2)

                result = db.query(
                    (
                        "MATCH (a:User)-[r]->(b:User) "
                        "RETURN b.name, r.type, r.strength ORDER BY b.name"
                    )
                )

                self.assertEqual(
                    result.rows,
                    (("Bob", "KNOWS", 9), ("Carol", "FOLLOWS", 9)),
                )

    def test_cypher_match_set_supports_anonymous_relationship_endpoints(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (:User {name: 'Alice'})"
                        "-[:KNOWS {strength: 1}]->"
                        "(:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (:User {name: 'Alice'})"
                        "-[:FOLLOWS {strength: 2}]->"
                        "(:User {name: 'Carol'})"
                    )
                )

                updated = db.query(
                    (
                        "MATCH (:User {name: 'Alice'})-[r]->(:User) "
                        "SET r.strength = 7"
                    )
                )

                self.assertEqual(updated.rowcount, 2)

                result = db.query(
                    (
                        "MATCH (:User {name: 'Alice'})-[r]->(:User) "
                        "RETURN r.type, r.strength ORDER BY r.type"
                    )
                )

                self.assertEqual(result.rows, (("FOLLOWS", 7), ("KNOWS", 7)))

    def test_cypher_relationship_match_set_supports_and_within_or_groups(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2019, strength: 1}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 1}]->"
                        "(b:User {name: 'Carol'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 2}]->"
                        "(b:User {name: 'Dave'})"
                    )
                )

                updated = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
                        "SET r.since = 2030, r.strength = 9"
                    )
                )

                self.assertEqual(updated.rowcount, 2)

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "RETURN b.name, r.since, r.strength ORDER BY b.name"
                    )
                )

                self.assertEqual(
                    result.rows,
                    (("Bob", 2030, 9), ("Carol", 2022, 1), ("Dave", 2030, 9)),
                )

    def test_cypher_reverse_relationship_match_set_supports_and_within_or_groups(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2019, strength: 1}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 1}]->"
                        "(b:User {name: 'Carol'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 2}]->"
                        "(b:User {name: 'Dave'})"
                    )
                )

                updated = db.query(
                    (
                        "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                        "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
                        "SET r.since = 2040, r.strength = 7"
                    )
                )

                self.assertEqual(updated.rowcount, 2)

                result = db.query(
                    (
                        "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                        "RETURN b.name, r.since, r.strength ORDER BY b.name"
                    )
                )

                self.assertEqual(
                    result.rows,
                    (("Bob", 2040, 7), ("Carol", 2022, 1), ("Dave", 2040, 7)),
                )

    def test_cypher_supports_reverse_relationship_match(self) -> None:
        HumemDB = humemdb_class()

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
                    "MATCH (b:User)<-[:KNOWS]-(a:User) RETURN a.name, b.name",
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_cypher_match_where_supports_scalar_inequality_on_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 30})")
                db.query("CREATE (u:User {name: 'Bob', age: 40})")

                result = db.query(
                    "MATCH (u:User) WHERE u.age > 30 RETURN u.name ORDER BY u.age"
                )

                self.assertEqual(result.rows, (("Bob",),))

    def test_cypher_match_where_supports_scalar_inequality_on_relationships(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2019}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022}]->"
                        "(b:User {name: 'Carol'})"
                    )
                )

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE r.since >= 2020 RETURN b.name ORDER BY r.since"
                    )
                )

                self.assertEqual(result.rows, (("Carol",),))

    def test_cypher_match_set_supports_scalar_inequality_predicate(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 30, active: false})")
                db.query("CREATE (u:User {name: 'Bob', age: 40, active: false})")

                updated = db.query(
                    "MATCH (u:User) WHERE u.age >= 40 SET u.active = true"
                )

                self.assertEqual(updated.rowcount, 1)

                result = db.query(
                    "MATCH (u:User) WHERE u.active = true RETURN u.name ORDER BY u.name"
                )

                self.assertEqual(result.rows, (("Bob",),))

    def test_cypher_match_where_filters_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 30})")
                db.query("CREATE (u:User {name: 'Bob', age: 40})")

                result = db.query(
                    "MATCH (u:User) WHERE u.age = 40 RETURN u.name, u.age"
                )

                self.assertEqual(result.rows, (("Bob", 40),))

    def test_cypher_match_where_supports_top_level_or(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 30})")
                db.query("CREATE (u:User {name: 'Bob', age: 40})")
                db.query("CREATE (u:User {name: 'Carol', age: 25})")

                result = db.query(
                    (
                        "MATCH (u:User) "
                        "WHERE u.age >= 40 OR u.name = 'Alice' "
                        "RETURN u.name ORDER BY u.name"
                    )
                )

                self.assertEqual(result.rows, (("Alice",), ("Bob",)))

    def test_cypher_match_where_supports_and_within_or_groups(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: 'Alice', age: 30, active: true})"
                )
                db.query(
                    "CREATE (u:User {name: 'Bob', age: 40, active: false})"
                )
                db.query(
                    "CREATE (u:User {name: 'Carol', age: 40, active: true})"
                )

                result = db.query(
                    (
                        "MATCH (u:User) "
                        "WHERE u.age >= 40 AND u.active = true OR u.name = 'Alice' "
                        "RETURN u.name ORDER BY u.name"
                    )
                )

                self.assertEqual(result.rows, (("Alice",), ("Carol",)))

    def test_cypher_relationship_match_where_supports_and_within_or_groups(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2019, strength: 1}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 1}]->"
                        "(b:User {name: 'Carol'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 2}]->"
                        "(b:User {name: 'Dave'})"
                    )
                )

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE r.since >= 2022 AND r.strength >= 2 OR b.name = 'Bob' "
                        "RETURN b.name ORDER BY b.name"
                    )
                )

                self.assertEqual(result.rows, (("Bob",), ("Dave",)))

    def test_cypher_relationship_match_where_or_does_not_duplicate_overlap_rows(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 2}]->"
                        "(b:User {name: 'Bob'})"
                    )
                )
                db.query(
                    (
                        "CREATE (a:User {name: 'Alice'})"
                        "-[r:KNOWS {since: 2022, strength: 1}]->"
                        "(b:User {name: 'Carol'})"
                    )
                )

                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE r.since >= 2022 OR b.name = 'Bob' "
                        "RETURN b.name, r.id ORDER BY b.name, r.id"
                    )
                )

                self.assertEqual(result.rows, (("Bob", 1), ("Carol", 2)))

    def test_cypher_match_set_supports_top_level_or_predicate(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 30, active: false})")
                db.query("CREATE (u:User {name: 'Bob', age: 40, active: false})")
                db.query("CREATE (u:User {name: 'Carol', age: 25, active: false})")

                updated = db.query(
                    (
                        "MATCH (u:User) "
                        "WHERE u.age >= 40 OR u.name = 'Alice' "
                        "SET u.active = true"
                    )
                )

                self.assertEqual(updated.rowcount, 2)

                result = db.query(
                    "MATCH (u:User) WHERE u.active = true RETURN u.name ORDER BY u.name"
                )

                self.assertEqual(result.rows, (("Alice",), ("Bob",)))

    def test_cypher_match_set_supports_and_within_or_groups(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    "CREATE (u:User {name: 'Alice', age: 30, active: false})"
                )
                db.query(
                    "CREATE (u:User {name: 'Bob', age: 40, active: false})"
                )
                db.query(
                    "CREATE (u:User {name: 'Carol', age: 40, active: true})"
                )

                updated = db.query(
                    (
                        "MATCH (u:User) "
                        "WHERE u.age >= 40 AND u.active = true OR u.name = 'Alice' "
                        "SET u.active = true"
                    )
                )

                self.assertEqual(updated.rowcount, 2)

                result = db.query(
                    "MATCH (u:User) WHERE u.active = true RETURN u.name ORDER BY u.name"
                )

                self.assertEqual(result.rows, (("Alice",), ("Carol",)))

    def test_cypher_match_supports_order_by_and_limit_on_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                for name, age in (("Alice", 30), ("Bob", 40), ("Carol", 20)):
                    db.query(f"CREATE (u:User {{name: '{name}', age: {age}}})")

                result = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.age DESC LIMIT 2"
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Bob",), ("Alice",)))

    def test_cypher_match_supports_distinct_skip_and_limit_on_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                for name in ("Alice", "Alice", "Bob", "Carol"):
                    db.query(f"CREATE (u:User {{name: '{name}'}})")

                result = db.query(
                    "MATCH (u:User) RETURN DISTINCT u.name "
                    "ORDER BY u.name SKIP 1 LIMIT 2"
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Bob",), ("Carol",)))

    def test_cypher_match_supports_offset_and_limit_on_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                for name in ("Alice", "Bob", "Carol", "Dave", "Eve"):
                    db.query(f"CREATE (u:User {{name: '{name}'}})")

                result = db.query(
                    "MATCH (u:User) RETURN u.name ORDER BY u.name OFFSET 2 LIMIT 2"
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Carol",), ("Dave",)))

    def test_cypher_match_supports_string_where_predicates_on_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', region: 'north-east'})")
                db.query("CREATE (u:User {name: 'Alicia', region: 'south-east'})")
                db.query("CREATE (u:User {name: 'Bob', region: 'north-west'})")

                result = db.query(
                    "MATCH (u:User) "
                    "WHERE u.name STARTS WITH 'Ali' AND u.region ENDS WITH 'east' "
                    "RETURN u.name ORDER BY u.name"
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Alice",), ("Alicia",)))

    def test_cypher_match_supports_string_where_predicates_on_relationships(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    "CREATE (a:User {name: 'Alice'})"
                    "-[r:KNOWS {note: 'met at lunch'}]->"
                    "(b:User {name: 'Bob'})"
                )
                db.query(
                    "CREATE (a:User {name: 'Alice'})"
                    "-[r:KNOWS {note: 'met at conference'}]->"
                    "(b:User {name: 'Carol'})"
                )

                result = db.query(
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "WHERE r.note CONTAINS 'lunch' "
                    "RETURN b.name ORDER BY b.name"
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Bob",),))

    def test_cypher_match_supports_is_null_and_is_not_null_on_nodes(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    "CREATE (u:User {name: 'Alice', nickname: null, region: 'north'})"
                )
                db.query("CREATE (u:User {name: 'Bob', region: 'south'})")
                db.query(
                    "CREATE (u:User {name: 'Carol', nickname: 'C', region: null})"
                )

                null_result = db.query(
                    "MATCH (u:User) "
                    "WHERE u.nickname IS NULL "
                    "RETURN u.name ORDER BY u.name"
                )
                not_null_result = db.query(
                    "MATCH (u:User) "
                    "WHERE u.region IS NOT NULL "
                    "RETURN u.name ORDER BY u.name"
                )

                self.assertEqual(null_result.route, "sqlite")
                self.assertEqual(null_result.rows, (("Alice",), ("Bob",)))
                self.assertEqual(not_null_result.route, "sqlite")
                self.assertEqual(not_null_result.rows, (("Alice",), ("Bob",)))

    def test_cypher_match_supports_is_not_null_on_relationships(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"
            duckdb_path = Path(tmpdir) / "humem.duckdb"

            with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
                db.query(
                    "CREATE (a:User {name: 'Alice'})"
                    "-[r:KNOWS {note: null}]->"
                    "(b:User {name: 'Bob'})"
                )
                db.query(
                    "CREATE (a:User {name: 'Alice'})"
                    "-[r:KNOWS {note: 'met'}]->"
                    "(b:User {name: 'Carol'})"
                )

                result = db.query(
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "WHERE r.note IS NOT NULL "
                    "RETURN b.name ORDER BY b.name"
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Carol",),))

    def test_cypher_match_supports_order_by_and_limit_on_relationships(self) -> None:
        HumemDB = humemdb_class()

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
                result = db.query(cypher)

                expected = (("Dave", 2022), ("Bob", 2020))
                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, expected)

    def test_cypher_match_supports_skip_without_limit_on_relationships(self) -> None:
        HumemDB = humemdb_class()

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

                result = db.query(
                    "MATCH (a:User)-[r:KNOWS]->(b:User) "
                    "RETURN b.name ORDER BY r.since DESC SKIP 1"
                )

                self.assertEqual(result.route, "sqlite")
                self.assertEqual(result.rows, (("Bob",), ("Carol",)))

    def test_internal_cypher_plan_can_run_on_duckdb(self) -> None:
        HumemDB = humemdb_class()
        db_module = importlib.import_module("humemdb.db")
        plan_query = getattr(db_module, "_plan_query")

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

                plan = plan_query(
                    "MATCH (a:User)-[:KNOWS]->(b:User) RETURN a.name, b.name",
                    route="duckdb",
                    params=None,
                )
                result = getattr(db, "_execute_cypher_query_plan")(plan)

                self.assertEqual(result.rows, (("Alice", "Bob"),))

    def test_cypher_persists_graph_data_across_reopen(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', active: true})")

            with HumemDB(str(sqlite_path)) as db:
                result = db.query(
                    "MATCH (u:User) WHERE u.active = true RETURN u.name, u.active",
                )

                self.assertEqual(result.rows, (("Alice", True),))

    def test_cypher_supports_parenthesized_where_expression(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 35, active: true})")
                db.query("CREATE (u:User {name: 'Bob', age: 45, active: false})")
                db.query("CREATE (u:User {name: 'Carol', age: 5, active: true})")

                result = db.query(
                    (
                        "MATCH (u:User) "
                        "WHERE (u.age > 30 OR u.age < 10) AND u.active = true "
                        "RETURN u.name ORDER BY u.name"
                    )
                )

                self.assertEqual(result.rows, (("Alice",), ("Carol",)))

    def test_cypher_rejects_not_where_expression(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "HumemCypher v0"):
                    db.query(
                        (
                            "MATCH (u:User) "
                            "WHERE NOT (u.age > 30 OR u.age < 10) "
                            "RETURN u.name"
                        )
                    )

    def test_cypher_rejects_match_set_assignment_to_unknown_node_alias(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "matched node alias"):
                    db.query("MATCH (u:User {name: 'Alice'}) SET v.age = 31")

    def test_cypher_rejects_match_set_assignment_to_unknown_relationship_alias(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(
                    ValueError,
                    "matched relationship alias",
                ):
                    db.query(
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "SET a.name = 'Bob'"
                    )

    def test_cypher_rejects_unknown_where_alias(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(ValueError, "unknown alias"):
                    db.query(
                        "MATCH (u:User {name: 'Alice'}) WHERE v.age = 31 RETURN u.name"
                    )

    def test_cypher_rejects_non_equality_relationship_type_comparison(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(
                    ValueError,
                    "equality predicates for relationship field 'type'",
                ):
                    db.query(
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "WHERE r.type > 'FOLLOWS' RETURN b.name"
                    )

    def test_cypher_rejects_positional_params(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(NotImplementedError, "named parameter"):
                    db.query("CREATE (u:User {name: $name})", params=("Alice",))

    def test_query_infers_cypher_create_and_match_on_sqlite(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("CREATE (u:User {name: 'Alice', age: 30})")

                self.assertEqual(created.query_type, "cypher")
                self.assertEqual(created.rows[0][0], 1)

                result = db.query("MATCH (u:User {name: 'Alice'}) RETURN u.name, u.age")

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.rows, (("Alice", 30),))

    def test_query_infers_cypher_for_uppercase_multiline_starters(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = db.query("  CREATE\n(u:User {name: 'Alice', age: 30})")

                self.assertEqual(created.query_type, "cypher")
                self.assertEqual(created.rows[0][0], 1)

                updated = db.query("\tMATCH\n(u:User {name: 'Alice'}) SET u.age = 31")

                self.assertEqual(updated.query_type, "cypher")

                result = db.query("\nMATCH\t(u:User {name: 'Alice'}) RETURN u.age")

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.rows, ((31,),))

    def test_query_surfaces_generated_cypher_syntax_errors(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                with self.assertRaisesRegex(
                    ValueError,
                    "Generated Cypher frontend reported syntax errors",
                ):
                    db.query("MATCH (u RETURN u")

    def test_cypher_match_set_updates_multiple_properties(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query("CREATE (u:User {name: 'Alice', age: 30, active: false})")

                updated = db.query(
                    "MATCH (u:User {name: 'Alice'}) "
                    "SET u.age = 31, u.active = true"
                )

                self.assertEqual(updated.query_type, "cypher")
                self.assertEqual(updated.rowcount, 1)

                result = db.query(
                    "MATCH (u:User {name: 'Alice'}) RETURN u.age, u.active"
                )

                self.assertEqual(result.rows, ((31, True),))

    def test_query_does_not_infer_mixed_case_cypher(self) -> None:
        HumemDB = humemdb_class()

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
