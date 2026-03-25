from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.support import humemdb_class, vector_module


class TestVectorAPI(unittest.TestCase):
    def test_search_vectors_returns_expected_matches(self) -> None:
        HumemDB = humemdb_class()

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
        HumemDB = humemdb_class()

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
        HumemDB = humemdb_class()

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
        HumemDB = humemdb_class()

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
        HumemDB = humemdb_class()

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
        HumemDB = humemdb_class()

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
                self.assertIsNone(direct.query_type)
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
                self.assertEqual(sql_candidate_filtered.query_type, "sql")
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
                self.assertEqual(cypher_candidate_filtered.query_type, "cypher")
                self.assertEqual(
                    cypher_candidate_filtered.rows[0][:3],
                    ("graph_node", "", 1),
                )

    def test_raw_sql_vector_write_invalidates_cached_index(self) -> None:
        HumemDB = humemdb_class()
        vector = vector_module()

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
        HumemDB = humemdb_class()

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
                self.assertIsNone(result.query_type)

    def test_preload_vectors_ignores_missing_vector_table(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path), preload_vectors=True) as db:
                self.assertFalse(db.vectors_cached())

