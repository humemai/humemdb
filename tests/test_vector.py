from __future__ import annotations

import importlib
import tempfile
import unittest
from pathlib import Path

import numpy as np


def _vector_module():
    return importlib.import_module("humemdb.vector")


def _humemdb_class():
    return importlib.import_module("humemdb").HumemDB


class HumemVectorTest(unittest.TestCase):
    def test_vector_blob_roundtrip(self) -> None:
        vector = _vector_module()

        blob = vector.encode_vector_blob([1.5, -2.0, 3.25])
        decoded = vector.decode_vector_blob(blob, dimension=3)

        np.testing.assert_allclose(
            decoded,
            np.array([1.5, -2.0, 3.25], dtype=np.float32),
        )

    def test_exact_vector_index_returns_expected_cosine_order(self) -> None:
        vector = _vector_module()

        index = vector.ExactVectorIndex(
            item_ids=np.array(
                [
                    ("direct", "", 101),
                    ("direct", "", 102),
                    ("direct", "", 103),
                ],
                dtype=object,
            ),
            matrix=np.array(
                [
                    [1.0, 0.0],
                    [0.9, 0.1],
                    [0.0, 1.0],
                ],
                dtype=np.float32,
            ),
            metric="cosine",
        )

        result = index.search([1.0, 0.0], top_k=2)

        self.assertEqual(tuple(match.target_id for match in result), (101, 102))

    def test_exact_vector_index_supports_candidate_filtering(self) -> None:
        vector = _vector_module()

        index = vector.ExactVectorIndex(
            item_ids=np.array(
                [
                    ("direct", "", 1),
                    ("direct", "", 2),
                    ("direct", "", 3),
                ],
                dtype=object,
            ),
            matrix=np.array(
                [
                    [1.0, 0.0],
                    [0.9, 0.1],
                    [0.0, 1.0],
                ],
                dtype=np.float32,
            ),
            metric="cosine",
        )

        result = index.search([1.0, 0.0], top_k=2, candidate_indexes=[1, 2])

        self.assertEqual(tuple(match.target_id for match in result), (2, 3))

    def test_scalar_quantized_vector_index_keeps_top_match_on_simple_input(
        self,
    ) -> None:
        vector = _vector_module()

        index = vector.ScalarQuantizedVectorIndex.from_matrix(
            item_ids=np.array(
                [
                    ("direct", "", 11),
                    ("direct", "", 12),
                    ("direct", "", 13),
                ],
                dtype=object,
            ),
            matrix=np.array(
                [
                    [1.0, 0.0, 0.0],
                    [0.8, 0.2, 0.0],
                    [0.0, 1.0, 0.0],
                ],
                dtype=np.float32,
            ),
            metric="cosine",
        )

        result = index.search([1.0, 0.0, 0.0], top_k=2)

        self.assertEqual(result[0].target_id, 11)

    def test_vector_sqlite_roundtrip_loads_vector_set(self) -> None:
        vector = _vector_module()
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                vector.ensure_vector_schema(db.sqlite)
                with db.transaction(route="sqlite"):
                    vector.insert_vectors(
                        db.sqlite,
                        [
                            (1, [1.0, 0.0]),
                            (2, [0.0, 1.0]),
                        ],
                    )

                item_ids, matrix = vector.load_vector_matrix(db.sqlite)

        self.assertEqual(
            item_ids.tolist(),
            [("direct", "", 1), ("direct", "", 2)],
        )
        np.testing.assert_allclose(
            matrix,
            np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
        )

    def test_sql_owned_vectors_work_through_sql_and_vector_query(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, "
                        "title TEXT NOT NULL, "
                        "topic TEXT NOT NULL, "
                        "embedding BLOB)"
                    ),
                    route="sqlite",
                )

                db.executemany(
                    (
                        "INSERT INTO docs (id, title, topic, embedding) "
                        "VALUES (?, ?, ?, ?)"
                    ),
                    [
                        (1, "Alpha one", "alpha", [0.0, 1.0]),
                        (2, "Alpha two", "alpha", [0.8, 0.2]),
                        (3, "Beta one", "beta", [0.0, 1.0]),
                    ],
                    route="sqlite",
                )

                db.query(
                    "UPDATE docs SET embedding = ? WHERE id = ?",
                    route="sqlite",
                    params=([1.0, 0.0], 1),
                )

                relational = db.query(
                    "SELECT id, title FROM docs WHERE topic = ? ORDER BY id",
                    route="sqlite",
                    params=("alpha",),
                )
                self.assertEqual(
                    relational.rows,
                    ((1, "Alpha one"), (2, "Alpha two")),
                )

                vector_result = db.query(
                    "SELECT id FROM docs WHERE topic = ? ORDER BY id",
                    route="sqlite",
                    query_type="vector",
                    params={
                        "query": [1.0, 0.0],
                        "top_k": 3,
                        "scope_query_type": "sql",
                        "scope_params": ("alpha",),
                    },
                )

                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )
                self.assertAlmostEqual(vector_result.rows[0][3], 1.0, places=6)

    def test_cypher_owned_vectors_work_through_cypher_and_vector_query(self) -> None:
        HumemDB = _humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                created = []
                for name, cohort, embedding in (
                    ("Alice", "alpha", [0.0, 1.0]),
                    ("Bob", "alpha", [0.8, 0.2]),
                    ("Carol", "beta", [0.0, 1.0]),
                ):
                    result = db.query(
                        (
                            "CREATE (u:User {"
                            "name: $name, cohort: $cohort, embedding: $embedding})"
                        ),
                        route="sqlite",
                        query_type="cypher",
                        params={
                            "name": name,
                            "cohort": cohort,
                            "embedding": embedding,
                        },
                    )
                    created.append(result.rows[0][0])

                db.query(
                    "MATCH (u:User {name: 'Alice'}) SET u.embedding = $embedding",
                    route="sqlite",
                    query_type="cypher",
                    params={"embedding": [1.0, 0.0]},
                )

                graph_result = db.query(
                    (
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "RETURN u.id, u.name ORDER BY u.id"
                    ),
                    route="sqlite",
                    query_type="cypher",
                )
                self.assertEqual(
                    graph_result.rows,
                    ((created[0], "Alice"), (created[1], "Bob")),
                )

                vector_result = db.query(
                    "MATCH (u:User {cohort: 'alpha'}) RETURN u.id ORDER BY u.id",
                    route="sqlite",
                    query_type="vector",
                    params={
                        "query": [1.0, 0.0],
                        "top_k": 3,
                        "scope_query_type": "cypher",
                    },
                )

                self.assertEqual(
                    tuple((row[0], row[1], row[2]) for row in vector_result.rows),
                    (
                        ("graph_node", "", created[0]),
                        ("graph_node", "", created[1]),
                    ),
                )
                self.assertAlmostEqual(vector_result.rows[0][3], 1.0, places=6)


if __name__ == "__main__":
    unittest.main()
