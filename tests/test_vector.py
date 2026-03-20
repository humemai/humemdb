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
            item_ids=np.array([101, 102, 103]),
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

        self.assertEqual(tuple(match.item_id for match in result), (101, 102))

    def test_exact_vector_index_supports_candidate_filtering(self) -> None:
        vector = _vector_module()

        index = vector.ExactVectorIndex(
            item_ids=np.array([1, 2, 3]),
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

        self.assertEqual(tuple(match.item_id for match in result), (2, 3))

    def test_scalar_quantized_vector_index_keeps_top_match_on_simple_input(
        self,
    ) -> None:
        vector = _vector_module()

        index = vector.ScalarQuantizedVectorIndex.from_matrix(
            item_ids=np.array([11, 12, 13]),
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

        self.assertEqual(result[0].item_id, 11)

    def test_vector_sqlite_roundtrip_loads_collection(self) -> None:
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
                            (1, "default", 7, [1.0, 0.0]),
                            (2, "default", 7, [0.0, 1.0]),
                        ],
                    )

                item_ids, bucket_ids, matrix = vector.load_vector_matrix(
                    db.sqlite,
                    collection="default",
                )

        np.testing.assert_array_equal(item_ids, np.array([1, 2], dtype=np.int64))
        np.testing.assert_array_equal(bucket_ids, np.array([7, 7], dtype=np.int32))
        np.testing.assert_allclose(
            matrix,
            np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float32),
        )


if __name__ == "__main__":
    unittest.main()
