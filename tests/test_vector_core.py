from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np

from tests.support import humemdb_class, vector_module


class TestVectorCore(unittest.TestCase):
    def test_vector_blob_roundtrip(self) -> None:
        vector = vector_module()

        blob = vector.encode_vector_blob([1.5, -2.0, 3.25])
        decoded = vector.decode_vector_blob(blob, dimension=3)

        np.testing.assert_allclose(
            decoded,
            np.array([1.5, -2.0, 3.25], dtype=np.float32),
        )

    def test_exact_vector_index_returns_expected_cosine_order(self) -> None:
        vector = vector_module()

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
        vector = vector_module()

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
        vector = vector_module()

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
        vector = vector_module()
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
                vector.ensure_vector_schema(db.sqlite)
                with db.transaction():
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
