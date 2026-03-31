from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from humemdb import HumemDB
from humemdb.vector import (
    IndexedVectorRuntimeConfig,
    _LanceDBVectorIndex,
    encode_vector_blob,
)


class TestVectorAPI(unittest.TestCase):
    def test_search_vectors_uses_tiered_runtime_above_hot_cut(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=256,
                )
                inserted_ids = db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        *([[0.0, 1.0]] * 256),
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )
                self.assertEqual(
                    (
                        inserted_ids[0],
                        inserted_ids[1],
                        inserted_ids[-2],
                        inserted_ids[-1],
                    ),
                    (1, 2, 259, 260),
                )

                result = db.search_vectors([1.0, 0.0], top_k=3)

                self.assertEqual(tuple(row[2] for row in result.rows), (2, 260, 259))
                self.assertEqual(set(db._snapshot_vector_index_cache), {"cosine"})

    def test_search_vectors_keeps_small_snapshot_delta_exact_until_refresh_trigger(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=2_000,
                )
                inserted_ids = db.insert_vectors(
                    [
                        [1.0, 0.0],
                        [0.99, 0.01],
                        *([[0.0, 1.0]] * 1298),
                    ]
                )
                self.assertEqual(inserted_ids[:2], (1, 2))

                result = db.search_vectors([1.0, 0.0], top_k=2)
                self.assertEqual(tuple(row[2] for row in result.rows), (1, 2))
                self.assertFalse(db._snapshot_vector_index_cache)

    def test_search_vectors_returns_expected_matches(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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

    def test_insert_vectors_keeps_cached_snapshot_for_pending_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=10,
                )
                inserted_ids = db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        *([[0.0, 1.0]] * 256),
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )
                self.assertEqual(
                    (
                        inserted_ids[0],
                        inserted_ids[1],
                        inserted_ids[-2],
                        inserted_ids[-1],
                    ),
                    (1, 2, 259, 260),
                )

                first_result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(first_result.rows[0][2], 2)
                self.assertEqual(set(db._snapshot_vector_index_cache), {"cosine"})
                first_snapshot_rows = (
                    db._snapshot_vector_index_cache["cosine"].item_ids.size
                )

                inserted_ids = db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [0.0, 1.0],
                    ]
                )
                self.assertEqual(inserted_ids, (261, 262))
                self.assertEqual(
                    db._snapshot_vector_index_cache["cosine"].item_ids.size,
                    first_snapshot_rows,
                )

                second_result = db.search_vectors([1.0, 0.0], top_k=3)
                self.assertEqual(
                    tuple(row[2] for row in second_result.rows),
                    (2, 260, 259),
                )
                self.assertEqual(
                    db._snapshot_vector_index_cache["cosine"].item_ids.size,
                    first_snapshot_rows,
                )

    def test_insert_vectors_refreshes_snapshot_after_pending_delta_threshold(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=10,
                )
                db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        *([[0.0, 1.0]] * 256),
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )

                db.search_vectors([1.0, 0.0], top_k=1)
                first_snapshot_rows = (
                    db._snapshot_vector_index_cache["cosine"].item_ids.size
                )
                self.assertEqual(first_snapshot_rows, 260)

                db.insert_vectors([[0.0, 1.0]] * 10)

                result = db.search_vectors([1.0, 0.0], top_k=3)
                self.assertEqual(tuple(row[2] for row in result.rows), (2, 260, 259))
                self.assertTrue(db._await_vector_runtime_snapshot_refresh())
                self.assertEqual(
                    db._snapshot_vector_index_cache["cosine"].item_ids.size,
                    270,
                )

    def test_pause_vector_index_defers_background_refresh_until_resumed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=10,
                )
                db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        *([[0.0, 1.0]] * 256),
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )

                built = db.build_vector_index(index_name="direct_similarity_idx")
                self.assertEqual(built["state"], "ready")

                paused = db.pause_vector_index(index_name="direct_similarity_idx")
                self.assertTrue(paused["maintenance_paused"])

                db.insert_vectors([[0.0, 1.0]] * 10)

                paused_state = db.inspect_vector_index(
                    index_name="direct_similarity_idx"
                )
                self.assertEqual(paused_state["state"], "ready")
                self.assertTrue(paused_state["maintenance_paused"])
                self.assertFalse(paused_state["refresh_in_progress"])
                self.assertGreater(paused_state["delta_rows"], 0)
                self.assertFalse(db._await_vector_runtime_snapshot_refresh())

                result = db.search_vectors([1.0, 0.0], top_k=3)
                self.assertEqual(tuple(row[2] for row in result.rows), (2, 260, 259))

                resumed = db.resume_vector_index(index_name="direct_similarity_idx")
                self.assertFalse(resumed["maintenance_paused"])

                refreshed = db.refresh_vector_index(
                    index_name="direct_similarity_idx"
                )
                self.assertFalse(refreshed["maintenance_paused"])
                self.assertEqual(refreshed["delta_rows"], 0)

    def test_background_snapshot_refresh_reports_runtime_state_and_promotes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"
            original_from_matrix = _LanceDBVectorIndex.from_matrix
            allow_second_build = threading.Event()
            second_build_started = threading.Event()
            build_count = 0

            def delayed_from_matrix(*args: object, **kwargs: object) -> object:
                nonlocal build_count
                build_count += 1
                if build_count == 2:
                    second_build_started.set()
                    if not allow_second_build.wait(timeout=5):
                        raise TimeoutError("Timed out waiting for background refresh")
                return original_from_matrix(*args, **kwargs)

            with mock.patch(
                "humemdb.db._LanceDBVectorIndex.from_matrix",
                side_effect=delayed_from_matrix,
            ):
                with HumemDB(base_path) as db:
                    db._vector_runtime_config = IndexedVectorRuntimeConfig(
                        ann_min_vectors=1,
                    )
                    db.insert_vectors(
                        [
                            [0.0, 1.0],
                            [1.0, 0.0],
                            *([[0.0, 1.0]] * 256),
                            [0.97, 0.03],
                            [0.99, 0.01],
                        ]
                    )

                    db.search_vectors([1.0, 0.0], top_k=1)
                    first_state = db._inspect_vector_runtime()
                    self.assertEqual(first_state["snapshot_rows"], 260)

                    db.insert_vectors([[0.0, 1.0]])
                    result = db.search_vectors([1.0, 0.0], top_k=3)
                    self.assertEqual(
                        tuple(row[2] for row in result.rows),
                        (2, 260, 259),
                    )
                    self.assertTrue(second_build_started.wait(timeout=5))

                    in_progress = db._inspect_vector_runtime()
                    self.assertTrue(in_progress["refresh_in_progress"])
                    self.assertEqual(in_progress["delta_rows"], 1)
                    self.assertEqual(in_progress["snapshot_rows"], 260)

                    allow_second_build.set()
                    self.assertTrue(db._await_vector_runtime_snapshot_refresh())
                    final_state = db._inspect_vector_runtime()
                    self.assertFalse(final_state["refresh_in_progress"])
                    self.assertEqual(final_state["delta_rows"], 0)
                    self.assertEqual(final_state["snapshot_rows"], 261)

    def test_delete_vectors_keeps_snapshot_until_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=1,
                )
                db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        *([[0.0, 1.0]] * 256),
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )

                db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(
                    db._inspect_vector_runtime()["snapshot_rows"],
                    260,
                )

                self.assertEqual(db.delete_vectors([2]), 1)

                result = db.search_vectors([1.0, 0.0], top_k=2)
                self.assertEqual(tuple(row[2] for row in result.rows), (260, 259))
                in_progress = db._inspect_vector_runtime()
                self.assertEqual(in_progress["snapshot_rows"], 260)
                self.assertEqual(in_progress["tombstone_rows"], 1)

                self.assertTrue(db._await_vector_runtime_snapshot_refresh())
                final_state = db._inspect_vector_runtime()
                self.assertEqual(final_state["snapshot_rows"], 259)
                self.assertEqual(final_state["tombstone_rows"], 0)

    def test_reopen_uses_persisted_snapshot_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=256,
                )
                db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        *([[0.0, 1.0]] * 256),
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )
                db.search_vectors([1.0, 0.0], top_k=1)
                first_state = db._inspect_vector_runtime()
                self.assertEqual(first_state["snapshot_rows"], 260)
                generation = first_state["snapshot_generation"]

            with mock.patch(
                "humemdb.db._LanceDBVectorIndex.from_matrix",
                side_effect=AssertionError("snapshot should reopen, not rebuild"),
            ):
                with HumemDB(base_path) as db:
                    db._vector_runtime_config = IndexedVectorRuntimeConfig(
                        ann_min_vectors=256,
                    )
                    result = db.search_vectors([1.0, 0.0], top_k=3)
                    self.assertEqual(
                        tuple(row[2] for row in result.rows),
                        (2, 260, 259),
                    )
                    second_state = db._inspect_vector_runtime()
                    self.assertEqual(second_state["snapshot_rows"], 260)
                    self.assertEqual(
                        second_state["snapshot_generation"],
                        generation,
                    )

    def test_public_vector_index_lifecycle_methods_manage_named_index_state(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=256,
                )
                db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        *([[0.0, 1.0]] * 256),
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )

                built = db.build_vector_index(index_name="docs_embedding_idx")
                self.assertEqual(built["name"], "docs_embedding_idx")
                self.assertTrue(built["enabled"])
                self.assertEqual(built["state"], "ready")
                self.assertEqual(built["snapshot_rows"], 260)

                dropped = db.drop_vector_index(index_name="docs_embedding_idx")
                self.assertFalse(dropped["enabled"])
                self.assertEqual(dropped["state"], "disabled")
                self.assertEqual(dropped["snapshot_rows"], 0)

                result = db.search_vectors([1.0, 0.0], top_k=3)
                self.assertEqual(tuple(row[2] for row in result.rows), (2, 260, 259))
                after_search = db.inspect_vector_index(index_name="docs_embedding_idx")
                self.assertFalse(after_search["enabled"])
                self.assertEqual(after_search["snapshot_rows"], 0)

                refreshed = db.refresh_vector_index(index_name="docs_embedding_idx")
                self.assertTrue(refreshed["enabled"])
                self.assertEqual(refreshed["state"], "ready")
                self.assertEqual(refreshed["snapshot_rows"], 260)

    def test_insert_vectors_can_use_explicit_direct_ids(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={
                        "query": [0.8, 0.2],
                    },
                )
                self.assertEqual(cypher_candidate_filtered.query_type, "cypher")
                self.assertEqual(cypher_candidate_filtered.rows[0][0], 1)

    def test_raw_sql_vector_write_invalidates_cached_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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
                        "embedding": encode_vector_blob([1.0, 0.0]),
                    },
                )

                second_result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(second_result.rows[0][2], 3)

    def test_preload_vectors_warms_existing_vector_set(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                inserted_ids = db.insert_vectors(
                    [
                        [1.0, 0.0],
                        [0.0, 1.0],
                    ]
                )
                self.assertEqual(inserted_ids, (1, 2))

            with HumemDB(base_path, preload_vectors=True) as db:
                self.assertTrue(db.vectors_cached())

                result = db.search_vectors([1.0, 0.0], top_k=1)
                self.assertEqual(result.rows[0][2], 1)
                self.assertIsNone(result.query_type)

    def test_preload_vectors_ignores_missing_vector_table(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"
            with HumemDB(base_path, preload_vectors=True) as db:
                self.assertFalse(db.vectors_cached())

    def test_search_vectors_falls_back_on_tiny_snapshot_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=256,
                )
                inserted_ids = db.insert_vectors(
                    [
                        [0.0, 1.0],
                        [1.0, 0.0],
                        [0.97, 0.03],
                        [0.99, 0.01],
                    ]
                )
                self.assertEqual(inserted_ids, (1, 2, 3, 4))

                result = db.search_vectors([1.0, 0.0], top_k=3)

                self.assertEqual(tuple(row[2] for row in result.rows), (2, 4, 3))
                self.assertFalse(db._snapshot_vector_index_cache)
