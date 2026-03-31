from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from humemdb import HumemDB
from humemdb.vector import IndexedVectorRuntimeConfig


class TestVectorQueries(unittest.TestCase):
    def test_vector_index_admin_commands_support_idempotent_forms_and_errors(
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

                db.query(
                    "CREATE INDEX docs_embedding_idx ON docs USING ivfpq "
                    "(embedding vector_cosine_ops)"
                )
                db.query(
                    "ALTER VECTOR INDEX docs_embedding_idx PAUSE MAINTENANCE"
                )
                db.query(
                    "ALTER VECTOR INDEX docs_embedding_idx RESUME MAINTENANCE"
                )
                db.query("REFRESH VECTOR INDEX docs_embedding_idx")
                db.query("REBUILD VECTOR INDEX docs_embedding_idx")

                with self.assertRaises(Exception):
                    db.query("REINDEX INDEX docs_embedding_idx")

                with self.assertRaises(Exception):
                    db.query(
                        "ALTER INDEX docs_embedding_idx "
                        "SET (maintenance_paused = on)"
                    )

                with self.assertRaises(Exception):
                    db.query(
                        "ALTER INDEX docs_embedding_idx "
                        "SET (maintenance_paused = off)"
                    )

                db.query(
                    "CREATE INDEX IF NOT EXISTS docs_embedding_idx ON docs USING ivfpq "
                    "(embedding vector_cosine_ops)"
                )
                db.query("DROP INDEX docs_embedding_idx")
                db.query("DROP INDEX IF EXISTS docs_embedding_idx")

                db.query(
                    "CREATE VECTOR INDEX graph_similarity_idx IF NOT EXISTS "
                    "FOR (u:User) ON (u.embedding) "
                    "OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}"
                )
                db.query(
                    "ALTER VECTOR INDEX graph_similarity_idx "
                    "PAUSE MAINTENANCE"
                )
                db.query(
                    "ALTER VECTOR INDEX graph_similarity_idx "
                    "RESUME MAINTENANCE"
                )
                db.query("REFRESH VECTOR INDEX graph_similarity_idx")
                db.query("REBUILD VECTOR INDEX graph_similarity_idx")

                with self.assertRaises(Exception):
                    db.query(
                        "ALTER VECTOR INDEX graph_similarity_idx "
                        "SET {maintenancePaused: true}"
                    )

                with self.assertRaises(Exception):
                    db.query(
                        "ALTER VECTOR INDEX graph_similarity_idx "
                        "SET {maintenancePaused: false}"
                    )

                db.query(
                    "CREATE VECTOR INDEX graph_similarity_idx IF NOT EXISTS "
                    "FOR (u:User) ON (u.embedding) "
                    "OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}"
                )
                db.query("DROP VECTOR INDEX graph_similarity_idx")
                db.query("DROP VECTOR INDEX IF EXISTS graph_similarity_idx")

                db.query(
                    "CREATE VECTOR INDEX graph_dot_idx IF NOT EXISTS "
                    "FOR (u:User) ON (u.embedding) "
                    "OPTIONS {indexConfig: {`vector.similarity_function`: 'euclidean'}}"
                )
                self.assertEqual(
                    db.query("SHOW VECTOR INDEXES").rows[0][1],
                    "l2",
                )
                db.query("DROP VECTOR INDEX graph_dot_idx")

                db.query(
                    "CREATE INDEX docs_embedding_idx ON docs USING ivfpq "
                    "(embedding vector_cosine_ops)"
                )
                with self.assertRaisesRegex(ValueError, "already managed by index"):
                    db.query(
                        "CREATE INDEX docs_embedding_idx_v2 ON docs USING ivfpq "
                        "(embedding vector_cosine_ops)"
                    )

    def test_sql_vector_index_admin_commands_manage_public_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=256,
                )
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    [
                        {"id": 1, "title": "Cold miss", "embedding": [0.0, 1.0]},
                        {"id": 2, "title": "Cold best", "embedding": [1.0, 0.0]},
                        *[
                            {
                                "id": idx,
                                "title": f"Cold filler {idx}",
                                "embedding": [0.0, 1.0],
                            }
                            for idx in range(3, 259)
                        ],
                        {"id": 259, "title": "Hot third", "embedding": [0.97, 0.03]},
                        {"id": 260, "title": "Hot second", "embedding": [0.99, 0.01]},
                    ],
                )

                created = db.query(
                    "CREATE INDEX docs_embedding_idx ON docs USING ivfpq "
                    "(embedding vector_cosine_ops)"
                )
                self.assertEqual(created.query_type, "sql")
                self.assertEqual(created.rows[0][0], "docs_embedding_idx")
                self.assertEqual(created.rows[0][3], "ready")
                self.assertEqual(created.rows[0][6], 260)

                catalog = db.query("SELECT * FROM humemdb_vector_indexes")
                self.assertEqual(catalog.query_type, "sql")
                self.assertEqual(len(catalog.rows), 1)
                self.assertEqual(catalog.rows[0][0], "docs_embedding_idx")
                self.assertFalse(catalog.rows[0][-1])

                paused = db.query(
                    "ALTER VECTOR INDEX docs_embedding_idx PAUSE MAINTENANCE"
                )
                self.assertTrue(paused.rows[0][-1])
                self.assertEqual(paused.rows[0][3], "ready")

                resumed = db.query(
                    "ALTER VECTOR INDEX docs_embedding_idx RESUME MAINTENANCE"
                )
                self.assertFalse(resumed.rows[0][-1])

                refreshed = db.query("REFRESH VECTOR INDEX docs_embedding_idx")
                self.assertFalse(refreshed.rows[0][-1])

                rebuilt = db.query("REBUILD VECTOR INDEX docs_embedding_idx")
                self.assertEqual(rebuilt.rows[0][3], "ready")

                dropped = db.query("DROP INDEX docs_embedding_idx")
                self.assertFalse(dropped.rows[0][2])
                self.assertEqual(dropped.rows[0][3], "disabled")
                self.assertFalse(dropped.rows[0][-1])

                after_drop = db.query("SELECT * FROM humemdb_vector_indexes")
                self.assertEqual(after_drop.rows, ())
                self.assertEqual(
                    db.inspect_vector_index(index_name="docs_embedding_idx")["state"],
                    "disabled",
                )

    def test_cypher_vector_index_admin_commands_manage_public_lifecycle(self) -> None:
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

                created = db.query(
                    "CREATE VECTOR INDEX user_embedding_idx IF NOT EXISTS "
                    "FOR (u:User) ON (u.embedding) "
                    "OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}"
                )
                self.assertEqual(created.query_type, "cypher")
                self.assertEqual(created.rows[0][0], "user_embedding_idx")
                self.assertEqual(created.rows[0][3], "ready")

                shown = db.query("SHOW VECTOR INDEXES")
                self.assertEqual(shown.query_type, "cypher")
                self.assertEqual(len(shown.rows), 1)
                self.assertEqual(shown.rows[0][0], "user_embedding_idx")
                self.assertFalse(shown.rows[0][-1])

                paused = db.query(
                    "ALTER VECTOR INDEX user_embedding_idx PAUSE MAINTENANCE"
                )
                self.assertTrue(paused.rows[0][-1])
                self.assertEqual(paused.rows[0][3], "ready")

                resumed = db.query(
                    "ALTER VECTOR INDEX user_embedding_idx RESUME MAINTENANCE"
                )
                self.assertFalse(resumed.rows[0][-1])

                refreshed = db.query("REFRESH VECTOR INDEX user_embedding_idx")
                self.assertFalse(refreshed.rows[0][-1])

                rebuilt = db.query("REBUILD VECTOR INDEX user_embedding_idx")
                self.assertEqual(rebuilt.rows[0][3], "ready")

                dropped = db.query("DROP VECTOR INDEX user_embedding_idx")
                self.assertFalse(dropped.rows[0][2])
                self.assertEqual(dropped.rows[0][3], "disabled")
                self.assertFalse(dropped.rows[0][-1])

                shown_after = db.query("SHOW VECTOR INDEXES")
                self.assertEqual(shown_after.rows, ())

    def test_cypher_vector_query_accepts_created_named_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE (:User {id: $id, name: $name, embedding: $embedding})",
                    params={"id": 1, "name": "Alice", "embedding": [1.0, 0.0]},
                )
                db.query(
                    "CREATE (:User {id: $id, name: $name, embedding: $embedding})",
                    params={"id": 2, "name": "Bob", "embedding": [0.0, 1.0]},
                )
                db.query(
                    "CREATE VECTOR INDEX user_embedding_idx IF NOT EXISTS "
                    "FOR (u:User) ON (u.embedding) "
                    "OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}"
                )

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User) "
                        "RETURN node.id, score"
                    ),
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.rows[0][0], 1)

    def test_cypher_query_nodes_supports_neo4j_like_vector_search_subset(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE (:User {id: $id, name: $name, embedding: $embedding})",
                    params={"id": 1, "name": "Alice", "embedding": [1.0, 0.0]},
                )
                db.query(
                    "CREATE (:User {id: $id, name: $name, embedding: $embedding})",
                    params={"id": 2, "name": "Bob", "embedding": [0.0, 1.0]},
                )
                db.query(
                    "CREATE VECTOR INDEX user_embedding_idx IF NOT EXISTS "
                    "FOR (u:User) ON (u.embedding) "
                    "OPTIONS {indexConfig: {`vector.similarity_function`: 'cosine'}}"
                )

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score RETURN node.id, score"
                    ),
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(result.query_type, "cypher")
                self.assertEqual(result.columns, ("node.id", "score"))
                self.assertEqual(result.rows[0][0], 1)
                self.assertGreater(result.rows[0][1], 0.9)

    def test_sql_vector_query_uses_tiered_runtime_above_hot_cut(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=256,
                )
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    [
                        {"id": 1, "title": "Cold miss", "embedding": [0.0, 1.0]},
                        {"id": 2, "title": "Cold best", "embedding": [1.0, 0.0]},
                        *[
                            {
                                "id": idx,
                                "title": f"Cold filler {idx}",
                                "embedding": [0.0, 1.0],
                            }
                            for idx in range(3, 259)
                        ],
                        {"id": 259, "title": "Hot third", "embedding": [0.97, 0.03]},
                        {"id": 260, "title": "Hot second", "embedding": [0.99, 0.01]},
                    ],
                )

                result = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 3",
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("sql_row", "docs", 2),
                        ("sql_row", "docs", 260),
                        ("sql_row", "docs", 259),
                    ),
                )

    def test_sql_vector_query_keeps_small_snapshot_delta_exact_until_refresh_trigger(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=2_000,
                )
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    [
                        {"id": 1, "title": "Cold best", "embedding": [1.0, 0.0]},
                        {
                            "id": 2,
                            "title": "Cold second",
                            "embedding": [0.99, 0.01],
                        },
                        *[
                            {
                                "id": idx,
                                "title": f"Hot filler {idx}",
                                "embedding": [0.0, 1.0],
                            }
                            for idx in range(3, 1301)
                        ],
                    ],
                )

                result = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 2",
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )
                self.assertFalse(db._snapshot_vector_index_cache)

    def test_sql_vector_insert_keeps_snapshot_for_pending_delta(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=10,
                )
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    [
                        {"id": 1, "title": "Cold miss", "embedding": [0.0, 1.0]},
                        {"id": 2, "title": "Cold best", "embedding": [1.0, 0.0]},
                        *[
                            {
                                "id": idx,
                                "title": f"Cold filler {idx}",
                                "embedding": [0.0, 1.0],
                            }
                            for idx in range(3, 259)
                        ],
                        {"id": 259, "title": "Hot third", "embedding": [0.97, 0.03]},
                        {"id": 260, "title": "Hot second", "embedding": [0.99, 0.01]},
                    ],
                )

                db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={"query": [1.0, 0.0]},
                )
                first_snapshot_rows = (
                    db._snapshot_vector_index_cache["cosine"].item_ids.size
                )

                db.executemany(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    [
                        {"id": 261, "title": "New filler 261", "embedding": [0.0, 1.0]},
                        {"id": 262, "title": "New filler 262", "embedding": [0.0, 1.0]},
                    ],
                )

                result = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 3",
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("sql_row", "docs", 2),
                        ("sql_row", "docs", 260),
                        ("sql_row", "docs", 259),
                    ),
                )
                self.assertEqual(
                    db._snapshot_vector_index_cache["cosine"].item_ids.size,
                    first_snapshot_rows,
                )

    def test_cypher_create_with_embedding_keeps_snapshot_for_pending_delta(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=10,
                )
                for idx in range(1, 261):
                    embedding = [0.0, 1.0]
                    if idx == 2:
                        embedding = [1.0, 0.0]
                    elif idx == 259:
                        embedding = [0.97, 0.03]
                    elif idx == 260:
                        embedding = [0.99, 0.01]
                    db.query(
                        (
                            "CREATE (u:User {"
                            "name: $name, cohort: $cohort, embedding: $embedding})"
                        ),
                        params={
                            "name": f"User {idx}",
                            "cohort": "alpha",
                            "embedding": embedding,
                        },
                    )

                db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                first_state = db._inspect_vector_runtime()
                self.assertEqual(first_state["snapshot_rows"], 260)

                for idx in range(261, 263):
                    db.query(
                        (
                            "CREATE (u:User {"
                            "name: $name, cohort: $cohort, embedding: $embedding})"
                        ),
                        params={
                            "name": f"User {idx}",
                            "cohort": "alpha",
                            "embedding": [0.0, 1.0],
                        },
                    )

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 3, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(
                    tuple(row[0] for row in result.rows),
                    (2, 259, 260),
                )
                second_state = db._inspect_vector_runtime()
                self.assertEqual(second_state["total_rows"], 262)
                self.assertEqual(second_state["indexed_rows"], 260)
                self.assertEqual(second_state["delta_rows"], 2)
                self.assertEqual(second_state["snapshot_rows"], 260)

    def test_sql_delete_keeps_snapshot_until_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=1,
                )
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    [
                        {"id": 1, "title": "Cold miss", "embedding": [0.0, 1.0]},
                        {"id": 2, "title": "Cold best", "embedding": [1.0, 0.0]},
                        *[
                            {
                                "id": idx,
                                "title": f"Cold filler {idx}",
                                "embedding": [0.0, 1.0],
                            }
                            for idx in range(3, 259)
                        ],
                        {"id": 259, "title": "Hot third", "embedding": [0.97, 0.03]},
                        {"id": 260, "title": "Hot second", "embedding": [0.99, 0.01]},
                    ],
                )

                db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={"query": [1.0, 0.0]},
                )

                deleted = db.query(
                    "DELETE FROM docs WHERE id = $id",
                    params={"id": 2},
                )
                self.assertEqual(deleted.rowcount, 1)

                result = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 2",
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (("sql_row", "docs", 260), ("sql_row", "docs", 259)),
                )
                in_progress = db._inspect_vector_runtime()
                self.assertEqual(in_progress["snapshot_rows"], 260)
                self.assertEqual(in_progress["tombstone_rows"], 1)

                self.assertTrue(db._await_vector_runtime_snapshot_refresh())
                final_state = db._inspect_vector_runtime()
                self.assertEqual(final_state["snapshot_rows"], 259)
                self.assertEqual(final_state["tombstone_rows"], 0)

    def test_cypher_delete_keeps_snapshot_until_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db._vector_runtime_config = IndexedVectorRuntimeConfig(
                    ann_min_vectors=1,
                )
                for idx in range(1, 261):
                    embedding = [0.0, 1.0]
                    if idx == 2:
                        embedding = [1.0, 0.0]
                    elif idx == 259:
                        embedding = [0.97, 0.03]
                    elif idx == 260:
                        embedding = [0.99, 0.01]
                    db.query(
                        (
                            "CREATE (u:User {"
                            "name: $name, cohort: $cohort, embedding: $embedding})"
                        ),
                        params={
                            "name": f"User {idx}",
                            "cohort": "alpha",
                            "embedding": embedding,
                        },
                    )

                db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )

                deleted = db.query(
                    "MATCH (u:User {name: $name}) DETACH DELETE u",
                    params={"name": "User 2"},
                )
                self.assertEqual(deleted.rowcount, 1)

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 2, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(
                    tuple(row[0] for row in result.rows),
                    (259, 260),
                )
                in_progress = db._inspect_vector_runtime()
                self.assertEqual(in_progress["snapshot_rows"], 260)
                self.assertEqual(in_progress["tombstone_rows"], 1)

                self.assertTrue(db._await_vector_runtime_snapshot_refresh())
                final_state = db._inspect_vector_runtime()
                self.assertEqual(final_state["snapshot_rows"], 259)
                self.assertEqual(final_state["tombstone_rows"], 0)

    def test_sql_owned_vectors_work_through_sql_and_vector_query(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, "
                        "title TEXT NOT NULL, "
                        "topic TEXT NOT NULL, "
                        "embedding BLOB)"
                    ),
                )

                db.executemany(
                    (
                        "INSERT INTO docs (id, title, topic, embedding) "
                        "VALUES ($id, $title, $topic, $embedding)"
                    ),
                    [
                        {
                            "id": 1,
                            "title": "Alpha one",
                            "topic": "alpha",
                            "embedding": [0.0, 1.0],
                        },
                        {
                            "id": 2,
                            "title": "Alpha two",
                            "topic": "alpha",
                            "embedding": [0.8, 0.2],
                        },
                        {
                            "id": 3,
                            "title": "Beta one",
                            "topic": "beta",
                            "embedding": [0.0, 1.0],
                        },
                    ],
                )

                db.query(
                    "UPDATE docs SET embedding = $embedding WHERE id = $id",
                    params={"embedding": [1.0, 0.0], "id": 1},
                )

                relational = db.query(
                    "SELECT id, title FROM docs WHERE topic = $topic ORDER BY id",
                    params={"topic": "alpha"},
                )
                self.assertEqual(
                    relational.rows,
                    ((1, "Alpha one"), (2, "Alpha two")),
                )

                vector_result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={
                        "query": [1.0, 0.0],
                        "topic": "alpha",
                    },
                )

                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )
                self.assertAlmostEqual(vector_result.rows[0][3], 1.0, places=6)

    def test_cypher_owned_vectors_work_through_cypher_and_vector_query(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
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
                        params={
                            "name": name,
                            "cohort": cohort,
                            "embedding": embedding,
                        },
                    )
                    created.append(result.rows[0][0])

                db.query(
                    "MATCH (u:User {name: 'Alice'}) SET u.embedding = $embedding",
                    params={"embedding": [1.0, 0.0]},
                )

                graph_result = db.query(
                    (
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "RETURN u.id, u.name ORDER BY u.id"
                    )
                )
                self.assertEqual(
                    graph_result.rows,
                    ((created[0], "Alice"), (created[1], "Bob")),
                )

                vector_result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 3, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={
                        "query": [1.0, 0.0],
                    },
                )

                self.assertEqual(
                    tuple(row[0] for row in vector_result.rows),
                    (created[0], created[1]),
                )
                self.assertAlmostEqual(vector_result.rows[0][1], 1.0, places=6)

    def test_sql_insert_with_embedding_updates_row_and_vector_store(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, "
                        "title TEXT NOT NULL, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )

                inserted = db.executemany(
                    (
                        "INSERT INTO docs (id, title, topic, embedding) "
                        "VALUES ($id, $title, $topic, $embedding)"
                    ),
                    [
                        {
                            "id": 1,
                            "title": "Alpha one",
                            "topic": "alpha",
                            "embedding": [1.0, 0.0],
                        },
                        {
                            "id": 2,
                            "title": "Alpha two",
                            "topic": "alpha",
                            "embedding": [0.8, 0.2],
                        },
                        {
                            "id": 3,
                            "title": "Beta one",
                            "topic": "beta",
                            "embedding": [0.0, 1.0],
                        },
                    ],
                )

                self.assertEqual(inserted.rowcount, 3)

                relational = db.query("SELECT id, title, topic FROM docs ORDER BY id")
                self.assertEqual(
                    relational.rows,
                    (
                        (1, "Alpha one", "alpha"),
                        (2, "Alpha two", "alpha"),
                        (3, "Beta one", "beta"),
                    ),
                )

                vector_result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={
                        "query": [1.0, 0.0],
                        "topic": "alpha",
                    },
                )
                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )

    def test_sql_insert_with_auto_ids_updates_row_and_vector_store(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, "
                        "title TEXT NOT NULL, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )

                inserted = db.executemany(
                    (
                        "INSERT INTO docs (title, topic, embedding) "
                        "VALUES ($title, $topic, $embedding)"
                    ),
                    [
                        {
                            "title": "Alpha one",
                            "topic": "alpha",
                            "embedding": [1.0, 0.0],
                        },
                        {
                            "title": "Alpha two",
                            "topic": "alpha",
                            "embedding": [0.8, 0.2],
                        },
                        {
                            "title": "Beta one",
                            "topic": "beta",
                            "embedding": [0.0, 1.0],
                        },
                    ],
                )

                self.assertEqual(inserted.rowcount, 3)

                relational = db.query("SELECT id, title, topic FROM docs ORDER BY id")
                self.assertEqual(
                    relational.rows,
                    (
                        (1, "Alpha one", "alpha"),
                        (2, "Alpha two", "alpha"),
                        (3, "Beta one", "beta"),
                    ),
                )

                vector_result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={
                        "query": [1.0, 0.0],
                        "topic": "alpha",
                    },
                )
                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )

    def test_sql_single_insert_with_auto_id_updates_row_and_vector_store(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )

                db.query(
                    "INSERT INTO docs (title, embedding) VALUES ($title, $embedding)",
                    params={"title": "Alpha", "embedding": [1.0, 0.0]},
                )

                relational = db.query("SELECT id, title FROM docs ORDER BY id")
                self.assertEqual(relational.rows, ((1, "Alpha"),))

                vector_result = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(vector_result.rows[0][:3], ("sql_row", "docs", 1))

    def test_sql_update_with_embedding_updates_row_and_vector_store(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, title TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.query(
                    (
                        "INSERT INTO docs (id, title, embedding) "
                        "VALUES ($id, $title, $embedding)"
                    ),
                    params={"id": 1, "title": "Alpha", "embedding": [0.0, 1.0]},
                )

                first = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(first.rows[0][:3], ("sql_row", "docs", 1))

                db.query(
                    "UPDATE docs SET embedding = $embedding WHERE id = $id",
                    params={"embedding": [1.0, 0.0], "id": 1},
                )

                second = db.query(
                    "SELECT id FROM docs ORDER BY embedding <=> $query LIMIT 1",
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(second.rows[0][:3], ("sql_row", "docs", 1))
                self.assertAlmostEqual(second.rows[0][3], 1.0, places=6)

    def test_cypher_create_with_embedding_keeps_node_and_vector_write_together(
        self,
    ) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                node_ids = []
                for name, cohort, embedding in (
                    ("Alice", "alpha", [1.0, 0.0]),
                    ("Bob", "alpha", [0.85, 0.15]),
                    ("Carol", "beta", [0.0, 1.0]),
                ):
                    created = db.query(
                        (
                            "CREATE (u:User {"
                            "name: $name, cohort: $cohort, embedding: $embedding})"
                        ),
                        params={
                            "name": name,
                            "cohort": cohort,
                            "embedding": embedding,
                        },
                    )
                    node_ids.append(created.rows[0][0])
                node_ids = tuple(node_ids)

                self.assertEqual(len(node_ids), 3)

                graph_result = db.query(
                    "MATCH (u:User) RETURN u.id, u.name ORDER BY u.id"
                )
                self.assertEqual(
                    graph_result.rows,
                    (
                        (node_ids[0], "Alice"),
                        (node_ids[1], "Bob"),
                        (node_ids[2], "Carol"),
                    ),
                )

                vector_result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 3, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(
                    tuple(row[0] for row in vector_result.rows),
                    (node_ids[0], node_ids[1]),
                )

    def test_cypher_match_set_embedding_updates_vector_store(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                created = db.query(
                    (
                        "CREATE (u:User {"
                        "name: $name, cohort: $cohort, embedding: $embedding})"
                    ),
                    params={
                        "name": "Alice",
                        "cohort": "alpha",
                        "embedding": [0.0, 1.0],
                    },
                )
                node_id = created.rows[0][0]

                db.query(
                    "MATCH (u:User {name: 'Alice'}) SET u.embedding = $embedding",
                    params={"embedding": [1.0, 0.0]},
                )

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(result.rows[0][0], node_id)
                self.assertAlmostEqual(result.rows[0][1], 1.0, places=6)

    def test_cypher_match_set_updates_vector_and_scalar_properties_together(
        self,
    ) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                created = db.query(
                    (
                        "CREATE (u:User {"
                        "name: $name, cohort: $cohort, embedding: $embedding})"
                    ),
                    params={
                        "name": "Alice",
                        "cohort": "alpha",
                        "embedding": [0.0, 1.0],
                    },
                )
                node_id = created.rows[0][0]

                updated = db.query(
                    "MATCH (u:User {name: $name}) "
                    "SET u.embedding = $embedding, u.cohort = $cohort",
                    params={
                        "name": "Alice",
                        "embedding": [1.0, 0.0],
                        "cohort": "beta",
                    },
                )

                self.assertEqual(updated.rowcount, 1)

                graph_result = db.query(
                    "MATCH (u:User {name: 'Alice'}) RETURN u.cohort"
                )
                self.assertEqual(graph_result.rows, (("beta",),))

                vector_result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'beta'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(vector_result.rows[0][0], node_id)
                self.assertAlmostEqual(vector_result.rows[0][1], 1.0, places=6)

    def test_cypher_detach_delete_invalidates_graph_vector_cache(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                created = db.query(
                    (
                        "CREATE (u:User {"
                        "name: $name, cohort: $cohort, embedding: $embedding})"
                    ),
                    params={
                        "name": "Alice",
                        "cohort": "alpha",
                        "embedding": [1.0, 0.0],
                    },
                )
                node_id = created.rows[0][0]

                initial = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(initial.rows[0][0], node_id)
                self.assertTrue(db.vectors_cached())

                deleted = db.query(
                    "MATCH (u:User {name: 'Alice'}) DETACH DELETE u"
                )

                self.assertEqual(deleted.rowcount, 1)
                self.assertFalse(db.vectors_cached())

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(result.rows, ())

    def test_cypher_rejects_second_vector_property_for_same_node(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                created = db.query(
                    (
                        "CREATE (u:User {"
                        "name: $name, cohort: $cohort, embedding: $embedding})"
                    ),
                    params={
                        "name": "Alice",
                        "cohort": "alpha",
                        "embedding": [0.0, 1.0],
                    },
                )
                node_id = created.rows[0][0]

                with self.assertRaisesRegex(
                    ValueError,
                    "only one vector-valued property per node",
                ):
                    db.query(
                        "MATCH (u:User {name: 'Alice'}) SET u.profile = $embedding",
                        params={"embedding": [1.0, 0.0]},
                    )

                graph_result = db.query(
                    "MATCH (u:User {name: 'Alice'}) RETURN u.embedding, u.profile"
                )
                self.assertEqual(graph_result.rows, (((0.0, 1.0), None),))

                vector_result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 1, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [0.0, 1.0]},
                )
                self.assertEqual(vector_result.rows[0][0], node_id)
                self.assertAlmostEqual(vector_result.rows[0][1], 1.0, places=6)

    def test_sql_vector_syntax_supports_candidate_query_filter(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, topic, embedding) "
                        "VALUES ($id, $topic, $embedding)"
                    ),
                    [
                        {"id": 1, "topic": "alpha", "embedding": [1.0, 0.0]},
                        {"id": 2, "topic": "alpha", "embedding": [0.8, 0.2]},
                        {"id": 3, "topic": "beta", "embedding": [1.0, 0.0]},
                    ],
                )

                result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={"query": [1.0, 0.0], "topic": "alpha"},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )

    def test_sql_vector_syntax_supports_ast_parsed_ordering_shape(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, topic, embedding) "
                        "VALUES ($id, $topic, $embedding)"
                    ),
                    [
                        {"id": 1, "topic": "alpha", "embedding": [1.0, 0.0]},
                        {"id": 2, "topic": "alpha", "embedding": [0.8, 0.2]},
                        {"id": 3, "topic": "beta", "embedding": [1.0, 0.0]},
                    ],
                )

                result = db.query(
                    (
                        "SELECT d.id FROM docs AS d WHERE d.topic = $topic "
                        "ORDER BY d.embedding <=> $query NULLS LAST LIMIT $limit"
                    ),
                    params={"query": [1.0, 0.0], "topic": "alpha", "limit": 2},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (("sql_row", "docs", 1), ("sql_row", "docs", 2)),
                )

    def test_sql_vector_syntax_dot_operator_still_works(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, topic, embedding) "
                        "VALUES ($id, $topic, $embedding)"
                    ),
                    [
                        {"id": 1, "topic": "alpha", "embedding": [1.0, 0.0]},
                        {"id": 2, "topic": "alpha", "embedding": [0.5, 0.5]},
                        {"id": 3, "topic": "beta", "embedding": [0.0, 1.0]},
                    ],
                )

                result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <#> $query LIMIT 1"
                    ),
                    params={"query": [1.0, 0.0], "topic": "alpha"},
                )

                self.assertEqual(result.rows[0][:3], ("sql_row", "docs", 1))

    def test_sql_vector_dot_rejects_invalid_candidate_query(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                with self.assertRaisesRegex(
                    ValueError,
                    "candidate query must be valid HumemSQL v0",
                ):
                    db.query(
                        (
                            "SELECT id FROM docs WHERE "
                            "ORDER BY embedding <#> $query LIMIT 1"
                        ),
                        params={"query": [1.0, 0.0]},
                    )

    def test_sql_vector_syntax_keeps_large_fraction_candidate_query_exact(
        self,
    ) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE docs ("
                        "id INTEGER PRIMARY KEY, topic TEXT NOT NULL, embedding BLOB)"
                    )
                )
                db.executemany(
                    (
                        "INSERT INTO docs (id, topic, embedding) "
                        "VALUES ($id, $topic, $embedding)"
                    ),
                    [
                        {"id": 1, "topic": "alpha", "embedding": [0.8, 0.2]},
                        {"id": 2, "topic": "alpha", "embedding": [0.75, 0.25]},
                        {"id": 3, "topic": "alpha", "embedding": [0.7, 0.3]},
                        {"id": 4, "topic": "alpha", "embedding": [0.65, 0.35]},
                        {"id": 5, "topic": "beta", "embedding": [1.0, 0.0]},
                    ],
                )

                result = db.query(
                    (
                        "SELECT id FROM docs WHERE topic = $topic "
                        "ORDER BY embedding <=> $query LIMIT 5"
                    ),
                    params={"query": [1.0, 0.0], "topic": "alpha"},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("sql_row", "docs", 1),
                        ("sql_row", "docs", 2),
                        ("sql_row", "docs", 3),
                        ("sql_row", "docs", 4),
                    ),
                )

    def test_cypher_vector_syntax_supports_candidate_query_filter(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                alice = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Alice', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [1.0, 0.0]},
                )
                bob = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Bob', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [0.85, 0.15]},
                )
                db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Carol', cohort: 'beta', embedding: $embedding})"
                    ),
                    params={"embedding": [1.0, 0.0]},
                )

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', 3, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(
                    tuple(row[0] for row in result.rows),
                    (alice.rows[0][0], bob.rows[0][0]),
                )

    def test_cypher_vector_syntax_supports_parameterized_search_limit(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                alice = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Alice', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [1.0, 0.0]},
                )
                bob = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Bob', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [0.85, 0.15]},
                )
                db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Carol', cohort: 'beta', embedding: $embedding})"
                    ),
                    params={"embedding": [1.0, 0.0]},
                )

                result = db.query(
                    (
                        "CALL db.index.vector.queryNodes("
                        "'user_embedding_idx', $limit, $query) "
                        "YIELD node, score MATCH (node:User {cohort: 'alpha'}) "
                        "RETURN node.id, score ORDER BY node.id"
                    ),
                    params={"query": [1.0, 0.0], "limit": 2},
                )

                self.assertEqual(
                    tuple(row[0] for row in result.rows),
                    (alice.rows[0][0], bob.rows[0][0]),
                )

    def test_cypher_vector_query_accepts_lowercase_search_keywords(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir) / "humem"

            with HumemDB(base_path) as db:
                alice = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Alice', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [1.0, 0.0]},
                )
                bob = db.query(
                    (
                        "CREATE (u:User {"
                        "name: 'Bob', cohort: 'alpha', embedding: $embedding})"
                    ),
                    params={"embedding": [0.9, 0.1]},
                )

                result = db.query(
                    (
                        "call db.index.vector.queryNodes("
                        "'user_embedding_idx', $limit, $query) "
                        "yield node, score match (node:User {cohort: 'alpha'}) "
                        "return node.id, score order by node.id"
                    ),
                    params={"query": [1.0, 0.0], "limit": 2},
                )

                self.assertEqual(
                    tuple(row[0] for row in result.rows),
                    (alice.rows[0][0], bob.rows[0][0]),
                )
