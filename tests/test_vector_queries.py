from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests.support import humemdb_class


class TestVectorQueries(unittest.TestCase):
    def test_sql_owned_vectors_work_through_sql_and_vector_query(self) -> None:
        HumemDB = humemdb_class()

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
        HumemDB = humemdb_class()

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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={
                        "query": [1.0, 0.0],
                    },
                )

                self.assertEqual(
                    tuple((row[0], row[1], row[2]) for row in vector_result.rows),
                    (("graph_node", "", created[0]), ("graph_node", "", created[1])),
                )
                self.assertAlmostEqual(vector_result.rows[0][3], 1.0, places=6)

    def test_sql_insert_with_embedding_updates_row_and_vector_store(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(
                    tuple(row[:3] for row in vector_result.rows),
                    (
                        ("graph_node", "", node_ids[0]),
                        ("graph_node", "", node_ids[1]),
                    ),
                )

    def test_cypher_match_set_embedding_updates_vector_store(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 1) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(result.rows[0][2], node_id)
                self.assertAlmostEqual(result.rows[0][3], 1.0, places=6)

    def test_cypher_match_set_updates_vector_and_scalar_properties_together(
        self,
    ) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'beta'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 1) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(vector_result.rows[0][2], node_id)
                self.assertAlmostEqual(vector_result.rows[0][3], 1.0, places=6)

    def test_cypher_detach_delete_invalidates_graph_vector_cache(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 1) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(initial.rows[0][2], node_id)
                self.assertTrue(db.vectors_cached())

                deleted = db.query(
                    "MATCH (u:User {name: 'Alice'}) DETACH DELETE u"
                )

                self.assertEqual(deleted.rowcount, 1)
                self.assertFalse(db.vectors_cached())

                result = db.query(
                    (
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 1) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )
                self.assertEqual(result.rows, ())

    def test_cypher_rejects_second_vector_property_for_same_node(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 1) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [0.0, 1.0]},
                )
                self.assertEqual(vector_result.rows[0][2], node_id)
                self.assertAlmostEqual(vector_result.rows[0][3], 1.0, places=6)

    def test_sql_vector_syntax_supports_candidate_query_filter(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0]},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("graph_node", "", alice.rows[0][0]),
                        ("graph_node", "", bob.rows[0][0]),
                    ),
                )

    def test_cypher_vector_syntax_supports_parameterized_search_limit(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "SEARCH u IN (VECTOR INDEX embedding FOR $query LIMIT $limit) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0], "limit": 2},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("graph_node", "", alice.rows[0][0]),
                        ("graph_node", "", bob.rows[0][0]),
                    ),
                )

    def test_cypher_vector_query_accepts_lowercase_search_keywords(self) -> None:
        HumemDB = humemdb_class()

        with tempfile.TemporaryDirectory() as tmpdir:
            sqlite_path = Path(tmpdir) / "humem.sqlite3"

            with HumemDB(str(sqlite_path)) as db:
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
                        "MATCH (u:User {cohort: 'alpha'}) "
                        "search u in (vector index embedding for $query limit $limit) "
                        "RETURN u.id ORDER BY u.id"
                    ),
                    params={"query": [1.0, 0.0], "limit": 2},
                )

                self.assertEqual(
                    tuple(row[:3] for row in result.rows),
                    (
                        ("graph_node", "", alice.rows[0][0]),
                        ("graph_node", "", bob.rows[0][0]),
                    ),
                )
