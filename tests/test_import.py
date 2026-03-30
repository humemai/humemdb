from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from humemdb import HumemDB


class HumemDBImportTest(unittest.TestCase):
    def test_import_table_loads_csv_using_header_columns(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "humem"
            csv_path = root / "users.csv"
            csv_path.write_text("id,name,city\n1,Alice,Berlin\n2,Bob,Paris\n")

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE users ("
                        "id INTEGER PRIMARY KEY, "
                        "name TEXT NOT NULL, "
                        "city TEXT NOT NULL"
                        ")"
                    )
                )

                imported = db.import_table("users", csv_path)
                result = db.query("SELECT id, name, city FROM users ORDER BY id")

                self.assertEqual(imported, 2)
                self.assertEqual(
                    result.rows,
                    ((1, "Alice", "Berlin"), (2, "Bob", "Paris")),
                )

    def test_import_table_supports_headerless_csv_with_explicit_columns(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "humem"
            csv_path = root / "users.csv"
            csv_path.write_text("1,Alice,Berlin\n2,Bob,Paris\n")

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE users ("
                        "id INTEGER PRIMARY KEY, "
                        "name TEXT NOT NULL, "
                        "city TEXT NOT NULL"
                        ")"
                    )
                )

                imported = db.import_table(
                    "users",
                    csv_path,
                    columns=("id", "name", "city"),
                    header=False,
                )
                result = db.query("SELECT id, name, city FROM users ORDER BY id")

                self.assertEqual(imported, 2)
                self.assertEqual(
                    result.rows,
                    ((1, "Alice", "Berlin"), (2, "Bob", "Paris")),
                )

    def test_import_table_supports_explicit_column_subset(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "humem"
            csv_path = root / "users.csv"
            csv_path.write_text(
                "id,name,city,ignored\n1,Alice,Berlin,x\n2,Bob,Paris,y\n"
            )

            with HumemDB(base_path) as db:
                db.query(
                    (
                        "CREATE TABLE users ("
                        "name TEXT NOT NULL, "
                        "id INTEGER PRIMARY KEY"
                        ")"
                    )
                )

                imported = db.import_table(
                    "users",
                    csv_path,
                    columns=("name", "id"),
                )
                result = db.query("SELECT id, name FROM users ORDER BY id")

                self.assertEqual(imported, 2)
                self.assertEqual(result.rows, ((1, "Alice"), (2, "Bob")))

    def test_import_table_rolls_back_all_rows_when_one_chunk_fails(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "humem"
            csv_path = root / "users.csv"
            csv_path.write_text("id,name\n1,Alice\n1,Bob\n")

            with HumemDB(base_path) as db:
                db.query(
                    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
                )

                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "UNIQUE constraint failed",
                ):
                    db.import_table("users", csv_path, chunk_size=1)

                result = db.query("SELECT COUNT(*) FROM users")

                self.assertEqual(result.rows, ((0,),))

    def test_import_nodes_loads_typed_graph_node_properties(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "humem"
            csv_path = root / "users.csv"
            csv_path.write_text(
                "id,name,age,active\n1,Alice,30,true\n2,Bob,41,false\n"
            )

            with HumemDB(base_path) as db:
                imported = db.import_nodes(
                    "User",
                    csv_path,
                    id_column="id",
                    property_types={"age": "integer", "active": "boolean"},
                )
                result = db.query(
                    (
                        "MATCH (u:User) RETURN u.id, u.name, u.age, u.active "
                        "ORDER BY u.id"
                    )
                )

                self.assertEqual(imported, 2)
                self.assertEqual(
                    result.rows,
                    ((1, "Alice", 30, True), (2, "Bob", 41, False)),
                )

    def test_import_edges_loads_relationships_between_imported_nodes(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "humem"
            nodes_csv = root / "users.csv"
            edges_csv = root / "knows.csv"
            nodes_csv.write_text("id,name\n1,Alice\n2,Bob\n3,Cory\n")
            edges_csv.write_text("from_id,to_id,since\n1,2,2020\n2,3,2021\n")

            with HumemDB(base_path) as db:
                db.import_nodes("User", nodes_csv, id_column="id")
                imported = db.import_edges(
                    "KNOWS",
                    edges_csv,
                    source_id_column="from_id",
                    target_id_column="to_id",
                    property_types={"since": "integer"},
                )
                result = db.query(
                    (
                        "MATCH (a:User)-[r:KNOWS]->(b:User) "
                        "RETURN a.id, r.since, b.id ORDER BY a.id"
                    )
                )

                self.assertEqual(imported, 2)
                self.assertEqual(result.rows, ((1, 2020, 2), (2, 2021, 3)))

    def test_import_edges_rolls_back_when_endpoints_do_not_exist(self) -> None:

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_path = root / "humem"
            nodes_csv = root / "users.csv"
            edges_csv = root / "knows.csv"
            nodes_csv.write_text("id,name\n1,Alice\n2,Bob\n")
            edges_csv.write_text("from_id,to_id\n1,2\n2,99\n")

            with HumemDB(base_path) as db:
                db.import_nodes("User", nodes_csv, id_column="id")

                with self.assertRaisesRegex(
                    sqlite3.IntegrityError,
                    "FOREIGN KEY constraint failed",
                ):
                    db.import_edges(
                        "KNOWS",
                        edges_csv,
                        source_id_column="from_id",
                        target_id_column="to_id",
                        chunk_size=1,
                    )

                result = db.query(
                    "MATCH (a:User)-[r:KNOWS]->(b:User) RETURN r.id ORDER BY r.id"
                )

                self.assertEqual(result.rows, ())
