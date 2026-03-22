from __future__ import annotations

import tempfile
from pathlib import Path

from humemdb import HumemDB


EARLY_CLUSTER_SIZE = 24_000
LATE_CLUSTER_SIZE = 12_000
DIMENSIONS = 16


def _embedding(base: float, secondary: float, tertiary: float) -> list[float]:
    return [base, secondary, tertiary, *([0.0] * (DIMENSIONS - 3))]


REFRESH_VECTOR = _embedding(0.88, 0.44, 0.22)


def build_vectors() -> list[tuple[int, list[float]]]:
    rows: list[tuple[int, list[float]]] = []
    for item_id in range(1, EARLY_CLUSTER_SIZE + 1):
        if item_id <= 8_000:
            secondary = item_id / 100_000.0
            vector = _embedding(1.0, secondary, 0.0)
        elif item_id <= 16_000:
            secondary = (item_id - 8_000) / 20_000.0
            vector = _embedding(0.25, 0.95 - secondary, 0.0)
        else:
            tertiary = (item_id - 16_000) / 20_000.0
            vector = _embedding(0.1, 0.0, 0.9 - tertiary)
        rows.append((item_id, vector))

    start_id = EARLY_CLUSTER_SIZE + 1
    for offset in range(LATE_CLUSTER_SIZE):
        item_id = start_id + offset
        rows.append((item_id, _embedding(0.0, 0.0, 1.0 - offset / 100_000.0)))
    return rows


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        direct_sqlite_path = Path(tmpdir) / "vectors-direct.sqlite3"
        row_sqlite_path = Path(tmpdir) / "vectors-rows.sqlite3"
        graph_sqlite_path = Path(tmpdir) / "vectors-graph.sqlite3"

        with HumemDB(str(direct_sqlite_path)) as db:
            db.insert_vectors(build_vectors())
            db.set_vector_metadata(
                [
                    (1, {"cluster": "early", "tier": "primary"}),
                    (2, {"cluster": "early", "tier": "primary"}),
                    (24_001, {"cluster": "late", "tier": "secondary"}),
                ]
            )
            top_matches = db.search_vectors(
                _embedding(1.0, 0.0, 0.0),
                top_k=5,
                metric="cosine",
            )
            filtered_matches = db.search_vectors(
                _embedding(1.0, 0.0, 0.0),
                top_k=5,
                metric="cosine",
                filters={"cluster": "early", "tier": "primary"},
            )
            raw_query_result = db.query(
                "",
                route="sqlite",
                query_type="vector",
                params={
                    "query": _embedding(1.0, 0.0, 0.0),
                    "top_k": 5,
                    "metric": "cosine",
                },
            )
            late_cluster_result = db.search_vectors(
                _embedding(0.0, 0.0, 1.0),
                top_k=3,
                metric="cosine",
            )
            db.insert_vectors([(99_001, REFRESH_VECTOR)])
            refreshed_result = db.search_vectors(
                REFRESH_VECTOR,
                top_k=1,
                metric="cosine",
            )

        with HumemDB(str(row_sqlite_path)) as db:
            db.query(
                (
                    "CREATE TABLE vector_scope ("
                    "id INTEGER PRIMARY KEY, cluster TEXT NOT NULL, embedding BLOB)"
                ),
                route="sqlite",
            )
            db.executemany(
                "INSERT INTO vector_scope (id, cluster, embedding) VALUES (?, ?, ?)",
                [
                    (101_001, "early", _embedding(1.0, 0.0, 0.0)),
                    (101_002, "early", _embedding(1.0, 0.0, 0.0)),
                    (101_003, "late", _embedding(0.0, 0.0, 1.0)),
                ],
                route="sqlite",
            )
            sql_scoped_result = db.query(
                "SELECT id FROM vector_scope WHERE cluster = ? ORDER BY id",
                route="sqlite",
                query_type="vector",
                params={
                    "query": _embedding(1.0, 0.0, 0.0),
                    "top_k": 5,
                    "metric": "cosine",
                    "scope_query_type": "sql",
                    "scope_params": ("early",),
                },
            )

        with HumemDB(str(graph_sqlite_path)) as db:
            graph_ids = []
            for name, cluster, embedding in (
                ("early-a", "early", _embedding(1.0, 0.0, 0.0)),
                ("early-b", "early", _embedding(1.0, 0.0, 0.0)),
                ("late-a", "late", _embedding(0.0, 0.0, 1.0)),
            ):
                created = db.query(
                    (
                        "CREATE (u:VectorNode {"
                        "name: $name, cluster: $cluster, embedding: $embedding})"
                    ),
                    route="sqlite",
                    query_type="cypher",
                    params={
                        "name": name,
                        "cluster": cluster,
                        "embedding": embedding,
                    },
                )
                graph_ids.append(created.rows[0][0])
            graph_ids = tuple(graph_ids)
            cypher_scoped_result = db.query(
                "MATCH (u:VectorNode {cluster: 'early'}) RETURN u.id ORDER BY u.id",
                route="sqlite",
                query_type="vector",
                params={
                    "query": _embedding(1.0, 0.0, 0.0),
                    "top_k": 5,
                    "metric": "cosine",
                    "scope_query_type": "cypher",
                },
            )

        assert len(top_matches.rows) == 5
        assert all(row[0] <= 8_000 for row in top_matches.rows)
        assert all(abs(row[1] - 1.0) < 1e-6 for row in top_matches.rows)
        assert tuple(row[0] for row in filtered_matches.rows) == (1, 2)
        assert all(row[0] <= 8_000 for row in raw_query_result.rows)
        assert all(abs(row[1] - 1.0) < 1e-6 for row in raw_query_result.rows)
        assert tuple(row[0] for row in sql_scoped_result.rows) == (101_001, 101_002)
        assert tuple(row[0] for row in cypher_scoped_result.rows) == graph_ids[:2]
        assert all(row[0] > EARLY_CLUSTER_SIZE for row in late_cluster_result.rows)
        assert all(abs(row[1] - 1.0) < 1e-6 for row in late_cluster_result.rows)
        assert len(refreshed_result.rows) == 1
        assert refreshed_result.rows[0][0] == 99_001
        assert abs(refreshed_result.rows[0][1] - 1.0) < 1e-6

        print("Top matches:", top_matches.rows)
        print("Filtered matches:", filtered_matches.rows)
        print("Raw vector query result:", raw_query_result.rows)
        print("SQL-scoped result:", sql_scoped_result.rows)
        print("Cypher-scoped result:", cypher_scoped_result.rows)
        print("Late-cluster result:", late_cluster_result.rows)
        print("Refreshed result:", refreshed_result.rows)


if __name__ == "__main__":
    main()
