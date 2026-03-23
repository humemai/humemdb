from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

from humemdb import HumemDB


EARLY_CLUSTER_SIZE = 24_000
LATE_CLUSTER_SIZE = 12_000
DIMENSIONS = 16


def _embedding(base: float, secondary: float, tertiary: float) -> list[float]:
    return [base, secondary, tertiary, *([0.0] * (DIMENSIONS - 3))]


REFRESH_VECTOR = _embedding(0.88, 0.44, 0.22)


def build_vectors() -> list[list[float]]:
    rows: list[list[float]] = []
    for index in range(EARLY_CLUSTER_SIZE):
        item_id = index + 1
        if item_id <= 8_000:
            secondary = item_id / 100_000.0
            vector = _embedding(1.0, secondary, 0.0)
        elif item_id <= 16_000:
            secondary = (item_id - 8_000) / 20_000.0
            vector = _embedding(0.25, 0.95 - secondary, 0.0)
        else:
            tertiary = (item_id - 16_000) / 20_000.0
            vector = _embedding(0.1, 0.0, 0.9 - tertiary)
        rows.append(vector)

    for offset in range(LATE_CLUSTER_SIZE):
        rows.append(_embedding(0.0, 0.0, 1.0 - offset / 100_000.0))
    return rows


def _direct_record(
    embedding: list[float],
    *,
    metadata: dict[str, str],
) -> dict[str, object]:
    return {"embedding": embedding, "metadata": metadata}


def _create_vector_node(
    db: Any,
    *,
    name: str,
    cluster: str,
    embedding: list[float],
) -> int:
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
    first_row = created.first()
    if first_row is None:
        raise ValueError("Cypher CREATE did not return the created node id.")
    return int(first_row[0])


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        direct_sqlite_path = Path(tmpdir) / "vectors-direct.sqlite3"
        row_sqlite_path = Path(tmpdir) / "vectors-rows.sqlite3"
        graph_sqlite_path = Path(tmpdir) / "vectors-graph.sqlite3"

        with HumemDB(str(direct_sqlite_path)) as db:
            base_direct_rows = build_vectors()
            direct_rows: list[dict[str, object] | list[float]] = list(base_direct_rows)
            direct_rows[0] = _direct_record(
                base_direct_rows[0],
                metadata={"cluster": "early", "tier": "primary"},
            )
            direct_rows[1] = _direct_record(
                base_direct_rows[1],
                metadata={"cluster": "early", "tier": "primary"},
            )
            direct_rows[EARLY_CLUSTER_SIZE] = _direct_record(
                base_direct_rows[EARLY_CLUSTER_SIZE],
                metadata={"cluster": "late", "tier": "secondary"},
            )
            db.insert_vectors(direct_rows)
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
            refreshed_ids = db.insert_vectors([REFRESH_VECTOR])
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
                "INSERT INTO vector_scope (cluster, embedding) VALUES (?, ?)",
                [
                    ("early", _embedding(1.0, 0.0, 0.0)),
                    ("early", _embedding(1.0, 0.0, 0.0)),
                    ("late", _embedding(0.0, 0.0, 1.0)),
                ],
                route="sqlite",
            )
            sql_row_ids = tuple(
                row[0]
                for row in db.query(
                    "SELECT id FROM vector_scope ORDER BY id",
                    route="sqlite",
                ).rows
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
            graph_ids = tuple(
                _create_vector_node(
                    db,
                    name=name,
                    cluster=cluster,
                    embedding=embedding,
                )
                for name, cluster, embedding in (
                    ("early-a", "early", _embedding(1.0, 0.0, 0.0)),
                    ("early-b", "early", _embedding(1.0, 0.0, 0.0)),
                    ("late-a", "late", _embedding(0.0, 0.0, 1.0)),
                )
            )
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
        assert all(row[0] == "direct" for row in top_matches.rows)
        assert all(row[2] <= 8_000 for row in top_matches.rows)
        assert all(abs(row[3] - 1.0) < 1e-6 for row in top_matches.rows)
        assert tuple(row[2] for row in filtered_matches.rows) == (1, 2)
        assert all(row[0] == "direct" for row in raw_query_result.rows)
        assert all(row[2] <= 8_000 for row in raw_query_result.rows)
        assert all(abs(row[3] - 1.0) < 1e-6 for row in raw_query_result.rows)
        assert tuple(row[:3] for row in sql_scoped_result.rows) == (
            ("sql_row", "vector_scope", sql_row_ids[0]),
            ("sql_row", "vector_scope", sql_row_ids[1]),
        )
        assert tuple(row[:3] for row in cypher_scoped_result.rows) == (
            ("graph_node", "", graph_ids[0]),
            ("graph_node", "", graph_ids[1]),
        )
        assert all(row[2] > EARLY_CLUSTER_SIZE for row in late_cluster_result.rows)
        assert all(abs(row[3] - 1.0) < 1e-6 for row in late_cluster_result.rows)
        assert len(refreshed_result.rows) == 1
        assert refreshed_result.rows[0][:3] == ("direct", "", refreshed_ids[0])
        assert abs(refreshed_result.rows[0][3] - 1.0) < 1e-6

        print("Top matches:", top_matches.rows)
        print("Filtered matches:", filtered_matches.rows)
        print("Raw vector query result:", raw_query_result.rows)
        print("SQL-scoped result:", sql_scoped_result.rows)
        print("Cypher-scoped result:", cypher_scoped_result.rows)
        print("Late-cluster result:", late_cluster_result.rows)
        print("Refreshed result:", refreshed_result.rows)


if __name__ == "__main__":
    main()
