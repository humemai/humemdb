from __future__ import annotations

import tempfile
from pathlib import Path

from humemdb import HumemDB


DEFAULT_COLLECTION_SIZE = 24_000
ARCHIVE_COLLECTION_SIZE = 12_000
DIMENSIONS = 16


def _embedding(base: float, secondary: float, tertiary: float) -> list[float]:
    return [base, secondary, tertiary, *([0.0] * (DIMENSIONS - 3))]


REFRESH_VECTOR = _embedding(0.88, 0.44, 0.22)


def build_default_vectors() -> list[tuple[int, str, int, list[float]]]:
    rows: list[tuple[int, str, int, list[float]]] = []
    for item_id in range(1, DEFAULT_COLLECTION_SIZE + 1):
        if item_id <= 8_000:
            bucket = 0
            secondary = item_id / 100_000.0
            vector = _embedding(1.0, secondary, 0.0)
        elif item_id <= 16_000:
            bucket = 1
            secondary = (item_id - 8_000) / 20_000.0
            vector = _embedding(0.25, 0.95 - secondary, 0.0)
        else:
            bucket = 2
            tertiary = (item_id - 16_000) / 20_000.0
            vector = _embedding(0.1, 0.0, 0.9 - tertiary)
        rows.append((item_id, "default", bucket, vector))
    return rows


def build_archive_vectors() -> list[tuple[int, str, int, list[float]]]:
    rows: list[tuple[int, str, int, list[float]]] = []
    start_id = DEFAULT_COLLECTION_SIZE + 1
    for offset in range(ARCHIVE_COLLECTION_SIZE):
        item_id = start_id + offset
        rows.append(
            (
                item_id,
                "archive",
                9,
                _embedding(0.0, 0.0, 1.0 - offset / 100_000.0),
            )
        )
    return rows


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = Path(tmpdir) / "vectors.sqlite3"

        with HumemDB(str(sqlite_path)) as db:
            db.insert_vectors(build_default_vectors())
            db.insert_vectors(build_archive_vectors())

            top_matches = db.search_vectors(
                "default",
                _embedding(1.0, 0.0, 0.0),
                top_k=5,
                metric="cosine",
            )
            bucket_matches = db.search_vectors(
                "default",
                _embedding(1.0, 0.0, 0.0),
                top_k=5,
                metric="cosine",
                bucket=1,
            )
            raw_query_result = db.query(
                "default",
                route="sqlite",
                query_type="vector",
                params={
                    "query": _embedding(1.0, 0.0, 0.0),
                    "top_k": 5,
                    "metric": "cosine",
                    "bucket": 0,
                },
            )
            archive_result = db.search_vectors(
                "archive",
                _embedding(0.0, 0.0, 1.0),
                top_k=3,
                metric="cosine",
            )
            db.insert_vectors([(99_001, "default", 0, REFRESH_VECTOR)])
            refreshed_result = db.search_vectors(
                "default",
                REFRESH_VECTOR,
                top_k=1,
                metric="cosine",
            )

        assert len(top_matches.rows) == 5
        assert all(row[0] <= 8_000 for row in top_matches.rows)
        assert all(abs(row[1] - 1.0) < 1e-6 for row in top_matches.rows)
        assert len(bucket_matches.rows) == 5
        assert all(8_001 <= row[0] <= 16_000 for row in bucket_matches.rows)
        assert all(row[0] <= 8_000 for row in raw_query_result.rows)
        assert all(abs(row[1] - 1.0) < 1e-6 for row in raw_query_result.rows)
        assert all(row[0] > DEFAULT_COLLECTION_SIZE for row in archive_result.rows)
        assert all(abs(row[1] - 1.0) < 1e-6 for row in archive_result.rows)
        assert len(refreshed_result.rows) == 1
        assert refreshed_result.rows[0][0] == 99_001
        assert abs(refreshed_result.rows[0][1] - 1.0) < 1e-6

        print("Top matches:", top_matches.rows)
        print("Bucket-filtered matches:", bucket_matches.rows)
        print("Raw vector query result:", raw_query_result.rows)
        print("Archive collection result:", archive_result.rows)
        print("Refreshed collection result:", refreshed_result.rows)


if __name__ == "__main__":
    main()
