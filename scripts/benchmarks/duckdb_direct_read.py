from __future__ import annotations

import argparse
import statistics
import tempfile
import time
from pathlib import Path
from typing import Final

from humemdb import HumemDB

QUERY_SHAPES: Final[dict[str, str]] = {
    "point_lookup": "SELECT amount FROM events WHERE id = 4242",
    "filtered_range": (
        "SELECT COUNT(*) AS matched "
        "FROM events "
        "WHERE user_id BETWEEN 100 AND 300"
    ),
    "aggregate_topk": (
        "SELECT user_id, SUM(amount) AS total_amount "
        "FROM events "
        "GROUP BY user_id "
        "ORDER BY total_amount DESC "
        "LIMIT 10"
    ),
    "join_aggregate": (
        "SELECT users.region, AVG(events.amount) AS avg_amount "
        "FROM events "
        "JOIN users ON users.id = events.user_id "
        "GROUP BY users.region "
        "ORDER BY avg_amount DESC"
    ),
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark DuckDB direct reads over the SQLite source of truth."
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Number of rows to seed into SQLite before benchmarking.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=5,
        help="Number of timed repetitions to run for each route.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10_000,
        help="Number of synthetic rows to insert per SQLite batch.",
    )
    parser.add_argument(
        "--users",
        type=int,
        default=1_000,
        help="Number of distinct synthetic user ids to distribute across rows.",
    )
    return parser.parse_args()


def _seed_rows(db: HumemDB, rows: int, *, batch_size: int, users: int) -> None:
    db.query(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, region TEXT NOT NULL)",
        route="sqlite",
    )
    user_batch = [(index, f"region_{index % 20}") for index in range(1, users + 1)]
    db.executemany(
        "INSERT INTO users (id, region) VALUES (?, ?)",
        user_batch,
        route="sqlite",
    )

    db.query(
        "CREATE TABLE events (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)",
        route="sqlite",
    )

    for start in range(0, rows, batch_size):
        stop = min(start + batch_size, rows)
        batch = [((index % users) + 1, index % 100) for index in range(start, stop)]
        db.executemany(
            "INSERT INTO events (user_id, amount) VALUES (?, ?)",
            batch,
            route="sqlite",
        )


def _time_query(
    db: HumemDB,
    route: str,
    repetitions: int,
    query: str,
) -> list[float]:
    timings: list[float] = []

    for _ in range(repetitions):
        started = time.perf_counter()
        db.query(query, route=route)
        timings.append(time.perf_counter() - started)

    return timings


def _format_seconds(seconds: float) -> str:
    return f"{seconds * 1_000:.2f} ms"


def main() -> None:
    args = _parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = Path(tmpdir) / "bench.sqlite3"
        duckdb_path = Path(tmpdir) / "bench.duckdb"

        with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
            _seed_rows(
                db,
                args.rows,
                batch_size=args.batch_size,
                users=args.users,
            )

            benchmark_results: dict[str, tuple[float, float]] = {}
            for name, query in QUERY_SHAPES.items():
                sqlite_timings = _time_query(
                    db,
                    "sqlite",
                    args.repetitions,
                    query,
                )
                duckdb_timings = _time_query(
                    db,
                    "duckdb",
                    args.repetitions,
                    query,
                )
                benchmark_results[name] = (
                    statistics.mean(sqlite_timings),
                    statistics.mean(duckdb_timings),
                )

        print(f"Rows: {args.rows}")
        print(f"Repetitions: {args.repetitions}")
        print(f"Batch size: {args.batch_size}")
        print(f"Distinct users: {args.users}")
        print()

        for name, (sqlite_mean, duckdb_mean) in benchmark_results.items():
            print(f"Query shape: {name}")
            print(f"  SQLite mean: {_format_seconds(sqlite_mean)}")
            print(f"  DuckDB mean: {_format_seconds(duckdb_mean)}")
            if duckdb_mean > 0:
                print(f"  SQLite/DuckDB ratio: {sqlite_mean / duckdb_mean:.2f}x")
            print()


if __name__ == "__main__":
    main()
