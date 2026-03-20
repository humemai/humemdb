from __future__ import annotations

import argparse
import os
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from humemdb import HumemDB


@dataclass(frozen=True, slots=True)
class QueryWorkload:
    """Benchmark workload definition for one SQL query shape."""

    family: str
    query: str


@dataclass(frozen=True, slots=True)
class TimingSummary:
    """Aggregate timing metrics for one benchmark stage."""

    mean: float
    stdev: float
    minimum: float
    maximum: float


QUERY_WORKLOADS: Final[dict[str, QueryWorkload]] = {
    "event_point_lookup": QueryWorkload(
        family="oltp",
        query="SELECT amount FROM events WHERE id = 424242",
    ),
    "event_filtered_range": QueryWorkload(
        family="oltp",
        query=(
            "SELECT COUNT(*) AS matched "
            "FROM events "
            "WHERE user_id BETWEEN 1000 AND 5000"
        ),
    ),
    "event_aggregate_topk": QueryWorkload(
        family="analytics",
        query=(
            "SELECT user_id, SUM(amount) AS total_amount "
            "FROM events "
            "GROUP BY user_id "
            "ORDER BY total_amount DESC "
            "LIMIT 10"
        ),
    ),
    "event_region_join": QueryWorkload(
        family="analytics",
        query=(
            "SELECT users.region, AVG(events.amount) AS avg_amount "
            "FROM events "
            "JOIN users ON users.id = events.user_id "
            "GROUP BY users.region "
            "ORDER BY avg_amount DESC"
        ),
    ),
    "document_tag_rollup": QueryWorkload(
        family="document",
        query=(
            "SELECT documents.category, COUNT(*) AS tagged_docs "
            "FROM documents "
            "JOIN document_tags ON document_tags.document_id = documents.id "
            "JOIN tags ON tags.id = document_tags.tag_id "
            "WHERE documents.status = 'published' AND tags.name = 'tag_42' "
            "GROUP BY documents.category "
            "ORDER BY tagged_docs DESC"
        ),
    ),
    "memory_hot_rollup": QueryWorkload(
        family="memory",
        query=(
            "SELECT topic, AVG(token_count) AS avg_tokens, "
            "SUM(importance) AS total_importance "
            "FROM memory_chunks "
            "WHERE is_hot = 1 "
            "GROUP BY topic "
            "ORDER BY total_importance DESC "
            "LIMIT 10"
        ),
    ),
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark DuckDB direct reads over the SQLite source of truth using "
            "multiple relational workload families."
        )
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=50_000,
        help="Number of synthetic event rows to seed into SQLite.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=5,
        help="Number of timed repetitions to run for each route.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Number of untimed warmup iterations per query and route.",
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
        help="Number of distinct synthetic users.",
    )
    parser.add_argument(
        "--tags",
        type=int,
        default=512,
        help="Number of synthetic tags for document workloads.",
    )
    return parser.parse_args()


def _document_count(rows: int, users: int) -> int:
    return max(rows // 20, users)


def _memory_chunk_count(rows: int, documents: int) -> int:
    return max(rows // 10, documents)


def _seed_rows(
    db: HumemDB,
    rows: int,
    *,
    batch_size: int,
    users: int,
    tags: int,
) -> dict[str, float | int]:
    documents = _document_count(rows, users)
    memory_chunks = _memory_chunk_count(rows, documents)

    started = time.perf_counter()
    with db.transaction(route="sqlite"):
        _seed_users(db, users)
        _seed_events(db, rows=rows, batch_size=batch_size, users=users)
        _seed_documents(db, documents=documents, batch_size=batch_size, users=users)
        _seed_tags(db, tags=tags)
        _seed_document_tags(
            db,
            documents=documents,
            batch_size=batch_size,
            tags=tags,
        )
        _seed_memory_chunks(
            db,
            chunks=memory_chunks,
            batch_size=batch_size,
            documents=documents,
            users=users,
        )

    _create_indexes(db)
    seed_seconds = time.perf_counter() - started
    return {
        "users": users,
        "events": rows,
        "documents": documents,
        "tags": tags,
        "document_tags": documents * 3,
        "memory_chunks": memory_chunks,
        "seed_seconds": seed_seconds,
    }


def _seed_users(db: HumemDB, users: int) -> None:
    db.query(
        (
            "CREATE TABLE users ("
            "id INTEGER PRIMARY KEY, "
            "region TEXT NOT NULL, "
            "tier TEXT NOT NULL, "
            "is_active INTEGER NOT NULL)"
        ),
        route="sqlite",
    )
    batch = [
        (
            index,
            f"region_{index % 20}",
            ("free", "pro", "enterprise")[index % 3],
            1 if index % 5 != 0 else 0,
        )
        for index in range(1, users + 1)
    ]
    db.executemany(
        "INSERT INTO users (id, region, tier, is_active) VALUES (?, ?, ?, ?)",
        batch,
        route="sqlite",
    )


def _seed_events(db: HumemDB, *, rows: int, batch_size: int, users: int) -> None:
    db.query(
        (
            "CREATE TABLE events ("
            "id INTEGER PRIMARY KEY, "
            "user_id INTEGER NOT NULL, "
            "amount INTEGER NOT NULL, "
            "event_type TEXT NOT NULL, "
            "created_day INTEGER NOT NULL)"
        ),
        route="sqlite",
    )

    for start in range(0, rows, batch_size):
        stop = min(start + batch_size, rows)
        batch = [
            (
                (index % users) + 1,
                index % 100,
                ("view", "click", "purchase", "share")[index % 4],
                index % 365,
            )
            for index in range(start, stop)
        ]
        db.executemany(
            (
                "INSERT INTO events (user_id, amount, event_type, created_day) "
                "VALUES (?, ?, ?, ?)"
            ),
            batch,
            route="sqlite",
        )


def _seed_documents(
    db: HumemDB,
    *,
    documents: int,
    batch_size: int,
    users: int,
) -> None:
    db.query(
        (
            "CREATE TABLE documents ("
            "id INTEGER PRIMARY KEY, "
            "owner_user_id INTEGER NOT NULL, "
            "category TEXT NOT NULL, "
            "status TEXT NOT NULL, "
            "score INTEGER NOT NULL)"
        ),
        route="sqlite",
    )

    for start in range(1, documents + 1, batch_size):
        stop = min(start + batch_size - 1, documents)
        batch = [
            (
                document_id,
                ((document_id * 7) % users) + 1,
                f"category_{document_id % 24}",
                ("draft", "review", "published")[document_id % 3],
                document_id % 1000,
            )
            for document_id in range(start, stop + 1)
        ]
        db.executemany(
            (
                "INSERT INTO documents (id, owner_user_id, category, status, score) "
                "VALUES (?, ?, ?, ?, ?)"
            ),
            batch,
            route="sqlite",
        )


def _seed_tags(db: HumemDB, *, tags: int) -> None:
    db.query(
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        route="sqlite",
    )
    batch = [(tag_id, f"tag_{tag_id}") for tag_id in range(1, tags + 1)]
    db.executemany(
        "INSERT INTO tags (id, name) VALUES (?, ?)",
        batch,
        route="sqlite",
    )


def _seed_document_tags(
    db: HumemDB,
    *,
    documents: int,
    batch_size: int,
    tags: int,
) -> None:
    db.query(
        (
            "CREATE TABLE document_tags ("
            "document_id INTEGER NOT NULL, "
            "tag_id INTEGER NOT NULL)"
        ),
        route="sqlite",
    )

    batch: list[tuple[int, int]] = []
    for document_id in range(1, documents + 1):
        batch.extend(
            [
                (document_id, ((document_id) % tags) + 1),
                (document_id, ((document_id * 3) % tags) + 1),
                (document_id, ((document_id * 7) % tags) + 1),
            ]
        )
        if len(batch) >= batch_size:
            db.executemany(
                "INSERT INTO document_tags (document_id, tag_id) VALUES (?, ?)",
                batch,
                route="sqlite",
            )
            batch = []

    if batch:
        db.executemany(
            "INSERT INTO document_tags (document_id, tag_id) VALUES (?, ?)",
            batch,
            route="sqlite",
        )


def _seed_memory_chunks(
    db: HumemDB,
    *,
    chunks: int,
    batch_size: int,
    documents: int,
    users: int,
) -> None:
    db.query(
        (
            "CREATE TABLE memory_chunks ("
            "id INTEGER PRIMARY KEY, "
            "document_id INTEGER NOT NULL, "
            "owner_user_id INTEGER NOT NULL, "
            "topic TEXT NOT NULL, "
            "importance INTEGER NOT NULL, "
            "token_count INTEGER NOT NULL, "
            "is_hot INTEGER NOT NULL)"
        ),
        route="sqlite",
    )

    for start in range(1, chunks + 1, batch_size):
        stop = min(start + batch_size - 1, chunks)
        batch = [
            (
                chunk_id,
                ((chunk_id * 5) % documents) + 1,
                ((chunk_id * 3) % users) + 1,
                f"topic_{chunk_id % 128}",
                chunk_id % 100,
                128 + (chunk_id % 2048),
                1 if chunk_id % 5 == 0 else 0,
            )
            for chunk_id in range(start, stop + 1)
        ]
        db.executemany(
            (
                "INSERT INTO memory_chunks ("
                "id, document_id, owner_user_id, topic, importance, token_count, is_hot"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)"
            ),
            batch,
            route="sqlite",
        )


def _create_indexes(db: HumemDB) -> None:
    index_statements = [
        "CREATE INDEX idx_events_user_id ON events (user_id)",
        "CREATE INDEX idx_events_created_day ON events (created_day)",
        "CREATE INDEX idx_documents_status_category ON documents (status, category)",
        "CREATE INDEX idx_document_tags_tag_doc ON document_tags (tag_id, document_id)",
        "CREATE INDEX idx_memory_chunks_hot_topic ON memory_chunks (is_hot, topic)",
    ]
    for statement in index_statements:
        db.sqlite.execute(statement)


def _summarize(timings: list[float]) -> TimingSummary:
    return TimingSummary(
        mean=statistics.mean(timings),
        stdev=statistics.pstdev(timings),
        minimum=min(timings),
        maximum=max(timings),
    )


def _time_query(
    db: HumemDB,
    route: str,
    repetitions: int,
    warmup: int,
    query: str,
) -> TimingSummary:
    for _ in range(warmup):
        db.query(query, route=route)

    timings: list[float] = []
    for _ in range(repetitions):
        started = time.perf_counter()
        db.query(query, route=route)
        timings.append(time.perf_counter() - started)

    return _summarize(timings)


def _format_seconds(seconds: float) -> str:
    return f"{seconds * 1_000:.2f} ms"


def _print_summary(label: str, summary: TimingSummary) -> None:
    print(
        f"  {label}: mean={_format_seconds(summary.mean)} "
        f"std={_format_seconds(summary.stdev)} "
        f"min={_format_seconds(summary.minimum)} "
        f"max={_format_seconds(summary.maximum)}"
    )


def main() -> None:
    args = _parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = Path(tmpdir) / "bench.sqlite3"
        duckdb_path = Path(tmpdir) / "bench.duckdb"

        with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
            dataset = _seed_rows(
                db,
                args.rows,
                batch_size=args.batch_size,
                users=args.users,
                tags=args.tags,
            )

            print(f"Thread limit: {os.environ.get('HUMEMDB_THREADS', 'default')}")
            print(f"Warmup iterations: {args.warmup}")
            print(f"Timed repetitions: {args.repetitions}")
            print(f"Batch size: {args.batch_size}")
            print(f"Users: {dataset['users']}")
            print(f"Events: {dataset['events']}")
            print(f"Documents: {dataset['documents']}")
            print(f"Tags: {dataset['tags']}")
            print(f"Document tags: {dataset['document_tags']}")
            print(f"Memory chunks: {dataset['memory_chunks']}")
            print(f"Seed time: {_format_seconds(float(dataset['seed_seconds']))}")
            print()

            for name, workload in QUERY_WORKLOADS.items():
                sqlite_summary = _time_query(
                    db,
                    "sqlite",
                    args.repetitions,
                    args.warmup,
                    workload.query,
                )
                duckdb_summary = _time_query(
                    db,
                    "duckdb",
                    args.repetitions,
                    args.warmup,
                    workload.query,
                )

                print(f"Query shape: {name}")
                print(f"  Family: {workload.family}")
                _print_summary("SQLite", sqlite_summary)
                _print_summary("DuckDB", duckdb_summary)
                if duckdb_summary.mean > 0:
                    print(
                        "  SQLite/DuckDB mean ratio: "
                        f"{sqlite_summary.mean / duckdb_summary.mean:.2f}x"
                    )
                print()


if __name__ == "__main__":
    main()
