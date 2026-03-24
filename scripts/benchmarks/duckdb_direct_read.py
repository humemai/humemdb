from __future__ import annotations

import argparse
import json
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
    shape: str
    selectivity: str
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
        shape="point_lookup",
        selectivity="high",
        query="SELECT amount FROM events WHERE id = 424242",
    ),
    "event_filtered_range": QueryWorkload(
        family="oltp",
        shape="filtered_range",
        selectivity="medium",
        query=(
            "SELECT COUNT(*) AS matched "
            "FROM events "
            "WHERE user_id BETWEEN 1000 AND 5000"
        ),
    ),
    "event_type_hot_window": QueryWorkload(
        family="oltp",
        shape="filtered_ordered_limit",
        selectivity="high",
        query=(
            "SELECT id, amount, created_day "
            "FROM events "
            "WHERE event_type = 'purchase' AND created_day BETWEEN 10 AND 40 "
            "ORDER BY amount DESC "
            "LIMIT 25"
        ),
    ),
    "event_aggregate_topk": QueryWorkload(
        family="analytics",
        shape="scan_group_limit",
        selectivity="low",
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
        shape="join_group_order",
        selectivity="low",
        query=(
            "SELECT users.region, AVG(events.amount) AS avg_amount "
            "FROM events "
            "JOIN users ON users.id = events.user_id "
            "GROUP BY users.region "
            "ORDER BY avg_amount DESC"
        ),
    ),
    "event_active_user_join_lookup": QueryWorkload(
        family="oltp_join",
        shape="selective_join_lookup",
        selectivity="high",
        query=(
            "SELECT events.id, events.amount, users.tier "
            "FROM events "
            "JOIN users ON users.id = events.user_id "
            "WHERE users.is_active = 1 AND users.region = 'region_7' "
            "AND events.created_day = 42 "
            "ORDER BY events.id "
            "LIMIT 50"
        ),
    ),
    "event_active_user_rollup": QueryWorkload(
        family="analytics",
        shape="filtered_join_group",
        selectivity="medium",
        query=(
            "SELECT users.tier, events.event_type, COUNT(*) AS matched_events "
            "FROM events "
            "JOIN users ON users.id = events.user_id "
            "WHERE users.is_active = 1 "
            "GROUP BY users.tier, events.event_type "
            "ORDER BY matched_events DESC"
        ),
    ),
    "event_cte_daily_rollup": QueryWorkload(
        family="analytics",
        shape="cte_group_order",
        selectivity="low",
        query=(
            "WITH daily AS ("
            "SELECT created_day, user_id, SUM(amount) AS day_total "
            "FROM events "
            "GROUP BY created_day, user_id"
            ") "
            "SELECT created_day, AVG(day_total) AS avg_total "
            "FROM daily "
            "GROUP BY created_day "
            "ORDER BY avg_total DESC "
            "LIMIT 15"
        ),
    ),
    "event_window_rank": QueryWorkload(
        family="analytics",
        shape="window_partition_order",
        selectivity="medium",
        query=(
            "SELECT user_id, amount, amount_rank "
            "FROM ("
            "SELECT user_id, amount, "
            "ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY amount DESC) "
            "AS amount_rank "
            "FROM events"
            ") ranked "
            "WHERE amount_rank <= 3"
        ),
    ),
    "event_exists_region_filter": QueryWorkload(
        family="mixed",
        shape="exists_filter",
        selectivity="medium",
        query=(
            "SELECT COUNT(*) AS matched "
            "FROM events e "
            "WHERE EXISTS ("
            "SELECT 1 FROM users u "
            "WHERE u.id = e.user_id AND u.region = 'region_3' AND u.is_active = 1"
            ")"
        ),
    ),
    "document_tag_rollup": QueryWorkload(
        family="document",
        shape="selective_multi_join_group",
        selectivity="high",
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
    "document_owner_region_rollup": QueryWorkload(
        family="document",
        shape="broad_multi_join_group",
        selectivity="medium",
        query=(
            "SELECT users.region, documents.status, AVG(documents.score) AS avg_score "
            "FROM documents "
            "JOIN users ON users.id = documents.owner_user_id "
            "WHERE users.is_active = 1 "
            "GROUP BY users.region, documents.status "
            "ORDER BY avg_score DESC"
        ),
    ),
    "document_distinct_owner_regions": QueryWorkload(
        family="document",
        shape="distinct_join_projection",
        selectivity="medium",
        query=(
            "SELECT DISTINCT users.region, documents.language "
            "FROM documents "
            "JOIN users ON users.id = documents.owner_user_id "
            "WHERE documents.status = 'published'"
        ),
    ),
    "memory_hot_rollup": QueryWorkload(
        family="memory",
        shape="filtered_group_limit",
        selectivity="medium",
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
    "memory_owner_join_lookup": QueryWorkload(
        family="memory",
        shape="selective_join_lookup",
        selectivity="high",
        query=(
            "SELECT memory_chunks.id, memory_chunks.topic, users.region "
            "FROM memory_chunks "
            "JOIN users ON users.id = memory_chunks.owner_user_id "
            "WHERE memory_chunks.is_hot = 1 "
            "AND users.tier = 'enterprise' "
            "ORDER BY memory_chunks.importance DESC "
            "LIMIT 30"
        ),
    ),
    "memory_owner_exists_projection": QueryWorkload(
        family="memory",
        shape="exists_projection",
        selectivity="medium",
        query=(
            "SELECT topic, token_count, summary_length "
            "FROM memory_chunks m "
            "WHERE EXISTS ("
            "SELECT 1 FROM users u "
            "WHERE u.id = m.owner_user_id AND u.tier = 'pro'"
            ") "
            "ORDER BY importance DESC "
            "LIMIT 100"
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
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write machine-readable benchmark results as JSON.",
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
            "cohort TEXT NOT NULL, "
            "signup_day INTEGER NOT NULL, "
            "is_active INTEGER NOT NULL)"
        ),
        route="sqlite",
    )
    batch = [
        {
            "id": index,
            "region": f"region_{index % 20}",
            "tier": ("free", "pro", "enterprise")[index % 3],
            "cohort": f"cohort_{index % 12}",
            "signup_day": index % 365,
            "is_active": 1 if index % 5 != 0 else 0,
        }
        for index in range(1, users + 1)
    ]
    db.executemany(
        (
            "INSERT INTO users (id, region, tier, cohort, signup_day, is_active) "
            "VALUES ($id, $region, $tier, $cohort, $signup_day, $is_active)"
        ),
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
            "device_type TEXT NOT NULL, "
            "channel TEXT NOT NULL, "
            "created_day INTEGER NOT NULL)"
        ),
        route="sqlite",
    )

    for start in range(0, rows, batch_size):
        stop = min(start + batch_size, rows)
        batch = [
            {
                "user_id": (index % users) + 1,
                "amount": index % 100,
                "event_type": ("view", "click", "purchase", "share")[index % 4],
                "device_type": ("mobile", "desktop", "tablet")[index % 3],
                "channel": ("organic", "ads", "email", "partner")[index % 4],
                "created_day": index % 365,
            }
            for index in range(start, stop)
        ]
        db.executemany(
            (
                "INSERT INTO events ("
                "user_id, amount, event_type, device_type, channel, created_day"
                ") VALUES ("
                "$user_id, $amount, $event_type, $device_type, $channel, $created_day"
                ")"
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
            "language TEXT NOT NULL, "
            "title TEXT NOT NULL, "
            "score INTEGER NOT NULL)"
        ),
        route="sqlite",
    )

    for start in range(1, documents + 1, batch_size):
        stop = min(start + batch_size - 1, documents)
        batch = [
            {
                "id": document_id,
                "owner_user_id": ((document_id * 7) % users) + 1,
                "category": f"category_{document_id % 24}",
                "status": ("draft", "review", "published")[document_id % 3],
                "language": ("en", "es", "fr", "de")[document_id % 4],
                "title": f"document_title_{document_id}",
                "score": document_id % 1000,
            }
            for document_id in range(start, stop + 1)
        ]
        db.executemany(
            (
                "INSERT INTO documents ("
                "id, owner_user_id, category, status, language, title, score"
                ") VALUES ("
                "$id, $owner_user_id, $category, $status, $language, $title, $score"
                ")"
            ),
            batch,
            route="sqlite",
        )


def _seed_tags(db: HumemDB, *, tags: int) -> None:
    db.query(
        "CREATE TABLE tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        route="sqlite",
    )
    batch = [{"id": tag_id, "name": f"tag_{tag_id}"} for tag_id in range(1, tags + 1)]
    db.executemany(
        "INSERT INTO tags (id, name) VALUES ($id, $name)",
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

    batch: list[dict[str, int]] = []
    for document_id in range(1, documents + 1):
        batch.extend(
            [
                {"document_id": document_id, "tag_id": ((document_id) % tags) + 1},
                {"document_id": document_id, "tag_id": ((document_id * 3) % tags) + 1},
                {"document_id": document_id, "tag_id": ((document_id * 7) % tags) + 1},
            ]
        )
        if len(batch) >= batch_size:
            db.executemany(
                (
                    "INSERT INTO document_tags (document_id, tag_id) "
                    "VALUES ($document_id, $tag_id)"
                ),
                batch,
                route="sqlite",
            )
            batch = []

    if batch:
        db.executemany(
            (
                "INSERT INTO document_tags (document_id, tag_id) "
                "VALUES ($document_id, $tag_id)"
            ),
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
            "summary_length INTEGER NOT NULL, "
            "is_hot INTEGER NOT NULL)"
        ),
        route="sqlite",
    )

    for start in range(1, chunks + 1, batch_size):
        stop = min(start + batch_size - 1, chunks)
        batch = [
            {
                "id": chunk_id,
                "document_id": ((chunk_id * 5) % documents) + 1,
                "owner_user_id": ((chunk_id * 3) % users) + 1,
                "topic": f"topic_{chunk_id % 128}",
                "importance": chunk_id % 100,
                "token_count": 128 + (chunk_id % 2048),
                "summary_length": 32 + (chunk_id % 512),
                "is_hot": 1 if chunk_id % 5 == 0 else 0,
            }
            for chunk_id in range(start, stop + 1)
        ]
        db.executemany(
            (
                "INSERT INTO memory_chunks ("
                "id, document_id, owner_user_id, topic, importance, token_count, "
                "summary_length, is_hot"
                ") VALUES ("
                "$id, $document_id, $owner_user_id, $topic, "
                "$importance, $token_count, $summary_length, $is_hot"
                ")"
            ),
            batch,
            route="sqlite",
        )


def _create_indexes(db: HumemDB) -> None:
    index_statements = [
        "CREATE INDEX idx_events_user_id ON events (user_id)",
        "CREATE INDEX idx_events_created_day ON events (created_day)",
        (
            "CREATE INDEX idx_events_type_day_amount "
            "ON events (event_type, created_day, amount)"
        ),
        "CREATE INDEX idx_documents_status_category ON documents (status, category)",
        "CREATE INDEX idx_documents_status_language ON documents (status, language)",
        "CREATE INDEX idx_document_tags_tag_doc ON document_tags (tag_id, document_id)",
        "CREATE INDEX idx_memory_chunks_hot_topic ON memory_chunks (is_hot, topic)",
        "CREATE INDEX idx_users_region_active ON users (region, is_active)",
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


def _summary_dict(summary: TimingSummary) -> dict[str, float]:
    """Return a JSON-friendly summary payload."""

    return {
        "mean_ms": summary.mean * 1_000,
        "stdev_ms": summary.stdev * 1_000,
        "min_ms": summary.minimum * 1_000,
        "max_ms": summary.maximum * 1_000,
    }


def main() -> None:
    args = _parse_args()
    json_results: dict[str, object] = {
        "benchmark": "duckdb_direct_read",
        "thread_limit": os.environ.get("HUMEMDB_THREADS", "default"),
        "warmup": args.warmup,
        "repetitions": args.repetitions,
        "batch_size": args.batch_size,
        "workloads": {},
    }

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
            json_results["dataset"] = dict(dataset)

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
                print(f"  Shape: {workload.shape}")
                print(f"  Selectivity: {workload.selectivity}")
                _print_summary("SQLite", sqlite_summary)
                _print_summary("DuckDB", duckdb_summary)
                if duckdb_summary.mean > 0:
                    print(
                        "  SQLite/DuckDB mean ratio: "
                        f"{sqlite_summary.mean / duckdb_summary.mean:.2f}x"
                    )
                print()
                cast_workloads = json_results["workloads"]
                assert isinstance(cast_workloads, dict)
                cast_workloads[name] = {
                    "family": workload.family,
                    "shape": workload.shape,
                    "selectivity": workload.selectivity,
                    "query": workload.query,
                    "sqlite": _summary_dict(sqlite_summary),
                    "duckdb": _summary_dict(duckdb_summary),
                    "sqlite_duckdb_ratio": (
                        sqlite_summary.mean / duckdb_summary.mean
                        if duckdb_summary.mean > 0
                        else None
                    ),
                }

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(json_results, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
