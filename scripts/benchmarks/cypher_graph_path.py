from __future__ import annotations

import argparse
import json
import os
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from humemdb import HumemDB
from humemdb.cypher import MatchNodePlan
from humemdb.cypher import MatchRelationshipPlan
from humemdb.cypher import _bind_plan_values
from humemdb.cypher import _compile_match_plan
from humemdb.cypher import ensure_graph_schema
from humemdb.cypher import parse_cypher


@dataclass(frozen=True, slots=True)
class QueryWorkload:
    """Benchmark workload definition for one Cypher query shape."""

    family: str
    shape: str
    selectivity: str
    query: str
    params: dict[str, str | int | float | bool | None]


@dataclass(frozen=True, slots=True)
class TimingSummary:
    """Aggregate timing metrics for one benchmark stage."""

    mean: float
    stdev: float
    minimum: float
    maximum: float


@dataclass(frozen=True, slots=True)
class GraphDataset:
    """Synthetic graph shape and representative query parameter values."""

    total_nodes: int
    user_count: int
    document_count: int
    topic_count: int
    team_count: int
    knows_edges: int
    authored_edges: int
    tagged_edges: int
    member_of_edges: int
    total_storage_rows: int


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark HumemCypher parse, compile, and execution costs over a "
            "multi-label SQLite-backed graph path."
        )
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=5_000,
        help="Total number of synthetic graph nodes to seed across all labels.",
    )
    parser.add_argument(
        "--fanout",
        type=int,
        default=3,
        help="Number of outgoing KNOWS edges to create per user node.",
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=10,
        help="Number of timed repetitions for each benchmark stage.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=2,
        help="Number of warmup iterations to run before timing.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5_000,
        help="Number of rows to insert per SQLite batch during seeding.",
    )
    parser.add_argument(
        "--tag-fanout",
        type=int,
        default=2,
        help="Number of TAGGED edges to create per document.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write machine-readable benchmark results as JSON.",
    )
    return parser.parse_args()


def _workloads(dataset: GraphDataset) -> dict[str, QueryWorkload]:
    midpoint_user = max(1, dataset.user_count // 2)
    midpoint_document = max(1, dataset.document_count // 2)
    midpoint_topic = max(1, dataset.topic_count // 2)
    midpoint_team = max(1, dataset.team_count // 2)
    return {
        "user_lookup": QueryWorkload(
            family="node",
            shape="anchored_node_lookup",
            selectivity="high",
            query=(
                "MATCH (u:User) "
                "WHERE u.name = $name "
                "RETURN u.name, u.region, u.active, u.reputation"
            ),
            params={"name": f"user_{midpoint_user}"},
        ),
        "document_lookup": QueryWorkload(
            family="node",
            shape="anchored_node_lookup",
            selectivity="high",
            query=(
                "MATCH (d:Document) "
                "WHERE d.title = $title "
                "RETURN d.title, d.category, d.published, d.score"
            ),
            params={"title": f"document_{midpoint_document}"},
        ),
        "topic_lookup": QueryWorkload(
            family="node",
            shape="anchored_node_lookup",
            selectivity="high",
            query=(
                "MATCH (t:Topic) "
                "WHERE t.slug = $slug "
                "RETURN t.slug, t.domain, t.trending"
            ),
            params={"slug": f"topic_{midpoint_topic}"},
        ),
        "social_expand": QueryWorkload(
            family="edge",
            shape="broad_relationship_expand",
            selectivity="low",
            query=(
                "MATCH (a:User)-[:KNOWS]->(b:User) "
                "WHERE a.region = $region AND b.active = $active "
                "RETURN a.name, b.name"
            ),
            params={"region": "region_11", "active": True},
        ),
        "social_expand_ordered": QueryWorkload(
            family="edge",
            shape="ordered_relationship_expand",
            selectivity="low",
            query=(
                "MATCH (a:User)-[:KNOWS]->(b:User) "
                "WHERE a.region = $region "
                "RETURN a.name, b.name, b.reputation "
                "ORDER BY b.reputation DESC "
                "LIMIT 500"
            ),
            params={"region": "region_11"},
        ),
        "social_expand_unfiltered": QueryWorkload(
            family="edge",
            shape="full_relationship_expand",
            selectivity="very_low",
            query=(
                "MATCH (a:User)-[:KNOWS]->(b:User) "
                "RETURN a.name, b.name "
                "LIMIT 5000"
            ),
            params={},
        ),
        "social_reverse_since_anchor": QueryWorkload(
            family="edge",
            shape="relationship_property_anchor",
            selectivity="medium",
            query=(
                "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                "WHERE r.since = $since AND b.active = $active "
                "RETURN a.name, b.name"
            ),
            params={"since": 2021, "active": True},
        ),
        "author_expand": QueryWorkload(
            family="edge",
            shape="selective_relationship_expand",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[:AUTHORED]->(d:Document) "
                "WHERE u.region = $region AND d.published = $published "
                "RETURN u.name, d.title"
            ),
            params={"region": "region_7", "published": True},
        ),
        "author_expand_ordered": QueryWorkload(
            family="edge",
            shape="ordered_relationship_expand",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[:AUTHORED]->(d:Document) "
                "WHERE d.published = $published "
                "RETURN u.name, d.title, d.score "
                "ORDER BY d.score DESC "
                "LIMIT 100"
            ),
            params={"published": True},
        ),
        "tagged_expand": QueryWorkload(
            family="edge",
            shape="selective_relationship_expand",
            selectivity="high",
            query=(
                "MATCH (d:Document)-[:TAGGED]->(t:Topic) "
                "WHERE d.category = $category AND t.domain = $domain "
                "RETURN d.title, t.slug"
            ),
            params={"category": "category_5", "domain": "domain_3"},
        ),
        "tagged_topic_fanout": QueryWorkload(
            family="edge",
            shape="topic_fanout_expand",
            selectivity="low",
            query=(
                "MATCH (d:Document)-[:TAGGED]->(t:Topic) "
                "WHERE t.domain = $domain "
                "RETURN d.title, t.slug"
            ),
            params={"domain": "domain_3"},
        ),
        "team_lookup": QueryWorkload(
            family="node",
            shape="anchored_node_lookup",
            selectivity="high",
            query=(
                "MATCH (g:Team) "
                "WHERE g.slug = $slug "
                "RETURN g.slug, g.region, g.size_band"
            ),
            params={"slug": f"team_{midpoint_team}"},
        ),
        "team_membership_expand": QueryWorkload(
            family="edge",
            shape="membership_expand",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[:MEMBER_OF]->(g:Team) "
                "WHERE g.region = $region AND u.active = $active "
                "RETURN u.name, g.slug"
            ),
            params={"region": "region_5", "active": True},
        ),
        "team_membership_ordered": QueryWorkload(
            family="edge",
            shape="ordered_membership_expand",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[:MEMBER_OF]->(g:Team) "
                "WHERE g.size_band = $size_band "
                "RETURN u.name, g.slug, u.reputation "
                "ORDER BY u.reputation DESC "
                "LIMIT 250"
            ),
            params={"size_band": "band_3"},
        ),
    }


def _dataset_counts(total_nodes: int, fanout: int, tag_fanout: int) -> GraphDataset:
    user_count = (total_nodes * 45) // 100
    document_count = (total_nodes * 30) // 100
    topic_count = total_nodes - user_count - document_count
    team_count = max(1, topic_count // 3)
    topic_count -= team_count
    knows_edges = user_count * fanout
    authored_edges = document_count
    tagged_edges = document_count * tag_fanout
    member_of_edges = user_count

    user_props = user_count * 4
    document_props = document_count * 4
    topic_props = topic_count * 3
    team_props = team_count * 3
    edge_props = knows_edges + authored_edges + tagged_edges + member_of_edges

    return GraphDataset(
        total_nodes=total_nodes,
        user_count=user_count,
        document_count=document_count,
        topic_count=topic_count,
        team_count=team_count,
        knows_edges=knows_edges,
        authored_edges=authored_edges,
        tagged_edges=tagged_edges,
        member_of_edges=member_of_edges,
        total_storage_rows=(
            total_nodes
            + user_props
            + document_props
            + topic_props
            + team_props
            + knows_edges
            + authored_edges
            + tagged_edges
            + member_of_edges
            + edge_props
        ),
    )


def _seed_graph(
    db: HumemDB,
    *,
    nodes: int,
    fanout: int,
    tag_fanout: int,
    batch_size: int,
) -> tuple[GraphDataset, float]:
    ensure_graph_schema(db.sqlite)
    dataset = _dataset_counts(nodes, fanout, tag_fanout)

    started = time.perf_counter()
    with db.transaction(route="sqlite"):
        _seed_user_nodes(db, count=dataset.user_count, batch_size=batch_size)
        _seed_document_nodes(
            db,
            start_id=dataset.user_count + 1,
            count=dataset.document_count,
            batch_size=batch_size,
        )
        _seed_topic_nodes(
            db,
            start_id=dataset.user_count + dataset.document_count + 1,
            count=dataset.topic_count,
            batch_size=batch_size,
        )
        _seed_team_nodes(
            db,
            start_id=(
                dataset.user_count
                + dataset.document_count
                + dataset.topic_count
                + 1
            ),
            count=dataset.team_count,
            batch_size=batch_size,
        )
        _seed_knows_edges(
            db,
            user_count=dataset.user_count,
            fanout=fanout,
            batch_size=batch_size,
        )
        _seed_authored_edges(
            db,
            user_count=dataset.user_count,
            document_start_id=dataset.user_count + 1,
            document_count=dataset.document_count,
            starting_edge_id=dataset.knows_edges + 1,
            batch_size=batch_size,
        )
        _seed_tagged_edges(
            db,
            document_start_id=dataset.user_count + 1,
            document_count=dataset.document_count,
            topic_start_id=dataset.user_count + dataset.document_count + 1,
            topic_count=dataset.topic_count,
            starting_edge_id=dataset.knows_edges + dataset.authored_edges + 1,
            tag_fanout=tag_fanout,
            batch_size=batch_size,
        )
        _seed_member_of_edges(
            db,
            user_count=dataset.user_count,
            team_start_id=(
                dataset.user_count
                + dataset.document_count
                + dataset.topic_count
                + 1
            ),
            team_count=dataset.team_count,
            starting_edge_id=(
                dataset.knows_edges
                + dataset.authored_edges
                + dataset.tagged_edges
                + 1
            ),
            batch_size=batch_size,
        )
    return dataset, time.perf_counter() - started


def _seed_user_nodes(db: HumemDB, *, count: int, batch_size: int) -> None:
    for start in range(1, count + 1, batch_size):
        stop = min(start + batch_size - 1, count)
        node_rows = [(node_id, "User") for node_id in range(start, stop + 1)]
        property_rows: list[tuple[int, str, str | None, str]] = []

        for node_id in range(start, stop + 1):
            property_rows.extend(
                [
                    (node_id, "name", f"user_{node_id}", "string"),
                    (node_id, "region", f"region_{node_id % 20}", "string"),
                    (
                        node_id,
                        "active",
                        "true" if node_id % 3 != 0 else "false",
                        "boolean",
                    ),
                    (node_id, "reputation", str(node_id % 1000), "integer"),
                ]
            )

        _insert_nodes(db, node_rows, property_rows)


def _seed_document_nodes(
    db: HumemDB,
    *,
    start_id: int,
    count: int,
    batch_size: int,
) -> None:
    stop_id = start_id + count - 1
    for batch_start in range(start_id, stop_id + 1, batch_size):
        batch_stop = min(batch_start + batch_size - 1, stop_id)
        node_rows = [
            (node_id, "Document") for node_id in range(batch_start, batch_stop + 1)
        ]
        property_rows: list[tuple[int, str, str | None, str]] = []

        for node_id in range(batch_start, batch_stop + 1):
            local_id = node_id - start_id + 1
            property_rows.extend(
                [
                    (node_id, "title", f"document_{local_id}", "string"),
                    (node_id, "category", f"category_{local_id % 24}", "string"),
                    (
                        node_id,
                        "published",
                        "true" if local_id % 4 != 0 else "false",
                        "boolean",
                    ),
                    (node_id, "score", str(local_id % 5000), "integer"),
                ]
            )

        _insert_nodes(db, node_rows, property_rows)


def _seed_topic_nodes(
    db: HumemDB,
    *,
    start_id: int,
    count: int,
    batch_size: int,
) -> None:
    stop_id = start_id + count - 1
    for batch_start in range(start_id, stop_id + 1, batch_size):
        batch_stop = min(batch_start + batch_size - 1, stop_id)
        node_rows = [
            (node_id, "Topic") for node_id in range(batch_start, batch_stop + 1)
        ]
        property_rows: list[tuple[int, str, str | None, str]] = []

        for node_id in range(batch_start, batch_stop + 1):
            local_id = node_id - start_id + 1
            property_rows.extend(
                [
                    (node_id, "slug", f"topic_{local_id}", "string"),
                    (node_id, "domain", f"domain_{local_id % 12}", "string"),
                    (
                        node_id,
                        "trending",
                        "true" if local_id % 5 == 0 else "false",
                        "boolean",
                    ),
                ]
            )

        _insert_nodes(db, node_rows, property_rows)


def _seed_team_nodes(
    db: HumemDB,
    *,
    start_id: int,
    count: int,
    batch_size: int,
) -> None:
    stop_id = start_id + count - 1
    for batch_start in range(start_id, stop_id + 1, batch_size):
        batch_stop = min(batch_start + batch_size - 1, stop_id)
        node_rows = [
            (node_id, "Team") for node_id in range(batch_start, batch_stop + 1)
        ]
        property_rows: list[tuple[int, str, str | None, str]] = []

        for node_id in range(batch_start, batch_stop + 1):
            local_id = node_id - start_id + 1
            property_rows.extend(
                [
                    (node_id, "slug", f"team_{local_id}", "string"),
                    (node_id, "region", f"region_{local_id % 20}", "string"),
                    (node_id, "size_band", f"band_{local_id % 8}", "string"),
                ]
            )

        _insert_nodes(db, node_rows, property_rows)


def _insert_nodes(
    db: HumemDB,
    node_rows: list[tuple[int, str]],
    property_rows: list[tuple[int, str, str | None, str]],
) -> None:
    db.sqlite.executemany(
        "INSERT INTO graph_nodes (id, label) VALUES (?, ?)",
        node_rows,
    )
    db.sqlite.executemany(
        (
            "INSERT INTO graph_node_properties (node_id, key, value, value_type) "
            "VALUES (?, ?, ?, ?)"
        ),
        property_rows,
    )


def _seed_knows_edges(
    db: HumemDB,
    *,
    user_count: int,
    fanout: int,
    batch_size: int,
) -> None:
    edge_id = 1
    edge_rows: list[tuple[int, str, int, int]] = []
    property_rows: list[tuple[int, str, str | None, str]] = []

    for from_node_id in range(1, user_count + 1):
        for offset in range(1, fanout + 1):
            to_node_id = ((from_node_id + offset - 1) % user_count) + 1
            edge_rows.append((edge_id, "KNOWS", from_node_id, to_node_id))
            property_rows.append(
                (edge_id, "since", str(2018 + (edge_id % 6)), "integer")
            )
            edge_id += 1
        if len(edge_rows) >= batch_size:
            _flush_edge_batches(db, edge_rows, property_rows)
            edge_rows = []
            property_rows = []

    if edge_rows:
        _flush_edge_batches(db, edge_rows, property_rows)


def _seed_authored_edges(
    db: HumemDB,
    *,
    user_count: int,
    document_start_id: int,
    document_count: int,
    starting_edge_id: int,
    batch_size: int,
) -> None:
    edge_id = starting_edge_id
    edge_rows: list[tuple[int, str, int, int]] = []
    property_rows: list[tuple[int, str, str | None, str]] = []

    for offset in range(document_count):
        document_id = document_start_id + offset
        author_id = (offset % user_count) + 1
        edge_rows.append((edge_id, "AUTHORED", author_id, document_id))
        property_rows.append((edge_id, "year", str(2019 + (offset % 6)), "integer"))
        edge_id += 1
        if len(edge_rows) >= batch_size:
            _flush_edge_batches(db, edge_rows, property_rows)
            edge_rows = []
            property_rows = []

    if edge_rows:
        _flush_edge_batches(db, edge_rows, property_rows)


def _seed_tagged_edges(
    db: HumemDB,
    *,
    document_start_id: int,
    document_count: int,
    topic_start_id: int,
    topic_count: int,
    starting_edge_id: int,
    tag_fanout: int,
    batch_size: int,
) -> None:
    edge_id = starting_edge_id
    edge_rows: list[tuple[int, str, int, int]] = []
    property_rows: list[tuple[int, str, str | None, str]] = []

    for offset in range(document_count):
        document_id = document_start_id + offset
        for tag_offset in range(tag_fanout):
            topic_id = topic_start_id + ((offset + tag_offset) % topic_count)
            edge_rows.append((edge_id, "TAGGED", document_id, topic_id))
            property_rows.append(
                (edge_id, "weight", str((offset + tag_offset) % 100), "integer")
            )
            edge_id += 1
        if len(edge_rows) >= batch_size:
            _flush_edge_batches(db, edge_rows, property_rows)
            edge_rows = []
            property_rows = []

    if edge_rows:
        _flush_edge_batches(db, edge_rows, property_rows)


def _seed_member_of_edges(
    db: HumemDB,
    *,
    user_count: int,
    team_start_id: int,
    team_count: int,
    starting_edge_id: int,
    batch_size: int,
) -> None:
    edge_id = starting_edge_id
    edge_rows: list[tuple[int, str, int, int]] = []
    property_rows: list[tuple[int, str, str | None, str]] = []

    for user_id in range(1, user_count + 1):
        team_id = team_start_id + ((user_id - 1) % team_count)
        edge_rows.append((edge_id, "MEMBER_OF", user_id, team_id))
        property_rows.append((edge_id, "role", f"role_{user_id % 5}", "string"))
        edge_id += 1
        if len(edge_rows) >= batch_size:
            _flush_edge_batches(db, edge_rows, property_rows)
            edge_rows = []
            property_rows = []

    if edge_rows:
        _flush_edge_batches(db, edge_rows, property_rows)


def _flush_edge_batches(
    db: HumemDB,
    edge_rows: list[tuple[int, str, int, int]],
    property_rows: list[tuple[int, str, str | None, str]],
) -> None:
    db.sqlite.executemany(
        (
            "INSERT INTO graph_edges (id, type, from_node_id, to_node_id) "
            "VALUES (?, ?, ?, ?)"
        ),
        edge_rows,
    )
    db.sqlite.executemany(
        (
            "INSERT INTO graph_edge_properties (edge_id, key, value, value_type) "
            "VALUES (?, ?, ?, ?)"
        ),
        property_rows,
    )


def _summarize(timings: list[float]) -> TimingSummary:
    return TimingSummary(
        mean=statistics.mean(timings),
        stdev=statistics.pstdev(timings),
        minimum=min(timings),
        maximum=max(timings),
    )


def _time_callable(
    operation: Callable[[], object],
    *,
    warmup: int,
    repetitions: int,
) -> TimingSummary:
    for _ in range(warmup):
        operation()

    timings: list[float] = []
    for _ in range(repetitions):
        started = time.perf_counter()
        operation()
        timings.append(time.perf_counter() - started)

    return _summarize(timings)


def _compile_workload(workload: QueryWorkload):
    plan = _bind_plan_values(parse_cypher(workload.query), workload.params)
    if not isinstance(plan, (MatchNodePlan, MatchRelationshipPlan)):
        raise ValueError("Graph benchmark only supports MATCH-based workloads.")
    return _compile_match_plan(plan)


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
        "benchmark": "cypher_graph_path",
        "thread_limit": os.environ.get("HUMEMDB_THREADS", "default"),
        "warmup": args.warmup,
        "repetitions": args.repetitions,
        "batch_size": args.batch_size,
        "fanout": args.fanout,
        "tag_fanout": args.tag_fanout,
        "workloads": {},
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = Path(tmpdir) / "graph.sqlite3"
        duckdb_path = Path(tmpdir) / "graph.duckdb"

        with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
            dataset, seed_seconds = _seed_graph(
                db,
                nodes=args.nodes,
                fanout=args.fanout,
                tag_fanout=args.tag_fanout,
                batch_size=args.batch_size,
            )
            workloads = _workloads(dataset)

            print(f"Thread limit: {os.environ.get('HUMEMDB_THREADS', 'default')}")
            print(f"Total nodes: {dataset.total_nodes}")
            print(f"Users: {dataset.user_count}")
            print(f"Documents: {dataset.document_count}")
            print(f"Topics: {dataset.topic_count}")
            print(f"Teams: {dataset.team_count}")
            print(f"KNOWS edges: {dataset.knows_edges}")
            print(f"AUTHORED edges: {dataset.authored_edges}")
            print(f"TAGGED edges: {dataset.tagged_edges}")
            print(f"MEMBER_OF edges: {dataset.member_of_edges}")
            print(f"Approx graph table rows: {dataset.total_storage_rows}")
            print(f"User fanout: {args.fanout}")
            print(f"Document tag fanout: {args.tag_fanout}")
            print(f"Warmup iterations: {args.warmup}")
            print(f"Timed repetitions: {args.repetitions}")
            print(f"Batch size: {args.batch_size}")
            print(f"Seed time: {_format_seconds(seed_seconds)}")
            print()
            json_results["dataset"] = {
                "total_nodes": dataset.total_nodes,
                "users": dataset.user_count,
                "documents": dataset.document_count,
                "topics": dataset.topic_count,
                "teams": dataset.team_count,
                "knows_edges": dataset.knows_edges,
                "authored_edges": dataset.authored_edges,
                "tagged_edges": dataset.tagged_edges,
                "member_of_edges": dataset.member_of_edges,
                "total_storage_rows": dataset.total_storage_rows,
                "seed_seconds": seed_seconds,
            }

            for name, workload in workloads.items():
                parse_summary = _time_callable(
                    lambda workload=workload: parse_cypher(workload.query),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )
                compile_summary = _time_callable(
                    lambda workload=workload: _compile_workload(workload),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )
                compiled = _compile_workload(workload)

                sqlite_sql_summary = _time_callable(
                    lambda compiled=compiled: db.sqlite.execute(
                        compiled.sql,
                        compiled.params,
                    ),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )
                duckdb_sql_summary = _time_callable(
                    lambda compiled=compiled: db.duckdb.execute(
                        compiled.sql,
                        compiled.params,
                    ),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )
                sqlite_cypher_summary = _time_callable(
                    lambda workload=workload: db.query(
                        workload.query,
                        route="sqlite",
                        params=workload.params,
                    ),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )
                duckdb_cypher_summary = _time_callable(
                    lambda workload=workload: db.query(
                        workload.query,
                        route="duckdb",
                        params=workload.params,
                    ),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )

                print(f"Workload: {name}")
                print(f"  Family: {workload.family}")
                print(f"  Shape: {workload.shape}")
                print(f"  Selectivity: {workload.selectivity}")
                _print_summary("Cypher parse", parse_summary)
                _print_summary("Cypher bind+compile", compile_summary)
                _print_summary("SQLite raw SQL", sqlite_sql_summary)
                _print_summary("DuckDB raw SQL", duckdb_sql_summary)
                _print_summary("SQLite Cypher end-to-end", sqlite_cypher_summary)
                _print_summary("DuckDB Cypher end-to-end", duckdb_cypher_summary)
                print()
                cast_workloads = json_results["workloads"]
                assert isinstance(cast_workloads, dict)
                cast_workloads[name] = {
                    "family": workload.family,
                    "shape": workload.shape,
                    "selectivity": workload.selectivity,
                    "query": workload.query,
                    "params": workload.params,
                    "cypher_parse": _summary_dict(parse_summary),
                    "cypher_compile": _summary_dict(compile_summary),
                    "sqlite_raw_sql": _summary_dict(sqlite_sql_summary),
                    "duckdb_raw_sql": _summary_dict(duckdb_sql_summary),
                    "sqlite_cypher": _summary_dict(sqlite_cypher_summary),
                    "duckdb_cypher": _summary_dict(duckdb_cypher_summary),
                }

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(json_results, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
