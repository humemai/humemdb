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
from humemdb.cypher import _ensure_graph_schema
from humemdb.cypher import parse_cypher


_GRAPH_INDEX_SET_SQL: dict[str, tuple[str, ...]] = {
    "baseline": (),
    "node-prop-covering": (
        "CREATE INDEX IF NOT EXISTS idx_graph_node_props_node_key_value_type_value "
        "ON graph_node_properties(node_id, key, value_type, value)",
    ),
    "edge-prop-covering": (
        "CREATE INDEX IF NOT EXISTS idx_graph_edge_props_edge_key_value_type_value "
        "ON graph_edge_properties(edge_id, key, value_type, value)",
    ),
    "targeted-covering": (
        "CREATE INDEX IF NOT EXISTS idx_graph_node_props_node_key_value_type_value "
        "ON graph_node_properties(node_id, key, value_type, value)",
    ),
}


@dataclass(frozen=True, slots=True)
class QueryWorkload:
    """Benchmark workload definition for one Cypher query shape."""

    family: str
    shape: str
    selectivity: str
    query: str
    params: dict[str, str | int | float | bool | None]
    comparison_group: str | None = None
    order_variant: str | None = None


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
    """Parse CLI arguments for the graph path benchmark."""

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
    parser.add_argument(
        "--index-set",
        choices=tuple(_GRAPH_INDEX_SET_SQL),
        default="baseline",
        help="Named extra graph-index set to apply after seeding.",
    )
    return parser.parse_args()


def _apply_graph_index_set(db: HumemDB, *, index_set: str) -> None:
    """Apply one named graph-index experiment to the seeded SQLite graph tables."""

    try:
        statements = _GRAPH_INDEX_SET_SQL[index_set]
    except KeyError as exc:
        raise ValueError(f"Unknown graph index set {index_set!r}.") from exc

    sqlite = db._sqlite
    for statement in statements:
        sqlite.execute(statement)


def _workloads(dataset: GraphDataset) -> dict[str, QueryWorkload]:
    """Build the workload catalog for the seeded graph dataset."""

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
        "user_distinct_region_offset": QueryWorkload(
            family="node",
            shape="distinct_paginated_projection",
            selectivity="medium",
            query=(
                "MATCH (u:User) "
                "RETURN DISTINCT u.region "
                "ORDER BY u.region OFFSET 5 LIMIT 10"
            ),
            params={},
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
        "social_expand_offset": QueryWorkload(
            family="edge",
            shape="offset_relationship_expand",
            selectivity="low",
            query=(
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE a.region = $region "
                "RETURN a.name, b.name, r.since "
                "ORDER BY r.since DESC OFFSET 50 LIMIT 250"
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
        "social_expand_untyped": QueryWorkload(
            family="edge",
            shape="untyped_relationship_expand",
            selectivity="low",
            query=(
                "MATCH (a:User)-[r]->(b:User) "
                "WHERE a.region = $region AND b.active = $active "
                "RETURN a.name, r.type, b.name "
                "LIMIT 500"
            ),
            params={"region": "region_11", "active": True},
        ),
        "social_expand_type_alternation": QueryWorkload(
            family="edge",
            shape="relationship_type_alternation",
            selectivity="low",
            query=(
                "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) "
                "WHERE a.region = $region "
                "RETURN a.name, r.type, b.name "
                "ORDER BY a.name, b.name LIMIT 500"
            ),
            params={"region": "region_11"},
        ),
        "social_expand_anonymous_endpoints": QueryWorkload(
            family="edge",
            shape="anonymous_endpoint_expand",
            selectivity="low",
            query=(
                "MATCH (:User {region: $region})-[r:KNOWS]->(:User {active: $active}) "
                "RETURN r.type, r.since "
                "ORDER BY r.since DESC LIMIT 500"
            ),
            params={"region": "region_11", "active": True},
        ),
        "social_type_filtered_region_expand": QueryWorkload(
            family="edge",
            shape="endpoint_plus_type_filter",
            selectivity="medium",
            query=(
                "MATCH (a:User)-[r]->(b:User) "
                "WHERE r.type = $type AND a.region = $region AND b.active = $active "
                "RETURN a.name, r.type, b.name "
                "ORDER BY a.name, b.name LIMIT 500"
            ),
            params={"type": "KNOWS", "region": "region_11", "active": True},
        ),
        "social_reverse_expand_ordered": QueryWorkload(
            family="edge",
            shape="reverse_relationship_expand",
            selectivity="low",
            query=(
                "MATCH (b:User)<-[r:KNOWS]-(a:User) "
                "WHERE a.region = $region "
                "RETURN a.name, b.name, r.since "
                "ORDER BY r.since DESC LIMIT 500"
            ),
            params={"region": "region_11"},
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
        "social_mixed_boolean": QueryWorkload(
            family="edge",
            shape="mixed_boolean_expand",
            selectivity="medium",
            query=(
                "MATCH (a:User)-[r:KNOWS]->(b:User) "
                "WHERE r.since = $since AND b.active = $active OR a.name = $name "
                "RETURN a.name, b.name "
                "ORDER BY a.name, b.name LIMIT 250"
            ),
            params={"since": 2021, "active": True, "name": f"user_{midpoint_user}"},
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
            comparison_group="author_expand_score_order",
            order_variant="ordered",
        ),
        "author_expand_unordered": QueryWorkload(
            family="edge",
            shape="unordered_relationship_expand",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[:AUTHORED]->(d:Document) "
                "WHERE d.published = $published "
                "RETURN u.name, d.title, d.score "
                "LIMIT 100"
            ),
            params={"published": True},
            comparison_group="author_expand_score_order",
            order_variant="unordered",
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
        "team_membership_role_region": QueryWorkload(
            family="edge",
            shape="edge_property_plus_endpoint_filter",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[r:MEMBER_OF]->(g:Team) "
                "WHERE r.role = $role AND g.region = $region "
                "RETURN u.name, g.slug, r.role "
                "ORDER BY u.name LIMIT 250"
            ),
            params={"role": "role_3", "region": "region_5"},
            comparison_group="team_membership_name_order",
            order_variant="ordered",
        ),
        "team_membership_role_region_unordered": QueryWorkload(
            family="edge",
            shape="edge_property_plus_endpoint_filter",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[r:MEMBER_OF]->(g:Team) "
                "WHERE r.role = $role AND g.region = $region "
                "RETURN u.name, g.slug, r.role "
                "LIMIT 250"
            ),
            params={"role": "role_3", "region": "region_5"},
            comparison_group="team_membership_name_order",
            order_variant="unordered",
        ),
        "team_membership_type_band": QueryWorkload(
            family="edge",
            shape="endpoint_plus_type_filter",
            selectivity="medium",
            query=(
                "MATCH (u:User)-[r]->(g:Team) "
                "WHERE r.type = $type AND g.size_band = $size_band "
                "AND u.active = $active "
                "RETURN u.name, g.slug, u.reputation "
                "ORDER BY u.reputation DESC "
                "LIMIT 250"
            ),
            params={"type": "MEMBER_OF", "size_band": "band_3", "active": True},
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
        "tagged_weight_domain": QueryWorkload(
            family="edge",
            shape="edge_property_plus_endpoint_filter",
            selectivity="medium",
            query=(
                "MATCH (d:Document)-[r:TAGGED]->(t:Topic) "
                "WHERE r.weight = $weight AND t.domain = $domain "
                "RETURN d.title, t.slug, r.weight "
                "ORDER BY d.title LIMIT 250"
            ),
            params={"weight": 17, "domain": "domain_3"},
            comparison_group="tagged_title_order",
            order_variant="ordered",
        ),
        "tagged_weight_domain_unordered": QueryWorkload(
            family="edge",
            shape="edge_property_plus_endpoint_filter",
            selectivity="medium",
            query=(
                "MATCH (d:Document)-[r:TAGGED]->(t:Topic) "
                "WHERE r.weight = $weight AND t.domain = $domain "
                "RETURN d.title, t.slug, r.weight "
                "LIMIT 250"
            ),
            params={"weight": 17, "domain": "domain_3"},
            comparison_group="tagged_title_order",
            order_variant="unordered",
        ),
    }


def _dataset_counts(total_nodes: int, fanout: int, tag_fanout: int) -> GraphDataset:
    """Derive node, edge, and storage counts from the requested graph scale."""

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
    """Seed the full synthetic graph and return its dataset summary and duration."""

    _ensure_graph_schema(db._sqlite)
    dataset = _dataset_counts(nodes, fanout, tag_fanout)

    started = time.perf_counter()
    with db.transaction():
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
    """Seed synthetic User nodes and their properties."""

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
    """Seed synthetic Document nodes and their properties."""

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
    """Seed synthetic Topic nodes and their properties."""

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
    """Seed synthetic Team nodes and their properties."""

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
    """Insert one batch of graph nodes and their property rows."""

    sqlite = db._sqlite
    sqlite.executemany(
        "INSERT INTO graph_nodes (id, label) VALUES (?, ?)",
        node_rows,
    )
    sqlite.executemany(
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
    """Seed synthetic KNOWS relationships and their properties."""

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
    """Seed synthetic AUTHORED relationships and their properties."""

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
    """Seed synthetic TAGGED relationships and their properties."""

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
    """Seed synthetic MEMBER_OF relationships and their properties."""

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
    """Write one buffered batch of graph edges and edge properties."""

    sqlite = db._sqlite
    sqlite.executemany(
        (
            "INSERT INTO graph_edges (id, type, from_node_id, to_node_id) "
            "VALUES (?, ?, ?, ?)"
        ),
        edge_rows,
    )
    sqlite.executemany(
        (
            "INSERT INTO graph_edge_properties (edge_id, key, value, value_type) "
            "VALUES (?, ?, ?, ?)"
        ),
        property_rows,
    )


def _summarize(timings: list[float]) -> TimingSummary:
    """Summarize one set of benchmark timing samples."""

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
    """Warm up and time one zero-argument benchmark stage."""

    for _ in range(warmup):
        operation()

    timings: list[float] = []
    for _ in range(repetitions):
        started = time.perf_counter()
        operation()
        timings.append(time.perf_counter() - started)

    return _summarize(timings)


def _compile_workload(workload: QueryWorkload):
    """Compile one admitted Cypher workload into an executable match plan."""

    plan = _bind_plan_values(parse_cypher(workload.query), workload.params)
    if not isinstance(plan, (MatchNodePlan, MatchRelationshipPlan)):
        raise ValueError("Graph benchmark only supports MATCH-based workloads.")
    return _compile_match_plan(plan)


def _cypher_feature_dict(workload: QueryWorkload) -> dict[str, int | bool | str]:
    """Return lightweight graph-planning features for one admitted Cypher workload."""

    compiled = _compile_workload(workload)
    sql = compiled.sql
    return {
        "family": workload.family,
        "shape": workload.shape,
        "selectivity": workload.selectivity,
        "graph_nodes_join_count": sql.count("JOIN graph_nodes AS"),
        "graph_edges_join_count": sql.count("JOIN graph_edges AS"),
        "node_property_join_count": sql.count("JOIN graph_node_properties AS"),
        "edge_property_join_count": sql.count("JOIN graph_edge_properties AS"),
        "anchors_node_properties": "FROM graph_node_properties AS" in sql,
        "anchors_edge_properties": "FROM graph_edge_properties AS" in sql,
        "direct_node_label_filter": ".label " in sql,
        "direct_edge_type_filter": ".type " in sql,
        "has_distinct": "SELECT DISTINCT" in sql,
        "has_order_by": " ORDER BY " in sql,
        "has_offset": " OFFSET " in sql,
        "has_limit": " LIMIT " in sql,
    }


def _sqlite_plan_summary_from_details(
    details: list[str],
) -> dict[str, int | bool | list[str]]:
    """Summarize SQLite EXPLAIN QUERY PLAN detail lines for machine-readable output."""

    upper_details = [detail.upper() for detail in details]
    index_mentions = sorted(
        {
            detail
            for detail, upper in zip(details, upper_details, strict=False)
            if "USING INDEX" in upper or "USING COVERING INDEX" in upper
        }
    )
    return {
        "detail_count": len(details),
        "node_search_count": sum("GRAPH_NODES" in detail for detail in upper_details),
        "edge_search_count": sum("GRAPH_EDGES" in detail for detail in upper_details),
        "node_property_search_count": sum(
            "GRAPH_NODE_PROPERTIES" in detail for detail in upper_details
        ),
        "edge_property_search_count": sum(
            "GRAPH_EDGE_PROPERTIES" in detail for detail in upper_details
        ),
        "uses_temp_btree": any("USE TEMP B-TREE" in detail for detail in upper_details),
        "index_mentions": index_mentions,
    }


def _sqlite_plan_summary(
    db: HumemDB,
    *,
    sql: str,
    params: tuple[object, ...],
) -> dict[str, int | bool | list[str]]:
    """Return a summarized SQLite EXPLAIN QUERY PLAN payload for one compiled query."""

    explain_rows = db._sqlite.execute(
        f"EXPLAIN QUERY PLAN {sql}",
        params,
    ).rows
    details = [str(row[3]) for row in explain_rows]
    return _sqlite_plan_summary_from_details(details)


def _format_seconds(seconds: float) -> str:
    """Format seconds as a millisecond string for console output."""

    return f"{seconds * 1_000:.2f} ms"


def _print_summary(label: str, summary: TimingSummary) -> None:
    """Print one timing summary in the benchmark's text format."""

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
    """Seed the graph, run the workloads, and emit benchmark results."""

    args = _parse_args()
    json_results: dict[str, object] = {
        "benchmark": "cypher_graph_path",
        "index_set": args.index_set,
        "thread_limit": os.environ.get("HUMEMDB_THREADS", "default"),
        "warmup": args.warmup,
        "repetitions": args.repetitions,
        "batch_size": args.batch_size,
        "fanout": args.fanout,
        "tag_fanout": args.tag_fanout,
        "workloads": {},
    }

    with tempfile.TemporaryDirectory() as tmpdir:
        base_path = Path(tmpdir) / "graph"

        with HumemDB(base_path) as db:
            dataset, seed_seconds = _seed_graph(
                db,
                nodes=args.nodes,
                fanout=args.fanout,
                tag_fanout=args.tag_fanout,
                batch_size=args.batch_size,
            )
            _apply_graph_index_set(db, index_set=args.index_set)
            workloads = _workloads(dataset)

            print(f"Thread limit: {os.environ.get('HUMEMDB_THREADS', 'default')}")
            print(f"Index set: {args.index_set}")
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
                    lambda compiled=compiled: db._sqlite.execute(
                        compiled.sql,
                        compiled.params,
                    ),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )
                duckdb_sql_summary = _time_callable(
                    lambda compiled=compiled: db._duckdb.execute(
                        compiled.sql,
                        compiled.params,
                    ),
                    warmup=args.warmup,
                    repetitions=args.repetitions,
                )
                public_cypher_summary = _time_callable(
                    lambda workload=workload: db.query(
                        workload.query,
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
                _print_summary("Public Cypher end-to-end", public_cypher_summary)
                print()
                cast_workloads = json_results["workloads"]
                assert isinstance(cast_workloads, dict)
                cast_workloads[name] = {
                    "family": workload.family,
                    "shape": workload.shape,
                    "selectivity": workload.selectivity,
                    "comparison_group": workload.comparison_group,
                    "order_variant": workload.order_variant,
                    "query": workload.query,
                    "params": workload.params,
                    "cypher_features": _cypher_feature_dict(workload),
                    "sqlite_plan_summary": _sqlite_plan_summary(
                        db,
                        sql=compiled.sql,
                        params=compiled.params,
                    ),
                    "cypher_parse": _summary_dict(parse_summary),
                    "cypher_compile": _summary_dict(compile_summary),
                    "sqlite_raw_sql": _summary_dict(sqlite_sql_summary),
                    "duckdb_raw_sql": _summary_dict(duckdb_sql_summary),
                    "public_cypher": _summary_dict(public_cypher_summary),
                }

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(
            json.dumps(json_results, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )


if __name__ == "__main__":
    main()
