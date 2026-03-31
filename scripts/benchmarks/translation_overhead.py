from __future__ import annotations

import argparse
import statistics
import time
from dataclasses import dataclass

from humemdb.cypher import MatchNodePlan
from humemdb.cypher import MatchRelationshipPlan
from humemdb.cypher import _bind_plan_values
from humemdb.cypher import _compile_match_plan
from humemdb.cypher import parse_cypher
from humemdb.db import _plan_cypher_query
from humemdb.sql import _translate_sql_plan_cached
from humemdb.sql import translate_sql


@dataclass(frozen=True, slots=True)
class SQLWorkload:
    """One PostgreSQL-like SQL translation workload."""

    family: str
    complexity: str
    query: str


@dataclass(frozen=True, slots=True)
class CypherWorkload:
    """One HumemCypher translation workload."""

    family: str
    complexity: str
    query: str
    params: dict[str, str | int | float | bool | None]


@dataclass(frozen=True, slots=True)
class TimingSummary:
    """Aggregate timing metrics for one benchmark stage."""

    mean: float
    stdev: float
    minimum: float
    maximum: float


SQL_WORKLOADS: dict[str, SQLWorkload] = {
    "literal_projection": SQLWorkload(
        family="oltp",
        complexity="simple",
        query="SELECT 1::INTEGER AS value, 'Alice' ILIKE 'aLiCe' AS matched",
    ),
    "point_lookup": SQLWorkload(
        family="oltp",
        complexity="simple",
        query=(
            "SELECT id::INTEGER AS user_id, name "
            "FROM users "
            "WHERE name ILIKE 'user 0042' "
            "ORDER BY id "
            "LIMIT 5"
        ),
    ),
    "filtered_aggregate": SQLWorkload(
        family="analytics",
        complexity="simple",
        query=(
            "SELECT status, COUNT(*) AS order_count "
            "FROM orders "
            "WHERE created_at >= DATE '2026-01-01' "
            "GROUP BY status "
            "ORDER BY order_count DESC"
        ),
    ),
    "join_aggregate": SQLWorkload(
        family="analytics",
        complexity="medium",
        query=(
            "SELECT u.segment, AVG(o.total_cents) AS avg_total "
            "FROM orders o "
            "JOIN users u ON u.id = o.user_id "
            "WHERE o.status = 'paid' "
            "GROUP BY u.segment "
            "ORDER BY avg_total DESC"
        ),
    ),
    "windowed_filter": SQLWorkload(
        family="oltp",
        complexity="medium",
        query=(
            "SELECT id, created_at "
            "FROM events "
            "WHERE created_at >= DATE '2026-01-01' "
            "AND created_at < DATE '2026-02-01' "
            "ORDER BY created_at DESC "
            "LIMIT 20"
        ),
    ),
    "case_and_exists": SQLWorkload(
        family="analytics",
        complexity="medium",
        query=(
            "SELECT u.id, "
            "CASE WHEN EXISTS ("
            "SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.status = 'paid'"
            ") THEN 'buyer' ELSE 'prospect' END AS cohort "
            "FROM users u "
            "WHERE u.deleted_at IS NULL "
            "ORDER BY u.id "
            "LIMIT 50"
        ),
    ),
    "union_rollup": SQLWorkload(
        family="analytics",
        complexity="medium",
        query=(
            "SELECT region, total_orders "
            "FROM ("
            "SELECT region, total_orders FROM regional_totals "
            "UNION ALL "
            "SELECT 'global' AS region, SUM(total_orders) AS total_orders "
            "FROM regional_totals"
            ") AS combined "
            "ORDER BY total_orders DESC"
        ),
    ),
    "cte_multi_join": SQLWorkload(
        family="analytics",
        complexity="complex",
        query=(
            "WITH recent_paid AS ("
            "SELECT user_id, total_cents, created_at "
            "FROM orders "
            "WHERE status = 'paid' AND created_at >= DATE '2026-01-01'"
            "), top_users AS ("
            "SELECT user_id, SUM(total_cents) AS spent_cents "
            "FROM recent_paid "
            "GROUP BY user_id"
            ") "
            "SELECT u.segment, c.name AS country, AVG(t.spent_cents) AS avg_spend "
            "FROM top_users t "
            "JOIN users u ON u.id = t.user_id "
            "LEFT JOIN countries c ON c.id = u.country_id "
            "GROUP BY u.segment, c.name "
            "ORDER BY avg_spend DESC, u.segment"
        ),
    ),
    "windowed_rank_cte": SQLWorkload(
        family="analytics",
        complexity="complex",
        query=(
            "WITH ranked AS ("
            "SELECT e.user_id, e.event_type, e.created_at, "
            "ROW_NUMBER() OVER ("
            "PARTITION BY e.user_id ORDER BY e.created_at DESC"
            ") AS rn "
            "FROM events e"
            ") "
            "SELECT user_id, event_type, created_at "
            "FROM ranked "
            "WHERE rn <= 3 "
            "ORDER BY user_id, created_at DESC"
        ),
    ),
}


CYPHER_WORKLOADS: dict[str, CypherWorkload] = {
    "node_anchor": CypherWorkload(
        family="node",
        complexity="simple",
        query=(
            "MATCH (u:User {name: $name}) "
            "RETURN u.name, u.region, u.active"
        ),
        params={"name": "user_0042"},
    ),
    "node_lookup": CypherWorkload(
        family="node",
        complexity="simple",
        query=(
            "MATCH (u:User) "
            "WHERE u.name = $name "
            "RETURN u.name, u.region, u.active "
            "ORDER BY u.name LIMIT 5"
        ),
        params={"name": "user_0042"},
    ),
    "node_range_filter": CypherWorkload(
        family="node",
        complexity="medium",
        query=(
            "MATCH (u:User) "
            "WHERE u.age >= $min_age "
            "RETURN u.name, u.age, u.region "
            "ORDER BY u.age, u.name LIMIT 10"
        ),
        params={"min_age": 30},
    ),
    "node_string_predicates": CypherWorkload(
        family="node",
        complexity="medium",
        query=(
            "MATCH (u:User) "
            "WHERE u.name STARTS WITH $prefix AND u.region CONTAINS $fragment "
            "RETURN u.name, u.region "
            "ORDER BY u.name LIMIT 10"
        ),
        params={"prefix": "user_00", "fragment": "region_"},
    ),
    "node_null_predicates": CypherWorkload(
        family="node",
        complexity="medium",
        query=(
            "MATCH (u:User) "
            "WHERE u.nickname IS NULL AND u.region IS NOT NULL "
            "RETURN u.name, u.region "
            "ORDER BY u.name LIMIT 10"
        ),
        params={},
    ),
    "node_mixed_boolean": CypherWorkload(
        family="node",
        complexity="complex",
        query=(
            "MATCH (u:User) "
            "WHERE u.age >= $min_age AND u.active = $active OR u.name = $name "
            "RETURN u.name, u.age, u.region "
            "ORDER BY u.name LIMIT 10"
        ),
        params={"min_age": 40, "active": True, "name": "user_0042"},
    ),
    "node_parenthesized_boolean": CypherWorkload(
        family="node",
        complexity="complex",
        query=(
            "MATCH (u:User) "
            "WHERE (u.age >= $min_age OR u.name = $name) AND u.active = $active "
            "RETURN u.name, u.age, u.region "
            "ORDER BY u.name LIMIT 10"
        ),
        params={"min_age": 40, "name": "user_0042", "active": True},
    ),
    "node_distinct_offset": CypherWorkload(
        family="node",
        complexity="medium",
        query=(
            "MATCH (u:User) "
            "RETURN DISTINCT u.region "
            "ORDER BY u.region OFFSET 5 LIMIT 10"
        ),
        params={},
    ),
    "relationship_expand": CypherWorkload(
        family="edge",
        complexity="medium",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.region = $region AND r.strength = $strength "
            "RETURN a.name, r.since, b.name "
            "ORDER BY r.since DESC, a.name LIMIT 5"
        ),
        params={"region": "region_3", "strength": 8},
    ),
    "relationship_untyped": CypherWorkload(
        family="edge",
        complexity="medium",
        query=(
            "MATCH (a:User)-[r]->(b:User) "
            "WHERE a.region = $region AND b.active = $active "
            "RETURN a.name, r.type, b.name "
            "ORDER BY a.name LIMIT 10"
        ),
        params={"region": "region_3", "active": True},
    ),
    "relationship_offset_window": CypherWorkload(
        family="edge",
        complexity="medium",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.region = $region "
            "RETURN a.name, r.since, b.name "
            "ORDER BY r.since DESC OFFSET 25 LIMIT 50"
        ),
        params={"region": "region_3"},
    ),
    "relationship_string_predicates": CypherWorkload(
        family="edge",
        complexity="complex",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.note CONTAINS $fragment AND b.name ENDS WITH $suffix "
            "RETURN a.name, r.note, b.name "
            "ORDER BY b.name LIMIT 10"
        ),
        params={"fragment": "met", "suffix": "42"},
    ),
    "relationship_null_predicates": CypherWorkload(
        family="edge",
        complexity="complex",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.note IS NOT NULL "
            "RETURN a.name, r.note, b.name "
            "ORDER BY b.name LIMIT 10"
        ),
        params={},
    ),
    "relationship_type_alternation": CypherWorkload(
        family="edge",
        complexity="complex",
        query=(
            "MATCH (a:User)-[r:KNOWS|FOLLOWS]->(b:User) "
            "WHERE a.region = $region "
            "RETURN a.name, r.type, b.name "
            "ORDER BY a.name LIMIT 10"
        ),
        params={"region": "region_3"},
    ),
    "relationship_reverse": CypherWorkload(
        family="edge",
        complexity="medium",
        query=(
            "MATCH (b:User)<-[r:KNOWS]-(a:User) "
            "WHERE b.region = $region "
            "RETURN a.name, r.type, b.name "
            "ORDER BY a.name LIMIT 10"
        ),
        params={"region": "region_5"},
    ),
    "relationship_anonymous_endpoints": CypherWorkload(
        family="edge",
        complexity="complex",
        query=(
            "MATCH (:User {region: $region})-[r:KNOWS]->(:User {active: $active}) "
            "RETURN r.type, r.since "
            "ORDER BY r.since DESC LIMIT 10"
        ),
        params={"region": "region_3", "active": True},
    ),
    "relationship_property_anchor": CypherWorkload(
        family="edge",
        complexity="complex",
        query=(
            "MATCH (a:User {region: $region})-[r:KNOWS {strength: $strength}]->"
            "(b:User {active: $active}) "
            "RETURN a.name, r.since, b.name "
            "ORDER BY r.since DESC, b.name LIMIT 10"
        ),
        params={"region": "region_3", "strength": 8, "active": True},
    ),
    "relationship_dense_return": CypherWorkload(
        family="edge",
        complexity="complex",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE a.name = $name AND b.active = $active AND r.since = $since "
            "RETURN a.name, a.region, r.type, r.since, b.name, b.region "
            "ORDER BY r.since DESC, a.name, b.name LIMIT 5"
        ),
        params={"name": "user_0042", "active": True, "since": 2024},
    ),
    "relationship_mixed_boolean": CypherWorkload(
        family="edge",
        complexity="complex",
        query=(
            "MATCH (a:User)-[r:KNOWS]->(b:User) "
            "WHERE r.since >= $since AND r.strength >= $strength OR b.name = $name "
            "RETURN a.name, r.since, r.strength, b.name "
            "ORDER BY b.name LIMIT 10"
        ),
        params={"since": 2022, "strength": 2, "name": "user_0042"},
    ),
}


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the translation-overhead benchmark."""

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark SQL and Cypher frontend translation overhead separately "
            "from backend execution."
        )
    )
    parser.add_argument(
        "--repetitions",
        type=int,
        default=1000,
        help="Number of timed repetitions per benchmark stage.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=100,
        help="Number of untimed warmup iterations per benchmark stage.",
    )
    return parser.parse_args()


def _summarize(timings: list[float]) -> TimingSummary:
    """Summarize one set of duration samples."""

    return TimingSummary(
        mean=statistics.mean(timings),
        stdev=statistics.pstdev(timings),
        minimum=min(timings),
        maximum=max(timings),
    )


def _time_callable(operation, *, warmup: int, repetitions: int) -> TimingSummary:
    """Warm up and time one zero-argument operation."""

    for _ in range(warmup):
        operation()

    timings: list[float] = []
    for _ in range(repetitions):
        started = time.perf_counter()
        operation()
        timings.append(time.perf_counter() - started)
    return _summarize(timings)


def _format_seconds(seconds: float) -> str:
    """Format seconds as a millisecond string for console output."""

    return f"{seconds * 1_000:.4f} ms"


def _print_summary(label: str, summary: TimingSummary) -> None:
    """Print one timing summary in the benchmark's report format."""

    print(
        f"    {label}: mean={_format_seconds(summary.mean)} "
        f"std={_format_seconds(summary.stdev)} "
        f"min={_format_seconds(summary.minimum)} "
        f"max={_format_seconds(summary.maximum)}"
    )


def _sql_cold_translation(query: str, target: str) -> str:
    """Measure SQL translation with the translation cache cleared."""

    _translate_sql_plan_cached.cache_clear()
    return translate_sql(query, target=target)


def _sql_hot_translation(query: str, target: str) -> str:
    """Measure SQL translation with the translation cache left warm."""

    return translate_sql(query, target=target)


def _compile_cypher_bound(plan, params: dict[str, str | int | float | bool | None]):
    """Bind one Cypher plan and compile it into a backend match plan."""

    bound_plan = _bind_plan_values(plan, params)
    if not isinstance(bound_plan, (MatchNodePlan, MatchRelationshipPlan)):
        raise ValueError(
            "Translation benchmark only supports MATCH-based Cypher workloads."
        )
    return _compile_match_plan(bound_plan)


def _plan_cypher_runtime(query: str):
    """Plan Cypher through the real generated-first runtime planning path."""

    return _plan_cypher_query(query)[0]


def main() -> None:
    """Run the SQL and Cypher translation-overhead benchmarks."""

    args = _parse_args()

    print("Translation overhead benchmark")
    print(f"Warmup iterations: {args.warmup}")
    print(f"Timed repetitions: {args.repetitions}")
    print()

    print("SQL translation")
    for name, workload in SQL_WORKLOADS.items():
        print(f"  Workload: {name}")
        print(f"    Family: {workload.family}")
        print(f"    Complexity: {workload.complexity}")
        for target in ("sqlite", "duckdb"):
            try:
                translated = translate_sql(workload.query, target=target)
            except ValueError as exc:
                raise ValueError(
                    f"SQL workload '{name}' failed for target '{target}'."
                ) from exc
            hot_summary = _time_callable(
                lambda query=workload.query, target=target: _sql_hot_translation(
                    query, target
                ),
                warmup=args.warmup,
                repetitions=args.repetitions,
            )
            cold_summary = _time_callable(
                lambda query=workload.query, target=target: _sql_cold_translation(
                    query, target
                ),
                warmup=args.warmup,
                repetitions=args.repetitions,
            )
            print(f"    Target: {target}")
            _print_summary("cached translate_sql(...)", hot_summary)
            _print_summary("uncached translate_sql(...)", cold_summary)
            print(f"    Output length: {len(translated)} chars")
        print()

    print("Cypher translation")
    print(
        "  Note: parse timing measures the handwritten parser directly, while "
        "runtime planning measures the generated-first planning path that "
        "`db.query(...)` uses for admitted Cypher shapes."
    )
    print(
        "  Compilation remains route-agnostic today; the same compiled SQL is "
        "sent to SQLite or DuckDB."
    )
    for name, workload in CYPHER_WORKLOADS.items():
        runtime_plan = _plan_cypher_runtime(workload.query)
        parse_summary = _time_callable(
            lambda query=workload.query: parse_cypher(query),
            warmup=args.warmup,
            repetitions=args.repetitions,
        )
        runtime_plan_summary = _time_callable(
            lambda query=workload.query: _plan_cypher_runtime(query),
            warmup=args.warmup,
            repetitions=args.repetitions,
        )
        compile_summary = _time_callable(
            lambda runtime_plan=runtime_plan,
            params=workload.params: _compile_cypher_bound(runtime_plan, params),
            warmup=args.warmup,
            repetitions=args.repetitions,
        )
        compiled = _compile_cypher_bound(runtime_plan, workload.params)

        print(f"  Workload: {name}")
        print(f"    Family: {workload.family}")
        print(f"    Complexity: {workload.complexity}")
        _print_summary("parse_cypher(...)", parse_summary)
        _print_summary("runtime plan", runtime_plan_summary)
        _print_summary("bind+compile", compile_summary)
        print(f"    Output length: {len(compiled.sql)} chars")
        print(f"    Bound params: {len(compiled.params)}")
        print()


if __name__ == "__main__":
    main()
