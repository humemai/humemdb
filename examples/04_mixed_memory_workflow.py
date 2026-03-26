from __future__ import annotations

import tempfile
from collections import defaultdict
from pathlib import Path
from time import perf_counter
from typing import Callable, Iterable

from humemdb import HumemDB


DIMENSIONS = 128
PROJECT_COUNT = 256
TOPIC_COUNT = 512
TEAM_COUNT = 64
SERVICE_NODE_COUNT = 30_000
PROFILE_NODE_COUNT = 50_000
RUNBOOK_COUNT = 25_000
DIRECT_VECTOR_COUNT = 100_000

FOCUS_PROJECT = "atlas"
FOCUS_SPRINT = "2026-sprint-focus"
FOCUS_RELEASE = "2026.04-focus"

AXIS_INDEX = {
    "routing": 0,
    "latency": 1,
    "benchmark": 2,
    "vector": 3,
    "sync": 4,
    "profile": 5,
    "graph": 6,
    "incident": 7,
    "analytics": 8,
    "warehouse": 9,
    "release": 10,
    "security": 11,
    "auth": 12,
    "cache": 13,
    "support": 14,
    "memory": 15,
    "ops": 16,
    "platform": 17,
    "retrieval": 18,
    "service": 19,
    "api": 20,
    "edge": 21,
    "project": 22,
    "governance": 23,
}

TABLE_COLUMNS = {
    "work_items": 13,
    "memory_notes": 12,
    "incident_reports": 12,
    "sprint_snapshots": 12,
    "service_catalog": 12,
    "owner_roster": 12,
    "project_releases": 12,
    "eval_runs": 12,
    "deployment_runs": 12,
    "feature_flags": 12,
    "audit_events": 11,
    "knowledge_chunks": 11,
}

TABLE_ROW_COUNTS = {
    "work_items": 12_000,
    "memory_notes": 8_000,
    "incident_reports": 6_000,
    "sprint_snapshots": 4_096,
    "service_catalog": 30_000,
    "owner_roster": 50_000,
    "project_releases": 3_072,
    "eval_runs": 5_120,
    "deployment_runs": 7_168,
    "feature_flags": 5_120,
    "audit_events": 10_240,
    "knowledge_chunks": 10_240,
}

SPECIAL_TEAM_SLUGS = (
    "platform",
    "retrieval",
    "ops",
    "analytics",
    "security",
)
SPECIAL_TOPIC_SLUGS = (
    "routing",
    "vectors",
    "incidents",
    "release",
    "service-map",
    "security",
)
SPECIAL_SERVICES = (
    {
        "slug": "graph-api",
        "team": "platform",
        "tier": "critical",
        "runtime": "python",
        "region": "emea",
        "embedding": {
            "routing": 1.0,
            "graph": 0.92,
            "latency": 0.82,
            "api": 0.8,
        },
    },
    {
        "slug": "profile-sync",
        "team": "retrieval",
        "tier": "high",
        "runtime": "python",
        "region": "amer",
        "embedding": {
            "vector": 1.0,
            "sync": 0.96,
            "profile": 0.9,
            "service": 0.42,
        },
    },
    {
        "slug": "edge-cache",
        "team": "ops",
        "tier": "high",
        "runtime": "rust",
        "region": "emea",
        "embedding": {
            "graph": 0.86,
            "cache": 1.0,
            "latency": 0.62,
            "edge": 0.9,
            "routing": 0.22,
        },
    },
    {
        "slug": "auth-gateway",
        "team": "security",
        "tier": "high",
        "runtime": "go",
        "region": "apac",
        "embedding": {
            "auth": 1.0,
            "security": 0.82,
            "ops": 0.55,
        },
    },
    {
        "slug": "release-bus",
        "team": "platform",
        "tier": "medium",
        "runtime": "java",
        "region": "amer",
        "embedding": {
            "release": 1.0,
            "benchmark": 0.4,
            "governance": 0.5,
        },
    },
)
SPECIAL_OWNERS = (
    {
        "name": "Ada",
        "team": "platform",
        "role": "lead",
        "region": "emea",
        "level": "staff",
        "timezone": "UTC+1",
        "skill": "routing",
        "backup": "release",
        "embedding": {
            "routing": 1.0,
            "graph": 0.9,
            "latency": 0.7,
            "platform": 0.6,
        },
    },
    {
        "name": "Bea",
        "team": "retrieval",
        "role": "vector-owner",
        "region": "amer",
        "level": "senior",
        "timezone": "UTC-5",
        "skill": "vectors",
        "backup": "profiles",
        "embedding": {
            "vector": 1.0,
            "sync": 0.95,
            "profile": 0.9,
            "retrieval": 0.6,
            "incident": 0.2,
        },
    },
    {
        "name": "Cory",
        "team": "analytics",
        "role": "observer",
        "region": "emea",
        "level": "senior",
        "timezone": "UTC+0",
        "skill": "analytics",
        "backup": "warehouse",
        "embedding": {
            "analytics": 1.0,
            "warehouse": 0.82,
            "release": 0.18,
        },
    },
    {
        "name": "Dev",
        "team": "ops",
        "role": "incident-commander",
        "region": "amer",
        "level": "staff",
        "timezone": "UTC-8",
        "skill": "incidents",
        "backup": "auth",
        "embedding": {
            "incident": 0.92,
            "auth": 0.82,
            "ops": 0.8,
            "support": 0.4,
            "sync": 0.2,
        },
    },
    {
        "name": "Eli",
        "team": "platform",
        "role": "memory-architect",
        "region": "emea",
        "level": "principal",
        "timezone": "UTC+1",
        "skill": "memory",
        "backup": "service-map",
        "embedding": {
            "memory": 0.94,
            "graph": 0.72,
            "project": 0.8,
            "service": 0.25,
        },
    },
    {
        "name": "Faye",
        "team": "security",
        "role": "security-review",
        "region": "apac",
        "level": "staff",
        "timezone": "UTC+9",
        "skill": "security",
        "backup": "auth",
        "embedding": {
            "security": 1.0,
            "auth": 0.8,
            "governance": 0.62,
            "profile": 0.05,
        },
    },
)


def _make_timer() -> Callable[[str], None]:
    start = perf_counter()
    last = start

    def report(step: str) -> None:
        nonlocal last
        now = perf_counter()
        print(
            f"[timing] {step}: +{now - last:.3f}s step, {now - start:.3f}s total"
        )
        last = now

    return report


def _embedding(**weights: float) -> list[float]:
    values = [0.0] * DIMENSIONS
    for axis, weight in weights.items():
        values[AXIS_INDEX[axis]] = weight
    return values


def _candidate_ids(query_result, top_k: int) -> tuple[int, ...]:
    return tuple(int(row[2]) for row in query_result.rows[:top_k])


def _ordered_lookup_rows(
    db,
    *,
    table: str,
    ids: tuple[int, ...],
    columns: tuple[str, ...],
) -> tuple[tuple[object, ...], ...]:
    params = {f"id_{index}": item_id for index, item_id in enumerate(ids)}
    placeholders = ", ".join(f"$id_{index}" for index in range(len(ids)))
    column_list = ", ".join(("id", *columns))
    rows = db.query(
        f"SELECT {column_list} FROM {table} WHERE id IN ({placeholders})",
        params=params,
    )
    rows_by_id = {int(row[0]): row[1:] for row in rows.rows}
    return tuple(rows_by_id[item_id] for item_id in ids)


def _executemany_in_batches(
    db,
    sql: str,
    rows: Iterable[dict[str, object]],
    *,
    batch_size: int = 500,
) -> int:
    batch: list[dict[str, object]] = []
    count = 0
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            db.executemany(sql, batch)
            count += len(batch)
            batch = []
    if batch:
        db.executemany(sql, batch)
        count += len(batch)
    return count


def _project_slug(index: int) -> str:
    if index == 0:
        return FOCUS_PROJECT
    return f"project-{index:03d}"


def _team_slug(index: int) -> str:
    if index < len(SPECIAL_TEAM_SLUGS):
        return SPECIAL_TEAM_SLUGS[index]
    return f"team-{index:02d}"


def _topic_slug(index: int) -> str:
    if index < len(SPECIAL_TOPIC_SLUGS):
        return SPECIAL_TOPIC_SLUGS[index]
    return f"topic-{index:03d}"


def _profile_name(index: int) -> str:
    if index < len(SPECIAL_OWNERS):
        return SPECIAL_OWNERS[index]["name"]
    return f"profile-{index:05d}"


def _service_slug(index: int) -> str:
    if index < len(SPECIAL_SERVICES):
        return SPECIAL_SERVICES[index]["slug"]
    return f"svc-{index:05d}"


def _service_project_slug(index: int) -> str:
    if index < len(SPECIAL_SERVICES):
        return FOCUS_PROJECT
    return _project_slug(1 + ((index - len(SPECIAL_SERVICES)) % (PROJECT_COUNT - 1)))


def _service_team_slug(index: int) -> str:
    if index < len(SPECIAL_SERVICES):
        return SPECIAL_SERVICES[index]["team"]
    return _team_slug(
        len(SPECIAL_TEAM_SLUGS)
        + ((index - len(SPECIAL_SERVICES)) % (TEAM_COUNT - len(SPECIAL_TEAM_SLUGS)))
    )


def _service_topic_slug(index: int) -> str:
    if index < len(SPECIAL_SERVICES):
        return SPECIAL_TOPIC_SLUGS[index % len(SPECIAL_TOPIC_SLUGS)]
    return _topic_slug(
        len(SPECIAL_TOPIC_SLUGS)
        + ((index - len(SPECIAL_SERVICES)) % (TOPIC_COUNT - len(SPECIAL_TOPIC_SLUGS)))
    )


def _profile_project_slug(index: int) -> str:
    if index < len(SPECIAL_OWNERS):
        return FOCUS_PROJECT
    return _project_slug(1 + ((index - len(SPECIAL_OWNERS)) % (PROJECT_COUNT - 1)))


def _profile_team_slug(index: int) -> str:
    if index < len(SPECIAL_OWNERS):
        return SPECIAL_OWNERS[index]["team"]
    return _team_slug(
        len(SPECIAL_TEAM_SLUGS)
        + ((index - len(SPECIAL_OWNERS)) % (TEAM_COUNT - len(SPECIAL_TEAM_SLUGS)))
    )


def create_relational_tables(db) -> None:
    with db.transaction():
        db.query(
            (
                "CREATE TABLE work_items ("
                "id INTEGER PRIMARY KEY, "
                "project_slug TEXT NOT NULL, owner_name TEXT NOT NULL, "
                "service_slug TEXT NOT NULL, sprint_name TEXT NOT NULL, "
                "release_name TEXT NOT NULL, status TEXT NOT NULL, "
                "priority INTEGER NOT NULL, severity_band TEXT NOT NULL, "
                "category TEXT NOT NULL, opened_day INTEGER NOT NULL, "
                "summary TEXT NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE memory_notes ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "topic_slug TEXT NOT NULL, kind TEXT NOT NULL, "
                "owner_name TEXT NOT NULL, language TEXT NOT NULL, "
                "status TEXT NOT NULL, source_system TEXT NOT NULL, "
                "revision INTEGER NOT NULL, created_day INTEGER NOT NULL, "
                "summary TEXT NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE incident_reports ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "service_slug TEXT NOT NULL, severity INTEGER NOT NULL, "
                "status TEXT NOT NULL, region TEXT NOT NULL, "
                "commander_name TEXT NOT NULL, started_day INTEGER NOT NULL, "
                "resolved_day INTEGER NOT NULL, channel TEXT NOT NULL, "
                "summary TEXT NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE sprint_snapshots ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "sprint_name TEXT NOT NULL, focus_area TEXT NOT NULL, "
                "risk_level TEXT NOT NULL, open_items INTEGER NOT NULL, "
                "blocked_items INTEGER NOT NULL, closed_items INTEGER NOT NULL, "
                "velocity INTEGER NOT NULL, burndown_ratio REAL NOT NULL, "
                "owner_team TEXT NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE service_catalog ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "service_slug TEXT NOT NULL, tier TEXT NOT NULL, "
                "runtime TEXT NOT NULL, region TEXT NOT NULL, "
                "team_slug TEXT NOT NULL, sla_minutes INTEGER NOT NULL, "
                "deploy_ring TEXT NOT NULL, active BOOLEAN NOT NULL, "
                "failure_budget REAL NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE owner_roster ("
                "id INTEGER PRIMARY KEY, owner_name TEXT NOT NULL, "
                "team_slug TEXT NOT NULL, project_slug TEXT NOT NULL, "
                "region TEXT NOT NULL, level TEXT NOT NULL, "
                "oncall BOOLEAN NOT NULL, timezone TEXT NOT NULL, "
                "primary_skill TEXT NOT NULL, backup_skill TEXT NOT NULL, "
                "tenure_months INTEGER NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE project_releases ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "release_name TEXT NOT NULL, train TEXT NOT NULL, "
                "status TEXT NOT NULL, risk_level TEXT NOT NULL, "
                "approver_name TEXT NOT NULL, change_count INTEGER NOT NULL, "
                "incident_count INTEGER NOT NULL, started_day INTEGER NOT NULL, "
                "summary TEXT NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE eval_runs ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "eval_name TEXT NOT NULL, dataset_slug TEXT NOT NULL, "
                "status TEXT NOT NULL, score REAL NOT NULL, "
                "regression_delta REAL NOT NULL, owner_name TEXT NOT NULL, "
                "created_day INTEGER NOT NULL, surface TEXT NOT NULL, "
                "summary TEXT NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE deployment_runs ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "service_slug TEXT NOT NULL, environment TEXT NOT NULL, "
                "status TEXT NOT NULL, deployer_name TEXT NOT NULL, "
                "started_day INTEGER NOT NULL, duration_s INTEGER NOT NULL, "
                "commit_sha TEXT NOT NULL, ring TEXT NOT NULL, "
                "summary TEXT NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE feature_flags ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "service_slug TEXT NOT NULL, flag_name TEXT NOT NULL, "
                "flag_type TEXT NOT NULL, status TEXT NOT NULL, "
                "owner_name TEXT NOT NULL, rollout_percent INTEGER NOT NULL, "
                "audience TEXT NOT NULL, created_day INTEGER NOT NULL, "
                "updated_day INTEGER NOT NULL, embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE audit_events ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "actor_name TEXT NOT NULL, entity_kind TEXT NOT NULL, "
                "entity_slug TEXT NOT NULL, action TEXT NOT NULL, "
                "channel TEXT NOT NULL, created_day INTEGER NOT NULL, "
                "severity_band TEXT NOT NULL, summary TEXT NOT NULL, "
                "embedding BLOB)"
            )
        )
        db.query(
            (
                "CREATE TABLE knowledge_chunks ("
                "id INTEGER PRIMARY KEY, project_slug TEXT NOT NULL, "
                "collection TEXT NOT NULL, language TEXT NOT NULL, "
                "status TEXT NOT NULL, shard_id INTEGER NOT NULL, "
                "token_count INTEGER NOT NULL, freshness_band TEXT NOT NULL, "
                "source_uri TEXT NOT NULL, title TEXT NOT NULL, embedding BLOB)"
            )
        )


def create_relational_indexes(db) -> None:
    # These indexes cover the example's repeated relational joins and filters.
    # They are app-side SQLite hygiene, not the main driver of vector-search speed.
    with db.transaction():
        for statement in (
            (
                "CREATE INDEX IF NOT EXISTS "
                "idx_work_items_project_sprint_release_status "
                "ON work_items("
                "project_slug, sprint_name, release_name, status, priority, "
                "owner_name, service_slug"
                ")"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_service_catalog_project_service "
                "ON service_catalog(project_slug, service_slug)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_owner_roster_project_owner "
                "ON owner_roster(project_slug, owner_name)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_sprint_snapshots_project_sprint "
                "ON sprint_snapshots(project_slug, sprint_name)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_project_releases_project_release "
                "ON project_releases(project_slug, release_name)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_memory_notes_project_status "
                "ON memory_notes(project_slug, status)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_incident_reports_project "
                "ON incident_reports(project_slug, service_slug, status)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_eval_runs_project "
                "ON eval_runs(project_slug, status, created_day)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_deployment_runs_project_service "
                "ON deployment_runs(project_slug, service_slug, environment, status)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_feature_flags_project_service "
                "ON feature_flags(project_slug, service_slug, status)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_audit_events_project_day "
                "ON audit_events(project_slug, created_day, entity_kind)"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_knowledge_chunks_project_collection "
                "ON knowledge_chunks(project_slug, collection, status)"
            ),
        ):
            db.query(statement)


def _iter_work_items() -> Iterable[dict[str, object]]:
    special_rows = (
        {
            "id": 1,
            "project_slug": FOCUS_PROJECT,
            "owner_name": "Ada",
            "service_slug": "edge-cache",
            "sprint_name": FOCUS_SPRINT,
            "release_name": FOCUS_RELEASE,
            "status": "blocked",
            "priority": 1,
            "severity_band": "high",
            "category": "benchmark",
            "opened_day": 2,
            "summary": "benchmark evidence handoff for traversal fan-out",
            "embedding": _embedding(
                benchmark=1.0,
                routing=0.78,
                cache=0.9,
                edge=0.82,
            ),
        },
        {
            "id": 2,
            "project_slug": FOCUS_PROJECT,
            "owner_name": "Ada",
            "service_slug": "graph-api",
            "sprint_name": FOCUS_SPRINT,
            "release_name": FOCUS_RELEASE,
            "status": "open",
            "priority": 1,
            "severity_band": "critical",
            "category": "routing",
            "opened_day": 1,
            "summary": "graph routing latency spike on graph-api",
            "embedding": _embedding(
                routing=1.0,
                latency=0.95,
                graph=0.86,
                api=0.72,
            ),
        },
        {
            "id": 3,
            "project_slug": FOCUS_PROJECT,
            "owner_name": "Bea",
            "service_slug": "profile-sync",
            "sprint_name": FOCUS_SPRINT,
            "release_name": FOCUS_RELEASE,
            "status": "open",
            "priority": 2,
            "severity_band": "medium",
            "category": "vector-sync",
            "opened_day": 3,
            "summary": "vector sync regression after profile updates",
            "embedding": _embedding(
                vector=1.0,
                sync=0.96,
                profile=0.9,
                retrieval=0.6,
            ),
        },
        {
            "id": 4,
            "project_slug": FOCUS_PROJECT,
            "owner_name": "Dev",
            "service_slug": "auth-gateway",
            "sprint_name": FOCUS_SPRINT,
            "release_name": FOCUS_RELEASE,
            "status": "open",
            "priority": 2,
            "severity_band": "high",
            "category": "incident-drill",
            "opened_day": 4,
            "summary": "incident drill for auth-gateway failover",
            "embedding": _embedding(
                incident=0.95,
                auth=0.9,
                ops=0.8,
                support=0.45,
            ),
        },
        {
            "id": 5,
            "project_slug": FOCUS_PROJECT,
            "owner_name": "Eli",
            "service_slug": "graph-api",
            "sprint_name": FOCUS_SPRINT,
            "release_name": FOCUS_RELEASE,
            "status": "open",
            "priority": 3,
            "severity_band": "medium",
            "category": "memory-layer",
            "opened_day": 5,
            "summary": "memory stitching across graph and table snapshots",
            "embedding": _embedding(
                memory=0.95,
                graph=0.8,
                project=0.85,
                service=0.25,
            ),
        },
    )
    for row in special_rows:
        yield row
    for item_id in range(6, TABLE_ROW_COUNTS["work_items"] + 1):
        offset = item_id - 6
        yield {
            "id": item_id,
            "project_slug": _project_slug(1 + (offset % (PROJECT_COUNT - 1))),
            "owner_name": _profile_name(
                len(SPECIAL_OWNERS)
                + (offset % (PROFILE_NODE_COUNT - len(SPECIAL_OWNERS)))
            ),
            "service_slug": _service_slug(
                len(SPECIAL_SERVICES)
                + (offset % (SERVICE_NODE_COUNT - len(SPECIAL_SERVICES)))
            ),
            "sprint_name": f"2026-sprint-{1 + (offset % 16):02d}",
            "release_name": f"2026.{1 + (offset % 12):02d}",
            "status": ("open", "blocked", "closed")[offset % 3],
            "priority": 1 + (offset % 4),
            "severity_band": ("low", "medium", "high")[offset % 3],
            "category": (
                "routing",
                "vector-sync",
                "incidents",
                "release",
                "analytics",
            )[offset % 5],
            "opened_day": 1 + (offset % 30),
            "summary": f"bulk work item {offset:05d}",
            "embedding": _embedding(
                routing=0.1 + ((offset % 7) / 20.0),
                vector=0.08 + ((offset % 5) / 22.0),
                incident=0.05 + ((offset % 6) / 25.0),
                graph=0.04 + ((offset % 9) / 30.0),
            ),
        }


def _iter_memory_notes() -> Iterable[dict[str, object]]:
    special_rows = (
        {
            "id": 101,
            "project_slug": FOCUS_PROJECT,
            "topic_slug": "routing",
            "kind": "decision",
            "owner_name": "Ada",
            "language": "python",
            "status": "published",
            "source_system": "notion",
            "revision": 4,
            "created_day": 2,
            "summary": "keep graph routing on SQLite for selective traversals",
            "embedding": _embedding(routing=1.0, graph=0.86, latency=0.55),
        },
        {
            "id": 102,
            "project_slug": FOCUS_PROJECT,
            "topic_slug": "vectors",
            "kind": "note",
            "owner_name": "Bea",
            "language": "python",
            "status": "published",
            "source_system": "docs",
            "revision": 2,
            "created_day": 3,
            "summary": "graph-owned vectors need cache-safe profile updates",
            "embedding": _embedding(vector=1.0, sync=0.72, profile=0.55),
        },
        {
            "id": 103,
            "project_slug": FOCUS_PROJECT,
            "topic_slug": "incidents",
            "kind": "runbook",
            "owner_name": "Dev",
            "language": "markdown",
            "status": "published",
            "source_system": "pagerduty",
            "revision": 7,
            "created_day": 4,
            "summary": (
                "incident escalations should include ownership and service "
                "context"
            ),
            "embedding": _embedding(incident=1.0, service=0.6, support=0.55),
        },
        {
            "id": 104,
            "project_slug": FOCUS_PROJECT,
            "topic_slug": "release",
            "kind": "decision",
            "owner_name": "Faye",
            "language": "markdown",
            "status": "published",
            "source_system": "release-train",
            "revision": 3,
            "created_day": 5,
            "summary": "release checks must preserve benchmark evidence",
            "embedding": _embedding(release=1.0, benchmark=0.65, governance=0.4),
        },
        {
            "id": 105,
            "project_slug": FOCUS_PROJECT,
            "topic_slug": "service-map",
            "kind": "note",
            "owner_name": "Eli",
            "language": "python",
            "status": "published",
            "source_system": "catalog",
            "revision": 5,
            "created_day": 6,
            "summary": "service map links graph-api and edge-cache ownership",
            "embedding": _embedding(service=1.0, graph=0.7, routing=0.45),
        },
    )
    for row in special_rows:
        yield row
    limit = TABLE_ROW_COUNTS["memory_notes"] - len(special_rows)
    for offset in range(limit):
        yield {
            "id": 106 + offset,
            "project_slug": _project_slug(1 + (offset % (PROJECT_COUNT - 1))),
            "topic_slug": _topic_slug(
                len(SPECIAL_TOPIC_SLUGS)
                + (offset % (TOPIC_COUNT - len(SPECIAL_TOPIC_SLUGS)))
            ),
            "kind": ("decision", "note", "runbook", "report")[offset % 4],
            "owner_name": _profile_name(
                len(SPECIAL_OWNERS)
                + (offset % (PROFILE_NODE_COUNT - len(SPECIAL_OWNERS)))
            ),
            "language": ("python", "sql", "markdown", "yaml")[offset % 4],
            "status": ("published", "draft", "archived")[offset % 3],
            "source_system": ("docs", "wiki", "runbook", "catalog")[offset % 4],
            "revision": 1 + (offset % 9),
            "created_day": 1 + (offset % 30),
            "summary": f"bulk memory note {offset:05d}",
            "embedding": _embedding(
                routing=0.03 + ((offset % 11) / 60.0),
                vector=0.04 + ((offset % 7) / 50.0),
                release=0.02 + ((offset % 5) / 60.0),
                service=0.05 + ((offset % 9) / 55.0),
            ),
        }


def _iter_incident_reports() -> Iterable[dict[str, object]]:
    special_rows = (
        {
            "id": 201,
            "project_slug": FOCUS_PROJECT,
            "service_slug": "graph-api",
            "severity": 1,
            "status": "open",
            "region": "emea",
            "commander_name": "Ada",
            "started_day": 2,
            "resolved_day": 5,
            "channel": "bridge-a",
            "summary": "p99 latency spike on graph-api expansion path",
            "embedding": _embedding(
                routing=1.0,
                latency=1.0,
                graph=0.8,
                api=0.7,
                incident=0.9,
            ),
        },
        {
            "id": 202,
            "project_slug": FOCUS_PROJECT,
            "service_slug": "profile-sync",
            "severity": 2,
            "status": "mitigated",
            "region": "amer",
            "commander_name": "Bea",
            "started_day": 3,
            "resolved_day": 4,
            "channel": "bridge-b",
            "summary": "vector sync backlog after profile writes",
            "embedding": _embedding(
                vector=1.0,
                sync=1.0,
                profile=0.8,
                incident=0.9,
            ),
        },
        {
            "id": 203,
            "project_slug": FOCUS_PROJECT,
            "service_slug": "edge-cache",
            "severity": 2,
            "status": "open",
            "region": "emea",
            "commander_name": "Dev",
            "started_day": 4,
            "resolved_day": 6,
            "channel": "bridge-c",
            "summary": "edge cache invalidation lag on fan-out joins",
            "embedding": _embedding(
                cache=1.0,
                edge=0.9,
                graph=0.6,
                incident=0.8,
            ),
        },
        {
            "id": 204,
            "project_slug": FOCUS_PROJECT,
            "service_slug": "release-bus",
            "severity": 3,
            "status": "closed",
            "region": "amer",
            "commander_name": "Faye",
            "started_day": 5,
            "resolved_day": 8,
            "channel": "bridge-d",
            "summary": "release train drift after benchmark gate lag",
            "embedding": _embedding(release=0.95, benchmark=0.65, incident=0.6),
        },
        {
            "id": 205,
            "project_slug": FOCUS_PROJECT,
            "service_slug": "auth-gateway",
            "severity": 2,
            "status": "monitoring",
            "region": "apac",
            "commander_name": "Dev",
            "started_day": 6,
            "resolved_day": 9,
            "channel": "bridge-e",
            "summary": "auth-gateway token churn after support rotation",
            "embedding": _embedding(auth=1.0, security=0.8, incident=0.7),
        },
    )
    for row in special_rows:
        yield row
    limit = TABLE_ROW_COUNTS["incident_reports"] - len(special_rows)
    for offset in range(limit):
        service_index = len(SPECIAL_SERVICES) + (
            offset % (SERVICE_NODE_COUNT - len(SPECIAL_SERVICES))
        )
        yield {
            "id": 206 + offset,
            "project_slug": _service_project_slug(service_index),
            "service_slug": _service_slug(service_index),
            "severity": 1 + (offset % 4),
            "status": ("open", "monitoring", "mitigated", "closed")[offset % 4],
            "region": ("emea", "amer", "apac")[offset % 3],
            "commander_name": _profile_name(
                len(SPECIAL_OWNERS)
                + (offset % (PROFILE_NODE_COUNT - len(SPECIAL_OWNERS)))
            ),
            "started_day": 1 + (offset % 28),
            "resolved_day": 2 + (offset % 28),
            "channel": f"bridge-{offset % 11}",
            "summary": f"bulk incident {offset:05d}",
            "embedding": _embedding(
                incident=0.4 + ((offset % 9) / 20.0),
                auth=0.04 + ((offset % 5) / 30.0),
                vector=0.02 + ((offset % 6) / 35.0),
                graph=0.03 + ((offset % 7) / 40.0),
            ),
        }


def _iter_sprint_snapshots() -> Iterable[dict[str, object]]:
    for project_index in range(PROJECT_COUNT):
        project_slug = _project_slug(project_index)
        for sprint_offset in range(16):
            snapshot_id = 301 + (project_index * 16) + sprint_offset
            sprint_name = (
                FOCUS_SPRINT
                if project_index == 0 and sprint_offset == 0
                else f"2026-sprint-{sprint_offset + 1:02d}"
            )
            yield {
                "id": snapshot_id,
                "project_slug": project_slug,
                "sprint_name": sprint_name,
                "focus_area": (
                    "routing-latency"
                    if project_index == 0 and sprint_offset == 0
                    else ("routing", "vectors", "release", "incidents")[
                        sprint_offset % 4
                    ]
                ),
                "risk_level": ("low", "medium", "high")[sprint_offset % 3],
                "open_items": 4 + ((project_index + sprint_offset) % 12),
                "blocked_items": 1 + ((project_index + sprint_offset) % 4),
                "closed_items": 8 + ((project_index * 3 + sprint_offset) % 14),
                "velocity": 20 + ((project_index + sprint_offset) % 35),
                "burndown_ratio": 0.45 + ((sprint_offset % 7) / 10.0),
                "owner_team": _team_slug(project_index % TEAM_COUNT),
                "embedding": _embedding(
                    routing=0.06 + ((project_index % 6) / 20.0),
                    vector=0.06 + ((sprint_offset % 5) / 20.0),
                    release=0.05 + ((sprint_offset % 4) / 20.0),
                ),
            }


def _iter_service_catalog() -> Iterable[dict[str, object]]:
    for service_index in range(SERVICE_NODE_COUNT):
        if service_index < len(SPECIAL_SERVICES):
            service = SPECIAL_SERVICES[service_index]
            yield {
                "id": 401 + service_index,
                "project_slug": FOCUS_PROJECT,
                "service_slug": service["slug"],
                "tier": service["tier"],
                "runtime": service["runtime"],
                "region": service["region"],
                "team_slug": service["team"],
                "sla_minutes": 15 + (service_index * 5),
                "deploy_ring": ("canary", "ring-1", "ring-2")[service_index % 3],
                "active": True,
                "failure_budget": 99.0 - (service_index * 0.2),
                "embedding": _embedding(**service["embedding"]),
            }
            continue
        offset = service_index - len(SPECIAL_SERVICES)
        yield {
            "id": 401 + service_index,
            "project_slug": _service_project_slug(service_index),
            "service_slug": _service_slug(service_index),
            "tier": ("critical", "high", "medium", "low")[offset % 4],
            "runtime": ("python", "rust", "go", "java")[offset % 4],
            "region": ("emea", "amer", "apac")[offset % 3],
            "team_slug": _service_team_slug(service_index),
            "sla_minutes": 10 + (offset % 45),
            "deploy_ring": ("canary", "ring-1", "ring-2", "ring-3")[offset % 4],
            "active": offset % 9 != 0,
            "failure_budget": 98.0 - ((offset % 20) / 10.0),
            "embedding": _embedding(
                service=0.4 + ((offset % 8) / 15.0),
                graph=0.04 + ((offset % 5) / 35.0),
                vector=0.03 + ((offset % 7) / 40.0),
                auth=0.02 + ((offset % 6) / 45.0),
            ),
        }


def _iter_owner_roster() -> Iterable[dict[str, object]]:
    for owner_index in range(PROFILE_NODE_COUNT):
        if owner_index < len(SPECIAL_OWNERS):
            owner = SPECIAL_OWNERS[owner_index]
            yield {
                "id": 701 + owner_index,
                "owner_name": owner["name"],
                "team_slug": owner["team"],
                "project_slug": FOCUS_PROJECT,
                "region": owner["region"],
                "level": owner["level"],
                "oncall": owner_index in {0, 1, 3, 5},
                "timezone": owner["timezone"],
                "primary_skill": owner["skill"],
                "backup_skill": owner["backup"],
                "tenure_months": 24 + (owner_index * 6),
                "embedding": _embedding(**owner["embedding"]),
            }
            continue
        offset = owner_index - len(SPECIAL_OWNERS)
        yield {
            "id": 701 + owner_index,
            "owner_name": _profile_name(owner_index),
            "team_slug": _profile_team_slug(owner_index),
            "project_slug": _profile_project_slug(owner_index),
            "region": ("emea", "amer", "apac")[offset % 3],
            "level": ("mid", "senior", "staff")[offset % 3],
            "oncall": offset % 5 == 0,
            "timezone": ("UTC+1", "UTC-5", "UTC+9")[offset % 3],
            "primary_skill": (
                "routing",
                "vectors",
                "security",
                "analytics",
                "incidents",
            )[offset % 5],
            "backup_skill": ("release", "service-map", "auth", "cache")[offset % 4],
            "tenure_months": 6 + (offset % 84),
            "embedding": _embedding(
                profile=0.35 + ((offset % 9) / 14.0),
                service=0.03 + ((offset % 5) / 50.0),
                graph=0.03 + ((offset % 7) / 45.0),
                incident=0.02 + ((offset % 6) / 55.0),
            ),
        }


def _iter_project_releases() -> Iterable[dict[str, object]]:
    for project_index in range(PROJECT_COUNT):
        project_slug = _project_slug(project_index)
        for release_offset in range(12):
            release_id = 1_001 + (project_index * 12) + release_offset
            release_name = (
                FOCUS_RELEASE
                if project_index == 0 and release_offset == 0
                else f"2026.{release_offset + 1:02d}"
            )
            yield {
                "id": release_id,
                "project_slug": project_slug,
                "release_name": release_name,
                "train": f"train-{release_offset % 3}",
                "status": ("planned", "active", "complete")[release_offset % 3],
                "risk_level": ("low", "medium", "high")[release_offset % 3],
                "approver_name": _profile_name(release_offset % PROFILE_NODE_COUNT),
                "change_count": 10 + ((project_index + release_offset) % 90),
                "incident_count": (project_index + release_offset) % 5,
                "started_day": 1 + ((project_index + release_offset) % 28),
                "summary": f"release summary {project_slug} {release_name}",
                "embedding": _embedding(
                    release=0.4 + ((release_offset % 5) / 10.0),
                    benchmark=0.02 + ((project_index % 7) / 40.0),
                    governance=0.04 + ((release_offset % 4) / 20.0),
                ),
            }


def _iter_eval_runs() -> Iterable[dict[str, object]]:
    special_rows = (
        {
            "id": 5_001,
            "project_slug": FOCUS_PROJECT,
            "eval_name": "routing-latency-eval",
            "dataset_slug": "atlas-routing-set",
            "status": "passed",
            "score": 0.98,
            "regression_delta": -0.01,
            "owner_name": "Ada",
            "created_day": 3,
            "surface": "graph",
            "summary": "routing eval confirms explicit ORDER BY contract",
            "embedding": _embedding(routing=1.0, benchmark=0.75, graph=0.7),
        },
        {
            "id": 5_002,
            "project_slug": FOCUS_PROJECT,
            "eval_name": "vector-sync-eval",
            "dataset_slug": "atlas-profile-set",
            "status": "warning",
            "score": 0.91,
            "regression_delta": 0.04,
            "owner_name": "Bea",
            "created_day": 4,
            "surface": "vector",
            "summary": "vector sync eval detects profile-write drift",
            "embedding": _embedding(vector=1.0, sync=0.92, profile=0.78),
        },
        {
            "id": 5_003,
            "project_slug": FOCUS_PROJECT,
            "eval_name": "incident-response-eval",
            "dataset_slug": "atlas-incident-set",
            "status": "passed",
            "score": 0.95,
            "regression_delta": -0.02,
            "owner_name": "Dev",
            "created_day": 5,
            "surface": "ops",
            "summary": "incident eval keeps auth-gateway playbook in tolerance",
            "embedding": _embedding(incident=1.0, auth=0.82, support=0.44),
        },
    )
    for row in special_rows:
        yield row
    remaining = TABLE_ROW_COUNTS["eval_runs"] - len(special_rows)
    for offset in range(remaining):
        yield {
            "id": 5_004 + offset,
            "project_slug": _project_slug(offset % PROJECT_COUNT),
            "eval_name": f"eval-{offset:05d}",
            "dataset_slug": f"dataset-{offset % 128:03d}",
            "status": ("passed", "warning", "failed")[offset % 3],
            "score": 0.70 + ((offset % 25) / 100.0),
            "regression_delta": -0.05 + ((offset % 11) / 100.0),
            "owner_name": _profile_name(offset % PROFILE_NODE_COUNT),
            "created_day": 1 + (offset % 28),
            "surface": ("sql", "graph", "vector", "ops")[offset % 4],
            "summary": f"bulk eval run {offset:05d}",
            "embedding": _embedding(
                benchmark=0.1 + ((offset % 10) / 15.0),
                vector=0.03 + ((offset % 5) / 40.0),
                routing=0.02 + ((offset % 7) / 45.0),
            ),
        }


def _iter_deployment_runs() -> Iterable[dict[str, object]]:
    for offset in range(TABLE_ROW_COUNTS["deployment_runs"]):
        service_index = offset % SERVICE_NODE_COUNT
        yield {
            "id": 9_001 + offset,
            "project_slug": _service_project_slug(service_index),
            "service_slug": _service_slug(service_index),
            "environment": ("dev", "stage", "prod")[offset % 3],
            "status": ("queued", "running", "succeeded", "failed")[offset % 4],
            "deployer_name": _profile_name(offset % PROFILE_NODE_COUNT),
            "started_day": 1 + (offset % 28),
            "duration_s": 60 + ((offset * 17) % 1_800),
            "commit_sha": f"c{offset:07x}",
            "ring": ("canary", "ring-1", "ring-2", "ring-3")[offset % 4],
            "summary": f"deployment run {offset:05d}",
            "embedding": _embedding(
                release=0.08 + ((offset % 7) / 25.0),
                service=0.1 + ((offset % 6) / 25.0),
                auth=0.02 + ((offset % 5) / 50.0),
            ),
        }


def _iter_feature_flags() -> Iterable[dict[str, object]]:
    for offset in range(TABLE_ROW_COUNTS["feature_flags"]):
        service_index = offset % SERVICE_NODE_COUNT
        yield {
            "id": 20_001 + offset,
            "project_slug": _service_project_slug(service_index),
            "service_slug": _service_slug(service_index),
            "flag_name": f"flag-{offset:05d}",
            "flag_type": ("boolean", "gradual", "ops")[offset % 3],
            "status": ("off", "partial", "on")[offset % 3],
            "owner_name": _profile_name(offset % PROFILE_NODE_COUNT),
            "rollout_percent": (offset * 7) % 101,
            "audience": ("internal", "beta", "global")[offset % 3],
            "created_day": 1 + (offset % 28),
            "updated_day": 2 + (offset % 28),
            "embedding": _embedding(
                service=0.06 + ((offset % 8) / 35.0),
                release=0.04 + ((offset % 6) / 40.0),
                auth=0.02 + ((offset % 4) / 50.0),
            ),
        }


def _iter_audit_events() -> Iterable[dict[str, object]]:
    for offset in range(TABLE_ROW_COUNTS["audit_events"]):
        yield {
            "id": 30_001 + offset,
            "project_slug": _project_slug(offset % PROJECT_COUNT),
            "actor_name": _profile_name(offset % PROFILE_NODE_COUNT),
            "entity_kind": ("service", "work-item", "eval", "flag")[offset % 4],
            "entity_slug": f"entity-{offset % 2_048:04d}",
            "action": ("create", "update", "approve", "delete")[offset % 4],
            "channel": ("api", "ui", "job", "sync")[offset % 4],
            "created_day": 1 + (offset % 28),
            "severity_band": ("low", "medium", "high")[offset % 3],
            "summary": f"audit event {offset:05d}",
            "embedding": _embedding(
                governance=0.07 + ((offset % 6) / 30.0),
                service=0.03 + ((offset % 5) / 45.0),
                incident=0.02 + ((offset % 4) / 50.0),
            ),
        }


def _iter_knowledge_chunks() -> Iterable[dict[str, object]]:
    for offset in range(TABLE_ROW_COUNTS["knowledge_chunks"]):
        yield {
            "id": 40_001 + offset,
            "project_slug": _project_slug(offset % PROJECT_COUNT),
            "collection": ("graph-guides", "vector-guides", "ops-guides")[offset % 3],
            "language": ("python", "sql", "markdown", "yaml")[offset % 4],
            "status": ("published", "draft", "archived")[offset % 3],
            "shard_id": offset % 128,
            "token_count": 250 + ((offset * 17) % 2_000),
            "freshness_band": ("fresh", "warm", "cold")[offset % 3],
            "source_uri": f"https://example.invalid/doc/{offset:05d}",
            "title": f"knowledge chunk {offset:05d}",
            "embedding": _embedding(
                graph=0.05 + ((offset % 7) / 30.0),
                vector=0.05 + ((offset % 5) / 30.0),
                memory=0.03 + ((offset % 4) / 35.0),
            ),
        }


def populate_relational_rows(db) -> dict[str, int]:
    insert_sql = {
        "work_items": (
            "INSERT INTO work_items (id, project_slug, owner_name, service_slug, "
            "sprint_name, release_name, status, priority, severity_band, category, "
            "opened_day, summary, embedding) VALUES ("
            "$id, $project_slug, $owner_name, $service_slug, $sprint_name, "
            "$release_name, $status, $priority, $severity_band, $category, "
            "$opened_day, $summary, $embedding)"
        ),
        "memory_notes": (
            "INSERT INTO memory_notes (id, project_slug, topic_slug, kind, owner_name, "
            "language, status, source_system, revision, created_day, "
            "summary, embedding) "
            "VALUES ($id, $project_slug, $topic_slug, $kind, $owner_name, $language, "
            "$status, $source_system, $revision, $created_day, $summary, $embedding)"
        ),
        "incident_reports": (
            "INSERT INTO incident_reports (id, project_slug, service_slug, severity, "
            "status, region, commander_name, started_day, resolved_day, channel, "
            "summary, embedding) VALUES ($id, $project_slug, $service_slug, $severity, "
            "$status, $region, $commander_name, $started_day, $resolved_day, $channel, "
            "$summary, $embedding)"
        ),
        "sprint_snapshots": (
            "INSERT INTO sprint_snapshots (id, project_slug, sprint_name, focus_area, "
            "risk_level, open_items, blocked_items, closed_items, velocity, "
            "burndown_ratio, owner_team, embedding) VALUES ($id, $project_slug, "
            "$sprint_name, $focus_area, $risk_level, $open_items, $blocked_items, "
            "$closed_items, $velocity, $burndown_ratio, $owner_team, $embedding)"
        ),
        "service_catalog": (
            "INSERT INTO service_catalog ("
            "id, project_slug, service_slug, tier, runtime, "
            "region, team_slug, sla_minutes, deploy_ring, active, failure_budget, "
            "embedding) VALUES ($id, $project_slug, $service_slug, $tier, $runtime, "
            "$region, $team_slug, $sla_minutes, $deploy_ring, $active, "
            "$failure_budget, "
            "$embedding)"
        ),
        "owner_roster": (
            "INSERT INTO owner_roster ("
            "id, owner_name, team_slug, project_slug, region, "
            "level, oncall, timezone, primary_skill, backup_skill, tenure_months, "
            "embedding) VALUES ($id, $owner_name, $team_slug, $project_slug, $region, "
            "$level, $oncall, $timezone, $primary_skill, $backup_skill, "
            "$tenure_months, $embedding)"
        ),
        "project_releases": (
            "INSERT INTO project_releases ("
            "id, project_slug, release_name, train, status, "
            "risk_level, approver_name, change_count, incident_count, started_day, "
            "summary, embedding) VALUES ($id, $project_slug, $release_name, $train, "
            "$status, $risk_level, $approver_name, $change_count, $incident_count, "
            "$started_day, $summary, $embedding)"
        ),
        "eval_runs": (
            "INSERT INTO eval_runs (id, project_slug, eval_name, dataset_slug, status, "
            "score, regression_delta, owner_name, created_day, surface, summary, "
            "embedding) VALUES ($id, $project_slug, $eval_name, "
            "$dataset_slug, $status, "
            "$score, $regression_delta, $owner_name, $created_day, $surface, $summary, "
            "$embedding)"
        ),
        "deployment_runs": (
            "INSERT INTO deployment_runs (id, project_slug, service_slug, environment, "
            "status, deployer_name, started_day, duration_s, commit_sha, "
            "ring, summary, "
            "embedding) VALUES ($id, $project_slug, $service_slug, "
            "$environment, $status, "
            "$deployer_name, $started_day, $duration_s, $commit_sha, $ring, $summary, "
            "$embedding)"
        ),
        "feature_flags": (
            "INSERT INTO feature_flags (id, project_slug, service_slug, flag_name, "
            "flag_type, status, owner_name, rollout_percent, audience, created_day, "
            "updated_day, embedding) VALUES ($id, $project_slug, $service_slug, "
            "$flag_name, $flag_type, $status, $owner_name, "
            "$rollout_percent, $audience, "
            "$created_day, $updated_day, $embedding)"
        ),
        "audit_events": (
            "INSERT INTO audit_events (id, project_slug, actor_name, entity_kind, "
            "entity_slug, action, channel, created_day, severity_band, summary, "
            "embedding) VALUES ($id, $project_slug, $actor_name, $entity_kind, "
            "$entity_slug, $action, $channel, $created_day, $severity_band, $summary, "
            "$embedding)"
        ),
        "knowledge_chunks": (
            "INSERT INTO knowledge_chunks (id, project_slug, collection, language, "
            "status, shard_id, token_count, freshness_band, source_uri, "
            "title, embedding) "
            "VALUES ($id, $project_slug, $collection, $language, $status, $shard_id, "
            "$token_count, $freshness_band, $source_uri, $title, $embedding)"
        ),
    }
    generators = {
        "work_items": _iter_work_items(),
        "memory_notes": _iter_memory_notes(),
        "incident_reports": _iter_incident_reports(),
        "sprint_snapshots": _iter_sprint_snapshots(),
        "service_catalog": _iter_service_catalog(),
        "owner_roster": _iter_owner_roster(),
        "project_releases": _iter_project_releases(),
        "eval_runs": _iter_eval_runs(),
        "deployment_runs": _iter_deployment_runs(),
        "feature_flags": _iter_feature_flags(),
        "audit_events": _iter_audit_events(),
        "knowledge_chunks": _iter_knowledge_chunks(),
    }
    counts: dict[str, int] = {}
    with db.transaction():
        for table_name, sql in insert_sql.items():
            counts[table_name] = _executemany_in_batches(
                db,
                sql,
                generators[table_name],
            )
    return counts


def populate_graph(db) -> tuple[dict[str, int], dict[str, int], tuple[int, int]]:
    profile_ids: dict[str, int] = {}
    service_ids: dict[str, int] = {}
    node_count = 0
    edge_count = 0

    with db.transaction():
        for project_index in range(PROJECT_COUNT):
            db.query(
                "CREATE (:Project {slug: $slug, focus: $focus})",
                params={
                    "slug": _project_slug(project_index),
                    "focus": (
                        "mixed-memory"
                        if project_index == 0
                        else ("graph", "vector", "release", "analytics")[
                            project_index % 4
                        ]
                    ),
                },
            )
            node_count += 1

        for topic_index in range(TOPIC_COUNT):
            db.query(
                "CREATE (:Topic {slug: $slug, area: $area, priority: $priority})",
                params={
                    "slug": _topic_slug(topic_index),
                    "area": ("graph", "vector", "ops", "release", "security")[
                        topic_index % 5
                    ],
                    "priority": 1 + (topic_index % 3),
                },
            )
            node_count += 1

        for team_index in range(TEAM_COUNT):
            db.query(
                "CREATE (:Team {slug: $slug, region: $region})",
                params={
                    "slug": _team_slug(team_index),
                    "region": ("emea", "amer", "apac")[team_index % 3],
                },
            )
            node_count += 1

        for service_index in range(SERVICE_NODE_COUNT):
            if service_index < len(SPECIAL_SERVICES):
                created = db.query(
                    (
                        "CREATE (:Service {slug: $slug, tier: $tier, "
                        "embedding: $embedding})"
                    ),
                    params={
                        "slug": SPECIAL_SERVICES[service_index]["slug"],
                        "tier": SPECIAL_SERVICES[service_index]["tier"],
                        "embedding": _embedding(
                            **SPECIAL_SERVICES[service_index]["embedding"]
                        ),
                    },
                )
                service_ids[SPECIAL_SERVICES[service_index]["slug"]] = int(
                    created.rows[0][0]
                )
            else:
                offset = service_index - len(SPECIAL_SERVICES)
                db.query(
                    (
                        "CREATE (:Service {slug: $slug, tier: $tier, "
                        "embedding: $embedding})"
                    ),
                    params={
                        "slug": _service_slug(service_index),
                        "tier": ("critical", "high", "medium", "low")[offset % 4],
                        "embedding": _embedding(
                            service=0.42 + ((offset % 8) / 15.0),
                            graph=0.05 + ((offset % 5) / 35.0),
                            vector=0.03 + ((offset % 6) / 40.0),
                        ),
                    },
                )
            node_count += 1

        for profile_index in range(PROFILE_NODE_COUNT):
            if profile_index < len(SPECIAL_OWNERS):
                created = db.query(
                    (
                        "CREATE (:Profile {name: $name, team: $team, "
                        "embedding: $embedding})"
                    ),
                    params={
                        "name": SPECIAL_OWNERS[profile_index]["name"],
                        "team": SPECIAL_OWNERS[profile_index]["team"],
                        "embedding": _embedding(
                            **SPECIAL_OWNERS[profile_index]["embedding"]
                        ),
                    },
                )
                profile_ids[SPECIAL_OWNERS[profile_index]["name"]] = int(
                    created.rows[0][0]
                )
            else:
                offset = profile_index - len(SPECIAL_OWNERS)
                db.query(
                    (
                        "CREATE (:Profile {name: $name, team: $team, "
                        "embedding: $embedding})"
                    ),
                    params={
                        "name": _profile_name(profile_index),
                        "team": _profile_team_slug(profile_index),
                        "embedding": _embedding(
                            profile=0.35 + ((offset % 9) / 14.0),
                            graph=0.04 + ((offset % 7) / 45.0),
                            incident=0.03 + ((offset % 6) / 50.0),
                        ),
                    },
                )
            node_count += 1

        for runbook_index in range(RUNBOOK_COUNT):
            db.query(
                "CREATE (:Runbook {slug: $slug, kind: $kind})",
                params={
                    "slug": f"runbook-{runbook_index:04d}",
                    "kind": ("routing", "vector", "incident", "release")[
                        runbook_index % 4
                    ],
                },
            )
            node_count += 1

        for profile_index in range(PROFILE_NODE_COUNT):
            db.query(
                (
                    "MATCH (p:Profile {name: $name}), "
                    "(j:Project {slug: $project_slug}) "
                    "CREATE (p)-[:OWNS {since: $since, role: $role}]->(j)"
                ),
                params={
                    "name": _profile_name(profile_index),
                    "project_slug": _profile_project_slug(profile_index),
                    "since": 2020 + (profile_index % 5),
                    "role": (
                        SPECIAL_OWNERS[profile_index]["role"]
                        if profile_index < len(SPECIAL_OWNERS)
                        else ("owner", "support", "reviewer")[profile_index % 3]
                    ),
                },
            )
            edge_count += 1
            db.query(
                (
                    "MATCH (p:Profile {name: $name}), (t:Team {slug: $team_slug}) "
                    "CREATE (p)-[:MEMBER_OF]->(t)"
                ),
                params={
                    "name": _profile_name(profile_index),
                    "team_slug": _profile_team_slug(profile_index),
                },
            )
            edge_count += 1
            if profile_index % 4 == 0:
                db.query(
                    (
                        "MATCH (a:Profile {name: $name}), "
                        "(b:Profile {name: $mentor_name}) "
                        "CREATE (a)-[:MENTORS {since: $since}]->(b)"
                    ),
                    params={
                        "name": _profile_name(profile_index),
                        "mentor_name": _profile_name(
                            (profile_index + 1) % PROFILE_NODE_COUNT
                        ),
                        "since": 2019 + (profile_index % 4),
                    },
                )
                edge_count += 1

        for project_index in range(PROJECT_COUNT):
            project_slug = _project_slug(project_index)
            if project_index == 0:
                topic_slugs = ("routing", "vectors", "incidents", "release")
            else:
                topic_slugs = tuple(
                    _topic_slug(
                        len(SPECIAL_TOPIC_SLUGS)
                        + (
                            (project_index * 4 + item)
                            % (TOPIC_COUNT - len(SPECIAL_TOPIC_SLUGS))
                        )
                    )
                    for item in range(4)
                )
            for priority, topic_slug in enumerate(topic_slugs, start=1):
                db.query(
                    (
                        "MATCH (j:Project {slug: $project_slug}), "
                        "(t:Topic {slug: $topic_slug}) "
                        "CREATE (j)-[:TRACKS {priority: $priority}]->(t)"
                    ),
                    params={
                        "project_slug": project_slug,
                        "topic_slug": topic_slug,
                        "priority": priority,
                    },
                )
                edge_count += 1

        for service_index in range(SERVICE_NODE_COUNT):
            service_slug = _service_slug(service_index)
            db.query(
                (
                    "MATCH (j:Project {slug: $project_slug}), "
                    "(s:Service {slug: $service_slug}) "
                    "CREATE (j)-[:DEPENDS_ON {criticality: $criticality}]->(s)"
                ),
                params={
                    "project_slug": _service_project_slug(service_index),
                    "service_slug": service_slug,
                    "criticality": ("critical", "high", "medium")[service_index % 3],
                },
            )
            edge_count += 1
            db.query(
                (
                    "MATCH (t:Team {slug: $team_slug}), "
                    "(s:Service {slug: $service_slug}) "
                    "CREATE (t)-[:SUPPORTS]->(s)"
                ),
                params={
                    "team_slug": _service_team_slug(service_index),
                    "service_slug": service_slug,
                },
            )
            edge_count += 1
            db.query(
                (
                    "MATCH (s:Service {slug: $service_slug}), "
                    "(t:Topic {slug: $topic_slug}) "
                    "CREATE (s)-[:RELATES_TO]->(t)"
                ),
                params={
                    "service_slug": service_slug,
                    "topic_slug": _service_topic_slug(service_index),
                },
            )
            edge_count += 1

        for runbook_index in range(RUNBOOK_COUNT):
            runbook_slug = f"runbook-{runbook_index:04d}"
            topic_slug = _topic_slug(runbook_index % TOPIC_COUNT)
            service_slug = _service_slug(runbook_index % SERVICE_NODE_COUNT)
            db.query(
                (
                    "MATCH (r:Runbook {slug: $runbook_slug}), "
                    "(t:Topic {slug: $topic_slug}) "
                    "CREATE (r)-[:COVERS]->(t)"
                ),
                params={"runbook_slug": runbook_slug, "topic_slug": topic_slug},
            )
            edge_count += 1
            db.query(
                (
                    "MATCH (r:Runbook {slug: $runbook_slug}), "
                    "(s:Service {slug: $service_slug}) "
                    "CREATE (r)-[:TARGETS]->(s)"
                ),
                params={"runbook_slug": runbook_slug, "service_slug": service_slug},
            )
            edge_count += 1

    return profile_ids, service_ids, (node_count, edge_count)


def populate_direct_vectors(db) -> tuple[int, ...]:
    inserted_ids: list[int] = []
    special_rows = [
        {
            "target_id": 90_001,
            "embedding": _embedding(routing=1.0, graph=0.8, latency=0.55),
            "metadata": {
                "scope": "playbook",
                "topic": "routing",
                "project": FOCUS_PROJECT,
            },
        },
        {
            "target_id": 90_002,
            "embedding": _embedding(vector=1.0, sync=0.9, profile=0.5),
            "metadata": {
                "scope": "playbook",
                "topic": "vectors",
                "project": FOCUS_PROJECT,
            },
        },
        {
            "target_id": 90_003,
            "embedding": _embedding(incident=1.0, auth=0.8, ops=0.5),
            "metadata": {
                "scope": "playbook",
                "topic": "incidents",
                "project": FOCUS_PROJECT,
            },
        },
        {
            "target_id": 90_004,
            "embedding": _embedding(service=1.0, graph=0.65, memory=0.4),
            "metadata": {
                "scope": "playbook",
                "topic": "service-map",
                "project": FOCUS_PROJECT,
            },
        },
        {
            "target_id": 90_005,
            "embedding": _embedding(benchmark=1.0, routing=0.8, cache=0.65),
            "metadata": {
                "scope": "playbook",
                "topic": "benchmarks",
                "project": FOCUS_PROJECT,
            },
        },
    ]
    inserted_ids.extend(db.insert_vectors(special_rows))
    batch: list[dict[str, object]] = []
    bulk_count = DIRECT_VECTOR_COUNT - len(special_rows)
    for offset in range(bulk_count):
        batch.append(
            {
                "target_id": 100_000 + offset,
                "embedding": _embedding(
                    routing=0.01 + ((offset % 13) / 80.0),
                    vector=0.02 + ((offset % 11) / 70.0),
                    graph=0.02 + ((offset % 9) / 75.0),
                    memory=0.01 + ((offset % 7) / 90.0),
                ),
                "metadata": {
                    "scope": "archive",
                    "topic": f"bulk-{offset % 64}",
                    "project": _project_slug(1 + (offset % (PROJECT_COUNT - 1))),
                },
            }
        )
        if len(batch) >= 500:
            inserted_ids.extend(db.insert_vectors(batch))
            batch = []
    if batch:
        inserted_ids.extend(db.insert_vectors(batch))
    return tuple(inserted_ids)


def main() -> None:
    report = _make_timer()

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)

        with HumemDB.open(root / "memory") as db:
            create_relational_tables(db)
            table_counts = populate_relational_rows(db)
            report("loaded twelve relational tables with 128-dimensional embeddings")

            create_relational_indexes(db)
            report("created application-side relational indexes")

            profile_ids, service_ids, graph_counts = populate_graph(db)
            report("created large multi-label project graph")

            direct_ids = populate_direct_vectors(db)
            report("inserted direct recall vectors")

            open_work = db.query(
                (
                    "SELECT w.owner_name, w.service_slug, s.team_slug, "
                    "snap.sprint_name, "
                    "rel.release_name, w.summary "
                    "FROM work_items AS w "
                    "JOIN service_catalog AS s ON s.project_slug = w.project_slug "
                    "AND s.service_slug = w.service_slug "
                    "JOIN owner_roster AS o ON o.project_slug = w.project_slug "
                    "AND o.owner_name = w.owner_name "
                    "JOIN sprint_snapshots AS snap ON "
                    "snap.project_slug = w.project_slug "
                    "AND snap.sprint_name = w.sprint_name "
                    "JOIN project_releases AS rel ON rel.project_slug = w.project_slug "
                    "AND rel.release_name = w.release_name "
                    "WHERE w.project_slug = $project_slug "
                    "AND w.sprint_name = $sprint_name "
                    "AND w.release_name = $release_name "
                    "AND w.status IN ('open', 'blocked') "
                    "ORDER BY w.priority, w.owner_name, w.service_slug LIMIT 5"
                ),
                params={
                    "project_slug": FOCUS_PROJECT,
                    "sprint_name": FOCUS_SPRINT,
                    "release_name": FOCUS_RELEASE,
                },
            )
            report("queried joined relational backlog view")

            note_match_ids = _candidate_ids(
                db.query(
                    (
                        "SELECT id FROM memory_notes "
                        "WHERE project_slug = $project_slug "
                        "AND status = 'published' ORDER BY embedding <=> $query LIMIT 3"
                    ),
                    params={
                        "project_slug": FOCUS_PROJECT,
                        "query": _embedding(routing=1.0, graph=0.8, service=0.7),
                    },
                ),
                top_k=3,
            )
            note_matches = _ordered_lookup_rows(
                db,
                table="memory_notes",
                ids=note_match_ids,
                columns=("topic_slug", "kind", "summary"),
            )
            incident_match_ids = _candidate_ids(
                db.query(
                    (
                        "SELECT id FROM incident_reports "
                        "WHERE project_slug = $project_slug "
                        "ORDER BY embedding <=> $query LIMIT 2"
                    ),
                    params={
                        "project_slug": FOCUS_PROJECT,
                        "query": _embedding(
                            vector=1.0,
                            sync=1.0,
                            profile=0.9,
                            incident=0.4,
                        ),
                    },
                ),
                top_k=2,
            )
            incident_matches = _ordered_lookup_rows(
                db,
                table="incident_reports",
                ids=incident_match_ids,
                columns=("service_slug", "severity", "status", "summary"),
            )
            eval_match_ids = _candidate_ids(
                db.query(
                    (
                        "SELECT id FROM eval_runs WHERE project_slug = $project_slug "
                        "ORDER BY embedding <=> $query LIMIT 2"
                    ),
                    params={
                        "project_slug": FOCUS_PROJECT,
                        "query": _embedding(vector=1.0, sync=0.9, benchmark=0.4),
                    },
                ),
                top_k=2,
            )
            eval_matches = _ordered_lookup_rows(
                db,
                table="eval_runs",
                ids=eval_match_ids,
                columns=("eval_name", "status", "summary"),
            )
            report("ran SQL-owned vector recall across notes incidents and evals")

            owner_rows = db.query(
                (
                    "MATCH (p:Profile)-[r:OWNS]->(j:Project) WHERE j.slug = 'atlas' "
                    "RETURN p.name, r.role ORDER BY p.name"
                )
            )
            member_rows = db.query(
                (
                    "MATCH (p:Profile)-[:MEMBER_OF]->(t:Team) RETURN p.name, t.slug "
                    "ORDER BY p.name"
                )
            )
            service_rows = db.query(
                (
                    "MATCH (j:Project)-[r:DEPENDS_ON]->(s:Service) "
                    "WHERE j.slug = 'atlas' "
                    "RETURN s.slug, r.criticality ORDER BY s.slug"
                )
            )
            support_rows = db.query(
                (
                    "MATCH (t:Team)-[:SUPPORTS]->(s:Service) RETURN t.slug, s.slug "
                    "ORDER BY t.slug, s.slug"
                )
            )
            topic_rows = db.query(
                (
                    "MATCH (j:Project)-[r:TRACKS]->(t:Topic) WHERE j.slug = 'atlas' "
                    "RETURN t.slug ORDER BY t.slug"
                )
            )
            report("ran graph traversals over owners teams services and topics")

            team_by_owner = {
                owner_name: team_slug
                for owner_name, team_slug in member_rows.rows
                if owner_name in {item["name"] for item in SPECIAL_OWNERS}
            }
            supported_services_by_team: dict[str, set[str]] = defaultdict(set)
            for team_slug, service_slug in support_rows.rows:
                supported_services_by_team[str(team_slug)].add(str(service_slug))
            atlas_services = {
                str(service_slug) for service_slug, _ in service_rows.rows
            }
            owner_service_context = tuple(
                (owner_name, role, team_by_owner[owner_name], service_slug)
                for owner_name, role in owner_rows.rows
                for service_slug in sorted(
                    atlas_services
                    & supported_services_by_team[team_by_owner[owner_name]]
                )
            )
            atlas_topics = tuple(topic_slug for (topic_slug,) in topic_rows.rows)

            graph_profile_match_ids = _candidate_ids(
                db.query(
                    (
                        "MATCH (p:Profile)-[:OWNS]->(:Project {slug: 'atlas'}) "
                        "SEARCH p IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
                        "RETURN p.id ORDER BY p.id"
                    ),
                    params={
                        "query": _embedding(
                            vector=1.0,
                            sync=1.0,
                            profile=0.9,
                            incident=0.4,
                        )
                    },
                ),
                top_k=3,
            )
            graph_matches = tuple(
                sorted(
                    name
                    for name, profile_id in profile_ids.items()
                    if profile_id in graph_profile_match_ids
                )
            )
            graph_service_match_ids = _candidate_ids(
                db.query(
                    (
                        "MATCH (:Project {slug: 'atlas'})-[:DEPENDS_ON]->(s:Service) "
                        "SEARCH s IN (VECTOR INDEX embedding FOR $query LIMIT 2) "
                        "RETURN s.id ORDER BY s.id"
                    ),
                    params={"query": _embedding(routing=1.0, graph=0.8, latency=0.7)},
                ),
                top_k=2,
            )
            graph_service_matches = tuple(
                sorted(
                    slug
                    for slug, service_id in service_ids.items()
                    if service_id in graph_service_match_ids
                )
            )
            report("ran graph-owned vector recall on profiles and services")

            direct_matches = db.search_vectors(
                _embedding(routing=1.0, graph=0.8, latency=0.7),
                top_k=3,
                metric="cosine",
                filters={"project": FOCUS_PROJECT, "scope": "playbook"},
            )
            report("ran direct vector recall")

            db.query(
                "UPDATE incident_reports SET embedding = $embedding WHERE id = $id",
                params={
                    "id": 205,
                    "embedding": _embedding(
                        vector=1.0,
                        sync=1.0,
                        profile=0.9,
                        incident=0.4,
                    ),
                },
            )
            db.query(
                "MATCH (p:Profile {name: 'Faye'}) SET p.embedding = $embedding",
                params={
                    "embedding": _embedding(
                        vector=1.0,
                        sync=1.0,
                        profile=0.9,
                        incident=0.4,
                    )
                },
            )
            db.query(
                "MATCH (s:Service {slug: 'profile-sync'}) SET s.embedding = $embedding",
                params={"embedding": _embedding(routing=1.0, graph=0.8, latency=0.7)},
            )
            report("updated SQL and graph embeddings")

            updated_incident_match_ids = _candidate_ids(
                db.query(
                    (
                        "SELECT id FROM incident_reports "
                        "WHERE project_slug = $project_slug "
                        "ORDER BY embedding <=> $query LIMIT 2"
                    ),
                    params={
                        "project_slug": FOCUS_PROJECT,
                        "query": _embedding(
                            vector=1.0,
                            sync=1.0,
                            profile=0.9,
                            incident=0.4,
                        ),
                    },
                ),
                top_k=2,
            )
            updated_graph_profile_ids = _candidate_ids(
                db.query(
                    (
                        "MATCH (p:Profile)-[:OWNS]->(:Project {slug: 'atlas'}) "
                        "SEARCH p IN (VECTOR INDEX embedding FOR $query LIMIT 3) "
                        "RETURN p.id ORDER BY p.id"
                    ),
                    params={
                        "query": _embedding(
                            vector=1.0,
                            sync=1.0,
                            profile=0.9,
                            incident=0.4,
                        )
                    },
                ),
                top_k=3,
            )
            updated_graph_service_ids = _candidate_ids(
                db.query(
                    (
                        "MATCH (:Project {slug: 'atlas'})-[:DEPENDS_ON]->(s:Service) "
                        "SEARCH s IN (VECTOR INDEX embedding FOR $query LIMIT 2) "
                        "RETURN s.id ORDER BY s.id"
                    ),
                    params={"query": _embedding(routing=1.0, graph=0.8, latency=0.7)},
                ),
                top_k=2,
            )
            updated_graph_matches = tuple(
                sorted(
                    name
                    for name, profile_id in profile_ids.items()
                    if profile_id in updated_graph_profile_ids
                )
            )
            updated_graph_service_matches = tuple(
                sorted(
                    slug
                    for slug, service_id in service_ids.items()
                    if service_id in updated_graph_service_ids
                )
            )
            report("reran vector recalls after updates")

        total_rows = sum(table_counts.values())
        total_columns = sum(TABLE_COLUMNS.values())
        assert len(table_counts) == 12
        assert total_rows == 151_056
        assert total_columns == 143
        assert graph_counts == (105_832, 253_524)
        assert len(direct_ids) == DIRECT_VECTOR_COUNT
        assert open_work.rows == (
            (
                "Ada",
                "edge-cache",
                "ops",
                FOCUS_SPRINT,
                FOCUS_RELEASE,
                "benchmark evidence handoff for traversal fan-out",
            ),
            (
                "Ada",
                "graph-api",
                "platform",
                FOCUS_SPRINT,
                FOCUS_RELEASE,
                "graph routing latency spike on graph-api",
            ),
            (
                "Bea",
                "profile-sync",
                "retrieval",
                FOCUS_SPRINT,
                FOCUS_RELEASE,
                "vector sync regression after profile updates",
            ),
            (
                "Dev",
                "auth-gateway",
                "security",
                FOCUS_SPRINT,
                FOCUS_RELEASE,
                "incident drill for auth-gateway failover",
            ),
            (
                "Eli",
                "graph-api",
                "platform",
                FOCUS_SPRINT,
                FOCUS_RELEASE,
                "memory stitching across graph and table snapshots",
            ),
        )
        assert tuple(row[0] for row in note_matches) == (
            "service-map",
            "routing",
            "incidents",
        )
        assert tuple(row[0] for row in incident_matches) == (
            "profile-sync",
            "auth-gateway",
        )
        assert tuple(row[0] for row in eval_matches) == (
            "vector-sync-eval",
            "eval-03584",
        )
        assert owner_service_context == (
            ("Ada", "lead", "platform", "graph-api"),
            ("Ada", "lead", "platform", "release-bus"),
            ("Bea", "vector-owner", "retrieval", "profile-sync"),
            ("Dev", "incident-commander", "ops", "edge-cache"),
            ("Eli", "memory-architect", "platform", "graph-api"),
            ("Eli", "memory-architect", "platform", "release-bus"),
            ("Faye", "security-review", "security", "auth-gateway"),
        )
        assert atlas_topics == ("incidents", "release", "routing", "vectors")
        assert graph_matches == ("Bea", "Dev", "Faye")
        assert graph_service_matches == ("edge-cache", "graph-api")
        assert tuple(row[:3] for row in direct_matches.rows) == (
            ("direct", "", 90_001),
            ("direct", "", 90_005),
            ("direct", "", 90_004),
        )
        assert updated_incident_match_ids == (205, 202)
        assert updated_graph_matches == ("Bea", "Dev", "Faye")
        assert updated_graph_service_matches == ("graph-api", "profile-sync")

        print(
            "Dataset summary:",
            {
                "dimensions": DIMENSIONS,
                "tables": len(table_counts),
                "total_columns": total_columns,
                "total_rows": total_rows,
                "graph_nodes": graph_counts[0],
                "graph_edges": graph_counts[1],
                "direct_vectors": len(direct_ids),
            },
        )
        print("Table counts:", table_counts)
        print("Atlas joined backlog:", open_work.rows)
        print("Atlas note matches:", note_matches)
        print("Atlas incident matches:", incident_matches)
        print("Atlas eval matches:", eval_matches)
        print("Atlas owner/service context:", owner_service_context)
        print("Atlas tracked topics:", atlas_topics)
        print("Atlas graph profile matches before update:", graph_matches)
        print("Atlas graph service matches before update:", graph_service_matches)
        print("Atlas graph profile matches after update:", updated_graph_matches)
        print(
            "Atlas graph service matches after update:",
            updated_graph_service_matches,
        )
        print("Atlas direct recall matches:", direct_matches.rows)


if __name__ == "__main__":
    main()
