from __future__ import annotations

import csv
import tempfile
from pathlib import Path
from time import perf_counter

from humemdb import HumemDB


ACCOUNT_COUNT = 20_000
EVENTS_PER_ACCOUNT = 4
SERVICE_COUNT = 10_000
EDGE_FANOUT = 2


def _make_timer() -> callable:
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


def _write_csv(
    path: Path,
    header: tuple[str, ...],
    rows: list[tuple[object, ...]],
) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def build_account_rows() -> list[tuple[object, ...]]:
    segments = ("enterprise", "startup", "public-sector", "research")
    regions = ("emea", "amer", "apac")
    rows: list[tuple[object, ...]] = []
    for account_id in range(1, ACCOUNT_COUNT + 1):
        rows.append(
            (
                account_id,
                f"Account {account_id:05d}",
                segments[(account_id - 1) % len(segments)],
                regions[(account_id - 1) % len(regions)],
                1 if account_id % 7 != 0 else 0,
            )
        )
    return rows


def build_event_rows() -> list[tuple[object, ...]]:
    event_types = ("signup", "upgrade", "renewal", "ticket")
    rows: list[tuple[object, ...]] = []
    event_id = 1
    for account_id in range(1, ACCOUNT_COUNT + 1):
        for offset in range(EVENTS_PER_ACCOUNT):
            rows.append(
                (
                    event_id,
                    account_id,
                    event_types[offset % len(event_types)],
                    f"2026-03-{((offset + account_id) % 28) + 1:02d}",
                    500 + ((account_id * 17 + offset * 31) % 9_500),
                )
            )
            event_id += 1
    return rows


def build_service_rows() -> list[tuple[object, ...]]:
    tiers = ("edge", "core", "stateful")
    regions = ("emea", "amer", "apac")
    rows: list[tuple[object, ...]] = []
    for service_id in range(1, SERVICE_COUNT + 1):
        rows.append(
            (
                service_id,
                f"service-{service_id:05d}",
                tiers[(service_id - 1) % len(tiers)],
                regions[(service_id - 1) % len(regions)],
                "true" if service_id % 9 != 0 else "false",
            )
        )
    return rows


def build_dependency_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for service_id in range(1, SERVICE_COUNT + 1):
        for offset in range(1, EDGE_FANOUT + 1):
            target_id = ((service_id - 1 + offset) % SERVICE_COUNT) + 1
            rows.append(
                (
                    service_id,
                    target_id,
                    5 + ((service_id * 13 + offset * 7) % 120),
                )
            )
    return rows


def main() -> None:
    report = _make_timer()

    account_rows = build_account_rows()
    event_rows = build_event_rows()
    service_rows = build_service_rows()
    dependency_rows = build_dependency_rows()
    report("built source datasets")

    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        accounts_csv = root / "accounts.csv"
        events_csv = root / "account_events.csv"
        services_csv = root / "services.csv"
        dependencies_csv = root / "dependencies.csv"

        _write_csv(
            accounts_csv,
            ("id", "name", "segment", "region", "active"),
            account_rows,
        )
        _write_csv(
            events_csv,
            ("id", "account_id", "event_type", "event_day", "value_cents"),
            event_rows,
        )
        _write_csv(
            services_csv,
            ("id", "name", "tier", "region", "healthy"),
            service_rows,
        )
        _write_csv(
            dependencies_csv,
            ("source_id", "target_id", "latency_ms"),
            dependency_rows,
        )
        report("wrote CSV fixtures")

        with HumemDB.open(root / "ingest") as db:
            with db.transaction():
                db.query(
                    (
                        "CREATE TABLE accounts ("
                        "id INTEGER PRIMARY KEY, "
                        "name TEXT NOT NULL, "
                        "segment TEXT NOT NULL, "
                        "region TEXT NOT NULL, "
                        "active BOOLEAN NOT NULL"
                        ")"
                    )
                )
                db.query(
                    (
                        "CREATE TABLE account_events ("
                        "id INTEGER PRIMARY KEY, "
                        "account_id INTEGER NOT NULL, "
                        "event_type TEXT NOT NULL, "
                        "event_day TEXT NOT NULL, "
                        "value_cents INTEGER NOT NULL"
                        ")"
                    )
                )
            report("created relational schema")

            imported_accounts = db.import_table(
                "accounts",
                accounts_csv,
                chunk_size=2_000,
            )
            imported_events = db.import_table(
                "account_events",
                events_csv,
                chunk_size=2_000,
            )
            report("imported relational CSV data")

            imported_services = db.import_nodes(
                "Service",
                services_csv,
                id_column="id",
                property_types={"healthy": "boolean"},
                chunk_size=2_000,
            )
            imported_dependencies = db.import_edges(
                "DEPENDS_ON",
                dependencies_csv,
                source_id_column="source_id",
                target_id_column="target_id",
                property_types={"latency_ms": "integer"},
                chunk_size=2_000,
            )
            report("imported graph CSV data")

            revenue_result = db.query(
                (
                    "SELECT a.segment, a.region, COUNT(*) AS event_count, "
                    "SUM(e.value_cents) AS total_value_cents "
                    "FROM accounts a "
                    "JOIN account_events e ON e.account_id = a.id "
                    "WHERE a.active = true "
                    "GROUP BY a.segment, a.region "
                    "ORDER BY total_value_cents DESC, a.segment, a.region "
                    "LIMIT 6"
                )
            )
            report("ran relational post-ingest read")

            dependency_result = db.query(
                (
                    "MATCH (s:Service)-[r:DEPENDS_ON]->(t:Service) "
                    "WHERE s.region = 'emea' AND s.healthy = true "
                    "RETURN s.name, r.latency_ms, t.name "
                    "ORDER BY r.latency_ms DESC, s.name LIMIT 6"
                )
            )
            report("ran graph post-ingest read")

            print(
                "Imported rows:",
                {
                    "accounts": imported_accounts,
                    "account_events": imported_events,
                    "services": imported_services,
                    "dependencies": imported_dependencies,
                },
            )
            print("Top relational groups:", revenue_result.rows)
            print("Top dependency rows:", dependency_result.rows)


if __name__ == "__main__":
    main()
