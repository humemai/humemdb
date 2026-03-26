from __future__ import annotations

import argparse
import csv
import json
import statistics
import tempfile
import time
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Callable, Iterator


HumemDB = import_module("humemdb").HumemDB
_cypher_module = import_module("humemdb.cypher")
_encode_property_value = getattr(_cypher_module, "_encode_property_value")
ensure_graph_schema = _cypher_module.ensure_graph_schema

TableBatch = list[tuple[int, str, str, str]]
NodeBatch = list[tuple[int, str, int, bool]]
EdgeBatch = list[tuple[int, int, int, int]]

TABLE_METHODS = (
    "import_table",
    "staging_normalize",
    "public_executemany",
    "internal_sqlite",
)
GRAPH_METHODS = (
    "import_api",
    "public_cypher_query",
    "internal_sqlite",
)


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    table_rows: int
    node_rows: int
    edge_fanout: int
    chunk_size: int
    warmup: int
    repetitions: int
    table_methods: tuple[str, ...]
    graph_methods: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TimingSummary:
    mean_ms: float
    stdev_ms: float
    minimum_ms: float
    maximum_ms: float

    def to_dict(self) -> dict[str, float]:
        return {
            "mean_ms": self.mean_ms,
            "stdev_ms": self.stdev_ms,
            "minimum_ms": self.minimum_ms,
            "maximum_ms": self.maximum_ms,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    config: BenchmarkConfig
    edge_rows: int
    stage_timings_ms: dict[str, dict[str, TimingSummary]]
    freshness_timings_ms: dict[str, dict[str, TimingSummary]]
    imported_counts: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        return {
            "config": {
                "table_rows": self.config.table_rows,
                "node_rows": self.config.node_rows,
                "edge_fanout": self.config.edge_fanout,
                "chunk_size": self.config.chunk_size,
                "warmup": self.config.warmup,
                "repetitions": self.config.repetitions,
                "table_methods": list(self.config.table_methods),
                "graph_methods": list(self.config.graph_methods),
            },
            "edge_rows": self.edge_rows,
            "stage_timings_ms": {
                stage: {
                    method: summary.to_dict()
                    for method, summary in methods.items()
                }
                for stage, methods in self.stage_timings_ms.items()
            },
            "freshness_timings_ms": {
                stage: {
                    method: summary.to_dict()
                    for method, summary in methods.items()
                }
                for stage, methods in self.freshness_timings_ms.items()
            },
            "imported_counts": self.imported_counts,
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark CSV-backed import_table(...), import_nodes(...), and "
            "import_edges(...) against public and internal manual ingest paths."
        )
    )
    parser.add_argument("--table-rows", type=int, default=50_000)
    parser.add_argument("--node-rows", type=int, default=20_000)
    parser.add_argument("--edge-fanout", type=int, default=2)
    parser.add_argument("--chunk-size", type=int, default=1_000)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument(
        "--table-methods",
        default=",".join(TABLE_METHODS),
        help=(
            "Comma-separated table ingest methods to run. "
            f"Available: {', '.join(TABLE_METHODS)}"
        ),
    )
    parser.add_argument(
        "--graph-methods",
        default=",".join(GRAPH_METHODS),
        help=(
            "Comma-separated graph ingest methods to run. "
            f"Available: {', '.join(GRAPH_METHODS)}"
        ),
    )
    parser.add_argument("--output", choices=("text", "json"), default="text")
    return parser.parse_args()


def _parse_method_list(
    raw: str,
    *,
    allowed: tuple[str, ...],
    flag: str,
) -> tuple[str, ...]:
    methods = tuple(part.strip() for part in raw.split(",") if part.strip())
    if not methods:
        raise ValueError(f"{flag} requires at least one method.")
    invalid = tuple(method for method in methods if method not in allowed)
    if invalid:
        raise ValueError(
            f"{flag} received unsupported methods: {', '.join(invalid)}"
        )
    if len(set(methods)) != len(methods):
        raise ValueError(f"{flag} does not allow duplicate methods.")
    return methods


def _summarize(samples_seconds: list[float]) -> TimingSummary:
    samples_ms = [sample * 1_000.0 for sample in samples_seconds]
    return TimingSummary(
        mean_ms=statistics.mean(samples_ms),
        stdev_ms=statistics.pstdev(samples_ms),
        minimum_ms=min(samples_ms),
        maximum_ms=max(samples_ms),
    )


def _write_table_csv(path: Path, *, rows: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "name", "city", "active"])
        for index in range(1, rows + 1):
            writer.writerow(
                [
                    index,
                    f"User {index:05d}",
                    ("Berlin", "Paris", "Lisbon", "Seoul")[index % 4],
                    "true" if index % 5 != 0 else "false",
                ]
            )


def _write_nodes_csv(path: Path, *, rows: int) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["id", "name", "age", "active"])
        for index in range(1, rows + 1):
            writer.writerow(
                [
                    index,
                    f"Person {index:05d}",
                    18 + (index % 50),
                    "true" if index % 4 != 0 else "false",
                ]
            )


def _write_edges_csv(path: Path, *, rows: int, fanout: int) -> int:
    edge_rows = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["from_id", "to_id", "since", "strength"])
        for from_id in range(1, rows + 1):
            for offset in range(1, fanout + 1):
                to_id = ((from_id + offset - 1) % rows) + 1
                writer.writerow(
                    [
                        from_id,
                        to_id,
                        2018 + ((from_id + offset) % 6),
                        1 + ((from_id + offset) % 5),
                    ]
                )
                edge_rows += 1
    return edge_rows


def _prepare_fixture_csvs(
    fixture_dir: Path,
    *,
    config: BenchmarkConfig,
) -> tuple[Path, Path, Path, int]:
    table_csv = fixture_dir / "users.csv"
    nodes_csv = fixture_dir / "people.csv"
    edges_csv = fixture_dir / "knows.csv"
    _write_table_csv(table_csv, rows=config.table_rows)
    _write_nodes_csv(nodes_csv, rows=config.node_rows)
    edge_rows = _write_edges_csv(
        edges_csv,
        rows=config.node_rows,
        fanout=config.edge_fanout,
    )
    return table_csv, nodes_csv, edges_csv, edge_rows


def _sqlite_engine(db: Any):
    return getattr(db, "_sqlite")


def _iter_table_batches(path: Path, *, chunk_size: int) -> Iterator[TableBatch]:
    batch: TableBatch = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            batch.append(
                (
                    int(row["id"]),
                    row["name"],
                    row["city"],
                    row["active"],
                )
            )
            if len(batch) >= chunk_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _iter_node_batches(path: Path, *, chunk_size: int) -> Iterator[NodeBatch]:
    batch: NodeBatch = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            batch.append(
                (
                    int(row["id"]),
                    row["name"],
                    int(row["age"]),
                    row["active"].strip().lower() == "true",
                )
            )
            if len(batch) >= chunk_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _iter_edge_batches(path: Path, *, chunk_size: int) -> Iterator[EdgeBatch]:
    batch: EdgeBatch = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            batch.append(
                (
                    int(row["from_id"]),
                    int(row["to_id"]),
                    int(row["since"]),
                    int(row["strength"]),
                )
            )
            if len(batch) >= chunk_size:
                yield batch
                batch = []
    if batch:
        yield batch


def _create_users_table(db: Any) -> None:
    db.query(
        (
            "CREATE TABLE users ("
            "id INTEGER PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "city TEXT NOT NULL, "
            "active BOOLEAN NOT NULL"
            ")"
        )
    )


def _create_users_staging_table(db: Any) -> None:
    db.query(
        (
            "CREATE TABLE users_staging ("
            "id TEXT NOT NULL, "
            "name TEXT NOT NULL, "
            "city TEXT NOT NULL, "
            "active TEXT NOT NULL"
            ")"
        )
    )


def _run_table_import_api(
    db: Any,
    table_csv: Path,
    *,
    chunk_size: int,
) -> int:
    return db.import_table("users", table_csv, chunk_size=chunk_size)


def _run_table_staging_normalize(
    db: Any,
    table_csv: Path,
    *,
    chunk_size: int,
) -> int:
    _create_users_staging_table(db)
    imported_rows = db.import_table(
        "users_staging",
        table_csv,
        chunk_size=chunk_size,
    )
    db.query(
        (
            "INSERT INTO users (id, name, city, active) "
            "SELECT "
            "CAST(id AS INTEGER), "
            "name, "
            "city, "
            "CASE "
            "WHEN LOWER(active) IN ('true', '1', 'yes') THEN 1 "
            "ELSE 0 "
            "END "
            "FROM users_staging"
        )
    )
    return imported_rows


def _run_table_public_executemany(
    db: Any,
    table_csv: Path,
    *,
    chunk_size: int,
) -> int:
    imported_rows = 0
    with db.transaction():
        for batch in _iter_table_batches(table_csv, chunk_size=chunk_size):
            mapped_batch = [
                {
                    "id": row_id,
                    "name": name,
                    "city": city,
                    "active": active,
                }
                for row_id, name, city, active in batch
            ]
            db.executemany(
                (
                    "INSERT INTO users (id, name, city, active) "
                    "VALUES ($id, $name, $city, $active)"
                ),
                mapped_batch,
            )
            imported_rows += len(batch)
    return imported_rows


def _run_table_internal_sqlite(
    db: Any,
    table_csv: Path,
    *,
    chunk_size: int,
) -> int:
    sqlite = _sqlite_engine(db)
    imported_rows = 0
    sqlite.begin()
    try:
        for batch in _iter_table_batches(table_csv, chunk_size=chunk_size):
            sqlite.executemany(
                (
                    "INSERT INTO users (id, name, city, active) "
                    "VALUES (?, ?, ?, ?)"
                ),
                batch,
            )
            imported_rows += len(batch)
    except Exception:
        sqlite.rollback()
        raise
    else:
        sqlite.commit()
    return imported_rows


def _run_node_import_api(
    db: Any,
    nodes_csv: Path,
    *,
    chunk_size: int,
) -> int:
    return db.import_nodes(
        "Person",
        nodes_csv,
        id_column="id",
        property_types={"age": "integer", "active": "boolean"},
        chunk_size=chunk_size,
    )


def _run_node_public_cypher(
    db: Any,
    nodes_csv: Path,
    *,
    chunk_size: int,
) -> int:
    imported_rows = 0
    with db.transaction():
        for batch in _iter_node_batches(nodes_csv, chunk_size=chunk_size):
            for node_id, name, age, active in batch:
                db.query(
                    (
                        "CREATE (n:Person {"
                        "id: $id, "
                        "name: $name, "
                        "age: $age, "
                        "active: $active"
                        "})"
                    ),
                    params={
                        "id": node_id,
                        "name": name,
                        "age": age,
                        "active": active,
                    },
                )
                imported_rows += 1
    return imported_rows


def _run_node_internal_sqlite(
    db: Any,
    nodes_csv: Path,
    *,
    chunk_size: int,
) -> int:
    sqlite = _sqlite_engine(db)
    ensure_graph_schema(sqlite)
    imported_rows = 0
    sqlite.begin()
    try:
        for batch in _iter_node_batches(nodes_csv, chunk_size=chunk_size):
            node_rows: list[tuple[int, str]] = []
            property_rows: list[tuple[int, str, str | None, str]] = []
            for node_id, name, age, active in batch:
                node_rows.append((node_id, "Person"))
                for key, value in (
                    ("name", name),
                    ("age", age),
                    ("active", active),
                ):
                    encoded_value, value_type = _encode_property_value(value)
                    property_rows.append((node_id, key, encoded_value, value_type))
            sqlite.executemany(
                "INSERT INTO graph_nodes (id, label) VALUES (?, ?)",
                node_rows,
                query_type="cypher",
            )
            sqlite.executemany(
                (
                    "INSERT INTO graph_node_properties "
                    "(node_id, key, value, value_type) VALUES (?, ?, ?, ?)"
                ),
                property_rows,
                query_type="cypher",
            )
            imported_rows += len(batch)
    except Exception:
        sqlite.rollback()
        raise
    else:
        sqlite.commit()
    return imported_rows


def _run_edge_import_api(
    db: Any,
    edges_csv: Path,
    *,
    chunk_size: int,
) -> int:
    return db.import_edges(
        "KNOWS",
        edges_csv,
        source_id_column="from_id",
        target_id_column="to_id",
        property_types={"since": "integer", "strength": "integer"},
        chunk_size=chunk_size,
    )


def _run_edge_public_cypher(
    db: Any,
    edges_csv: Path,
    *,
    chunk_size: int,
) -> int:
    imported_rows = 0
    with db.transaction():
        for batch in _iter_edge_batches(edges_csv, chunk_size=chunk_size):
            for from_id, to_id, since, strength in batch:
                db.query(
                    (
                        "MATCH (source:Person {id: $from_id}), "
                        "(target:Person {id: $to_id}) "
                        "CREATE (source)-[:KNOWS {"
                        "since: $since, "
                        "strength: $strength"
                        "}]->(target)"
                    ),
                    params={
                        "from_id": from_id,
                        "to_id": to_id,
                        "since": since,
                        "strength": strength,
                    },
                )
                imported_rows += 1
    return imported_rows


def _run_edge_internal_sqlite(
    db: Any,
    edges_csv: Path,
    *,
    chunk_size: int,
) -> int:
    sqlite = _sqlite_engine(db)
    ensure_graph_schema(sqlite)
    next_edge_id = 1
    sqlite.begin()
    try:
        for batch in _iter_edge_batches(edges_csv, chunk_size=chunk_size):
            edge_rows: list[tuple[int, str, int, int]] = []
            property_rows: list[tuple[int, str, str | None, str]] = []
            for from_id, to_id, since, strength in batch:
                edge_id = next_edge_id
                next_edge_id += 1
                edge_rows.append((edge_id, "KNOWS", from_id, to_id))
                for key, value in (("since", since), ("strength", strength)):
                    encoded_value, value_type = _encode_property_value(value)
                    property_rows.append((edge_id, key, encoded_value, value_type))
            sqlite.executemany(
                (
                    "INSERT INTO graph_edges (id, type, from_node_id, to_node_id) "
                    "VALUES (?, ?, ?, ?)"
                ),
                edge_rows,
                query_type="cypher",
            )
            sqlite.executemany(
                (
                    "INSERT INTO graph_edge_properties "
                    "(edge_id, key, value, value_type) VALUES (?, ?, ?, ?)"
                ),
                property_rows,
                query_type="cypher",
            )
    except Exception:
        sqlite.rollback()
        raise
    else:
        sqlite.commit()
    return next_edge_id - 1


def _summarize_stage_samples(
    stage_samples: dict[str, dict[str, list[float]]],
) -> dict[str, dict[str, TimingSummary]]:
    return {
        stage: {
            method: _summarize(samples)
            for method, samples in methods.items()
        }
        for stage, methods in stage_samples.items()
    }


def run_benchmark(config: BenchmarkConfig) -> BenchmarkReport:
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        fixture_dir = root / "fixtures"
        fixture_dir.mkdir(parents=True, exist_ok=True)
        table_csv, nodes_csv, edges_csv, edge_rows = _prepare_fixture_csvs(
            fixture_dir,
            config=config,
        )

        stage_samples: dict[str, dict[str, list[float]]] = {
            "table_ingest": {method: [] for method in config.table_methods},
            "node_ingest": {method: [] for method in config.graph_methods},
            "edge_ingest": {method: [] for method in config.graph_methods},
        }
        freshness_samples: dict[str, dict[str, list[float]]] = {
            "table_count_query": {method: [] for method in config.table_methods},
            "node_count_query": {method: [] for method in config.graph_methods},
            "edge_count_query": {method: [] for method in config.graph_methods},
        }

        imported_counts = {
            "table_rows": config.table_rows,
            "node_rows": config.node_rows,
            "edge_rows": edge_rows,
        }

        table_runners: dict[str, Callable[[Any, Path], int]] = {
            "import_table": lambda db, path: _run_table_import_api(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
            "staging_normalize": lambda db, path: _run_table_staging_normalize(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
            "public_executemany": lambda db, path: _run_table_public_executemany(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
            "internal_sqlite": lambda db, path: _run_table_internal_sqlite(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
        }
        node_runners: dict[str, Callable[[Any, Path], int]] = {
            "import_api": lambda db, path: _run_node_import_api(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
            "public_cypher_query": lambda db, path: _run_node_public_cypher(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
            "internal_sqlite": lambda db, path: _run_node_internal_sqlite(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
        }
        edge_runners: dict[str, Callable[[Any, Path], int]] = {
            "import_api": lambda db, path: _run_edge_import_api(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
            "public_cypher_query": lambda db, path: _run_edge_public_cypher(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
            "internal_sqlite": lambda db, path: _run_edge_internal_sqlite(
                db,
                path,
                chunk_size=config.chunk_size,
            ),
        }

        total_runs = config.warmup + config.repetitions
        for run_index in range(total_runs):
            run_dir = root / f"run-{run_index}"
            run_dir.mkdir(parents=True, exist_ok=True)

            for method_name in config.table_methods:
                runner = table_runners[method_name]
                with HumemDB.open(run_dir / f"table-{method_name}") as db:
                    _create_users_table(db)
                    started = time.perf_counter()
                    imported_table_rows = runner(db, table_csv)
                    table_import_elapsed = time.perf_counter() - started
                    started = time.perf_counter()
                    table_count = db.query("SELECT COUNT(*) FROM users")
                    table_count_elapsed = time.perf_counter() - started

                    if imported_table_rows != config.table_rows:
                        raise ValueError(
                            f"{method_name} imported an unexpected table row count."
                        )
                    if table_count.rows != ((config.table_rows,),):
                        raise ValueError(
                            f"{method_name} returned an unexpected table count."
                        )

                    if run_index >= config.warmup:
                        stage_samples["table_ingest"][method_name].append(
                            table_import_elapsed
                        )
                        freshness_samples["table_count_query"][method_name].append(
                            table_count_elapsed
                        )

            for method_name in config.graph_methods:
                with HumemDB.open(run_dir / f"graph-{method_name}") as db:
                    started = time.perf_counter()
                    imported_node_rows = node_runners[method_name](db, nodes_csv)
                    node_import_elapsed = time.perf_counter() - started
                    started = time.perf_counter()
                    node_count = db.query(
                        "MATCH (n:Person) RETURN n.id ORDER BY n.id"
                    )
                    node_count_elapsed = time.perf_counter() - started
                    started = time.perf_counter()
                    imported_edge_rows = edge_runners[method_name](db, edges_csv)
                    edge_import_elapsed = time.perf_counter() - started
                    started = time.perf_counter()
                    edge_count = db.query(
                        (
                            "MATCH (a:Person)-[r:KNOWS]->(b:Person) "
                            "RETURN r.id ORDER BY r.id"
                        )
                    )
                    edge_count_elapsed = time.perf_counter() - started

                    if imported_node_rows != config.node_rows:
                        raise ValueError(
                            f"{method_name} imported an unexpected node row count."
                        )
                    if len(node_count.rows) != config.node_rows:
                        raise ValueError(
                            f"{method_name} returned an unexpected node count."
                        )
                    if imported_edge_rows != edge_rows:
                        raise ValueError(
                            f"{method_name} imported an unexpected edge row count."
                        )
                    if len(edge_count.rows) != edge_rows:
                        raise ValueError(
                            f"{method_name} returned an unexpected edge count."
                        )

                    if run_index >= config.warmup:
                        stage_samples["node_ingest"][method_name].append(
                            node_import_elapsed
                        )
                        stage_samples["edge_ingest"][method_name].append(
                            edge_import_elapsed
                        )
                        freshness_samples["node_count_query"][method_name].append(
                            node_count_elapsed
                        )
                        freshness_samples["edge_count_query"][method_name].append(
                            edge_count_elapsed
                        )

        return BenchmarkReport(
            config=config,
            edge_rows=edge_rows,
            stage_timings_ms=_summarize_stage_samples(stage_samples),
            freshness_timings_ms=_summarize_stage_samples(freshness_samples),
            imported_counts=imported_counts,
        )


def _print_report(report: BenchmarkReport) -> None:
    print("CSV ingest benchmark configuration")
    print(f"  table_rows: {report.config.table_rows}")
    print(f"  node_rows: {report.config.node_rows}")
    print(f"  edge_rows: {report.edge_rows}")
    print(f"  edge_fanout: {report.config.edge_fanout}")
    print(f"  chunk_size: {report.config.chunk_size}")
    print(f"  warmup: {report.config.warmup}")
    print(f"  repetitions: {report.config.repetitions}")
    print(f"  table_methods: {', '.join(report.config.table_methods)}")
    print(f"  graph_methods: {', '.join(report.config.graph_methods)}")
    print()
    print("Import timings (ms)")
    for stage_name, methods in report.stage_timings_ms.items():
        print(f"  {stage_name}")
        for method_name, summary in methods.items():
            print(
                f"    {method_name}: mean={summary.mean_ms:.2f} "
                f"stdev={summary.stdev_ms:.2f} min={summary.minimum_ms:.2f} "
                f"max={summary.maximum_ms:.2f}"
            )
    print()
    print("Freshness query timings (ms)")
    for stage_name, methods in report.freshness_timings_ms.items():
        print(f"  {stage_name}")
        for method_name, summary in methods.items():
            print(
                f"    {method_name}: mean={summary.mean_ms:.2f} "
                f"stdev={summary.stdev_ms:.2f} min={summary.minimum_ms:.2f} "
                f"max={summary.maximum_ms:.2f}"
            )


def main() -> None:
    args = _parse_args()
    table_methods = _parse_method_list(
        args.table_methods,
        allowed=TABLE_METHODS,
        flag="--table-methods",
    )
    graph_methods = _parse_method_list(
        args.graph_methods,
        allowed=GRAPH_METHODS,
        flag="--graph-methods",
    )
    config = BenchmarkConfig(
        table_rows=args.table_rows,
        node_rows=args.node_rows,
        edge_fanout=args.edge_fanout,
        chunk_size=args.chunk_size,
        warmup=args.warmup,
        repetitions=args.repetitions,
        table_methods=table_methods,
        graph_methods=graph_methods,
    )
    report = run_benchmark(config)
    if args.output == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    _print_report(report)


if __name__ == "__main__":
    main()
