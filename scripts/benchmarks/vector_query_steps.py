from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Callable

import numpy as np

HumemDB = import_module("humemdb").HumemDB
cypher_module = import_module("humemdb.cypher")
sql_module = import_module("humemdb.sql")
vector_module = import_module("humemdb.vector")

MatchNodePlan = cypher_module.MatchNodePlan
MatchRelationshipPlan = cypher_module.MatchRelationshipPlan
_bind_plan_values = cypher_module._bind_plan_values
_compile_match_plan = cypher_module._compile_match_plan
parse_cypher = cypher_module.parse_cypher
_translate_sql_cached = sql_module._translate_sql_cached
translate_sql = sql_module.translate_sql
ExactVectorIndex = vector_module.ExactVectorIndex
load_vector_matrix = vector_module.load_vector_matrix


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    rows: int
    dimensions: int
    queries: int
    top_k: int
    warmup: int
    repetitions: int
    seed: int
    batch_size: int


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
    alpha_candidate_count: int
    stage_timings_ms: dict[str, float]
    latency_summaries_ms: dict[str, TimingSummary]

    def to_dict(self) -> dict[str, Any]:
        return {
            "config": {
                "rows": self.config.rows,
                "dimensions": self.config.dimensions,
                "queries": self.config.queries,
                "top_k": self.config.top_k,
                "warmup": self.config.warmup,
                "repetitions": self.config.repetitions,
                "seed": self.config.seed,
                "batch_size": self.config.batch_size,
            },
            "alpha_candidate_count": self.alpha_candidate_count,
            "stage_timings_ms": self.stage_timings_ms,
            "latency_summaries_ms": {
                name: summary.to_dict()
                for name, summary in self.latency_summaries_ms.items()
            },
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark step timings for vector ingest plus direct, SQL-scoped, and "
            "Cypher-scoped vector queries."
        )
    )
    parser.add_argument("--rows", type=int, default=1_000)
    parser.add_argument("--dimensions", type=int, default=384)
    parser.add_argument("--queries", type=int, default=16)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--output", choices=("text", "json"), default="text")
    return parser.parse_args()


def _chunked_rows(
    item_ids: np.ndarray,
    matrix: np.ndarray,
    *,
    batch_size: int,
) -> list[list[tuple[int, list[float]]]]:
    batches: list[list[tuple[int, list[float]]]] = []
    for start in range(0, len(item_ids), batch_size):
        stop = min(start + batch_size, len(item_ids))
        batch: list[tuple[int, list[float]]] = []
        for item_id, vector in zip(
            item_ids[start:stop],
            matrix[start:stop],
            strict=True,
        ):
            batch.append((int(item_id), vector.tolist()))
        batches.append(batch)
    return batches


def _make_dataset(
    config: BenchmarkConfig,
) -> tuple[np.ndarray, np.ndarray, list[np.ndarray]]:
    rng = np.random.default_rng(config.seed)
    matrix = rng.normal(size=(config.rows, config.dimensions)).astype(np.float32)
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    matrix = matrix / np.clip(norms, 1e-12, None)
    item_ids = np.arange(1, config.rows + 1, dtype=np.int64)

    query_indexes = rng.integers(0, config.rows // 2, size=config.queries)
    queries: list[np.ndarray] = []
    for index in query_indexes:
        query = matrix[index] + rng.normal(scale=0.01, size=config.dimensions).astype(
            np.float32
        )
        query = query / max(float(np.linalg.norm(query)), 1e-12)
        queries.append(query.astype(np.float32, copy=False))
    return item_ids, matrix, queries


def _summarize_ms(samples_seconds: list[float]) -> TimingSummary:
    samples_ms = [sample * 1_000.0 for sample in samples_seconds]
    return TimingSummary(
        mean_ms=statistics.mean(samples_ms),
        stdev_ms=statistics.pstdev(samples_ms),
        minimum_ms=min(samples_ms),
        maximum_ms=max(samples_ms),
    )


def _time_operation(
    operation: Callable[[], Any],
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
    return _summarize_ms(timings)


def _time_query_batch(
    operation: Callable[[np.ndarray], Any],
    queries: list[np.ndarray],
    *,
    warmup: int,
    repetitions: int,
) -> TimingSummary:
    for _ in range(warmup):
        for query in queries:
            operation(query)

    per_query_timings: list[float] = []
    for _ in range(repetitions):
        started = time.perf_counter()
        for query in queries:
            operation(query)
        elapsed = time.perf_counter() - started
        per_query_timings.append(elapsed / len(queries))
    return _summarize_ms(per_query_timings)


def _compile_cypher_bound(plan: Any, params: dict[str, Any]) -> Any:
    bound_plan = _bind_plan_values(plan, params)
    if not isinstance(bound_plan, (MatchNodePlan, MatchRelationshipPlan)):
        raise ValueError("This benchmark only supports MATCH-based Cypher workloads.")
    return _compile_match_plan(bound_plan)


def _seed_direct_vectors(db: Any, batches: list[list[tuple[int, list[float]]]]) -> None:
    with db.transaction(route="sqlite"):
        for batch in batches:
            db.insert_vectors(batch)


def _uncached_translate_sql(text: str) -> str:
    _translate_sql_cached.cache_clear()
    return translate_sql(text, target="sqlite")


def _candidate_item_ids_from_result(rows: tuple[tuple[Any, ...], ...]) -> set[int]:
    return {int(row[0]) for row in rows}


def _candidate_indexes_for_item_ids(
    vector_item_ids: np.ndarray,
    candidate_item_ids: set[int],
) -> tuple[int, ...]:
    return tuple(
        index
        for index, item_id in enumerate(vector_item_ids.tolist())
        if int(item_id) in candidate_item_ids
    )


def _seed_cypher_vectors(db: Any, item_ids: np.ndarray, matrix: np.ndarray) -> None:
    with db.transaction(route="sqlite"):
        for item_id, vector in zip(item_ids, matrix, strict=True):
            cohort = "alpha" if int(item_id) <= len(item_ids) // 2 else "beta"
            db.query(
                (
                    "CREATE (u:User {"
                    "id: $id, name: $name, cohort: $cohort, embedding: $embedding})"
                ),
                route="sqlite",
                query_type="cypher",
                params={
                    "id": int(item_id),
                    "name": f"user_{int(item_id):05d}",
                    "cohort": cohort,
                    "embedding": vector.tolist(),
                },
            )


def _print_report(report: BenchmarkReport) -> None:
    print("Vector query step benchmark")
    print(f"Rows: {report.config.rows}")
    print(f"Dimensions: {report.config.dimensions}")
    print(f"Queries per timed repetition: {report.config.queries}")
    print(f"Alpha candidate count: {report.alpha_candidate_count}")
    print()
    print("One-time stage timings")
    for name, value in report.stage_timings_ms.items():
        print(f"  {name}: {value:.3f} ms")
    print()
    print("Per-query latency summaries")
    for name, summary in report.latency_summaries_ms.items():
        print(
            f"  {name}: mean={summary.mean_ms:.3f} ms "
            f"std={summary.stdev_ms:.3f} ms "
            f"min={summary.minimum_ms:.3f} ms "
            f"max={summary.maximum_ms:.3f} ms"
        )


def run_benchmark(config: BenchmarkConfig) -> BenchmarkReport:
    item_ids, matrix, queries = _make_dataset(config)
    alpha_count = config.rows // 2
    batches = _chunked_rows(item_ids, matrix, batch_size=config.batch_size)

    sql_scope_text = "SELECT id FROM docs WHERE topic = ? ORDER BY id"
    sql_scope_params = ("alpha",)
    cypher_scope_text = "MATCH (u:User {cohort: $cohort}) RETURN u.id ORDER BY u.id"
    cypher_scope_params = {"cohort": "alpha"}

    stage_timings_ms: dict[str, float] = {}
    latency_summaries_ms: dict[str, TimingSummary] = {}

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        with HumemDB(str(tmp_path / "direct.sqlite3")) as direct_db:
            started = time.perf_counter()
            _seed_direct_vectors(direct_db, batches)
            stage_timings_ms["direct_ingest_ms"] = (
                time.perf_counter() - started
            ) * 1_000.0

            started = time.perf_counter()
            direct_db.preload_vectors()
            stage_timings_ms["direct_preload_ms"] = (
                time.perf_counter() - started
            ) * 1_000.0

            direct_item_ids, direct_matrix = load_vector_matrix(direct_db.sqlite)
            direct_index = ExactVectorIndex(
                item_ids=direct_item_ids,
                matrix=direct_matrix,
                metric="cosine",
            )
            latency_summaries_ms["direct_vector_search_only"] = _time_query_batch(
                lambda query: direct_index.search(query, top_k=config.top_k),
                queries,
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["direct_vector_query_end_to_end"] = _time_query_batch(
                lambda query: direct_db.search_vectors(query, top_k=config.top_k),
                queries,
                warmup=config.warmup,
                repetitions=config.repetitions,
            )

        with HumemDB(str(tmp_path / "sql.sqlite3")) as sql_db:
            started = time.perf_counter()
            sql_db.query(
                (
                    "CREATE TABLE docs ("
                    "id INTEGER PRIMARY KEY, "
                    "title TEXT NOT NULL, "
                    "topic TEXT NOT NULL, "
                    "embedding BLOB)"
                ),
                route="sqlite",
            )
            with sql_db.transaction(route="sqlite"):
                for batch in batches:
                    sql_rows = []
                    for item_id, vector in batch:
                        topic = "alpha" if item_id <= alpha_count else "beta"
                        sql_rows.append((item_id, f"doc_{item_id}", topic, vector))
                    sql_db.executemany(
                        (
                            "INSERT INTO docs (id, title, topic, embedding) "
                            "VALUES (?, ?, ?, ?)"
                        ),
                        sql_rows,
                        route="sqlite",
                    )
            stage_timings_ms["sql_owned_ingest_ms"] = (
                time.perf_counter() - started
            ) * 1_000.0

            started = time.perf_counter()
            sql_db.preload_vectors()
            stage_timings_ms["sql_owned_preload_ms"] = (
                time.perf_counter() - started
            ) * 1_000.0

            sql_item_ids, sql_matrix = load_vector_matrix(sql_db.sqlite)
            sql_index = ExactVectorIndex(
                item_ids=sql_item_ids,
                matrix=sql_matrix,
                metric="cosine",
            )
            sql_scope_result = sql_db.query(
                sql_scope_text,
                route="sqlite",
                params=sql_scope_params,
            )
            sql_candidate_item_ids = _candidate_item_ids_from_result(
                sql_scope_result.rows
            )
            sql_candidate_indexes = _candidate_indexes_for_item_ids(
                sql_item_ids,
                sql_candidate_item_ids,
            )

            latency_summaries_ms["sql_translate_cached"] = _time_operation(
                lambda: translate_sql(sql_scope_text, target="sqlite"),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["sql_translate_uncached"] = _time_operation(
                lambda: _uncached_translate_sql(sql_scope_text),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["sql_scope_query_only"] = _time_operation(
                lambda: sql_db.query(
                    sql_scope_text,
                    route="sqlite",
                    params=sql_scope_params,
                ),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["sql_candidate_mapping_only"] = _time_operation(
                lambda: _candidate_indexes_for_item_ids(
                    sql_item_ids,
                    sql_candidate_item_ids,
                ),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["sql_vector_search_only"] = _time_query_batch(
                lambda query: sql_index.search(
                    query,
                    top_k=config.top_k,
                    candidate_indexes=sql_candidate_indexes,
                ),
                queries,
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["sql_vector_query_end_to_end"] = _time_query_batch(
                lambda query: sql_db.query(
                    sql_scope_text,
                    route="sqlite",
                    query_type="vector",
                    params={
                        "query": query,
                        "top_k": config.top_k,
                        "scope_query_type": "sql",
                        "scope_params": sql_scope_params,
                    },
                ),
                queries,
                warmup=config.warmup,
                repetitions=config.repetitions,
            )

        with HumemDB(str(tmp_path / "cypher.sqlite3")) as cypher_db:
            started = time.perf_counter()
            _seed_cypher_vectors(cypher_db, item_ids, matrix)
            stage_timings_ms["cypher_owned_ingest_ms"] = (
                time.perf_counter() - started
            ) * 1_000.0

            started = time.perf_counter()
            cypher_db.preload_vectors()
            stage_timings_ms["cypher_owned_preload_ms"] = (
                time.perf_counter() - started
            ) * 1_000.0

            cypher_item_ids, cypher_matrix = load_vector_matrix(cypher_db.sqlite)
            cypher_index = ExactVectorIndex(
                item_ids=cypher_item_ids,
                matrix=cypher_matrix,
                metric="cosine",
            )
            cypher_plan = parse_cypher(cypher_scope_text)
            cypher_scope_result = cypher_db.query(
                cypher_scope_text,
                route="sqlite",
                query_type="cypher",
                params=cypher_scope_params,
            )
            cypher_candidate_item_ids = _candidate_item_ids_from_result(
                cypher_scope_result.rows
            )
            cypher_candidate_indexes = _candidate_indexes_for_item_ids(
                cypher_item_ids,
                cypher_candidate_item_ids,
            )

            latency_summaries_ms["cypher_parse_only"] = _time_operation(
                lambda: parse_cypher(cypher_scope_text),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["cypher_bind_compile"] = _time_operation(
                lambda: _compile_cypher_bound(cypher_plan, cypher_scope_params),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["cypher_scope_query_only"] = _time_operation(
                lambda: cypher_db.query(
                    cypher_scope_text,
                    route="sqlite",
                    query_type="cypher",
                    params=cypher_scope_params,
                ),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["cypher_candidate_mapping_only"] = _time_operation(
                lambda: _candidate_indexes_for_item_ids(
                    cypher_item_ids,
                    cypher_candidate_item_ids,
                ),
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["cypher_vector_search_only"] = _time_query_batch(
                lambda query: cypher_index.search(
                    query,
                    top_k=config.top_k,
                    candidate_indexes=cypher_candidate_indexes,
                ),
                queries,
                warmup=config.warmup,
                repetitions=config.repetitions,
            )
            latency_summaries_ms["cypher_vector_query_end_to_end"] = _time_query_batch(
                lambda query: cypher_db.query(
                    cypher_scope_text,
                    route="sqlite",
                    query_type="vector",
                    params={
                        "query": query,
                        "top_k": config.top_k,
                        "scope_query_type": "cypher",
                        "scope_params": cypher_scope_params,
                    },
                ),
                queries,
                warmup=config.warmup,
                repetitions=config.repetitions,
            )

    return BenchmarkReport(
        config=config,
        alpha_candidate_count=alpha_count,
        stage_timings_ms=stage_timings_ms,
        latency_summaries_ms=latency_summaries_ms,
    )


def main() -> None:
    args = _parse_args()
    report = run_benchmark(
        BenchmarkConfig(
            rows=args.rows,
            dimensions=args.dimensions,
            queries=args.queries,
            top_k=args.top_k,
            warmup=args.warmup,
            repetitions=args.repetitions,
            seed=args.seed,
            batch_size=args.batch_size,
        )
    )
    if args.output == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    _print_report(report)


if __name__ == "__main__":
    main()
