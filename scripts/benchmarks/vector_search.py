from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from humemdb.runtime import LANCEDB_THREADS_ENV, configure_runtime_threads_from_env

_THREAD_BUDGET = configure_runtime_threads_from_env(
    fallback_env_names=(LANCEDB_THREADS_ENV,),
)

lancedb = import_module("lancedb")
np = import_module("numpy")
pa = import_module("pyarrow")

HumemDB = import_module("humemdb").HumemDB
_vector_module = import_module("humemdb.vector")
ExactVectorIndex = _vector_module.ExactVectorIndex
ScalarQuantizedVectorIndex = _vector_module.ScalarQuantizedVectorIndex
ensure_vector_schema = _vector_module.ensure_vector_schema
insert_vectors = _vector_module.insert_vectors
load_vector_matrix = _vector_module.load_vector_matrix


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    rows: int
    dimensions: int
    queries: int
    top_k: int
    warmup: int
    repetitions: int
    metric: str
    buckets: int
    seed: int
    enable_numpy_sq8: bool = True
    lancedb_index_type: str = "IVF_PQ"
    lancedb_num_partitions: int | None = None
    lancedb_num_sub_vectors: int | None = None
    lancedb_num_bits: int = 8
    lancedb_m: int = 20
    lancedb_ef_construction: int = 300
    lancedb_sample_rate: int = 256
    lancedb_max_iterations: int = 50
    lancedb_target_partition_size: int | None = None
    lancedb_nprobes: int | None = None
    lancedb_refine_factor: int | None = None
    lancedb_ef: int | None = None
    lancedb_scalar_index_bucket: bool = False


@dataclass(frozen=True, slots=True)
class TimingSummary:
    mean: float
    stdev: float
    minimum: float
    maximum: float

    def to_dict(self) -> dict[str, float]:
        return {
            "mean": self.mean,
            "stdev": self.stdev,
            "minimum": self.minimum,
            "maximum": self.maximum,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    config: BenchmarkConfig
    row_count: int
    filtered_bucket: int
    filtered_candidate_count: int
    lancedb_thread_limit: str
    arrow_cpu_count: int
    numpy_thread_limit: int | None
    lancedb_index_settings: dict[str, Any]
    lancedb_search_settings: dict[str, Any]
    stage_timings_ms: dict[str, float]
    artifact_sizes_bytes: dict[str, int]
    latency_summaries_ms: dict[str, TimingSummary]
    recalls_at_k: dict[str, float]

    def to_dict(self) -> dict[str, Any]:
        return {
            "config": {
                "rows": self.config.rows,
                "dimensions": self.config.dimensions,
                "queries": self.config.queries,
                "top_k": self.config.top_k,
                "warmup": self.config.warmup,
                "repetitions": self.config.repetitions,
                "metric": self.config.metric,
                "buckets": self.config.buckets,
                "seed": self.config.seed,
            },
            "row_count": self.row_count,
            "filtered_bucket": self.filtered_bucket,
            "filtered_candidate_count": self.filtered_candidate_count,
            "lancedb_thread_limit": self.lancedb_thread_limit,
            "arrow_cpu_count": self.arrow_cpu_count,
            "numpy_thread_limit": self.numpy_thread_limit,
            "lancedb_index_settings": self.lancedb_index_settings,
            "lancedb_search_settings": self.lancedb_search_settings,
            "stage_timings_ms": self.stage_timings_ms,
            "artifact_sizes_bytes": self.artifact_sizes_bytes,
            "latency_summaries_ms": {
                name: summary.to_dict()
                for name, summary in self.latency_summaries_ms.items()
            },
            "recalls_at_k": self.recalls_at_k,
        }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark Phase 5 vector search across NumPy exact search, "
            "scalar-int8 search, and LanceDB flat/indexed search."
        )
    )
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--dimensions", type=int, default=384)
    parser.add_argument("--queries", type=int, default=64)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--metric", choices=("cosine", "dot", "l2"), default="cosine")
    parser.add_argument("--buckets", type=int, default=128)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--skip-numpy-sq8",
        action="store_true",
        help=(
            "Skip the scalar-int8 NumPy approximation path. Recommended for most "
            "current sweeps because NumPy SQ8 is usually slower than NumPy FP32 in "
            "this implementation."
        ),
    )
    parser.add_argument(
        "--lancedb-index-type",
        choices=(
            "IVF_FLAT",
            "IVF_SQ",
            "IVF_PQ",
            "IVF_RQ",
            "IVF_HNSW_SQ",
            "IVF_HNSW_PQ",
        ),
        default="IVF_PQ",
    )
    parser.add_argument("--lancedb-num-partitions", type=int)
    parser.add_argument("--lancedb-num-sub-vectors", type=int)
    parser.add_argument("--lancedb-num-bits", type=int, default=8)
    parser.add_argument("--lancedb-m", type=int, default=20)
    parser.add_argument("--lancedb-ef-construction", type=int, default=300)
    parser.add_argument("--lancedb-sample-rate", type=int, default=256)
    parser.add_argument("--lancedb-max-iterations", type=int, default=50)
    parser.add_argument("--lancedb-target-partition-size", type=int)
    parser.add_argument("--lancedb-nprobes", type=int)
    parser.add_argument("--lancedb-refine-factor", type=int)
    parser.add_argument("--lancedb-ef", type=int)
    parser.add_argument(
        "--lancedb-scalar-index-bucket",
        action="store_true",
        help="Create a scalar index on the bucket column for prefiltered searches.",
    )
    parser.add_argument("--output", choices=("text", "json"), default="text")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    config = BenchmarkConfig(
        rows=args.rows,
        dimensions=args.dimensions,
        queries=args.queries,
        top_k=args.top_k,
        warmup=args.warmup,
        repetitions=args.repetitions,
        metric=args.metric,
        buckets=args.buckets,
        seed=args.seed,
        enable_numpy_sq8=not args.skip_numpy_sq8,
        lancedb_index_type=args.lancedb_index_type,
        lancedb_num_partitions=args.lancedb_num_partitions,
        lancedb_num_sub_vectors=args.lancedb_num_sub_vectors,
        lancedb_num_bits=args.lancedb_num_bits,
        lancedb_m=args.lancedb_m,
        lancedb_ef_construction=args.lancedb_ef_construction,
        lancedb_sample_rate=args.lancedb_sample_rate,
        lancedb_max_iterations=args.lancedb_max_iterations,
        lancedb_target_partition_size=args.lancedb_target_partition_size,
        lancedb_nprobes=args.lancedb_nprobes,
        lancedb_refine_factor=args.lancedb_refine_factor,
        lancedb_ef=args.lancedb_ef,
        lancedb_scalar_index_bucket=args.lancedb_scalar_index_bucket,
    )
    report = run_benchmark(config)
    if args.output == "json":
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
        return
    _print_report(report)


def run_benchmark(config: BenchmarkConfig) -> BenchmarkReport:
    rng = np.random.default_rng(config.seed)
    thread_budget = _THREAD_BUDGET
    lancedb_thread_limit = (
        str(thread_budget.thread_count)
        if thread_budget.thread_count is not None
        else "default"
    )
    arrow_cpu_count = (
        thread_budget.arrow_cpu_count
        if thread_budget.arrow_cpu_count is not None
        else int(pa.cpu_count())
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = Path(tmpdir) / "humem.sqlite3"
        duckdb_path = Path(tmpdir) / "humem.duckdb"
        lance_path = Path(tmpdir) / "humem.lancedb"

        with HumemDB(str(sqlite_path), str(duckdb_path)) as db:
            started = time.perf_counter()
            rows = _seed_sqlite_vectors(
                db,
                rows=config.rows,
                dimensions=config.dimensions,
                buckets=config.buckets,
                rng=rng,
            )
            sqlite_seed_ms = (time.perf_counter() - started) * 1000.0

            started = time.perf_counter()
            item_ids, bucket_ids, matrix = load_vector_matrix(
                db.sqlite,
                collection="default",
            )
            sqlite_load_ms = (time.perf_counter() - started) * 1000.0

        queries = _make_queries(
            matrix,
            count=config.queries,
            metric=config.metric,
            rng=rng,
        )

        started = time.perf_counter()
        exact_index = ExactVectorIndex(
            item_ids=item_ids,
            matrix=matrix,
            metric=config.metric,
        )
        numpy_exact_build_ms = (time.perf_counter() - started) * 1000.0

        sq8_index = None
        numpy_sq8_build_ms = 0.0
        if config.enable_numpy_sq8:
            started = time.perf_counter()
            sq8_index = ScalarQuantizedVectorIndex.from_matrix(
                item_ids=item_ids,
                matrix=matrix,
                metric=config.metric,
            )
            numpy_sq8_build_ms = (time.perf_counter() - started) * 1000.0

        bucket_value = int(bucket_ids[len(bucket_ids) // 3])
        filtered_candidates = np.flatnonzero(bucket_ids == bucket_value)

        exact_truth = _exact_truth(
            exact_index,
            queries,
            top_k=config.top_k,
            filtered_candidates=filtered_candidates,
        )

        started = time.perf_counter()
        lance_table = _seed_lancedb_table(
            lance_path=lance_path,
            item_ids=item_ids,
            bucket_ids=bucket_ids,
            matrix=matrix,
        )
        lancedb_table_create_ms = (time.perf_counter() - started) * 1000.0

        lancedb_index_build_ms = _build_lancedb_index(
            lance_table,
            config=config,
        )
        lancedb_scalar_index_build_ms = _build_lancedb_scalar_index(
            lance_table,
            enabled=config.lancedb_scalar_index_bucket,
        )

        summaries = {
            "numpy_f32_global": _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: exact_index.search(query, top_k=config.top_k),
                queries=queries,
            ),
            "numpy_f32_filtered": _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: exact_index.search(
                    query,
                    top_k=config.top_k,
                    candidate_indexes=filtered_candidates,
                ),
                queries=queries,
            ),
            "lancedb_flat_global": _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: _search_lancedb_flat(
                    lance_table,
                    query,
                    top_k=config.top_k,
                    metric=config.metric,
                ),
                queries=queries,
            ),
            "lancedb_flat_filtered": _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: _search_lancedb_flat(
                    lance_table,
                    query,
                    top_k=config.top_k,
                    metric=config.metric,
                    bucket_value=bucket_value,
                ),
                queries=queries,
            ),
            "lancedb_indexed_default_global": _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: _search_lancedb_indexed(
                    lance_table,
                    query,
                    config=config,
                ),
                queries=queries,
            ),
            "lancedb_indexed_default_filtered": _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: _search_lancedb_indexed(
                    lance_table,
                    query,
                    config=config,
                    bucket_value=bucket_value,
                ),
                queries=queries,
            ),
        }
        if sq8_index is not None:
            summaries["numpy_sq8_global"] = _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: sq8_index.search(query, top_k=config.top_k),
                queries=queries,
            )
            summaries["numpy_sq8_filtered"] = _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: sq8_index.search(
                    query,
                    top_k=config.top_k,
                    candidate_indexes=filtered_candidates,
                ),
                queries=queries,
            )

        recalls = {
            "lancedb_flat_global": _recall_at_k(
                exact_truth["global"],
                [
                    _search_lancedb_flat(
                        lance_table,
                        query,
                        top_k=config.top_k,
                        metric=config.metric,
                    )
                    for query in queries
                ],
            ),
            "lancedb_flat_filtered": _recall_at_k(
                exact_truth["filtered"],
                [
                    _search_lancedb_flat(
                        lance_table,
                        query,
                        top_k=config.top_k,
                        metric=config.metric,
                        bucket_value=bucket_value,
                    )
                    for query in queries
                ],
            ),
            "lancedb_indexed_default_global": _recall_at_k(
                exact_truth["global"],
                [
                    _search_lancedb_indexed(
                        lance_table,
                        query,
                        config=config,
                    )
                    for query in queries
                ],
            ),
            "lancedb_indexed_default_filtered": _recall_at_k(
                exact_truth["filtered"],
                [
                    _search_lancedb_indexed(
                        lance_table,
                        query,
                        config=config,
                        bucket_value=bucket_value,
                    )
                    for query in queries
                ],
            ),
        }
        if sq8_index is not None:
            recalls["numpy_sq8_global"] = _recall_at_k(
                exact_truth["global"],
                [
                    _match_ids(sq8_index.search(query, top_k=config.top_k))
                    for query in queries
                ],
            )
            recalls["numpy_sq8_filtered"] = _recall_at_k(
                exact_truth["filtered"],
                [
                    _match_ids(
                        sq8_index.search(
                            query,
                            top_k=config.top_k,
                            candidate_indexes=filtered_candidates,
                        )
                    )
                    for query in queries
                ],
            )

    stage_timings_ms = {
        "sqlite_seed": sqlite_seed_ms,
        "sqlite_load_to_numpy": sqlite_load_ms,
        "numpy_f32_build": numpy_exact_build_ms,
        "numpy_sq8_build": numpy_sq8_build_ms,
        "lancedb_table_create": lancedb_table_create_ms,
        "lancedb_index_build": lancedb_index_build_ms,
        "lancedb_scalar_index_build": lancedb_scalar_index_build_ms,
    }
    artifact_sizes_bytes = {
        "numpy_f32_matrix": int(matrix.nbytes),
        "query_batch_f32": int(sum(query.nbytes for query in queries)),
    }
    if sq8_index is not None:
        artifact_sizes_bytes["numpy_sq8_quantized"] = int(sq8_index.quantized.nbytes)
        artifact_sizes_bytes["numpy_sq8_scales"] = int(sq8_index.scales.nbytes)
    return BenchmarkReport(
        config=config,
        row_count=rows,
        filtered_bucket=bucket_value,
        filtered_candidate_count=int(filtered_candidates.size),
        lancedb_thread_limit=lancedb_thread_limit,
        arrow_cpu_count=arrow_cpu_count,
        numpy_thread_limit=thread_budget.numpy_thread_limit,
        lancedb_index_settings=_lancedb_index_settings(config),
        lancedb_search_settings=_lancedb_search_settings(config),
        stage_timings_ms=stage_timings_ms,
        artifact_sizes_bytes=artifact_sizes_bytes,
        latency_summaries_ms=summaries,
        recalls_at_k=recalls,
    )


def _print_report(report: BenchmarkReport) -> None:
    print("Vector benchmark configuration")
    print(f"  rows: {report.row_count}")
    print(f"  dimensions: {report.config.dimensions}")
    print(f"  metric: {report.config.metric}")
    print(f"  queries: {report.config.queries}")
    print(f"  top_k: {report.config.top_k}")
    print(f"  buckets: {report.config.buckets}")
    print(f"  filtered_bucket: {report.filtered_bucket}")
    print(f"  filtered_candidate_count: {report.filtered_candidate_count}")
    print(f"  lancedb_thread_limit: {report.lancedb_thread_limit}")
    print(f"  arrow_cpu_count: {report.arrow_cpu_count}")
    print(f"  numpy_thread_limit: {report.numpy_thread_limit}")
    print(f"  lancedb_index_settings: {report.lancedb_index_settings}")
    print(f"  lancedb_search_settings: {report.lancedb_search_settings}")
    print()
    print("Stage timings (ms)")
    for name, value in report.stage_timings_ms.items():
        print(f"  {name}: {value:.2f}")
    print()
    print("Artifact sizes (bytes)")
    for name, value in report.artifact_sizes_bytes.items():
        print(f"  {name}: {value}")
    print()
    print("Latency summaries (ms)")
    for name, summary in report.latency_summaries_ms.items():
        print(
            f"  {name}: mean={summary.mean:.2f} stdev={summary.stdev:.2f} "
            f"min={summary.minimum:.2f} max={summary.maximum:.2f}"
        )
    print()
    print("Recall@k versus NumPy float32 exact")
    for name, recall in report.recalls_at_k.items():
        print(f"  {name}: {recall:.4f}")


def _seed_sqlite_vectors(
    db: HumemDB,
    *,
    rows: int,
    dimensions: int,
    buckets: int,
    rng: np.random.Generator,
) -> int:
    matrix = rng.normal(size=(rows, dimensions)).astype(np.float32)
    matrix = matrix / np.linalg.norm(matrix, axis=1, keepdims=True)
    bucket_ids = rng.integers(0, buckets, size=rows, dtype=np.int32)

    ensure_vector_schema(db.sqlite)
    with db.transaction(route="sqlite"):
        insert_vectors(
            db.sqlite,
            [
                (index + 1, "default", int(bucket_ids[index]), matrix[index])
                for index in range(rows)
            ],
        )
    return rows


def _make_queries(
    matrix: np.ndarray,
    *,
    count: int,
    metric: str,
    rng: np.random.Generator,
) -> list[np.ndarray]:
    selected = rng.choice(matrix.shape[0], size=count, replace=False)
    queries = [np.array(matrix[index], copy=True) for index in selected]
    if metric == "cosine":
        return queries
    noise = rng.normal(scale=0.01, size=(count, matrix.shape[1])).astype(np.float32)
    return [queries[index] + noise[index] for index in range(count)]


def _seed_lancedb_table(
    *,
    lance_path: Path,
    item_ids: np.ndarray,
    bucket_ids: np.ndarray,
    matrix: np.ndarray,
):
    db = lancedb.connect(str(lance_path))
    schema = pa.schema(
        [
            pa.field("item_id", pa.int64()),
            pa.field("bucket", pa.int32()),
            pa.field("vector", pa.list_(pa.float32(), matrix.shape[1])),
        ]
    )
    table = pa.table(
        {
            "item_id": pa.array(item_ids.tolist(), type=pa.int64()),
            "bucket": pa.array(bucket_ids.tolist(), type=pa.int32()),
            "vector": pa.array(
                matrix.tolist(),
                type=pa.list_(pa.float32(), matrix.shape[1]),
            ),
        },
        schema=schema,
    )
    return db.create_table("vectors", data=table, mode="overwrite")


def _build_lancedb_index(
    table,
    *,
    config: BenchmarkConfig,
) -> float:
    started = time.perf_counter()
    table.create_index(**_lancedb_index_kwargs(config))
    return (time.perf_counter() - started) * 1000.0


def _build_lancedb_scalar_index(
    table,
    *,
    enabled: bool,
) -> float:
    if not enabled:
        return 0.0
    started = time.perf_counter()
    table.create_scalar_index("bucket")
    return (time.perf_counter() - started) * 1000.0


def _search_lancedb_flat(
    table,
    query: np.ndarray,
    *,
    top_k: int,
    metric: str,
    bucket_value: int | None = None,
) -> tuple[int, ...]:
    builder = (
        table.search(query)
        .distance_type(metric)
        .limit(top_k)
        .bypass_vector_index()
    )
    if bucket_value is not None:
        builder = builder.where(f"bucket = {bucket_value}", prefilter=True)
    result = builder.select(["item_id", "_distance"]).to_list()
    return tuple(int(row["item_id"]) for row in result)


def _search_lancedb_indexed(
    table,
    query: np.ndarray,
    *,
    config: BenchmarkConfig,
    bucket_value: int | None = None,
) -> tuple[int, ...]:
    builder = (
        table.search(query)
        .distance_type(config.metric)
        .limit(config.top_k)
    )
    if config.lancedb_nprobes is not None:
        builder = builder.nprobes(config.lancedb_nprobes)
    if config.lancedb_refine_factor is not None:
        builder = builder.refine_factor(config.lancedb_refine_factor)
    if config.lancedb_ef is not None:
        builder = builder.ef(config.lancedb_ef)
    if bucket_value is not None:
        builder = builder.where(f"bucket = {bucket_value}", prefilter=True)
    result = builder.select(["item_id", "_distance"]).to_list()
    return tuple(int(row["item_id"]) for row in result)


def _lancedb_index_kwargs(config: BenchmarkConfig) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "metric": config.metric,
        "vector_column_name": "vector",
        "index_type": config.lancedb_index_type,
        "num_bits": config.lancedb_num_bits,
        "max_iterations": config.lancedb_max_iterations,
        "sample_rate": config.lancedb_sample_rate,
        "m": config.lancedb_m,
        "ef_construction": config.lancedb_ef_construction,
    }
    if config.lancedb_num_partitions is not None:
        kwargs["num_partitions"] = config.lancedb_num_partitions
    if config.lancedb_num_sub_vectors is not None:
        kwargs["num_sub_vectors"] = config.lancedb_num_sub_vectors
    if config.lancedb_target_partition_size is not None:
        kwargs["target_partition_size"] = config.lancedb_target_partition_size
    return kwargs


def _lancedb_index_settings(config: BenchmarkConfig) -> dict[str, Any]:
    return {
        "index_type": config.lancedb_index_type,
        "num_partitions": config.lancedb_num_partitions,
        "num_sub_vectors": config.lancedb_num_sub_vectors,
        "num_bits": config.lancedb_num_bits,
        "m": config.lancedb_m,
        "ef_construction": config.lancedb_ef_construction,
        "sample_rate": config.lancedb_sample_rate,
        "max_iterations": config.lancedb_max_iterations,
        "target_partition_size": config.lancedb_target_partition_size,
        "scalar_index_bucket": config.lancedb_scalar_index_bucket,
    }


def _lancedb_search_settings(config: BenchmarkConfig) -> dict[str, Any]:
    return {
        "nprobes": config.lancedb_nprobes,
        "refine_factor": config.lancedb_refine_factor,
        "ef": config.lancedb_ef,
    }


def _exact_truth(
    exact_index: ExactVectorIndex,
    queries: list[np.ndarray],
    *,
    top_k: int,
    filtered_candidates: np.ndarray,
) -> dict[str, list[tuple[int, ...]]]:
    return {
        "global": [
            tuple(match.item_id for match in exact_index.search(query, top_k=top_k))
            for query in queries
        ],
        "filtered": [
            tuple(
                match.item_id
                for match in exact_index.search(
                    query,
                    top_k=top_k,
                    candidate_indexes=filtered_candidates,
                )
            )
            for query in queries
        ],
    }


def _time_callable(
    *,
    warmup: int,
    repetitions: int,
    runner,
    queries: list[np.ndarray],
) -> TimingSummary:
    for _ in range(warmup):
        for query in queries:
            runner(query)

    timings = []
    for _ in range(repetitions):
        started = time.perf_counter()
        for query in queries:
            runner(query)
        timings.append((time.perf_counter() - started) * 1000.0 / len(queries))

    return TimingSummary(
        mean=statistics.mean(timings),
        stdev=statistics.stdev(timings) if len(timings) > 1 else 0.0,
        minimum=min(timings),
        maximum=max(timings),
    )


def _recall_at_k(
    expected: list[tuple[int, ...]],
    actual: list[tuple[int, ...]],
) -> float:
    matched = 0
    total = 0
    for expected_ids, actual_ids in zip(expected, actual, strict=True):
        expected_set = set(expected_ids)
        actual_set = set(actual_ids)
        matched += len(expected_set & actual_set)
        total += len(expected_ids)
    return matched / total if total else 0.0


def _match_ids(matches) -> tuple[int, ...]:
    return tuple(int(match.item_id) for match in matches)


if __name__ == "__main__":
    main()






