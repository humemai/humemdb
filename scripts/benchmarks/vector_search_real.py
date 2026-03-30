from __future__ import annotations

import argparse
import json
import resource
import statistics
import tempfile
import time
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Literal

from humemdb import HumemDB
from humemdb.runtime import LANCEDB_THREADS_ENV, configure_runtime_threads_from_env
from humemdb.vector import _ExactVectorIndex, _ensure_vector_schema, _load_vector_matrix

_THREAD_BUDGET = configure_runtime_threads_from_env(
    fallback_env_names=(LANCEDB_THREADS_ENV,),
)

lancedb = import_module("lancedb")
np = import_module("numpy")
pa = import_module("pyarrow")


_MIN_BENCHMARK_QUERIES = 100
_MIN_BENCHMARK_REPETITIONS = 3
_DEFAULT_NUMPY_EXACT_MAX_ROWS = 100_000
_SQLITE_VECTOR_TARGET = "direct"
_SQLITE_VECTOR_NAMESPACE = ""
_SQLITE_GROUP_TABLE = "benchmark_vector_groups"
_SQLITE_SEED_BATCH_SIZE = 10_000


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    dataset: str
    rows: int
    queries: int
    top_k: int
    warmup: int
    repetitions: int
    metric: Literal["cosine", "dot", "l2"]
    seed: int
    filter_source: str | None = None
    top_k_grid: tuple[int, ...] | None = None
    sample_mode: str = "auto"
    batch_size: int = 100_000
    lancedb_index_type: str = "IVF_PQ"
    lancedb_num_partitions: int | None = None
    lancedb_num_sub_vectors: int | None = None
    lancedb_num_bits: int = 8
    lancedb_sample_rate: int = 256
    lancedb_max_iterations: int = 50
    lancedb_target_partition_size: int | None = None
    lancedb_nprobes: int | None = None
    lancedb_refine_factor: int | None = None
    lancedb_scalar_index_prefilter: bool = False
    numpy_exact_max_rows: int | None = _DEFAULT_NUMPY_EXACT_MAX_ROWS


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
class DatasetInfo:
    name: str
    meta_path: Path
    count: int
    dimensions: int
    group_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PackagedGroundTruth:
    gt_path: Path
    query_ids: Any
    neighbors_by_query_id: dict[int, tuple[int, ...]]


@dataclass(frozen=True, slots=True)
class SelectedRange:
    row_start: int
    shard_path: Path
    shard_count: int
    offset: int
    count: int
    group_id: int
    group_name: str
    shard_global_start: int = 0


_MEMORY_SNAPSHOT_ORDER = (
    "start",
    "after_subset_plan",
    "after_query_selection",
    "after_filter_candidate_selection",
    "after_dataset_load",
    "after_sqlite_seed",
    "after_sqlite_exact_load",
    "after_numpy_query_materialize",
    "after_numpy_exact_build",
    "after_lancedb_table_create",
    "after_lancedb_ingest_first_batch",
    "after_lancedb_ingest_peak",
    "after_lancedb_ingest_complete",
    "after_duckdb_arrow_export",
    "after_lancedb_index_build",
    "after_lancedb_scalar_index_build",
    "after_numpy_exact_search",
    "after_lancedb_indexed_search",
    "after_query_and_recall",
    "final",
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the fixed hot-tier NumPy exact path and cold-tier LanceDB "
            "IVF_PQ path on real vector datasets."
        )
    )
    parser.add_argument(
        "--dataset",
        choices=("msmarco-10m", "stackoverflow-xlarge"),
        required=True,
    )
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--queries", type=int, default=_MIN_BENCHMARK_QUERIES)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument(
        "--top-k-grid",
        default=None,
        help=(
            "Optional comma-separated top-k grid. When provided, the benchmark "
            "reuses one SQLite/NumPy/LanceDB build across all requested top-k "
            "values."
        ),
    )
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--metric", choices=("cosine", "dot", "l2"), default="cosine")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--sample-mode",
        choices=("auto", "prefix", "stratified"),
        default="auto",
        help=(
            "How to choose the dataset subset. 'auto' uses stratified sampling for "
            "multi-corpus datasets and prefix loading otherwise."
        ),
    )
    parser.add_argument(
        "--filter-source",
        choices=("questions", "answers", "comments"),
        default=None,
        help="Optional real metadata-backed filter for stackoverflow-xlarge.",
    )
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument(
        "--lancedb-index-type",
        choices=("IVF_PQ",),
        default="IVF_PQ",
    )
    parser.add_argument("--lancedb-num-partitions", type=int)
    parser.add_argument("--lancedb-num-sub-vectors", type=int)
    parser.add_argument("--lancedb-num-bits", type=int, default=8)
    parser.add_argument("--lancedb-sample-rate", type=int, default=256)
    parser.add_argument("--lancedb-max-iterations", type=int, default=50)
    parser.add_argument("--lancedb-target-partition-size", type=int)
    parser.add_argument("--lancedb-nprobes", type=int)
    parser.add_argument("--lancedb-refine-factor", type=int)
    parser.add_argument(
        "--lancedb-scalar-index-prefilter",
        action="store_true",
        help="Create a scalar index on the metadata-backed filter column.",
    )
    parser.add_argument(
        "--numpy-exact-max-rows",
        type=int,
        default=_DEFAULT_NUMPY_EXACT_MAX_ROWS,
        help=(
            "Maximum row count where full NumPy exact search runs. "
            "Use 0 or a negative value to disable this cutoff."
        ),
    )
    parser.add_argument("--output", choices=("text", "json"), default="text")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    _validate_benchmark_sampling(args.queries, args.repetitions)
    if args.dataset == "msmarco-10m" and args.filter_source is not None:
        raise ValueError("--filter-source is only supported for stackoverflow-xlarge.")
    top_k_grid = _parse_optional_int_grid(args.top_k_grid)
    config = BenchmarkConfig(
        dataset=args.dataset,
        rows=args.rows,
        queries=args.queries,
        top_k=args.top_k,
        top_k_grid=top_k_grid,
        warmup=args.warmup,
        repetitions=args.repetitions,
        metric=args.metric,
        seed=args.seed,
        filter_source=args.filter_source,
        sample_mode=args.sample_mode,
        batch_size=args.batch_size,
        lancedb_index_type=args.lancedb_index_type,
        lancedb_num_partitions=args.lancedb_num_partitions,
        lancedb_num_sub_vectors=args.lancedb_num_sub_vectors,
        lancedb_num_bits=args.lancedb_num_bits,
        lancedb_sample_rate=args.lancedb_sample_rate,
        lancedb_max_iterations=args.lancedb_max_iterations,
        lancedb_target_partition_size=args.lancedb_target_partition_size,
        lancedb_nprobes=args.lancedb_nprobes,
        lancedb_refine_factor=args.lancedb_refine_factor,
        lancedb_scalar_index_prefilter=args.lancedb_scalar_index_prefilter,
        numpy_exact_max_rows=(
            args.numpy_exact_max_rows if args.numpy_exact_max_rows > 0 else None
        ),
    )
    report = run_benchmark(config)
    if args.output == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    _print_report(report)


def run_benchmark(config: BenchmarkConfig) -> dict[str, Any]:
    dataset = _dataset_info(config.dataset)
    if config.rows > dataset.count:
        raise ValueError(
            f"Requested rows={config.rows} exceeds dataset count={dataset.count}."
        )
    if (
        config.filter_source is not None
        and config.filter_source not in dataset.group_names
    ):
        raise ValueError(
            "Filter source "
            f"{config.filter_source!r} is not available for {dataset.name}."
        )

    rng = np.random.default_rng(config.seed)
    memory_snapshots_bytes: dict[str, int | None] = {
        "start": _current_rss_bytes(),
    }
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

    numpy_exact_enabled = (
        config.numpy_exact_max_rows is None
        or config.rows <= config.numpy_exact_max_rows
    )
    top_k_values = config.top_k_grid or (config.top_k,)

    started = time.perf_counter()
    selected_ranges, group_ids, group_lookup = _plan_dataset_subset(
        meta_path=dataset.meta_path,
        rows=config.rows,
        sample_mode=config.sample_mode,
    )
    subset_plan_ms = (time.perf_counter() - started) * 1000.0
    _snapshot_memory(memory_snapshots_bytes, "after_subset_plan")

    local_to_global_item_ids = None
    packaged_ground_truth = None
    eligible_query_indexes = None
    effective_queries = int(config.queries)
    if not numpy_exact_enabled:
        local_to_global_item_ids = _local_to_global_item_ids(
            selected_ranges=selected_ranges,
            rows=config.rows,
        )
        packaged_ground_truth = _load_packaged_ground_truth(dataset.meta_path)
        if packaged_ground_truth is None:
            raise ValueError(
                "NumPy exact is disabled for this run, but the dataset does not "
                "provide packaged ground-truth queries."
            )
        eligible_query_indexes = _eligible_packaged_gt_query_indexes(
            local_to_global_item_ids=local_to_global_item_ids,
            packaged_ground_truth=packaged_ground_truth,
        )
        if eligible_query_indexes.size == 0:
            raise ValueError(
                "No packaged ground-truth queries fall inside the sampled subset."
            )
        effective_queries = min(effective_queries, int(eligible_query_indexes.size))

    started = time.perf_counter()
    query_indexes = _query_indexes(
        group_ids=group_ids,
        group_lookup=group_lookup,
        filter_source=config.filter_source,
        queries=effective_queries,
        rng=rng,
        eligible_indexes=eligible_query_indexes,
    )
    query_selection_ms = (time.perf_counter() - started) * 1000.0
    _snapshot_memory(memory_snapshots_bytes, "after_query_selection")

    filtered_candidates = None
    filter_group_id = None
    filter_candidate_selection_ms = None
    if config.filter_source is not None:
        started = time.perf_counter()
        filter_group_id = _group_id_for_name(group_lookup, config.filter_source)
        filtered_candidates = np.flatnonzero(group_ids == filter_group_id)
        filter_candidate_selection_ms = (time.perf_counter() - started) * 1000.0
    _snapshot_memory(memory_snapshots_bytes, "after_filter_candidate_selection")

    matrix = None
    item_ids = None
    exact_index = None
    numpy_exact_build_ms = None
    sqlite_exact_load_ms = None
    sqlite_seed_ms = None
    numpy_query_materialize_ms = None

    with tempfile.TemporaryDirectory() as tmpdir:
        lance_path = Path(tmpdir) / "humem-real.lancedb"
        lancedb_ingest_stats: dict[str, float] = {}

        if numpy_exact_enabled:
            base_path = Path(tmpdir) / "humem-real"

            with HumemDB(base_path) as db:
                started = time.perf_counter()
                _seed_sqlite_subset(
                    db,
                    selected_ranges=selected_ranges,
                    dimensions=dataset.dimensions,
                )
                sqlite_seed_ms = (time.perf_counter() - started) * 1000.0
                dataset_load_ms = sqlite_seed_ms
                _snapshot_memory(memory_snapshots_bytes, "after_dataset_load")
                _snapshot_memory(memory_snapshots_bytes, "after_sqlite_seed")

                started = time.perf_counter()
                item_ids, matrix = _load_vector_matrix(db._sqlite)
                sqlite_exact_load_ms = (time.perf_counter() - started) * 1000.0
                _snapshot_memory(memory_snapshots_bytes, "after_sqlite_exact_load")

                started = time.perf_counter()
                queries = [
                    np.array(matrix[index], copy=True)
                    for index in query_indexes
                ]
                numpy_query_materialize_ms = (time.perf_counter() - started) * 1000.0
                _snapshot_memory(
                    memory_snapshots_bytes,
                    "after_numpy_query_materialize",
                )

                if matrix is not None and item_ids is not None:
                    started = time.perf_counter()
                    exact_index = _ExactVectorIndex(
                        item_ids=item_ids,
                        matrix=matrix,
                        metric=config.metric,
                    )
                    numpy_exact_build_ms = (time.perf_counter() - started) * 1000.0
                _snapshot_memory(memory_snapshots_bytes, "after_numpy_exact_build")

                lance_table = _seed_lancedb_table(
                    lance_path=lance_path,
                    dimensions=dataset.dimensions,
                    batch_size=config.batch_size,
                    duckdb=db._duckdb,
                    ingest_stats=lancedb_ingest_stats,
                    memory_snapshots_bytes=memory_snapshots_bytes,
                )
        else:
            dataset_load_ms = 0.0
            _snapshot_memory(memory_snapshots_bytes, "after_dataset_load")
            queries = _load_query_vectors(
                selected_ranges=selected_ranges,
                query_indexes=query_indexes,
                dimensions=dataset.dimensions,
            )
            _snapshot_memory(memory_snapshots_bytes, "after_sqlite_exact_load")
            _snapshot_memory(memory_snapshots_bytes, "after_numpy_query_materialize")
            _snapshot_memory(memory_snapshots_bytes, "after_numpy_exact_build")
            lance_table = _seed_lancedb_table_from_selected_ranges(
                lance_path=lance_path,
                selected_ranges=selected_ranges,
                dimensions=dataset.dimensions,
                batch_size=config.batch_size,
                ingest_stats=lancedb_ingest_stats,
                memory_snapshots_bytes=memory_snapshots_bytes,
            )

        lancedb_table_create_ms = lancedb_ingest_stats.get("table_create_ms")
        duckdb_arrow_export_ms = lancedb_ingest_stats.get("duckdb_arrow_export_ms")
        cold_tier_export_ms = lancedb_ingest_stats.get("cold_tier_export_ms")
        if "after_lancedb_table_create" not in memory_snapshots_bytes:
            _snapshot_memory(memory_snapshots_bytes, "after_lancedb_table_create")
        if "after_duckdb_arrow_export" not in memory_snapshots_bytes:
            _snapshot_memory(memory_snapshots_bytes, "after_duckdb_arrow_export")

        lancedb_index_build_ms = _build_lancedb_index(lance_table, config=config)
        _snapshot_memory(memory_snapshots_bytes, "after_lancedb_index_build")
        lancedb_scalar_index_build_ms = _build_lancedb_scalar_index(
            lance_table,
            enabled=(
                config.lancedb_scalar_index_prefilter
                and filter_group_id is not None
            ),
        )
        _snapshot_memory(memory_snapshots_bytes, "after_lancedb_scalar_index_build")

        summaries: dict[str, TimingSummary] = {}
        top_k_reports = []
        for top_k in top_k_values:
            summaries, recalls = _run_top_k_benchmark(
                top_k=top_k,
                config=config,
                queries=queries,
                exact_index=exact_index,
                query_indexes=query_indexes,
                filtered_candidates=filtered_candidates,
                filter_group_id=filter_group_id,
                lance_table=lance_table,
                local_to_global_item_ids=local_to_global_item_ids,
                group_ids=group_ids,
                packaged_ground_truth=packaged_ground_truth,
            )
            if "after_numpy_exact_search" not in memory_snapshots_bytes:
                _snapshot_memory(memory_snapshots_bytes, "after_numpy_exact_search")
            _snapshot_memory(memory_snapshots_bytes, "after_lancedb_indexed_search")
            _snapshot_memory(memory_snapshots_bytes, "after_query_and_recall")
            top_k_reports.append(
                {
                    "top_k": int(top_k),
                    "latency_summaries_ms": {
                        name: summary.to_dict()
                        for name, summary in summaries.items()
                    },
                    "recalls_at_k": recalls,
                }
            )

    stage_timings_ms = {
        "subset_plan": subset_plan_ms,
        "query_selection": query_selection_ms,
        "filter_candidate_selection": filter_candidate_selection_ms,
        "dataset_load": dataset_load_ms,
        "sqlite_seed": sqlite_seed_ms,
        "sqlite_exact_load": sqlite_exact_load_ms,
        "numpy_query_materialize": numpy_query_materialize_ms,
        "numpy_f32_build": numpy_exact_build_ms,
        "lancedb_table_create": lancedb_table_create_ms,
        "duckdb_arrow_export": duckdb_arrow_export_ms,
        "cold_tier_export": cold_tier_export_ms,
        "lancedb_index_build": lancedb_index_build_ms,
        "lancedb_scalar_index_build": lancedb_scalar_index_build_ms,
    }
    artifact_sizes_bytes = {
        "numpy_f32_matrix": 0 if matrix is None else int(matrix.nbytes),
        "query_batch_f32": int(sum(query.nbytes for query in queries)),
    }
    memory_snapshots_bytes["final"] = _current_rss_bytes()
    memory_stage_deltas_bytes = _memory_stage_deltas(memory_snapshots_bytes)
    base_report = {
        "dataset": dataset.name,
        "rows": int(config.rows),
        "dimensions": int(dataset.dimensions),
        "metric": config.metric,
        "requested_queries": int(config.queries),
        "queries": len(queries),
        "filter_source": config.filter_source,
        "numpy_exact_enabled": numpy_exact_enabled,
        "numpy_exact_max_rows": config.numpy_exact_max_rows,
        "sample_mode": _resolved_sample_mode(
            config.sample_mode,
            group_count=len(dataset.group_names),
        ),
        "filtered_candidate_count": (
            int(filtered_candidates.size) if filtered_candidates is not None else None
        ),
        "available_filter_sources": list(dataset.group_names),
        "ground_truth_source": (
            "numpy_exact"
            if numpy_exact_enabled
            else "packaged_gt_subset_filtered"
        ),
        "cold_tier_ingest_path": (
            "SQLite -> DuckDB (scan) -> Arrow batches -> LanceDB -> build index"
            if numpy_exact_enabled
            else "Selected shard memmaps -> Arrow batches -> LanceDB -> build index"
        ),
        "lancedb_thread_limit": lancedb_thread_limit,
        "arrow_cpu_count": arrow_cpu_count,
        "numpy_thread_limit": thread_budget.numpy_thread_limit,
        "lancedb_index_settings": _lancedb_index_settings(config),
        "lancedb_search_settings": _lancedb_search_settings(config),
        "stage_timings_ms": stage_timings_ms,
        "artifact_sizes_bytes": artifact_sizes_bytes,
        "numpy_exact_profile": {
            "matrix_rows": 0 if matrix is None else int(matrix.shape[0]),
            "query_count": len(queries),
            "query_vector_dimensions": int(dataset.dimensions),
            "matrix_bytes": artifact_sizes_bytes["numpy_f32_matrix"],
            "query_batch_bytes": artifact_sizes_bytes["query_batch_f32"],
        },
        "lancedb_ingest_profile": {
            "batch_count": lancedb_ingest_stats.get("batch_count"),
            "first_batch_rows": lancedb_ingest_stats.get("first_batch_rows"),
            "last_batch_rows": lancedb_ingest_stats.get("last_batch_rows"),
            "max_batch_rows": lancedb_ingest_stats.get("max_batch_rows"),
            "peak_batch_rss_bytes": lancedb_ingest_stats.get("peak_batch_rss_bytes"),
        },
        "memory_snapshots_bytes": memory_snapshots_bytes,
        "memory_stage_deltas_bytes": memory_stage_deltas_bytes,
        "memory_peak_rss_bytes": _peak_rss_bytes(),
    }
    return _finalize_top_k_reports(
        base_report=base_report,
        top_k_reports=top_k_reports,
    )


def _parse_optional_int_grid(raw: str | None) -> tuple[int, ...] | None:
    if raw is None:
        return None
    parts = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
    if not parts:
        raise ValueError("--top-k-grid must contain at least one integer.")
    return parts


def _run_top_k_benchmark(
    *,
    top_k: int,
    config: BenchmarkConfig,
    queries: list[Any],
    exact_index,
    query_indexes,
    filtered_candidates,
    filter_group_id: int | None,
    lance_table,
    local_to_global_item_ids,
    group_ids,
    packaged_ground_truth: PackagedGroundTruth | None,
) -> tuple[dict[str, TimingSummary], dict[str, float | None]]:
    summaries: dict[str, TimingSummary] = {}
    filtered_truth = None
    filtered_actual = None

    if exact_index is not None:
        summaries["numpy_f32_global"] = _time_callable(
            warmup=config.warmup,
            repetitions=config.repetitions,
            runner=lambda query: exact_index.search(query, top_k=top_k),
            queries=queries,
        )
    if filter_group_id is not None and filtered_candidates is not None:
        if exact_index is not None:
            summaries["numpy_f32_filtered"] = _time_callable(
                warmup=config.warmup,
                repetitions=config.repetitions,
                runner=lambda query: exact_index.search(
                    query,
                    top_k=top_k,
                    candidate_indexes=filtered_candidates,
                ),
                queries=queries,
            )
            filtered_truth = [
                tuple(
                    match.target_id
                    for match in exact_index.search(
                        query,
                        top_k=top_k,
                        candidate_indexes=filtered_candidates,
                    )
                )
                for query in queries
            ]

    global_truth = None
    if exact_index is not None:
        global_truth = [
            tuple(match.target_id for match in exact_index.search(query, top_k=top_k))
            for query in queries
        ]
    elif packaged_ground_truth is not None and local_to_global_item_ids is not None:
        global_truth = _packaged_truth_for_query_indexes(
            query_indexes=query_indexes,
            top_k=top_k,
            local_to_global_item_ids=local_to_global_item_ids,
            packaged_ground_truth=packaged_ground_truth,
        )
        if filter_group_id is not None:
            filtered_truth = _packaged_truth_for_query_indexes(
                query_indexes=query_indexes,
                top_k=top_k,
                local_to_global_item_ids=local_to_global_item_ids,
                packaged_ground_truth=packaged_ground_truth,
                group_ids=group_ids,
                filter_group_id=filter_group_id,
            )

    summaries["lancedb_indexed_global"] = _time_callable(
        warmup=config.warmup,
        repetitions=config.repetitions,
        runner=lambda query: _search_lancedb_indexed(
            lance_table,
            query,
            config=config,
            top_k=top_k,
        ),
        queries=queries,
    )
    global_actual = [
        _search_lancedb_indexed(lance_table, query, config=config, top_k=top_k)
        for query in queries
    ]
    if filter_group_id is not None and filtered_candidates is not None:
        summaries["lancedb_indexed_filtered"] = _time_callable(
            warmup=config.warmup,
            repetitions=config.repetitions,
            runner=lambda query: _search_lancedb_indexed(
                lance_table,
                query,
                config=config,
                top_k=top_k,
                filter_group_id=filter_group_id,
            ),
            queries=queries,
        )
        filtered_actual = [
            _search_lancedb_indexed(
                lance_table,
                query,
                config=config,
                top_k=top_k,
                filter_group_id=filter_group_id,
            )
            for query in queries
        ]

    recalls: dict[str, float | None] = {
        "lancedb_indexed_global": (
            _recall_at_k(global_truth, global_actual)
            if global_truth is not None
            else None
        ),
    }
    if filtered_truth is not None and filtered_actual is not None:
        recalls["lancedb_indexed_filtered"] = _recall_at_k(
            filtered_truth,
            filtered_actual,
        )
    elif filter_group_id is not None and filtered_candidates is not None:
        recalls["lancedb_indexed_filtered"] = None
    return summaries, recalls


def _finalize_top_k_reports(
    *,
    base_report: dict[str, Any],
    top_k_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    if len(top_k_reports) == 1:
        single = top_k_reports[0]
        return {
            **base_report,
            "top_k": int(single["top_k"]),
            "latency_summaries_ms": single["latency_summaries_ms"],
            "recalls_at_k": single["recalls_at_k"],
        }
    return {
        **base_report,
        "top_k_grid": [int(report["top_k"]) for report in top_k_reports],
        "top_k_reports": top_k_reports,
    }


def _dataset_info(name: str) -> DatasetInfo:
    root = Path(__file__).resolve().parents[2] / "examples" / "data"
    if name == "msmarco-10m":
        meta_path = root / "MSMARCO-10M" / "msmarco-passages-10000000.meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return DatasetInfo(
            name=name,
            meta_path=meta_path,
            count=int(meta["count"]),
            dimensions=int(meta["dim"]),
            group_names=("all",),
        )
    if name == "stackoverflow-xlarge":
        meta_path = (
            root
            / "stackoverflow-xlarge"
            / "vectors"
            / "stackoverflow-xlarge-all.meta.json"
        )
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        group_names = tuple(str(name) for name in meta.get("source_corpora", ("all",)))
        return DatasetInfo(
            name=name,
            meta_path=meta_path,
            count=int(meta["count"]),
            dimensions=int(meta["dim"]),
            group_names=group_names,
        )
    raise ValueError(f"Unknown dataset: {name!r}")


def _load_dataset_subset(
    *,
    meta_path: Path,
    rows: int,
    sample_mode: str,
) -> tuple[Any, Any, Any, dict[int, str]]:
    selected_ranges, group_ids, group_lookup = _plan_dataset_subset(
        meta_path=meta_path,
        rows=rows,
        sample_mode=sample_mode,
    )
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    dimensions = int(meta["dim"])
    matrix = _load_subset_matrix(
        selected_ranges=selected_ranges,
        rows=rows,
        dimensions=dimensions,
    )
    item_ids = np.arange(rows, dtype=np.int64)
    return item_ids, matrix, group_ids, group_lookup


def _plan_dataset_subset(
    *,
    meta_path: Path,
    rows: int,
    sample_mode: str,
) -> tuple[list[SelectedRange], Any, dict[int, str]]:
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    shards = _iter_shards(meta_path, meta)
    resolved_sample_mode = _resolved_sample_mode(
        sample_mode,
        group_count=len({str(shard.get("source_corpus") or "all") for shard in shards}),
    )
    group_ids = np.zeros(rows, dtype=np.int8)
    group_name_to_id: dict[str, int] = {}
    selected_ranges: list[SelectedRange] = []

    loaded = 0
    if resolved_sample_mode == "prefix":
        raw_selected_ranges = _prefix_selected_ranges(shards=shards, rows=rows)
    else:
        raw_selected_ranges = _stratified_selected_ranges(shards=shards, rows=rows)

    for selected in raw_selected_ranges:
        if loaded >= rows:
            break
        shard = selected["shard"]
        shard_offset = int(selected["offset"])
        shard_count = min(int(selected["count"]), rows - loaded)
        source_name = str(shard.get("source_corpus") or "all")
        group_id = group_name_to_id.setdefault(source_name, len(group_name_to_id))
        group_ids[loaded: loaded + shard_count] = group_id
        selected_ranges.append(
            SelectedRange(
                row_start=loaded,
                shard_path=Path(shard["path"]),
                shard_count=int(shard["count"]),
                offset=shard_offset,
                count=shard_count,
                group_id=group_id,
                group_name=source_name,
                shard_global_start=int(shard.get("start", 0)),
            )
        )
        loaded += shard_count

    if loaded != rows:
        raise ValueError(f"Expected to load {rows} rows, loaded {loaded}.")

    group_lookup = {group_id: name for name, group_id in group_name_to_id.items()}
    if not group_lookup:
        group_lookup = {0: "all"}
    return selected_ranges, group_ids, group_lookup


def _load_subset_matrix(
    *,
    selected_ranges: list[SelectedRange],
    rows: int,
    dimensions: int,
) -> Any:
    matrix = np.empty((rows, dimensions), dtype=np.float32)
    for selected_range in selected_ranges:
        shard_matrix = np.memmap(
            selected_range.shard_path,
            dtype=np.float32,
            mode="r",
            shape=(selected_range.shard_count, dimensions),
        )
        start = selected_range.row_start
        stop = start + selected_range.count
        shard_start = selected_range.offset
        shard_stop = shard_start + selected_range.count
        matrix[start:stop] = shard_matrix[shard_start:shard_stop]
    return matrix


def _load_query_vectors(
    *,
    selected_ranges: list[SelectedRange],
    query_indexes: Any,
    dimensions: int,
) -> list[Any]:
    query_indexes_array = np.asarray(query_indexes, dtype=np.int64)
    order = np.argsort(query_indexes_array, kind="stable")
    sorted_query_indexes = query_indexes_array[order]
    queries: list[Any] = [None] * int(query_indexes_array.size)

    for selected_range in selected_ranges:
        start = selected_range.row_start
        stop = start + selected_range.count
        left = int(np.searchsorted(sorted_query_indexes, start, side="left"))
        right = int(np.searchsorted(sorted_query_indexes, stop, side="left"))
        if left == right:
            continue
        shard_matrix = np.memmap(
            selected_range.shard_path,
            dtype=np.float32,
            mode="r",
            shape=(selected_range.shard_count, dimensions),
        )
        for position in range(left, right):
            query_index = int(sorted_query_indexes[position])
            shard_index = selected_range.offset + (query_index - start)
            queries[int(order[position])] = np.array(
                shard_matrix[shard_index],
                copy=True,
            )

    if any(query is None for query in queries):
        raise ValueError("Failed to load one or more query vectors from selection.")
    return queries


def _resolved_sample_mode(sample_mode: str, *, group_count: int) -> str:
    if sample_mode != "auto":
        return sample_mode
    if group_count > 1:
        return "stratified"
    return "prefix"


def _prefix_selected_ranges(
    *,
    shards: list[dict[str, Any]],
    rows: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    remaining = rows
    for shard in shards:
        if remaining <= 0:
            break
        take = min(int(shard["count"]), remaining)
        selected.append({"shard": shard, "offset": 0, "count": take})
        remaining -= take
    return selected


def _stratified_selected_ranges(
    *,
    shards: list[dict[str, Any]],
    rows: int,
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for shard in shards:
        group_name = str(shard.get("source_corpus") or "all")
        groups.setdefault(group_name, []).append(shard)

    group_counts = {
        name: sum(int(shard["count"]) for shard in group_shards)
        for name, group_shards in groups.items()
    }
    allocations = _proportional_allocations(group_counts, total=rows)

    selected: list[dict[str, Any]] = []
    for group_name, group_shards in groups.items():
        target = allocations.get(group_name, 0)
        if target <= 0:
            continue
        offset = 0
        remaining = target
        total_group_count = group_counts[group_name]
        stride = max(1, total_group_count // target)
        for shard in group_shards:
            if remaining <= 0:
                break
            shard_count = int(shard["count"])
            if offset >= shard_count:
                offset -= shard_count
                continue
            take = min(remaining, max(1, shard_count // stride))
            max_offset = max(shard_count - take, 0)
            shard_offset = min(offset, max_offset)
            selected.append(
                {
                    "shard": shard,
                    "offset": shard_offset,
                    "count": take,
                }
            )
            remaining -= take
            offset = (offset + stride) % max(shard_count, 1)
        if remaining > 0:
            tail_selected = _prefix_selected_ranges(shards=group_shards, rows=remaining)
            for extra in tail_selected:
                selected.append(extra)
    return selected


def _proportional_allocations(
    group_counts: dict[str, int],
    *,
    total: int,
) -> dict[str, int]:
    if total <= 0:
        return {name: 0 for name in group_counts}
    total_count = sum(group_counts.values())
    if total_count <= 0:
        raise ValueError("Cannot allocate rows across empty groups.")

    floors: dict[str, int] = {}
    remainders: list[tuple[float, str]] = []
    assigned = 0
    for name, count in group_counts.items():
        exact = total * count / total_count
        floor_value = min(count, int(exact))
        floors[name] = floor_value
        assigned += floor_value
        remainders.append((exact - floor_value, name))

    remaining = total - assigned
    for _, name in sorted(remainders, reverse=True):
        if remaining <= 0:
            break
        if floors[name] >= group_counts[name]:
            continue
        floors[name] += 1
        remaining -= 1
    return floors


def _iter_shards(meta_path: Path, meta: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(meta.get("shards"), list):
        return [
            {
                "path": meta_path.parent / str(shard["path"]),
                "count": int(shard["count"]),
                "start": int(shard.get("start", 0)),
                "source_corpus": shard.get("source_corpus"),
            }
            for shard in meta["shards"]
        ]

    shard_prefix = meta_path.name.removesuffix(".meta.json")
    shard_paths = sorted(meta_path.parent.glob(f"{shard_prefix}.shard*.f32"))
    shard_count = int(meta.get("shard_size", 100_000))
    start = 0
    shards = []
    for shard_path in shard_paths:
        file_count = shard_path.stat().st_size // (4 * int(meta["dim"]))
        shards.append(
            {
                "path": shard_path,
                "count": int(file_count),
                "start": start,
                "source_corpus": None,
            }
        )
        start += min(file_count, shard_count)
    return shards


def _query_indexes(
    *,
    group_ids: Any,
    group_lookup: dict[int, str],
    filter_source: str | None,
    queries: int,
    rng: Any,
    eligible_indexes: Any | None = None,
) -> Any:
    if filter_source is None:
        population = np.arange(group_ids.shape[0], dtype=np.int64)
    else:
        filter_group_id = _group_id_for_name(group_lookup, filter_source)
        population = np.flatnonzero(group_ids == filter_group_id)
    if eligible_indexes is not None:
        population = np.intersect1d(
            population,
            np.asarray(eligible_indexes, dtype=np.int64),
            assume_unique=True,
        )
    if population.size < queries:
        raise ValueError(
            "Requested queries="
            f"{queries} exceeds available query pool={population.size}."
        )
    return rng.choice(population, size=queries, replace=False)


def _local_to_global_item_ids(*, selected_ranges: list[SelectedRange], rows: int):
    local_to_global = np.empty(rows, dtype=np.int64)
    for selected_range in selected_ranges:
        start = selected_range.row_start
        stop = start + selected_range.count
        global_start = selected_range.shard_global_start + selected_range.offset
        local_to_global[start:stop] = np.arange(
            global_start,
            global_start + selected_range.count,
            dtype=np.int64,
        )
    return local_to_global


def _load_packaged_ground_truth(meta_path: Path) -> PackagedGroundTruth | None:
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    gt_name = meta.get("gt_file")
    if gt_name is None:
        gt_path = meta_path.with_name(
            meta_path.name.removesuffix(".meta.json") + ".gt.jsonl"
        )
    else:
        gt_path = meta_path.parent / str(gt_name)
    if not gt_path.exists():
        return None

    neighbors_by_query_id: dict[int, tuple[int, ...]] = {}
    query_ids: list[int] = []
    with gt_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            payload = json.loads(line)
            query_id = int(payload["query_id"])
            query_ids.append(query_id)
            neighbors_by_query_id[query_id] = tuple(
                int(entry["doc_id"]) for entry in payload.get("topk", [])
            )

    return PackagedGroundTruth(
        gt_path=gt_path,
        query_ids=np.asarray(query_ids, dtype=np.int64),
        neighbors_by_query_id=neighbors_by_query_id,
    )


def _eligible_packaged_gt_query_indexes(
    *,
    local_to_global_item_ids,
    packaged_ground_truth: PackagedGroundTruth,
):
    if packaged_ground_truth.query_ids.size == 0:
        return np.empty(0, dtype=np.int64)
    return np.flatnonzero(
        np.isin(
            local_to_global_item_ids,
            packaged_ground_truth.query_ids,
            assume_unique=False,
        )
    ).astype(np.int64, copy=False)


def _packaged_truth_for_query_indexes(
    *,
    query_indexes,
    top_k: int,
    local_to_global_item_ids,
    packaged_ground_truth: PackagedGroundTruth,
    group_ids=None,
    filter_group_id: int | None = None,
) -> list[tuple[int, ...]]:
    sorted_local_indexes = np.argsort(local_to_global_item_ids, kind="stable")
    sorted_global_ids = local_to_global_item_ids[sorted_local_indexes]
    truth: list[tuple[int, ...]] = []

    for query_index in np.asarray(query_indexes, dtype=np.int64):
        query_global_id = int(local_to_global_item_ids[int(query_index)])
        gt_neighbors = packaged_ground_truth.neighbors_by_query_id.get(query_global_id)
        if gt_neighbors is None:
            raise ValueError(
                f"Packaged ground truth is missing query_id={query_global_id}."
            )

        local_neighbors: list[int] = []
        for neighbor_global_id in gt_neighbors:
            found_index = int(
                np.searchsorted(sorted_global_ids, neighbor_global_id, side="left")
            )
            if found_index >= sorted_global_ids.shape[0]:
                continue
            if int(sorted_global_ids[found_index]) != int(neighbor_global_id):
                continue
            local_neighbor = int(sorted_local_indexes[found_index])
            if (
                filter_group_id is not None
                and group_ids is not None
                and int(group_ids[local_neighbor]) != int(filter_group_id)
            ):
                continue
            local_neighbors.append(local_neighbor)
            if len(local_neighbors) >= top_k:
                break
        truth.append(tuple(local_neighbors))
    return truth


def _group_id_for_name(group_lookup: dict[int, str], name: str) -> int:
    for group_id, group_name in group_lookup.items():
        if group_name == name:
            return int(group_id)
    raise ValueError(f"Unknown filter source: {name!r}")


def _seed_sqlite_subset(
    db,
    *,
    selected_ranges: list[SelectedRange],
    dimensions: int,
) -> None:
    sqlite = db._sqlite
    _ensure_vector_schema(sqlite)
    sqlite.execute(
        (
            f"CREATE TABLE IF NOT EXISTS {_SQLITE_GROUP_TABLE} ("
            "target_id INTEGER PRIMARY KEY, "
            "group_id INTEGER NOT NULL)"
        ),
        query_type="vector",
    )
    sqlite.execute(f"DELETE FROM {_SQLITE_GROUP_TABLE}", query_type="vector")

    connection = sqlite.connection
    connection.execute("BEGIN")
    try:
        for selected_range in selected_ranges:
            shard_matrix = np.memmap(
                selected_range.shard_path,
                dtype=np.float32,
                mode="r",
                shape=(selected_range.shard_count, dimensions),
            )
            for local_start in range(0, selected_range.count, _SQLITE_SEED_BATCH_SIZE):
                local_stop = min(
                    local_start + _SQLITE_SEED_BATCH_SIZE,
                    selected_range.count,
                )
                row_start = selected_range.row_start + local_start
                row_stop = selected_range.row_start + local_stop
                shard_start = selected_range.offset + local_start
                shard_stop = selected_range.offset + local_stop
                batch_matrix = np.asarray(
                    shard_matrix[shard_start:shard_stop],
                    dtype=np.float32,
                )
                target_ids = np.arange(row_start, row_stop, dtype=np.int64)
                connection.executemany(
                    (
                        "INSERT INTO vector_entries "
                        "(target, namespace, target_id, dimensions, embedding) "
                        "VALUES (?, ?, ?, ?, ?)"
                    ),
                    (
                        (
                            _SQLITE_VECTOR_TARGET,
                            _SQLITE_VECTOR_NAMESPACE,
                            int(target_id),
                            dimensions,
                            batch_matrix[index].tobytes(),
                        )
                        for index, target_id in enumerate(target_ids)
                    ),
                )
                connection.executemany(
                    (
                        f"INSERT INTO {_SQLITE_GROUP_TABLE} "
                        "(target_id, group_id) VALUES (?, ?)"
                    ),
                    (
                        (int(target_id), int(selected_range.group_id))
                        for target_id in target_ids
                    ),
                )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _seed_lancedb_table(
    *,
    lance_path: Path,
    dimensions: int,
    batch_size: int,
    duckdb,
    ingest_stats: dict[str, float] | None = None,
    memory_snapshots_bytes: dict[str, int | None] | None = None,
):
    started = time.perf_counter()
    db = lancedb.connect(str(lance_path))
    schema = pa.schema(
        [
            pa.field("item_id", pa.int64()),
            pa.field("group_id", pa.int8()),
            pa.field("vector", pa.list_(pa.float32(), dimensions)),
        ]
    )
    table = db.create_table("vectors", schema=schema, mode="overwrite")
    table_create_ms = (time.perf_counter() - started) * 1000.0
    if memory_snapshots_bytes is not None:
        memory_snapshots_bytes["after_lancedb_table_create"] = _current_rss_bytes()

    started = time.perf_counter()
    wrote_rows = False
    for batch in _iter_duckdb_arrow_batches(
        duckdb=duckdb,
        dimensions=dimensions,
        batch_size=batch_size,
    ):
        table.add(batch)
        wrote_rows = True
        _record_lancedb_ingest_batch(
            ingest_stats=ingest_stats,
            memory_snapshots_bytes=memory_snapshots_bytes,
            batch_rows=int(batch.num_rows),
        )
    if not wrote_rows:
        raise ValueError("No rows available to seed LanceDB table.")
    if memory_snapshots_bytes is not None:
        _snapshot_memory(memory_snapshots_bytes, "after_lancedb_ingest_complete")
        _snapshot_memory(memory_snapshots_bytes, "after_duckdb_arrow_export")
    if ingest_stats is not None:
        ingest_stats["table_create_ms"] = table_create_ms
        ingest_stats["duckdb_arrow_export_ms"] = (
            time.perf_counter() - started
        ) * 1000.0
        ingest_stats["cold_tier_export_ms"] = ingest_stats["duckdb_arrow_export_ms"]
    return table


def _seed_lancedb_table_from_selected_ranges(
    *,
    lance_path: Path,
    selected_ranges: list[SelectedRange],
    dimensions: int,
    batch_size: int,
    ingest_stats: dict[str, float] | None = None,
    memory_snapshots_bytes: dict[str, int | None] | None = None,
):
    started = time.perf_counter()
    db = lancedb.connect(str(lance_path))
    schema = pa.schema(
        [
            pa.field("item_id", pa.int64()),
            pa.field("group_id", pa.int8()),
            pa.field("vector", pa.list_(pa.float32(), dimensions)),
        ]
    )
    table = db.create_table("vectors", schema=schema, mode="overwrite")
    table_create_ms = (time.perf_counter() - started) * 1000.0
    if memory_snapshots_bytes is not None:
        memory_snapshots_bytes["after_lancedb_table_create"] = _current_rss_bytes()

    started = time.perf_counter()
    wrote_rows = False
    for batch in _iter_selected_range_arrow_batches(
        selected_ranges=selected_ranges,
        dimensions=dimensions,
        batch_size=batch_size,
    ):
        table.add(batch)
        wrote_rows = True
        _record_lancedb_ingest_batch(
            ingest_stats=ingest_stats,
            memory_snapshots_bytes=memory_snapshots_bytes,
            batch_rows=int(batch.num_rows),
        )
    if not wrote_rows:
        raise ValueError("No rows available to seed LanceDB table.")
    if memory_snapshots_bytes is not None:
        _snapshot_memory(memory_snapshots_bytes, "after_lancedb_ingest_complete")
        _snapshot_memory(memory_snapshots_bytes, "after_duckdb_arrow_export")
    if ingest_stats is not None:
        ingest_stats["table_create_ms"] = table_create_ms
        ingest_stats["cold_tier_export_ms"] = (
            time.perf_counter() - started
        ) * 1000.0
    return table


def _iter_duckdb_arrow_batches(
    *,
    duckdb,
    dimensions: int,
    batch_size: int,
):
    reader = duckdb.connection.execute(
        (
            "SELECT e.target_id AS item_id, "
            f"COALESCE(g.group_id, 0) AS group_id, "
            "e.embedding AS embedding "
            "FROM vector_entries AS e "
            f"LEFT JOIN {_SQLITE_GROUP_TABLE} AS g ON g.target_id = e.target_id "
            "WHERE e.target = ? AND e.namespace = ? "
            "ORDER BY e.target_id"
        ),
        [_SQLITE_VECTOR_TARGET, _SQLITE_VECTOR_NAMESPACE],
    ).to_arrow_reader(batch_size=batch_size)
    for batch in reader:
        yield _duckdb_record_batch_to_lancedb_table(
            batch=batch,
            dimensions=dimensions,
        )


def _iter_selected_range_arrow_batches(
    *,
    selected_ranges: list[SelectedRange],
    dimensions: int,
    batch_size: int,
):
    for selected_range in selected_ranges:
        shard_matrix = np.memmap(
            selected_range.shard_path,
            dtype=np.float32,
            mode="r",
            shape=(selected_range.shard_count, dimensions),
        )
        for local_start in range(0, selected_range.count, batch_size):
            local_stop = min(local_start + batch_size, selected_range.count)
            row_start = selected_range.row_start + local_start
            row_stop = selected_range.row_start + local_stop
            shard_start = selected_range.offset + local_start
            shard_stop = selected_range.offset + local_stop
            batch_matrix = np.asarray(
                shard_matrix[shard_start:shard_stop],
                dtype=np.float32,
            )
            target_ids = np.arange(row_start, row_stop, dtype=np.int64)
            group_ids = np.full(
                target_ids.shape[0],
                selected_range.group_id,
                dtype=np.int8,
            )
            yield _numpy_batch_to_lancedb_table(
                item_ids=target_ids,
                group_ids=group_ids,
                matrix=batch_matrix,
                dimensions=dimensions,
            )


def _duckdb_record_batch_to_lancedb_table(*, batch, dimensions: int):
    item_ids = batch.column(0)
    group_ids = pa.compute.cast(batch.column(1), pa.int8())
    vectors = _binary_embedding_column_to_fixed_size_list(
        batch.column(2),
        dimensions=dimensions,
    )
    return pa.table(
        {
            "item_id": item_ids,
            "group_id": group_ids,
            "vector": vectors,
        }
    )


def _numpy_batch_to_lancedb_table(*, item_ids, group_ids, matrix, dimensions: int):
    vectors = _matrix_to_fixed_size_list(matrix, dimensions=dimensions)
    return pa.table(
        {
            "item_id": pa.array(item_ids, type=pa.int64()),
            "group_id": pa.array(group_ids, type=pa.int8()),
            "vector": vectors,
        }
    )


def _matrix_to_fixed_size_list(matrix, *, dimensions: int):
    flat_values = np.asarray(matrix, dtype=np.float32).reshape(-1)
    return pa.FixedSizeListArray.from_arrays(
        pa.array(flat_values, type=pa.float32()),
        dimensions,
    )


def _snapshot_memory(
    snapshots: dict[str, int | None] | None,
    name: str,
) -> int | None:
    if snapshots is None:
        return None
    value = _current_rss_bytes()
    snapshots[name] = value
    return value


def _record_lancedb_ingest_batch(
    *,
    ingest_stats: dict[str, Any] | None,
    memory_snapshots_bytes: dict[str, int | None] | None,
    batch_rows: int,
) -> None:
    current_rss = _current_rss_bytes()
    if (
        memory_snapshots_bytes is not None
        and "after_lancedb_ingest_first_batch" not in memory_snapshots_bytes
    ):
        memory_snapshots_bytes["after_lancedb_ingest_first_batch"] = current_rss
    if ingest_stats is None:
        if memory_snapshots_bytes is not None:
            peak_existing = memory_snapshots_bytes.get("after_lancedb_ingest_peak")
            if peak_existing is None or (
                current_rss is not None and current_rss > peak_existing
            ):
                memory_snapshots_bytes["after_lancedb_ingest_peak"] = current_rss
        return
    batch_count = int(ingest_stats.get("batch_count", 0)) + 1
    ingest_stats["batch_count"] = batch_count
    if batch_count == 1:
        ingest_stats["first_batch_rows"] = batch_rows
    ingest_stats["last_batch_rows"] = batch_rows
    ingest_stats["max_batch_rows"] = max(
        int(ingest_stats.get("max_batch_rows", 0)),
        batch_rows,
    )
    peak_batch_rss = ingest_stats.get("peak_batch_rss_bytes")
    if peak_batch_rss is None or (
        current_rss is not None and current_rss > peak_batch_rss
    ):
        ingest_stats["peak_batch_rss_bytes"] = current_rss
        if memory_snapshots_bytes is not None:
            memory_snapshots_bytes["after_lancedb_ingest_peak"] = current_rss


def _binary_embedding_column_to_fixed_size_list(column, *, dimensions: int):
    array = column.combine_chunks() if hasattr(column, "combine_chunks") else column
    if array.null_count:
        raise ValueError("Encountered null embedding while exporting SQLite vectors.")
    if pa.types.is_large_binary(array.type):
        offset_dtype = np.int64
    else:
        offset_dtype = np.int32
    offset_buffer = array.buffers()[1]
    data_buffer = array.buffers()[2]
    offsets = np.frombuffer(offset_buffer, dtype=offset_dtype, count=len(array) + 1)
    expected_width = dimensions * 4
    widths = np.diff(offsets)
    if widths.size and np.any(widths != expected_width):
        raise ValueError(
            "SQLite vector blob width did not match the dataset dimension during "
            "DuckDB Arrow export."
        )
    flat_values = np.frombuffer(
        data_buffer,
        dtype=np.float32,
        count=len(array) * dimensions,
        offset=int(offsets[0]),
    )
    return pa.FixedSizeListArray.from_arrays(
        pa.array(flat_values, type=pa.float32()),
        dimensions,
    )


def _build_lancedb_index(table, *, config: BenchmarkConfig) -> float:
    started = time.perf_counter()
    table.create_index(**_lancedb_index_kwargs(config))
    return (time.perf_counter() - started) * 1000.0


def _build_lancedb_scalar_index(table, *, enabled: bool) -> float:
    if not enabled:
        return 0.0
    started = time.perf_counter()
    table.create_scalar_index("group_id")
    return (time.perf_counter() - started) * 1000.0


def _search_lancedb_indexed(
    table,
    query: Any,
    *,
    config: BenchmarkConfig,
    top_k: int,
    filter_group_id: int | None = None,
) -> tuple[int, ...]:
    builder = table.search(query).distance_type(config.metric).limit(top_k)
    if config.lancedb_nprobes is not None:
        builder = builder.nprobes(config.lancedb_nprobes)
    if config.lancedb_refine_factor is not None:
        builder = builder.refine_factor(config.lancedb_refine_factor)
    if filter_group_id is not None:
        builder = builder.where(f"group_id = {filter_group_id}", prefilter=True)
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
        "sample_rate": config.lancedb_sample_rate,
        "max_iterations": config.lancedb_max_iterations,
        "target_partition_size": config.lancedb_target_partition_size,
        "scalar_index_prefilter": config.lancedb_scalar_index_prefilter,
    }


def _lancedb_search_settings(config: BenchmarkConfig) -> dict[str, Any]:
    return {
        "nprobes": config.lancedb_nprobes,
        "refine_factor": config.lancedb_refine_factor,
    }


def _time_callable(
    *,
    warmup: int,
    repetitions: int,
    runner,
    queries: list[Any],
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
        matched += sum(1 for doc_id in actual_ids if doc_id in expected_set)
        total += len(expected_ids)
    return float(matched / total) if total else 1.0


def _print_report(report: dict[str, Any]) -> None:
    print("Real vector benchmark")
    print(f"  dataset: {report['dataset']}")
    print(f"  rows: {report['rows']}")
    print(f"  dimensions: {report['dimensions']}")
    print(f"  metric: {report['metric']}")
    print(f"  queries: {report['queries']}")
    if "top_k" in report:
        print(f"  top_k: {report['top_k']}")
    else:
        print(f"  top_k_grid: {report['top_k_grid']}")
    print(f"  filter_source: {report['filter_source']}")
    print(f"  numpy_exact_enabled: {report['numpy_exact_enabled']}")
    print(f"  filtered_candidate_count: {report['filtered_candidate_count']}")
    print(f"  lancedb_index_settings: {report['lancedb_index_settings']}")
    print(f"  lancedb_search_settings: {report['lancedb_search_settings']}")
    print()
    print("Stage timings (ms)")
    for name, value in report["stage_timings_ms"].items():
        if value is None:
            print(f"  {name}: n/a")
        else:
            print(f"  {name}: {value:.2f}")
    print()
    print("Memory snapshots (bytes)")
    for name, value in report["memory_snapshots_bytes"].items():
        if value is None:
            print(f"  {name}: n/a")
        else:
            print(f"  {name}: {value}")
    peak_rss_bytes = report.get("memory_peak_rss_bytes")
    if peak_rss_bytes is None:
        print("  peak_rss: n/a")
    else:
        print(f"  peak_rss: {peak_rss_bytes}")
    if "top_k_reports" in report:
        for top_k_report in report["top_k_reports"]:
            print()
            print(f"Latency summaries (ms) for top_k={top_k_report['top_k']}")
            for name, summary in top_k_report["latency_summaries_ms"].items():
                print(
                    f"  {name}: mean={summary['mean']:.2f} "
                    f"stdev={summary['stdev']:.2f} min={summary['minimum']:.2f} "
                    f"max={summary['maximum']:.2f}"
                )
            print()
            print(f"Recall@k for top_k={top_k_report['top_k']}")
            for name, value in top_k_report["recalls_at_k"].items():
                if value is None:
                    print(f"  {name}: n/a (NumPy exact skipped)")
                else:
                    print(f"  {name}: {value:.4f}")
        return
    print()
    print("Latency summaries (ms)")
    for name, summary in report["latency_summaries_ms"].items():
        print(
            f"  {name}: mean={summary['mean']:.2f} stdev={summary['stdev']:.2f} "
            f"min={summary['minimum']:.2f} max={summary['maximum']:.2f}"
        )
    print()
    print("Recall@k versus NumPy float32 exact")
    for name, value in report["recalls_at_k"].items():
        if value is None:
            print(f"  {name}: n/a (NumPy exact skipped)")
        else:
            print(f"  {name}: {value:.4f}")


def _current_rss_bytes() -> int | None:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as status_file:
            for line in status_file:
                if line.startswith("VmRSS:"):
                    parts = line.split()
                    if len(parts) >= 2:
                        return int(parts[1]) * 1024
    except OSError:
        return None
    return None


def _peak_rss_bytes() -> int | None:
    try:
        peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except OSError:
        return None
    if peak_rss_kib <= 0:
        return None
    return int(peak_rss_kib) * 1024


def _memory_stage_deltas(
    snapshots: dict[str, int | None],
) -> dict[str, int | None]:
    deltas: dict[str, int | None] = {}
    previous_value: int | None = None
    previous_name: str | None = None
    for name in _MEMORY_SNAPSHOT_ORDER:
        if name not in snapshots:
            continue
        current_value = snapshots[name]
        if previous_value is None or current_value is None:
            deltas[name] = None
        else:
            deltas[name] = current_value - previous_value
        previous_value = current_value
        previous_name = name
    if previous_name is None:
        return deltas
    return deltas


def _validate_benchmark_sampling(queries: int, repetitions: int) -> None:
    if queries < _MIN_BENCHMARK_QUERIES:
        raise ValueError(
            "Real vector benchmark requires at least "
            f"{_MIN_BENCHMARK_QUERIES} queries."
        )
    if repetitions < _MIN_BENCHMARK_REPETITIONS:
        raise ValueError(
            "Real vector benchmark requires at least "
            f"{_MIN_BENCHMARK_REPETITIONS} repetitions."
        )


if __name__ == "__main__":
    main()
