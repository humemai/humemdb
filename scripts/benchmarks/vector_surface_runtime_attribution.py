from __future__ import annotations

import argparse
import gc
import json
import os
import sqlite3
import statistics
import tempfile
import time
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, Literal

import numpy as np
import vector_search_real as real_benchmark
from humemdb import HumemDB
from humemdb.cypher import _ensure_graph_schema as _ensure_graph_schema_sqlite
from humemdb.vector import IndexedVectorRuntimeConfig
from humemdb.vector import LanceDBIndexConfig
from humemdb.vector import _ExactVectorIndex
from humemdb.vector import _LanceDBVectorIndex
from humemdb.vector import _ensure_vector_schema
from humemdb.vector import _insert_vectors


DEFAULT_DATASET = "msmarco-10m"
DEFAULT_ROWS = 1_000_000
DEFAULT_ANN_MIN_VECTORS = 100_000
DEFAULT_TOP_K = 10
DEFAULT_QUERIES = 100
DEFAULT_WARMUP = 5
DEFAULT_REPETITIONS = 1
DIRECT_INDEX_NAME = "direct_similarity_idx"
SQL_INDEX_NAME = "docs_embedding_idx"
CYPHER_INDEX_NAME = "user_embedding_idx"
SQL_TABLE_NAME = "docs"
CYPHER_LABEL = "User"


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    """Configuration for one multi-surface vector attribution run."""

    dataset: str
    rows: int
    queries: int
    top_k: int
    warmup: int
    repetitions: int
    ann_min_vectors: int
    metric: Literal["cosine", "dot", "l2"]
    seed: int
    sample_mode: str
    lancedb_num_partitions: int | None
    lancedb_num_sub_vectors: int | None
    lancedb_nprobes: int | None
    lancedb_refine_factor: int | None
    output: str


@dataclass(slots=True)
class StageAggregate:
    """Accumulated timings and memory peaks for one instrumented stage."""

    count: int = 0
    total_ms: float = 0.0
    max_rss_bytes: int = 0
    max_python_peak_bytes: int = 0

    def add(
        self,
        *,
        duration_ms: float,
        rss_bytes: int,
        python_peak_bytes: int,
    ) -> None:
        """Add one measured sample to the aggregate stage totals."""

        self.count += 1
        self.total_ms += duration_ms
        self.max_rss_bytes = max(self.max_rss_bytes, rss_bytes)
        self.max_python_peak_bytes = max(self.max_python_peak_bytes, python_peak_bytes)


@dataclass(slots=True)
class StageRecorder:
    """Collect per-stage timing samples during one benchmark operation."""

    stages: dict[str, StageAggregate] = field(default_factory=dict)

    def record(self, stage: str, duration_ms: float) -> None:
        """Record one stage sample with current process memory stats."""

        rss_bytes = _current_rss_bytes()
        python_peak_bytes = 0
        if tracemalloc.is_tracing():
            _, python_peak_bytes = tracemalloc.get_traced_memory()
        aggregate = self.stages.setdefault(stage, StageAggregate())
        aggregate.add(
            duration_ms=duration_ms,
            rss_bytes=rss_bytes,
            python_peak_bytes=python_peak_bytes,
        )

    def total_ms(self, stage: str) -> float:
        """Return the accumulated duration for the requested stage."""

        aggregate = self.stages.get(stage)
        if aggregate is None:
            return 0.0
        return aggregate.total_ms

    def to_dict(self) -> dict[str, dict[str, float | int]]:
        """Return a JSON-serializable view of the recorded stage samples."""

        return {
            stage: {
                "count": aggregate.count,
                "total_ms": aggregate.total_ms,
                "max_rss_bytes": aggregate.max_rss_bytes,
                "max_python_peak_bytes": aggregate.max_python_peak_bytes,
            }
            for stage, aggregate in sorted(self.stages.items())
        }


@dataclass(frozen=True, slots=True)
class RunMeasurement:
    """Captured timing and memory results for one measured operation."""

    total_ms: float
    orchestration_ms: float
    rss_before_bytes: int
    rss_after_bytes: int
    rss_delta_bytes: int
    python_peak_bytes: int
    stage_stats: dict[str, dict[str, float | int]]


@dataclass(slots=True)
class SurfaceContext:
    """Prepared benchmark state for one query surface and isolated database."""

    db: HumemDB
    metric: Literal["cosine", "dot", "l2"]
    top_k: int
    index_name: str
    surface: Literal["direct", "sql", "cypher"]
    timed_queries: list[np.ndarray]
    warmup_queries: list[np.ndarray]


@dataclass(frozen=True, slots=True)
class Scenario:
    """Description of one benchmark scenario and its setup hooks."""

    name: str
    description: str
    setup: Callable[[Path, BenchmarkConfig], SurfaceContext]
    operation: Callable[[SurfaceContext, np.ndarray | None], Any]
    verify: Callable[[SurfaceContext, Any, BenchmarkConfig], None]


def _parse_args() -> BenchmarkConfig:
    """Parse CLI arguments for the multi-surface attribution benchmark."""

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark direct, SQL, and Cypher vector index build/search "
            "independently using separate real-data HumemDB databases."
        )
    )
    parser.add_argument(
        "--dataset",
        choices=("msmarco-10m", "stackoverflow-xlarge"),
        default=DEFAULT_DATASET,
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--queries", type=int, default=DEFAULT_QUERIES)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP)
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument(
        "--ann-min-vectors",
        type=int,
        default=DEFAULT_ANN_MIN_VECTORS,
    )
    parser.add_argument(
        "--metric",
        choices=("cosine", "dot", "l2"),
        default="cosine",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--sample-mode",
        choices=("auto", "prefix", "stratified"),
        default="auto",
    )
    parser.add_argument("--lancedb-num-partitions", type=int, default=128)
    parser.add_argument("--lancedb-num-sub-vectors", type=int, default=128)
    parser.add_argument("--lancedb-nprobes", type=int, default=32)
    parser.add_argument("--lancedb-refine-factor", type=int, default=4)
    parser.add_argument("--output", choices=("text", "json"), default="text")
    args = parser.parse_args()
    return BenchmarkConfig(
        dataset=args.dataset,
        rows=args.rows,
        queries=args.queries,
        top_k=args.top_k,
        warmup=args.warmup,
        repetitions=args.repetitions,
        ann_min_vectors=args.ann_min_vectors,
        metric=args.metric,
        seed=args.seed,
        sample_mode=args.sample_mode,
        lancedb_num_partitions=args.lancedb_num_partitions,
        lancedb_num_sub_vectors=args.lancedb_num_sub_vectors,
        lancedb_nprobes=args.lancedb_nprobes,
        lancedb_refine_factor=args.lancedb_refine_factor,
        output=args.output,
    )


def _validate_config(config: BenchmarkConfig) -> None:
    """Validate one benchmark configuration before any heavy setup starts."""

    dataset_info = getattr(real_benchmark, "_dataset_info")
    dataset = dataset_info(config.dataset)
    if config.rows > dataset.count:
        raise ValueError(
            f"Requested rows={config.rows} exceeds dataset count={dataset.count}."
        )
    if config.queries < 100:
        raise ValueError("--queries must be at least 100.")
    if config.top_k < 1:
        raise ValueError("--top-k must be at least 1.")
    if config.warmup < 0:
        raise ValueError("--warmup cannot be negative.")
    if config.repetitions < 1:
        raise ValueError("--repetitions must be at least 1.")
    if config.ann_min_vectors < 0:
        raise ValueError("--ann-min-vectors cannot be negative.")
    if config.rows < max(config.ann_min_vectors, 256):
        raise ValueError(
            "--rows must be large enough for the ANN snapshot threshold and LanceDB "
            "training minimum."
        )


def _current_rss_bytes() -> int:
    """Return the current resident set size of the Python process."""

    statm_path = "/proc/self/statm"
    if os.path.exists(statm_path):
        with open(statm_path, "r", encoding="utf-8") as handle:
            resident_pages = int(handle.read().split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    return 0


def _configure_runtime(db: HumemDB, config: BenchmarkConfig) -> None:
    """Apply the requested ANN snapshot runtime settings to one benchmark database."""

    setattr(
        db,
        "_vector_runtime_config",
        IndexedVectorRuntimeConfig(
            ann_min_vectors=config.ann_min_vectors,
            lancedb=LanceDBIndexConfig(
                num_partitions=config.lancedb_num_partitions,
                num_sub_vectors=config.lancedb_num_sub_vectors,
                nprobes=config.lancedb_nprobes,
                refine_factor=config.lancedb_refine_factor,
            ),
        ),
    )


def _dataset_info(config: BenchmarkConfig):
    """Load packaged dataset metadata for the selected benchmark dataset."""

    dataset_info = getattr(real_benchmark, "_dataset_info")
    return dataset_info(config.dataset)


def _load_selected_ranges(
    config: BenchmarkConfig,
) -> list[real_benchmark.SelectedRange]:
    """Choose the dataset shard ranges that back this benchmark run."""

    dataset = _dataset_info(config)
    plan_dataset_subset = getattr(real_benchmark, "_plan_dataset_subset")
    selected_ranges, _, _ = plan_dataset_subset(
        meta_path=dataset.meta_path,
        rows=config.rows,
        sample_mode=config.sample_mode,
    )
    return list(selected_ranges)


def _load_query_vectors(
    *,
    config: BenchmarkConfig,
    selected_ranges: list[real_benchmark.SelectedRange],
) -> list[np.ndarray]:
    """Load the timed query vectors for one benchmark run."""

    dataset = _dataset_info(config)
    rng = np.random.default_rng(config.seed)
    query_indexes = rng.choice(
        np.arange(config.rows, dtype=np.int64),
        size=config.queries,
        replace=False,
    )
    load_query_vectors = getattr(real_benchmark, "_load_query_vectors")
    return list(
        load_query_vectors(
            selected_ranges=selected_ranges,
            query_indexes=query_indexes,
            dimensions=dataset.dimensions,
        )
    )


def _sqlite_engine(db: HumemDB) -> Any:
    """Return the SQLite engine owned by one benchmark database."""

    return getattr(db, "_sqlite")


def _invalidate_vector_caches(db: HumemDB) -> None:
    """Clear vector-runtime caches after direct benchmark seeding."""

    getattr(db, "_invalidate_exact_vector_cache")()
    getattr(db, "_clear_vector_tombstone_cache")()


def _make_warmup_queries(
    *,
    timed_queries: list[np.ndarray],
    warmup_count: int,
) -> list[np.ndarray]:
    """Repeat timed queries as needed to build the warmup query list."""

    if warmup_count <= 0:
        return []
    return [timed_queries[index % len(timed_queries)] for index in range(warmup_count)]


def _iter_range_batches(
    *,
    selected_ranges: list[real_benchmark.SelectedRange],
    dimensions: int,
    batch_size: int = 10_000,
) -> Iterator[tuple[np.ndarray, np.ndarray]]:
    """Yield contiguous id and embedding batches from the selected dataset ranges."""

    for selected_range in selected_ranges:
        shard_matrix = np.memmap(
            selected_range.shard_path,
            dtype=np.float32,
            mode="r",
            shape=(selected_range.shard_count, dimensions),
        )
        row_stop = selected_range.row_start + selected_range.count
        for row_start in range(selected_range.row_start, row_stop, batch_size):
            local_start = row_start - selected_range.row_start
            local_stop = min(local_start + batch_size, selected_range.count)
            shard_start = selected_range.offset + local_start
            shard_stop = selected_range.offset + local_stop
            embeddings = np.asarray(
                shard_matrix[shard_start:shard_stop],
                dtype=np.float32,
            )
            target_ids = np.arange(
                row_start + 1,
                row_start + 1 + (local_stop - local_start),
                dtype=np.int64,
            )
            yield target_ids, embeddings


def _seed_direct_surface(
    db: HumemDB,
    *,
    selected_ranges: list[real_benchmark.SelectedRange],
    dimensions: int,
) -> None:
    """Seed one isolated benchmark database for the direct vector surface."""

    sqlite_engine = _sqlite_engine(db)
    _ensure_vector_schema(sqlite_engine)
    for target_ids, embeddings in _iter_range_batches(
        selected_ranges=selected_ranges,
        dimensions=dimensions,
    ):
        rows = [
            (int(target_id), embeddings[index].tolist())
            for index, target_id in enumerate(target_ids)
        ]
        _insert_vectors(sqlite_engine, rows, target="direct", namespace="")
    _invalidate_vector_caches(db)


def _seed_sql_surface(
    db: HumemDB,
    *,
    selected_ranges: list[real_benchmark.SelectedRange],
    dimensions: int,
) -> None:
    """Seed one isolated benchmark database for the SQL vector surface."""

    sqlite_engine = _sqlite_engine(db)
    sqlite_engine.execute(
        (
            f"CREATE TABLE IF NOT EXISTS {SQL_TABLE_NAME} ("
            "id INTEGER PRIMARY KEY, "
            "cohort TEXT NOT NULL, "
            "embedding BLOB NOT NULL)"
        ),
        query_type="sql",
    )
    _ensure_vector_schema(sqlite_engine)
    for target_ids, embeddings in _iter_range_batches(
        selected_ranges=selected_ranges,
        dimensions=dimensions,
    ):
        relational_rows = [
            (
                int(target_id),
                "bench",
                sqlite3.Binary(
                    np.asarray(embeddings[index], dtype=np.float32).tobytes()
                ),
            )
            for index, target_id in enumerate(target_ids)
        ]
        sqlite_engine.executemany(
            f"INSERT INTO {SQL_TABLE_NAME} (id, cohort, embedding) VALUES (?, ?, ?)",
            relational_rows,
            query_type="sql",
        )
        vector_rows = [
            (int(target_id), embeddings[index].tolist())
            for index, target_id in enumerate(target_ids)
        ]
        _insert_vectors(
            sqlite_engine,
            vector_rows,
            target="sql_row",
            namespace=SQL_TABLE_NAME,
        )
    _invalidate_vector_caches(db)


def _seed_cypher_surface(
    db: HumemDB,
    *,
    selected_ranges: list[real_benchmark.SelectedRange],
    dimensions: int,
) -> None:
    """Seed one isolated benchmark database for the Cypher vector surface."""

    sqlite_engine = _sqlite_engine(db)
    _ensure_graph_schema_sqlite(sqlite_engine)
    _ensure_vector_schema(sqlite_engine)
    for target_ids, embeddings in _iter_range_batches(
        selected_ranges=selected_ranges,
        dimensions=dimensions,
    ):
        sqlite_engine.executemany(
            "INSERT INTO graph_nodes (id, label) VALUES (?, ?)",
            [(int(target_id), CYPHER_LABEL) for target_id in target_ids],
            query_type="cypher",
        )
        vector_rows = [
            (int(target_id), embeddings[index].tolist())
            for index, target_id in enumerate(target_ids)
        ]
        _insert_vectors(sqlite_engine, vector_rows, target="graph_node", namespace="")
    _invalidate_vector_caches(db)


def _wrap_instance_method(
    db: HumemDB,
    recorder: StageRecorder,
    restores: list[Callable[[], None]],
    name: str,
    stage: str,
) -> None:
    """Temporarily wrap one instance method to record its execution time."""

    original = getattr(db, name)

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        try:
            return original(*args, **kwargs)
        finally:
            recorder.record(stage, (time.perf_counter() - started) * 1000.0)

    setattr(db, name, wrapped)
    restores.append(lambda: setattr(db, name, original))


def _wrap_class_method(
    cls: type[Any],
    recorder: StageRecorder,
    restores: list[Callable[[], None]],
    name: str,
    stage: str,
) -> None:
    """Temporarily wrap one class method to record its execution time."""

    original = getattr(cls, name)

    def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter()
        try:
            return original(self, *args, **kwargs)
        finally:
            recorder.record(stage, (time.perf_counter() - started) * 1000.0)

    setattr(cls, name, wrapped)
    restores.append(lambda: setattr(cls, name, original))


def _install_instrumentation(
    db: HumemDB,
    recorder: StageRecorder,
    *,
    surface: Literal["direct", "sql", "cypher"],
) -> list[Callable[[], None]]:
    """Install timing wrappers for the stages relevant to one surface run."""

    restores: list[Callable[[], None]] = []
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_execute_vector_query",
        "vector_dispatch_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_execute_candidate_vector_query",
        "candidate_resolution_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_execute_sql_query_plan",
        "sql_query_plan_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_execute_cypher_query_plan",
        "cypher_query_plan_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_execute_exact_vector_search",
        "exact_fallback_path_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_execute_indexed_vector_search",
        "snapshot_rerank_path_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_build_public_vector_index",
        "build_snapshot_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_snapshot_vector_index_for",
        "snapshot_index_load_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_load_persisted_snapshot_index",
        "snapshot_persisted_load_ms",
    )
    _wrap_instance_method(
        db,
        recorder,
        restores,
        "_current_snapshot_data",
        "snapshot_materialization_ms",
    )
    if surface == "direct":
        _wrap_instance_method(
            db,
            recorder,
            restores,
            "_resolve_direct_vector_search",
            "direct_candidate_resolution_ms",
        )
    _wrap_class_method(
        _ExactVectorIndex,
        recorder,
        restores,
        "search",
        "numpy_exact_search_ms",
    )
    _wrap_class_method(
        _LanceDBVectorIndex,
        recorder,
        restores,
        "search",
        "snapshot_ann_search_ms",
    )
    return restores


def _run_one_measurement(
    context: SurfaceContext,
    operation: Callable[[SurfaceContext, np.ndarray | None], Any],
    query: np.ndarray | None,
) -> tuple[RunMeasurement, Any]:
    """Execute one operation under instrumentation and capture its measurements."""

    gc.collect()
    rss_before = _current_rss_bytes()
    recorder = StageRecorder()
    restores = _install_instrumentation(context.db, recorder, surface=context.surface)
    tracemalloc.start()
    started = time.perf_counter()
    try:
        result = operation(context, query)
    finally:
        total_ms = (time.perf_counter() - started) * 1000.0
        _, python_peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        for restore in reversed(restores):
            restore()
    rss_after = _current_rss_bytes()
    backend_search_ms = (
        recorder.total_ms("numpy_exact_search_ms")
        + recorder.total_ms("snapshot_ann_search_ms")
    )
    measurement = RunMeasurement(
        total_ms=total_ms,
        orchestration_ms=max(total_ms - backend_search_ms, 0.0),
        rss_before_bytes=rss_before,
        rss_after_bytes=rss_after,
        rss_delta_bytes=rss_after - rss_before,
        python_peak_bytes=python_peak_bytes,
        stage_stats=recorder.to_dict(),
    )
    return measurement, result


def _timing_summary(values: list[float]) -> dict[str, float]:
    """Return mean, spread, and range for one list of timing values."""

    return {
        "mean": statistics.fmean(values),
        "stdev": statistics.pstdev(values) if len(values) > 1 else 0.0,
        "minimum": min(values),
        "maximum": max(values),
    }


def _aggregate_stage_stats(
    measurements: list[RunMeasurement],
) -> dict[str, dict[str, float | dict[str, float]]]:
    """Aggregate stage timings across repeated measurements."""

    stage_names = sorted(
        {stage for measurement in measurements for stage in measurement.stage_stats}
    )
    aggregated: dict[str, dict[str, float | dict[str, float]]] = {}
    for stage in stage_names:
        totals = [
            float(measurement.stage_stats.get(stage, {}).get("total_ms", 0.0))
            for measurement in measurements
        ]
        counts = [
            float(measurement.stage_stats.get(stage, {}).get("count", 0))
            for measurement in measurements
        ]
        aggregated[stage] = {
            "count_mean": statistics.fmean(counts),
            "total_ms": _timing_summary(totals),
        }
    return aggregated


def _sql_metric_operator(metric: Literal["cosine", "dot", "l2"]) -> str:
    """Map one public metric name to the SQL vector operator family."""

    return {
        "cosine": "vector_cosine_ops",
        "dot": "vector_ip_ops",
        "l2": "vector_l2_ops",
    }[metric]


def _cypher_metric_option(metric: Literal["cosine", "dot", "l2"]) -> str:
    """Map one public metric name to the Cypher index option value."""

    return metric


def _setup_surface(
    *,
    root: Path,
    config: BenchmarkConfig,
    surface: Literal["direct", "sql", "cypher"],
) -> SurfaceContext:
    """Create and seed one isolated database for the requested query surface."""

    selected_ranges = _load_selected_ranges(config)
    dataset = _dataset_info(config)
    timed_queries = _load_query_vectors(config=config, selected_ranges=selected_ranges)

    db = HumemDB(root / surface)
    _configure_runtime(db, config)
    if surface == "direct":
        _seed_direct_surface(
            db,
            selected_ranges=selected_ranges,
            dimensions=dataset.dimensions,
        )
        index_name = DIRECT_INDEX_NAME
    elif surface == "sql":
        _seed_sql_surface(
            db,
            selected_ranges=selected_ranges,
            dimensions=dataset.dimensions,
        )
        index_name = SQL_INDEX_NAME
    else:
        _seed_cypher_surface(
            db,
            selected_ranges=selected_ranges,
            dimensions=dataset.dimensions,
        )
        index_name = CYPHER_INDEX_NAME

    return SurfaceContext(
        db=db,
        metric=config.metric,
        top_k=config.top_k,
        index_name=index_name,
        surface=surface,
        timed_queries=timed_queries,
        warmup_queries=_make_warmup_queries(
            timed_queries=timed_queries,
            warmup_count=config.warmup,
        ),
    )


def _setup_direct(root: Path, config: BenchmarkConfig) -> SurfaceContext:
    """Prepare the direct-surface benchmark context."""

    return _setup_surface(root=root, config=config, surface="direct")


def _setup_sql(root: Path, config: BenchmarkConfig) -> SurfaceContext:
    """Prepare the SQL-surface benchmark context."""

    return _setup_surface(root=root, config=config, surface="sql")


def _setup_cypher(root: Path, config: BenchmarkConfig) -> SurfaceContext:
    """Prepare the Cypher-surface benchmark context."""

    return _setup_surface(root=root, config=config, surface="cypher")


def _build_then_search(context: SurfaceContext, query: np.ndarray | None) -> Any:
    """Build the surface index and then run one search against it."""

    _run_build(context, None)
    return _run_search(context, query)


def _run_build(context: SurfaceContext, query: np.ndarray | None) -> Any:
    """Execute the surface-native index build statement for one context."""

    del query
    if context.surface == "direct":
        return context.db.build_vector_index(
            metric=context.metric,
            index_name=context.index_name,
        )
    if context.surface == "sql":
        return context.db.query(
            (
                f"CREATE INDEX {context.index_name} ON {SQL_TABLE_NAME} USING ivfpq "
                f"(embedding {_sql_metric_operator(context.metric)})"
            )
        )
    return context.db.query(
        (
            f"CREATE VECTOR INDEX {context.index_name} IF NOT EXISTS "
            f"FOR (u:{CYPHER_LABEL}) ON (u.embedding) "
            "OPTIONS {indexConfig: {`vector.similarity_function`: "
            f"'{_cypher_metric_option(context.metric)}'}}"
        )
    )


def _run_search(context: SurfaceContext, query: np.ndarray | None) -> Any:
    """Execute the surface-native search statement for one query vector."""

    assert query is not None
    if context.surface == "direct":
        return context.db.search_vectors(
            query.tolist(),
            top_k=context.top_k,
            metric=context.metric,
        )
    if context.surface == "sql":
        return context.db.query(
            (
                f"SELECT id FROM {SQL_TABLE_NAME} "
                "ORDER BY embedding <=> $query "
                "LIMIT $limit"
            ),
            params={"query": query.tolist(), "limit": context.top_k},
        )
    return context.db.query(
        (
            f"CALL db.index.vector.queryNodes('{context.index_name}', $limit, $query) "
            "YIELD node, score RETURN node.id, score"
        ),
        params={"query": query.tolist(), "limit": context.top_k},
    )


def _verify_build(
    context: SurfaceContext,
    result: Any,
    config: BenchmarkConfig,
) -> None:
    """Assert that one build scenario reached the ready indexed state."""

    state = context.db.inspect_vector_index(
        metric=config.metric,
        index_name=context.index_name,
    )
    assert state["state"] == "ready"
    assert state["snapshot_rows"] > 0
    assert state["total_rows"] == config.rows
    assert state["delta_rows"] == 0
    if context.surface == "direct":
        assert isinstance(result, dict)
    else:
        assert result.rowcount == 1


def _verify_search(
    context: SurfaceContext,
    result: Any,
    config: BenchmarkConfig,
) -> None:
    """Assert that one search scenario returned the expected result shape."""

    state = context.db.inspect_vector_index(
        metric=config.metric,
        index_name=context.index_name,
    )
    assert state["state"] == "ready"
    assert state["total_rows"] == config.rows
    if context.surface == "direct":
        assert result.rowcount == context.top_k
    else:
        assert result.rowcount == context.top_k


def _build_scenarios() -> tuple[Scenario, ...]:
    """Return the index-build scenarios covered by this benchmark."""

    return (
        Scenario(
            name="direct_build_snapshot",
            description=(
                "Build the direct-vector ANN snapshot in an isolated benchmark "
                "database."
            ),
            setup=_setup_direct,
            operation=_run_build,
            verify=_verify_build,
        ),
        Scenario(
            name="sql_build_snapshot",
            description=(
                "Build the SQL-surface ANN snapshot in an isolated benchmark "
                "database."
            ),
            setup=_setup_sql,
            operation=_run_build,
            verify=_verify_build,
        ),
        Scenario(
            name="cypher_build_snapshot",
            description=(
                "Build the Cypher-surface ANN snapshot in an isolated benchmark "
                "database."
            ),
            setup=_setup_cypher,
            operation=_run_build,
            verify=_verify_build,
        ),
    )


def _search_scenarios() -> tuple[Scenario, ...]:
    """Return the search scenarios covered by this benchmark."""

    return (
        Scenario(
            name="direct_search",
            description=(
                "Search through the direct vector API against a ready ANN "
                "snapshot plus exact-delta rerank runtime."
            ),
            setup=_setup_direct,
            operation=_build_then_search,
            verify=_verify_search,
        ),
        Scenario(
            name="sql_search",
            description=(
                "Search through the SQL vector surface against a ready ANN "
                "snapshot plus exact-delta rerank runtime."
            ),
            setup=_setup_sql,
            operation=_build_then_search,
            verify=_verify_search,
        ),
        Scenario(
            name="cypher_search",
            description=(
                "Search through the Cypher vector surface against a ready ANN "
                "snapshot plus exact-delta rerank runtime."
            ),
            setup=_setup_cypher,
            operation=_build_then_search,
            verify=_verify_search,
        ),
    )


def _benchmark_build_scenario(
    scenario: Scenario,
    config: BenchmarkConfig,
) -> dict[str, Any]:
    """Run one build scenario and summarize its single measurement."""

    with tempfile.TemporaryDirectory() as tmpdir:
        context = scenario.setup(Path(tmpdir), config)
        try:
            measurement, result = _run_one_measurement(
                context,
                scenario.operation,
                None,
            )
            scenario.verify(context, result, config)
            return {
                "description": scenario.description,
                "surface": context.surface,
                "timed_query_count": 0,
                "runtime_state": context.db.inspect_vector_index(
                    metric=config.metric,
                    index_name=context.index_name,
                ),
                "total_ms": _timing_summary([measurement.total_ms]),
                "orchestration_ms": _timing_summary([measurement.orchestration_ms]),
                "rss_delta_bytes": _timing_summary(
                    [float(measurement.rss_delta_bytes)]
                ),
                "python_peak_bytes": _timing_summary(
                    [float(measurement.python_peak_bytes)]
                ),
                "stage_breakdown": _aggregate_stage_stats([measurement]),
            }
        finally:
            context.db.close()


def _benchmark_search_scenario(
    scenario: Scenario,
    config: BenchmarkConfig,
) -> dict[str, Any]:
    """Run one search scenario across its warmup and timed query set."""

    with tempfile.TemporaryDirectory() as tmpdir:
        context = scenario.setup(Path(tmpdir), config)
        try:
            _run_build(context, None)
            for query in context.warmup_queries:
                _run_one_measurement(context, _run_search, query)
            measurements: list[RunMeasurement] = []
            for _ in range(config.repetitions):
                for query in context.timed_queries:
                    measurement, result = _run_one_measurement(
                        context,
                        _run_search,
                        query,
                    )
                    scenario.verify(context, result, config)
                    measurements.append(measurement)
            return {
                "description": scenario.description,
                "surface": context.surface,
                "warmup": len(context.warmup_queries),
                "repetitions": config.repetitions,
                "queries_per_repetition": len(context.timed_queries),
                "timed_query_count": len(measurements),
                "runtime_state": context.db.inspect_vector_index(
                    metric=config.metric,
                    index_name=context.index_name,
                ),
                "total_ms": _timing_summary(
                    [measurement.total_ms for measurement in measurements]
                ),
                "orchestration_ms": _timing_summary(
                    [measurement.orchestration_ms for measurement in measurements]
                ),
                "rss_delta_bytes": _timing_summary(
                    [
                        float(measurement.rss_delta_bytes)
                        for measurement in measurements
                    ]
                ),
                "python_peak_bytes": _timing_summary(
                    [
                        float(measurement.python_peak_bytes)
                        for measurement in measurements
                    ]
                ),
                "stage_breakdown": _aggregate_stage_stats(measurements),
            }
        finally:
            context.db.close()


def _print_text_report(config: BenchmarkConfig, results: dict[str, Any]) -> None:
    """Print the benchmark results in a compact human-readable report."""

    print("Benchmark: vector_snapshot_runtime_attribution")
    print(
        "Config:",
        (
            f"dataset={config.dataset}, rows={config.rows}, queries={config.queries}, "
            f"top_k={config.top_k}, ann_min_vectors={config.ann_min_vectors}, "
            f"warmup={config.warmup}, repetitions={config.repetitions}, "
            f"metric={config.metric}"
        ),
    )
    for scenario_name, summary in results.items():
        print()
        print(f"[{scenario_name}]")
        print(summary["description"])
        runtime_state = summary["runtime_state"]
        print(
            (
                f"surface={summary['surface']}, state={runtime_state['state']}, "
                f"total_rows={runtime_state['total_rows']}, "
                f"delta_rows={runtime_state['delta_rows']}, "
                f"snapshot_rows={runtime_state['snapshot_rows']}"
            )
        )
        print(
            (
                f"mean_total_ms={summary['total_ms']['mean']:.3f}, "
                f"mean_orchestration_ms={summary['orchestration_ms']['mean']:.3f}"
            )
        )


def main() -> None:
    """Run all build and search scenarios for the attribution benchmark."""

    config = _parse_args()
    _validate_config(config)
    results: dict[str, Any] = {}
    for scenario in _build_scenarios():
        results[scenario.name] = _benchmark_build_scenario(scenario, config)
    for scenario in _search_scenarios():
        results[scenario.name] = _benchmark_search_scenario(scenario, config)
    if config.output == "json":
        print(
            json.dumps(
                {
                    "benchmark": "vector_snapshot_runtime_attribution",
                    "config": {
                        "dataset": config.dataset,
                        "rows": config.rows,
                        "queries": config.queries,
                        "top_k": config.top_k,
                        "warmup": config.warmup,
                        "repetitions": config.repetitions,
                        "ann_min_vectors": config.ann_min_vectors,
                        "metric": config.metric,
                        "seed": config.seed,
                        "sample_mode": config.sample_mode,
                        "lancedb_num_partitions": config.lancedb_num_partitions,
                        "lancedb_num_sub_vectors": config.lancedb_num_sub_vectors,
                        "lancedb_nprobes": config.lancedb_nprobes,
                        "lancedb_refine_factor": config.lancedb_refine_factor,
                    },
                    "scenario_summaries": results,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    _print_text_report(config, results)


if __name__ == "__main__":
    main()
