"""High-level embedded database interface for HumemDB.

The `HumemDB` class is the public entry point for the current runtime. It owns the
SQLite and DuckDB engine wrappers, exposes a conservative routing API, and defines the
current lifecycle semantics for queries and transactions.

The current public surface is intentionally conservative:

- `db.query(...)` infers `HumemSQL v0` versus `HumemCypher v0` for the currently
- `db.query(...)` also infers language-level vector search from PostgreSQL-like SQL
    vector ordering or Neo4j-like Cypher `SEARCH ... VECTOR INDEX ...` text
- SQLite is the canonical public write target, including vectors
- DuckDB is read-only through the public API
- query routing is internal and conservative; callers do not choose engines
- PostgreSQL-like SQL is translated into backend SQL before execution
- transaction control is explicit on the canonical SQLite store

As the project grows, this module is where routing, query validation, and the portable
frontend surfaces will expand.
"""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
import csv
from dataclasses import dataclass
from functools import lru_cache
import json
import logging
import os
import re
from numbers import Integral
from pathlib import Path
from typing import (
    Any,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    TypeAlias,
    cast,
)

from ._vector_runtime import (
    DirectVectorSearchPlan as _DirectVectorSearchPlan,
    PendingTargetNamespacedVectorRow as _PendingTargetNamespacedVectorRow,
    ResolvedVectorCandidates as _ResolvedVectorCandidates,
    CandidateVectorQueryResult as _CandidateVectorQueryResult,
    CandidateVectorQueryPlan as _CandidateVectorQueryPlan,
    CypherVectorQueryPlan as _CypherVectorQueryPlan,
    SQLVectorQueryPlan as _SQLVectorQueryPlan,
    TargetNamespacedVectorRow as _TargetNamespacedVectorRow,
    VECTOR_RESULT_COLUMNS as _VECTOR_RESULT_COLUMNS,
    candidate_vector_result_from_query_result,
    is_vector_query_text as _is_vector_query_text,
    plan_candidate_vector_query as _plan_candidate_vector_query,
    plan_direct_vector_search as _plan_direct_vector_search,
    plan_sql_vector_write as _vectorrt_plan_sql_vector_write,
    plan_sql_vector_write_batch as _vectorrt_plan_sql_vector_write_batch,
)
from .cypher import (
    CreateNodePlan,
    CreateRelationshipFromSeparatePatternsPlan,
    CreateRelationshipPlan,
    DeleteNodePlan,
    DeleteRelationshipPlan,
    MatchCreateRelationshipBetweenNodesPlan,
    MatchCreateRelationshipPlan,
    SetNodePlan,
    SetRelationshipPlan,
    _CypherPlanShape as _CypherPlanShape,
    GraphPlan as _GraphPlan,
    _bind_plan_values,
    _encode_property_value,
    _analyze_cypher_plan,
    _ensure_graph_schema,
    _execute_cypher,
    _normalize_params,
)
from .cypher_frontend import lower_cypher_text as _lower_generated_cypher_text
from .engines import _DuckDBEngine, _SQLiteEngine
from .sql import (
    SQLTranslationPlan as _SQLTranslationPlan,
    translate_sql,
    translate_sql_plan,
)
from .types import (
    BatchParameters,
    InternalQueryType,
    QueryParameters,
    QueryResult,
    QueryType,
    Route,
)
from .vector import (
    _ExactVectorIndex,
    IndexedVectorRuntimeConfig,
    _LanceDBVectorIndex,
    _NamedVectorIndex,
    VectorMetric,
    _clear_vector_tombstones,
    _delete_named_vector_index,
    _delete_vector_index_snapshot_metadata,
    _delete_target_namespaced_vectors,
    _ensure_vector_schema,
    _insert_vectors,
    _list_vector_index_snapshot_metadata,
    _load_vector_index_snapshot_metadata,
    _list_named_vector_indexes,
    _load_named_vector_index,
    _load_named_vector_index_for_metric,
    _load_filtered_vector_target_keys,
    _load_vector_tombstones,
    _load_vector_matrix,
    _drop_lancedb_table,
    _upsert_vector_index_snapshot_metadata,
    _upsert_named_vector_index,
    _upsert_vectors,
    _upsert_vector_metadata,
)

logger = logging.getLogger(__name__)

_SQL_OLAP_THRESHOLDS_PATH_ENV = "HUMEMDB_SQL_OLAP_THRESHOLDS_PATH"

_CYPHER_MATCH_PREFIX = re.compile(r"^MATCH\b")
_CYPHER_OPTIONAL_MATCH_PREFIX = re.compile(r"^OPTIONAL\s+MATCH\b")
_CYPHER_CREATE_PATTERN = re.compile(r"^CREATE\s*\(")
_CYPHER_MERGE_PATTERN = re.compile(r"^MERGE\s*\(")
_CYPHER_UNWIND_PREFIX = re.compile(r"^UNWIND\b")
_CYPHER_CALL_PREFIX = re.compile(r"^CALL\b")
_CYPHER_RETURN_PREFIX = re.compile(r"^RETURN\b")
_CYPHER_REMOVE_PREFIX = re.compile(r"^REMOVE\b")
_VECTOR_INDEX_NAME_RE = r"[A-Za-z_][A-Za-z0-9_]*"
_VECTOR_INDEX_METRIC_RE = r"cosine|dot|l2"
_VECTOR_SQL_IDENTIFIER_RE = r"[A-Za-z_][A-Za-z0-9_\.]*"
_PGVECTOR_OPERATOR_CLASS_RE = r"vector_cosine_ops|vector_ip_ops|vector_l2_ops"
_SQL_CREATE_VECTOR_INDEX_RE = re.compile(
    (
        r"^\s*CREATE\s+INDEX\s+"
        r"(?:(?P<if_not_exists>IF\s+NOT\s+EXISTS)\s+)?"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s+"
        r"ON\s+VECTOR\s*\(\s*embedding\s*\)"
        r"(?:\s+WITH\s*\(\s*metric\s*=\s*"
        rf"(?P<metric>{_VECTOR_INDEX_METRIC_RE})\s*\))?\s*;?\s*$"
    ),
    re.IGNORECASE,
)
_SQL_CREATE_VECTOR_INDEX_PGVECTOR_RE = re.compile(
    (
        r"^\s*CREATE\s+INDEX\s+"
        r"(?:(?P<if_not_exists>IF\s+NOT\s+EXISTS)\s+)?"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s+"
        r"ON\s+"
        rf"(?P<table>{_VECTOR_SQL_IDENTIFIER_RE})\s+"
        r"USING\s+(?P<method>ivfpq)\s*"
        r"\(\s*"
        rf"(?P<column>{_VECTOR_SQL_IDENTIFIER_RE})\s+"
        rf"(?P<operator_class>{_PGVECTOR_OPERATOR_CLASS_RE})\s*"
        r"\)"
        r"(?:\s+WITH\s*\(.*\))?\s*;?\s*$"
    ),
    re.IGNORECASE | re.DOTALL,
)
_SQL_REFRESH_VECTOR_INDEX_RE = re.compile(
    rf"^\s*REFRESH\s+VECTOR\s+INDEX\s+(?P<name>{_VECTOR_INDEX_NAME_RE})\s*;?\s*$",
    re.IGNORECASE,
)
_SQL_REBUILD_VECTOR_INDEX_RE = re.compile(
    rf"^\s*REBUILD\s+VECTOR\s+INDEX\s+(?P<name>{_VECTOR_INDEX_NAME_RE})\s*;?\s*$",
    re.IGNORECASE,
)
_SQL_DROP_VECTOR_INDEX_RE = re.compile(
    (
        r"^\s*DROP\s+INDEX\s+"
        r"(?:(?P<if_exists>IF\s+EXISTS)\s+)?"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s*;?\s*$"
    ),
    re.IGNORECASE,
)
_SQL_ALTER_VECTOR_INDEX_PAUSE_MAINTENANCE_RE = re.compile(
    (
        r"^\s*ALTER\s+VECTOR\s+INDEX\s+"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s+"
        r"PAUSE\s+MAINTENANCE\s*;?\s*$"
    ),
    re.IGNORECASE,
)
_SQL_ALTER_VECTOR_INDEX_RESUME_MAINTENANCE_RE = re.compile(
    (
        r"^\s*ALTER\s+VECTOR\s+INDEX\s+"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s+"
        r"RESUME\s+MAINTENANCE\s*;?\s*$"
    ),
    re.IGNORECASE,
)
_SQL_LIST_VECTOR_INDEXES_RE = re.compile(
    r"^\s*SELECT\s+\*\s+FROM\s+humemdb_vector_indexes\s*;?\s*$",
    re.IGNORECASE,
)
_CYPHER_CREATE_VECTOR_INDEX_RE = re.compile(
    (
        r"^\s*CREATE\s+VECTOR\s+INDEX\s+"
        r"(?:(?P<if_not_exists>IF\s+NOT\s+EXISTS)\s+)?"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})"
        r"(?:\s+FOR\s+"
        rf"(?P<metric>{_VECTOR_INDEX_METRIC_RE}))?\s*;?\s*$"
    ),
    re.IGNORECASE,
)
_CYPHER_CREATE_VECTOR_INDEX_NEO4J_RE = re.compile(
    (
        r"^\s*CREATE\s+VECTOR\s+INDEX\s+"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s+"
        r"(?:(?P<if_not_exists>IF\s+NOT\s+EXISTS)\s+)?"
        r"FOR\s*\(\s*"
        rf"(?P<alias>{_VECTOR_INDEX_NAME_RE})\s*:\s*"
        rf"(?P<label>{_VECTOR_INDEX_NAME_RE})\s*"
        r"\)\s+ON\s*\(?\s*"
        rf"(?P<property_alias>{_VECTOR_INDEX_NAME_RE})\."
        rf"(?P<property>{_VECTOR_INDEX_NAME_RE})\s*\)?"
        r"(?:\s+OPTIONS\s*\{(?P<options>.*)\})?\s*;?\s*$"
    ),
    re.IGNORECASE | re.DOTALL,
)
_CYPHER_SHOW_VECTOR_INDEXES_RE = re.compile(
    r"^\s*SHOW\s+VECTOR\s+INDEXES\s*;?\s*$",
    re.IGNORECASE,
)
_CYPHER_DROP_VECTOR_INDEX_RE = re.compile(
    (
        r"^\s*DROP\s+VECTOR\s+INDEX\s+"
        r"(?:(?P<if_exists>IF\s+EXISTS)\s+)?"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s*;?\s*$"
    ),
    re.IGNORECASE,
)
_CYPHER_REFRESH_VECTOR_INDEX_RE = re.compile(
    rf"^\s*REFRESH\s+VECTOR\s+INDEX\s+(?P<name>{_VECTOR_INDEX_NAME_RE})\s*;?\s*$",
    re.IGNORECASE,
)
_CYPHER_REBUILD_VECTOR_INDEX_RE = re.compile(
    rf"^\s*REBUILD\s+VECTOR\s+INDEX\s+(?P<name>{_VECTOR_INDEX_NAME_RE})\s*;?\s*$",
    re.IGNORECASE,
)
_CYPHER_ALTER_VECTOR_INDEX_PAUSE_MAINTENANCE_RE = re.compile(
    (
        r"^\s*ALTER\s+VECTOR\s+INDEX\s+"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s+"
        r"PAUSE\s+MAINTENANCE\s*;?\s*$"
    ),
    re.IGNORECASE,
)
_CYPHER_ALTER_VECTOR_INDEX_RESUME_MAINTENANCE_RE = re.compile(
    (
        r"^\s*ALTER\s+VECTOR\s+INDEX\s+"
        rf"(?P<name>{_VECTOR_INDEX_NAME_RE})\s+"
        r"RESUME\s+MAINTENANCE\s*;?\s*$"
    ),
    re.IGNORECASE,
)

_DirectVectorMetadata: TypeAlias = Mapping[str, str | int | float | bool | None]
_DirectVectorRow: TypeAlias = (
    Sequence[float]
    | tuple[int, Sequence[float]]
    | Mapping[str, object]
)
_GraphImportPropertyType: TypeAlias = Literal[
    "string", "integer", "real", "boolean"
]


@dataclass(frozen=True, slots=True)
class _SnapshotRefreshResult:
    """Completed background snapshot build ready for promotion.

    Attributes:
        metric: Similarity metric used by the refreshed snapshot index.
        epoch: Refresh generation number associated with this build.
        index: Built LanceDB index ready to be promoted.
        item_indexes: Mapping from logical item ids to row indexes in the
            snapshot index.
    """

    metric: VectorMetric
    epoch: int
    index: _LanceDBVectorIndex
    item_indexes: dict[tuple[str, str, int], int]


@dataclass(frozen=True, slots=True)
class _ResolvedPublicVectorIndex:
    """Resolved public vector index reference for lifecycle/admin operations.

    Attributes:
        name: Public vector index name referenced by the user.
        metric: Similarity metric resolved for that public index.
        exists: Whether the named index currently exists.
    """

    name: str
    metric: VectorMetric
    exists: bool


def _build_snapshot_refresh_result(
    *,
    metric: VectorMetric,
    epoch: int,
    item_ids: Any,
    matrix: Any,
    lance_path: Path,
    config: Any,
) -> _SnapshotRefreshResult:
    """Build one LanceDB snapshot from an immutable matrix copy."""

    index = _LanceDBVectorIndex.from_matrix(
        item_ids=item_ids,
        matrix=matrix,
        metric=metric,
        lance_path=lance_path,
        config=config,
    )
    item_indexes = {
        (str(item_id[0]), str(item_id[1]), int(item_id[2])): index_value
        for index_value, item_id in enumerate(index.item_ids.tolist())
    }
    return _SnapshotRefreshResult(
        metric=metric,
        epoch=epoch,
        index=index,
        item_indexes=item_indexes,
    )


_VECTOR_RUNTIME_BACKGROUND_ERRORS = (
    ImportError,
    OSError,
    RuntimeError,
    TimeoutError,
    TypeError,
    ValueError,
)

_WorkloadKind: TypeAlias = Literal[
    "transactional_read",
    "transactional_write",
    "analytical_read",
    "graph_read",
    "graph_write",
    "vector_search",
]


@dataclass(frozen=True, slots=True)
class _WorkloadProfile:
    """Explainable workload classification derived from validated query structure.

    Attributes:
        kind: Small routing-oriented workload family such as transactional read,
            analytical read, graph read, or graph write.
        is_read_only: Whether execution should avoid write-capable routes.
        preferred_route: Engine route that currently best matches the workload.
        reason: Human-readable explanation of why this classification won.
    """

    kind: _WorkloadKind
    is_read_only: bool
    preferred_route: Route
    reason: str


@dataclass(frozen=True, slots=True)
class _RouteDecision:
    """Internal record of how one query route was selected.

    Attributes:
        selected_route: Concrete engine route chosen for execution.
        source: Whether the route came from internal automatic routing or an
            explicit internal override.
        reason: Human-readable explanation for the final route choice.
    """

    selected_route: Route
    source: Literal["automatic", "explicit"]
    reason: str


@dataclass(frozen=True, slots=True)
class _OlapRoutingRule:
    """One benchmark-calibrated SQL shape family that should route to DuckDB.

    Attributes:
        min_join_count: Minimum JOIN count required by the rule.
        min_aggregate_count: Minimum aggregate-expression count required.
        min_cte_count: Minimum CTE count required.
        min_window_count: Minimum window-function count required.
        min_exists_count: Minimum correlated `EXISTS` count required.
        require_group_by: Whether the rule requires `GROUP BY`.
        require_distinct: Whether the rule requires `DISTINCT`.
        require_order_by_or_limit: Whether the rule requires `ORDER BY` or
            `LIMIT`.
    """

    min_join_count: int = 0
    min_aggregate_count: int = 0
    min_cte_count: int = 0
    min_window_count: int = 0
    min_exists_count: int = 0
    require_group_by: bool = False
    require_distinct: bool = False
    require_order_by_or_limit: bool = False


@dataclass(frozen=True, slots=True)
class _OlapRoutingThresholds:
    """Benchmark-calibrated thresholds for admitting SQL OLAP reads to DuckDB.

    Attributes:
        benchmark_calibrated: Whether these thresholds came from benchmark output
            rather than conservative built-ins.
        min_join_count: Default minimum JOIN count for DuckDB admission.
        min_aggregate_count: Default minimum aggregate-expression count.
        min_cte_count: Default minimum CTE count.
        min_window_count: Default minimum window-function count.
        require_order_by_or_limit: Whether broader SQL reads must also contain
            `ORDER BY` or `LIMIT`.
        rules: Optional more specific shape families layered on top of the coarse
            thresholds.
    """

    benchmark_calibrated: bool
    min_join_count: int = 0
    min_aggregate_count: int = 0
    min_cte_count: int = 0
    min_window_count: int = 0
    require_order_by_or_limit: bool = False
    rules: tuple[_OlapRoutingRule, ...] = ()


_DEFAULT_OLAP_ROUTING_THRESHOLDS = _OlapRoutingThresholds(
    benchmark_calibrated=True,
)


@dataclass(frozen=True, slots=True)
class _QueryExecutionPlan:
    """Thin internal plan for one public `db.query(...)` call.

    Attributes:
        text: Original user query text.
        route: Engine route selected for execution.
        route_decision: Explanation of how the route was chosen.
        query_type: Normalized internal query kind.
        params: Normalized query parameters.
        workload: Routing-oriented workload classification.
        translated_text: Backend SQL emitted from the SQL translation layer when
            applicable.
        sql_plan: Lightweight SQL planning metadata when the query is SQL.
        cypher_plan: Parsed Cypher plan when the query is Cypher.
        cypher_shape: Lightweight Cypher shape metadata for routing and reporting.
        vector_plan: Lowered vector candidate-query plan when the text expresses a
            language-level vector search.
        sql_is_read_only: Cached SQL read-only flag when the query went through the
            SQL planner.
    """

    text: str
    route: Route
    route_decision: _RouteDecision
    query_type: InternalQueryType
    params: QueryParameters
    workload: _WorkloadProfile
    translated_text: str | None = None
    sql_plan: _SQLTranslationPlan | None = None
    cypher_plan: _GraphPlan | None = None
    cypher_shape: _CypherPlanShape | None = None
    vector_plan: _CandidateVectorQueryPlan | None = None
    sql_is_read_only: bool | None = None


@dataclass(frozen=True, slots=True)
class _BatchExecutionPlan:
    """Thin internal plan for one public `executemany(...)` call.

    Attributes:
        text: Original SQL statement text.
        route: Engine route selected for execution.
        params_seq: Normalized batch parameter sequence.
        workload: Routing-oriented workload classification.
        translated_text: Backend SQL emitted from the SQL translation layer.
    """

    text: str
    route: Route
    params_seq: BatchParameters
    translated_text: str


class _TransactionalEngine(Protocol):
    """Protocol for engine objects that support explicit transactions.

    The public `HumemDB` transaction helpers depend on this minimal surface so they
    can work with both SQLite and DuckDB without caring about driver details.

    Attributes:
        Implementations do not expose protocol-level data attributes; only the
        transaction lifecycle methods below are required.
    """

    def begin(self) -> None:
        """Start one explicit transaction on the engine.

        Raises:
            RuntimeError: If the backing engine already has an active transaction.
        """

        raise NotImplementedError

    def commit(self) -> None:
        """Commit the current explicit transaction on the engine.

        Returns:
            `None`.
        """

        raise NotImplementedError

    def rollback(self) -> None:
        """Roll back the current explicit transaction on the engine.

        Returns:
            `None`.
        """

        raise NotImplementedError


class HumemDB:
    """Main in-process entry point for HumemDB.

    Args:
        base_path: Public-facing base path for HumemDB storage. HumemDB derives
            companion files as `<base>.sqlite3` and `<base>.duckdb` internally.
            Missing parent directories and backing database files are created on first
            use; existing database files are reopened.
        preload_vectors: Optional eager vector preload flag. Use `False` to keep the
            exact vector set lazy-loaded or `True` to warm it on open when vector data
            already exists.

    Notes:
        Instantiating `HumemDB` opens both embedded database connections. Use the object
        as a context manager or call `close()` explicitly to release them.
    """

    def __init__(
        self,
        base_path: str | Path,
        *,
        preload_vectors: bool = False,
    ) -> None:
        """Create or open HumemDB from one public-facing base path.

        Args:
            base_path: Public-facing base path for HumemDB storage. HumemDB derives
                companion files as `<base>.sqlite3` and `<base>.duckdb` internally.
                Missing parent directories and backing database files are created on
                first use; existing database files are reopened.
            preload_vectors: Whether to warm the exact vector cache immediately when
                vector storage already exists.
        """

        self._sqlite_path = ""
        self._duckdb_path = None
        self._graph_schema_ready = False
        self._vector_schema_ready = False
        self._vector_matrix_cache = None
        self._vector_item_index_cache = None
        self._vector_namespace_index_cache = None
        self._vector_index_cache = {}
        self._vector_runtime_config = IndexedVectorRuntimeConfig()
        self._snapshot_vector_index_cache = {}
        self._snapshot_vector_item_index_cache = {}
        self._vector_tombstone_cache = None
        self._snapshot_refresh_executor = None
        self._snapshot_refresh_futures = {}
        self._snapshot_refresh_epoch = {}
        self._snapshot_generation_by_metric = {}

        base = Path(base_path)
        sqlite_path = base.with_suffix(".sqlite3")
        duckdb_path = base.with_suffix(".duckdb")

        self._initialize_runtime(
            sqlite_path=sqlite_path,
            duckdb_path=duckdb_path,
            preload_vectors=preload_vectors,
        )

    def _initialize_runtime(
        self,
        *,
        sqlite_path: Path,
        duckdb_path: Path | None,
        preload_vectors: bool,
    ) -> None:
        """Initialize embedded engines and lazy runtime state.

        Args:
            sqlite_path: Canonical SQLite backing path derived from the public base
                path.
            duckdb_path: Optional DuckDB backing path derived from the public base
                path.
            preload_vectors: Whether to warm the exact vector cache immediately when
                vector tables already exist.
        """

        self._sqlite_path = str(sqlite_path)
        self._duckdb_path = None if duckdb_path is None else str(duckdb_path)
        self._graph_schema_ready = False
        self._vector_schema_ready = False
        self._vector_matrix_cache: tuple[Any, Any] | None = None
        self._vector_item_index_cache: dict[tuple[str, str, int], int] | None = None
        self._vector_namespace_index_cache: (
            dict[tuple[str, str], tuple[int, ...]] | None
        ) = None
        self._vector_index_cache: dict[VectorMetric, _ExactVectorIndex] = {}
        self._vector_runtime_config = IndexedVectorRuntimeConfig()
        self._snapshot_vector_index_cache: dict[VectorMetric, _LanceDBVectorIndex] = {}
        self._snapshot_vector_item_index_cache: (
            dict[VectorMetric, dict[tuple[str, str, int], int]]
        ) = {}
        self._vector_tombstone_cache: (
            dict[VectorMetric, set[tuple[str, str, int]]] | None
        ) = None
        self._snapshot_refresh_executor: ThreadPoolExecutor | None = None
        self._snapshot_refresh_futures: (
            dict[VectorMetric, Future[_SnapshotRefreshResult]]
        ) = {}
        self._snapshot_refresh_epoch: dict[VectorMetric, int] = {}
        self._snapshot_generation_by_metric: dict[VectorMetric, int] = {}

        sqlite_path_obj = Path(self._sqlite_path)
        sqlite_path_obj.parent.mkdir(parents=True, exist_ok=True)

        self._sqlite = _SQLiteEngine(str(sqlite_path_obj))
        self._duckdb = _DuckDBEngine(self._duckdb_path)
        self._duckdb.attach_sqlite(str(sqlite_path_obj))
        logger.debug(
            "HumemDB initialized with sqlite_path=%s duckdb_path=%s",
            self._sqlite_path,
            self._duckdb_path,
        )
        if preload_vectors:
            self.preload_vectors()

    def query(
        self,
        text: str,
        *,
        params: QueryParameters = None,
    ) -> QueryResult:
        """Execute a query against the internally selected engine route.

        Args:
            text: Query text to execute.
            params: Optional named parameters. Public SQL, Cypher, and language-level
                vector queries use mapping-style params such as
                `{ "name": "Alice" }`.

                Vector intent is inferred from the query text itself. HumemSQL uses
                PostgreSQL-like `ORDER BY embedding <->|<=>|<#> $query LIMIT ...`
                forms. HumemCypher uses Neo4j-like
                `SEARCH ... VECTOR INDEX user_embedding_idx FOR $query LIMIT ...`
                forms.

        Returns:
            A normalized `QueryResult`.

        Raises:
            NotImplementedError: If an unsupported query type is requested.
            ValueError: If execution reaches an unsupported internal route or a write
                is planned for DuckDB.
        """

        admin_result = self._execute_vector_index_admin_query(text=text, params=params)
        if admin_result is not None:
            return admin_result

        plan = _plan_query(
            text,
            route=None,
            params=params,
        )
        return self._execute_query_plan(plan)

    def insert_vectors(
        self,
        rows: Sequence[_DirectVectorRow],
    ) -> tuple[int, ...]:
        """Insert direct vectors into the canonical SQLite store.

        Args:
            rows: Sequence of direct-vector inputs. Each row may be a plain embedding,
                an explicit `(target_id, embedding)` tuple for import-style writes, or a
                mapping with an `embedding` key plus optional `metadata` and `target_id`
                entries.

        Returns:
            The direct target ids assigned to the inserted rows, in input order.

        Notes:
            Plain embeddings are the default public path. When metadata should travel
            with the insert, pass rows such as
            `{"embedding": [...], "metadata": {"group": "alpha"}}`.
        """

        if not rows:
            return ()

        self._ensure_vector_schema()
        normalized_rows, assigned_ids, metadata_rows = _normalize_direct_vector_rows(
            self._sqlite,
            rows,
        )
        _insert_vectors(
            self._sqlite,
            normalized_rows,
            target="direct",
            namespace="",
        )
        if metadata_rows:
            _upsert_vector_metadata(
                self._sqlite,
                metadata_rows,
                target="direct",
                namespace="",
            )
        self._invalidate_exact_vector_cache()
        self._maybe_schedule_snapshot_refresh_after_write()
        return assigned_ids

    def delete_vectors(
        self,
        target_ids: Sequence[int],
    ) -> int:
        """Delete direct vectors from the canonical SQLite store.

        Args:
            target_ids: Direct-vector ids to remove.

        Returns:
            Number of direct vectors deleted.
        """

        if not target_ids:
            return 0

        self._ensure_vector_schema()
        deleted = _delete_target_namespaced_vectors(
            self._sqlite,
            tuple(("direct", "", int(target_id)) for target_id in target_ids),
        )
        if deleted == 0:
            return 0
        self._invalidate_exact_vector_cache()
        self._clear_vector_tombstone_cache()
        self._maybe_schedule_snapshot_refresh_after_write()
        return deleted

    def search_vectors(
        self,
        query: Sequence[float],
        *,
        top_k: int = 10,
        metric: VectorMetric = "cosine",
        filters: Mapping[str, str | int | float | bool | None] | None = None,
    ) -> QueryResult:
        """Search the current SQLite-backed vector set with the internal runtime.

        Args:
            query: Query embedding to rank against the current vector set.
            top_k: Maximum number of nearest matches to return.
            metric: Similarity metric to use for ranking.
            filters: Optional equality metadata filters for direct-vector rows.

        Returns:
            A normalized `QueryResult` with
            `(target, namespace, target_id, score)` rows.

        Notes:
            `search_vectors(...)` is the public direct-vector search surface. Small
            candidate sets stay on the exact NumPy path; larger ones can use the
            internal ANN snapshot plus exact-delta runtime while SQLite remains the
            canonical store. Vector search over SQL rows or graph nodes lives on
            language-level SQL and Cypher query syntax through `query(...)`.
        """

        direct_plan = _plan_direct_vector_search(
            query,
            top_k=top_k,
            metric=metric,
            filters=filters,
        )
        resolved_candidates = self._resolve_direct_vector_search(direct_plan)
        return self._execute_vector_search(
            query=direct_plan.query,
            top_k=direct_plan.top_k,
            metric=direct_plan.metric,
            resolved_candidates=resolved_candidates,
        )

    def inspect_vector_index(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> dict[str, Any]:
        """Return public lifecycle state for one metric-backed vector index."""

        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        if resolved.exists:
            state = self._public_vector_index_state(metric=resolved.metric)
        else:
            state = dict(self._inspect_vector_runtime(metric=resolved.metric))
            state["name"] = resolved.name
            state["enabled"] = True
            state["maintenance_paused"] = False
            state["state"] = "ready" if state["snapshot_rows"] > 0 else "exact_only"
        return state

    def build_vector_index(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> dict[str, Any]:
        """Enable and build one named metric-backed ANN snapshot."""

        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        self._ensure_public_vector_index_name(
            metric=resolved.metric,
            name=resolved.name,
        )
        self._build_public_vector_index(metric=resolved.metric, only_if_missing=True)
        return self._public_vector_index_state(metric=resolved.metric)

    def refresh_vector_index(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> dict[str, Any]:
        """Force one named metric-backed ANN snapshot to rebuild."""

        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        self._ensure_public_vector_index_name(
            metric=resolved.metric,
            name=resolved.name,
        )
        self._build_public_vector_index(metric=resolved.metric, only_if_missing=False)
        return self._public_vector_index_state(metric=resolved.metric)

    def pause_vector_index(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> dict[str, Any]:
        """Pause automatic ANN snapshot maintenance for one named vector index."""

        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        self._set_vector_index_maintenance_paused(
            metric=resolved.metric,
            index_name=resolved.name,
            paused=True,
        )
        return self._public_vector_index_state(metric=resolved.metric)

    def resume_vector_index(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> dict[str, Any]:
        """Resume automatic ANN snapshot maintenance for one named vector index."""

        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        self._set_vector_index_maintenance_paused(
            metric=resolved.metric,
            index_name=resolved.name,
            paused=False,
        )
        return self._public_vector_index_state(metric=resolved.metric)

    @contextmanager
    def deferred_vector_index_maintenance(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> Iterator[HumemDB]:
        """Pause automatic ANN snapshot maintenance within one ingest block."""

        self.pause_vector_index(metric=metric, index_name=index_name)
        try:
            yield self
        finally:
            self.resume_vector_index(metric=metric, index_name=index_name)

    def drop_vector_index(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> dict[str, Any]:
        """Disable and remove one named metric-backed vector index."""

        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        self._drop_public_vector_index(
            metric=resolved.metric,
            index_name=resolved.name,
            disable=True,
        )
        return self._public_vector_index_state(metric=resolved.metric)

    def await_vector_index_refresh(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> bool:
        """Wait for one pending background refresh on the metric, if any."""

        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        return self._await_vector_runtime_snapshot_refresh(metric=resolved.metric)

    def set_vector_metadata(
        self,
        rows: Sequence[tuple[int, Mapping[str, str | int | float | bool | None]]],
    ) -> int:
        """Insert or replace narrow equality-filterable direct-vector metadata.

        Args:
            rows: Sequence of `(target_id, metadata)` pairs for existing direct
                vectors.

        Returns:
            The total number of metadata key/value pairs written.
        """

        if not rows:
            return 0

        self._ensure_vector_schema()
        _upsert_vector_metadata(
            self._sqlite,
            rows,
            target="direct",
            namespace="",
        )
        return sum(len(metadata) for _, metadata in rows)

    def preload_vectors(self) -> bool:
        """Eagerly load the current vector set into the exact-search cache.

        Returns:
            `True` when vector storage exists and the cache was loaded, or `False`
            when no vector tables are present yet.
        """

        if not self._has_vector_table():
            return False

        self._load_vector_matrix()
        return True

    def vectors_cached(self) -> bool:
        """Return whether the current vector set is loaded in memory.

        Returns:
            `True` when the vector matrix cache is populated, otherwise `False`.
        """

        return self._vector_matrix_cache is not None

    def _vector_index_enabled(self, *, metric: VectorMetric) -> bool:
        """Return whether public snapshot-index lifecycle is enabled for one metric."""

        self._ensure_vector_schema()
        named_index = _load_named_vector_index_for_metric(self._sqlite, metric=metric)
        if named_index is None:
            return True
        return named_index.enabled

    def _vector_index_maintenance_paused(self, *, metric: VectorMetric) -> bool:
        """Return whether automatic snapshot maintenance is paused."""

        self._ensure_vector_schema()
        named_index = _load_named_vector_index_for_metric(self._sqlite, metric=metric)
        if named_index is None:
            return False
        return named_index.maintenance_paused

    def _set_vector_index_enabled(
        self,
        *,
        metric: VectorMetric,
        index_name: str | None = None,
        enabled: bool,
    ) -> None:
        """Persist whether one metric's vector index should stay enabled."""

        self._ensure_vector_schema()
        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        _upsert_named_vector_index(
            self._sqlite,
            name=resolved.name,
            metric=resolved.metric,
            enabled=enabled,
        )

    def _set_vector_index_maintenance_paused(
        self,
        *,
        metric: VectorMetric,
        index_name: str | None = None,
        paused: bool,
    ) -> None:
        """Persist whether automatic snapshot refresh should stay paused."""

        self._ensure_vector_schema()
        resolved = self._resolve_public_vector_index(
            metric=metric,
            index_name=index_name,
        )
        _upsert_named_vector_index(
            self._sqlite,
            name=resolved.name,
            metric=resolved.metric,
            enabled=self._vector_index_enabled(metric=resolved.metric),
            maintenance_paused=paused,
        )
        if paused and resolved.metric in self._snapshot_refresh_futures:
            self._snapshot_refresh_epoch[resolved.metric] = (
                self._snapshot_refresh_epoch.get(resolved.metric, 0) + 1
            )

    def _resolve_public_vector_index(
        self,
        *,
        metric: VectorMetric = "cosine",
        index_name: str | None = None,
    ) -> _ResolvedPublicVectorIndex:
        """Resolve one public vector index reference by name or metric."""

        self._ensure_vector_schema()
        if index_name is not None:
            named_index = _load_named_vector_index(self._sqlite, name=index_name)
            if named_index is not None:
                return _ResolvedPublicVectorIndex(
                    name=named_index.name,
                    metric=named_index.metric,
                    exists=True,
                )
            return _ResolvedPublicVectorIndex(
                name=index_name,
                metric=metric,
                exists=False,
            )

        named_index = _load_named_vector_index_for_metric(self._sqlite, metric=metric)
        if named_index is not None:
            return _ResolvedPublicVectorIndex(
                name=named_index.name,
                metric=named_index.metric,
                exists=True,
            )
        raise ValueError(
            "HumemDB vector index lifecycle methods now require an explicit "
            f"index_name for metric {metric!r} until that named index exists."
        )

    def _ensure_public_vector_index_name(
        self,
        *,
        metric: VectorMetric,
        name: str,
    ) -> _NamedVectorIndex:
        """Persist the public name bound to one metric-backed vector index."""

        self._ensure_vector_schema()
        existing_by_name = _load_named_vector_index(self._sqlite, name=name)
        if existing_by_name is not None:
            if existing_by_name.metric != metric:
                raise ValueError(
                    f"Vector index {name!r} already targets metric "
                    f"{existing_by_name.metric!r}, not {metric!r}."
                )
            _upsert_named_vector_index(
                self._sqlite,
                name=name,
                metric=metric,
                enabled=True,
                maintenance_paused=existing_by_name.maintenance_paused,
            )
            refreshed = _load_named_vector_index(self._sqlite, name=name)
            if refreshed is None:
                raise AssertionError("Named vector index registration did not persist")
            return refreshed

        existing_for_metric = _load_named_vector_index_for_metric(
            self._sqlite,
            metric=metric,
        )
        if existing_for_metric is not None and existing_for_metric.name != name:
            runtime_state = self._inspect_vector_runtime(metric=metric)
            if existing_for_metric.enabled or runtime_state["snapshot_rows"] > 0:
                raise ValueError(
                    f"Vector metric {metric!r} is already managed by index "
                    f"{existing_for_metric.name!r}; drop it before creating {name!r}."
                )
            _delete_named_vector_index(self._sqlite, name=existing_for_metric.name)

        maintenance_paused = (
            existing_for_metric.maintenance_paused
            if existing_for_metric is not None
            else False
        )
        _upsert_named_vector_index(
            self._sqlite,
            name=name,
            metric=metric,
            enabled=True,
            maintenance_paused=maintenance_paused,
        )
        created = _load_named_vector_index(self._sqlite, name=name)
        if created is None:
            raise AssertionError("Named vector index registration did not persist")
        return created

    def _metric_for_cypher_vector_query_index(self, index_name: str) -> VectorMetric:
        """Resolve the metric used by one Cypher vector query index reference."""

        resolved = self._resolve_public_vector_index(index_name=index_name)
        return resolved.metric

    def _current_snapshot_data(
        self,
        *,
        require_threshold: bool,
    ) -> tuple[Any, Any] | None:
        """Return the current full-corpus matrix snapshot for one metric."""

        if not self._has_vector_table():
            return None

        item_ids, matrix = self._load_vector_matrix()
        row_count = int(item_ids.size)
        if row_count == 0:
            return None
        if require_threshold:
            min_training_rows = (
                self._vector_runtime_config.lancedb.minimum_rows_for_training()
            )
            required_rows = self._vector_runtime_config.ann_index_required_rows(
                minimum_training_rows=min_training_rows,
            )
            if row_count < required_rows:
                return None
        return item_ids, matrix

    def _drop_public_vector_index(
        self,
        *,
        metric: VectorMetric,
        index_name: str | None = None,
        disable: bool,
    ) -> None:
        """Remove one metric's live snapshot and optionally disable rebuilds."""

        self._ensure_vector_schema()
        if disable:
            resolved = self._resolve_public_vector_index(
                metric=metric,
                index_name=index_name,
            )
            _upsert_named_vector_index(
                self._sqlite,
                name=resolved.name,
                metric=metric,
                enabled=False,
                maintenance_paused=False,
            )

        known_tables: set[str] = set()
        cached = self._snapshot_vector_index_cache.pop(metric, None)
        if cached is not None:
            known_tables.add(cached.config.table_name)
        self._snapshot_vector_item_index_cache.pop(metric, None)

        metadata = _load_vector_index_snapshot_metadata(self._sqlite, metric=metric)
        if metadata is not None:
            known_tables.add(metadata.table_name)
        _delete_vector_index_snapshot_metadata(self._sqlite, metric=metric)
        _clear_vector_tombstones(self._sqlite, metric=metric)
        self._clear_vector_tombstone_cache()

        refresh_future = self._snapshot_refresh_futures.get(metric)
        if refresh_future is not None:
            self._snapshot_refresh_epoch[metric] = (
                self._snapshot_refresh_epoch.get(metric, 0) + 1
            )

        for table_name in known_tables:
            try:
                _drop_lancedb_table(
                    lance_path=self._vector_lancedb_path(),
                    table_name=table_name,
                )
            except _VECTOR_RUNTIME_BACKGROUND_ERRORS:
                logger.debug(
                    "Ignoring failed LanceDB cleanup table=%s",
                    table_name,
                    exc_info=True,
                )

    def _build_public_vector_index(
        self,
        *,
        metric: VectorMetric,
        only_if_missing: bool,
    ) -> None:
        """Enable and build or rebuild one metric's ANN snapshot synchronously."""

        self._ensure_vector_schema()
        self._set_vector_index_enabled(metric=metric, enabled=True)
        self._maybe_promote_snapshot_refresh(metric)
        if only_if_missing:
            existing = self._snapshot_vector_index_cache.get(metric)
            if existing is None:
                existing = self._load_persisted_snapshot_index(metric=metric)
            if existing is not None:
                return

        snapshot_data = self._current_snapshot_data(
            require_threshold=True,
        )
        if snapshot_data is None:
            self._drop_public_vector_index(metric=metric, disable=False)
            return

        item_ids, matrix = snapshot_data
        refresh_result = _build_snapshot_refresh_result(
            metric=metric,
            epoch=self._snapshot_refresh_epoch.get(metric, 0),
            item_ids=item_ids,
            matrix=matrix,
            lance_path=self._vector_lancedb_path(),
            config=self._vector_runtime_config.lancedb.with_table_name(
                self._snapshot_vector_table_name(metric=metric)
            ),
        )
        previous = self._snapshot_vector_index_cache.get(metric)
        if previous is None:
            previous = self._load_persisted_snapshot_index(metric=metric)
        self._store_live_snapshot(metric=metric, index=refresh_result.index)
        _clear_vector_tombstones(self._sqlite, metric=metric)
        self._clear_vector_tombstone_cache()
        if (
            previous is not None
            and previous.config.table_name
            != refresh_result.index.config.table_name
        ):
            try:
                _drop_lancedb_table(
                    lance_path=self._vector_lancedb_path(),
                    table_name=previous.config.table_name,
                )
            except _VECTOR_RUNTIME_BACKGROUND_ERRORS:
                logger.debug(
                    "Ignoring failed cleanup for prior snapshot table=%s",
                    previous.config.table_name,
                    exc_info=True,
                )

    def _public_vector_index_state(
        self,
        *,
        metric: VectorMetric,
    ) -> dict[str, Any]:
        """Return public lifecycle state for one metric's vector index."""

        state = dict(self._inspect_vector_runtime(metric=metric))
        enabled = self._vector_index_enabled(metric=metric)
        maintenance_paused = self._vector_index_maintenance_paused(metric=metric)
        resolved = self._resolve_public_vector_index(metric=metric)
        state["name"] = resolved.name
        state["enabled"] = enabled
        state["maintenance_paused"] = maintenance_paused
        if not enabled:
            state["state"] = "disabled"
        elif state["refresh_in_progress"]:
            state["state"] = "refreshing"
        elif state["snapshot_rows"] > 0:
            state["state"] = "ready"
        else:
            state["state"] = "exact_only"
        return state

    def _public_vector_index_rows(self) -> tuple[tuple[Any, ...], ...]:
        """Return tabular public lifecycle rows for all visible vector indexes."""

        self._ensure_vector_schema()
        metrics = {
            metadata.metric
            for metadata in _list_vector_index_snapshot_metadata(self._sqlite)
        }
        metrics.update(
            named_index.metric
            for named_index in _list_named_vector_indexes(self._sqlite)
            if named_index.enabled
        )
        rows: list[tuple[Any, ...]] = []
        for metric in sorted(metrics):
            state = self._public_vector_index_state(metric=cast(VectorMetric, metric))
            if not state["enabled"]:
                continue
            rows.append(
                (
                    state["name"],
                    state["metric"],
                    state["enabled"],
                    state["state"],
                    state["total_rows"],
                    state["indexed_rows"],
                    state["snapshot_rows"],
                    state["delta_rows"],
                    state["tombstone_rows"],
                    state["ann_threshold_rows"],
                    state["refresh_in_progress"],
                    state["snapshot_table"],
                    state["snapshot_generation"],
                    state["maintenance_paused"],
                )
            )
        return tuple(rows)

    def _vector_index_query_result(
        self,
        *,
        rows: tuple[tuple[Any, ...], ...],
        query_type: QueryType,
    ) -> QueryResult:
        """Return a normalized admin result for public vector index lifecycle APIs."""

        return QueryResult(
            rows=rows,
            columns=(
                "name",
                "metric",
                "enabled",
                "state",
                "total_rows",
                "indexed_rows",
                "snapshot_rows",
                "delta_rows",
                "tombstone_rows",
                "ann_threshold_rows",
                "refresh_in_progress",
                "snapshot_table",
                "snapshot_generation",
                "maintenance_paused",
            ),
            route="sqlite",
            query_type=query_type,
            rowcount=len(rows),
        )

    def _execute_vector_index_admin_query(
        self,
        *,
        text: str,
        params: QueryParameters,
    ) -> QueryResult | None:
        """Execute one narrow public SQL/Cypher vector-index admin command."""

        if params not in (None, {}):
            return None

        if _SQL_LIST_VECTOR_INDEXES_RE.match(text):
            return self._vector_index_query_result(
                rows=self._public_vector_index_rows(),
                query_type="sql",
            )

        match = _SQL_CREATE_VECTOR_INDEX_PGVECTOR_RE.match(text)
        if match is None:
            match = _SQL_CREATE_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            operator_class = match.groupdict().get("operator_class")
            metric = _create_vector_index_metric(match.groupdict().get("metric"))
            if operator_class is not None:
                metric = _pgvector_operator_class_metric(operator_class)
            existing = _load_named_vector_index(self._sqlite, name=index_name)
            if existing is not None and (
                existing.enabled
                or metric in self._snapshot_vector_index_cache
                or _load_vector_index_snapshot_metadata(
                    self._sqlite,
                    metric=existing.metric,
                )
                is not None
            ):
                if match.group("if_not_exists") is None:
                    raise ValueError(f"Vector index {index_name!r} already exists.")
                return self._vector_index_query_result(
                    rows=(self._vector_index_state_tuple(metric=existing.metric),),
                    query_type="sql",
                )
            self._ensure_public_vector_index_name(metric=metric, name=index_name)
            self._build_public_vector_index(metric=metric, only_if_missing=True)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=metric),),
                query_type="sql",
            )

        match = _SQL_REFRESH_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.refresh_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="sql",
            )

        match = _SQL_REBUILD_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.build_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="sql",
            )

        match = _SQL_ALTER_VECTOR_INDEX_PAUSE_MAINTENANCE_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.pause_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="sql",
            )

        match = _SQL_ALTER_VECTOR_INDEX_RESUME_MAINTENANCE_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.resume_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="sql",
            )

        match = _SQL_DROP_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            state = self._public_vector_index_state(metric=resolved.metric)
            if not state["enabled"] and state["snapshot_rows"] == 0:
                if match.group("if_exists") is None:
                    raise ValueError(f"Vector index {index_name!r} does not exist.")
                return self._vector_index_query_result(
                    rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                    query_type="sql",
                )
            self._drop_public_vector_index(
                metric=resolved.metric,
                index_name=resolved.name,
                disable=True,
            )
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="sql",
            )

        if _CYPHER_SHOW_VECTOR_INDEXES_RE.match(text):
            return self._vector_index_query_result(
                rows=self._public_vector_index_rows(),
                query_type="cypher",
            )

        match = _CYPHER_CREATE_VECTOR_INDEX_NEO4J_RE.match(text)
        if match is None:
            match = _CYPHER_CREATE_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            options = match.groupdict().get("options")
            metric = _create_vector_index_metric(match.groupdict().get("metric"))
            if options is not None:
                metric = _neo4j_vector_similarity_metric(options)
            existing = _load_named_vector_index(self._sqlite, name=index_name)
            if existing is not None and (
                existing.enabled
                or metric in self._snapshot_vector_index_cache
                or _load_vector_index_snapshot_metadata(
                    self._sqlite,
                    metric=existing.metric,
                )
                is not None
            ):
                if match.group("if_not_exists") is None:
                    raise ValueError(f"Vector index {index_name!r} already exists.")
                return self._vector_index_query_result(
                    rows=(self._vector_index_state_tuple(metric=existing.metric),),
                    query_type="cypher",
                )
            self._ensure_public_vector_index_name(metric=metric, name=index_name)
            self._build_public_vector_index(metric=metric, only_if_missing=True)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=metric),),
                query_type="cypher",
            )

        match = _CYPHER_DROP_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            state = self._public_vector_index_state(metric=resolved.metric)
            if not state["enabled"] and state["snapshot_rows"] == 0:
                if match.group("if_exists") is None:
                    raise ValueError(f"Vector index {index_name!r} does not exist.")
                return self._vector_index_query_result(
                    rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                    query_type="cypher",
                )
            self._drop_public_vector_index(
                metric=resolved.metric,
                index_name=resolved.name,
                disable=True,
            )
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="cypher",
            )

        match = _CYPHER_REFRESH_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.refresh_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="cypher",
            )

        match = _CYPHER_REBUILD_VECTOR_INDEX_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.build_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="cypher",
            )

        match = _CYPHER_ALTER_VECTOR_INDEX_PAUSE_MAINTENANCE_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.pause_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="cypher",
            )

        match = _CYPHER_ALTER_VECTOR_INDEX_RESUME_MAINTENANCE_RE.match(text)
        if match is not None:
            index_name = match.group("name")
            resolved = self._resolve_public_vector_index(index_name=index_name)
            if not resolved.exists:
                raise ValueError(_unknown_vector_index_name_message(index_name))
            self.resume_vector_index(metric=resolved.metric, index_name=resolved.name)
            return self._vector_index_query_result(
                rows=(self._vector_index_state_tuple(metric=resolved.metric),),
                query_type="cypher",
            )

        return None

    def _vector_index_state_tuple(self, *, metric: VectorMetric) -> tuple[Any, ...]:
        """Return one public lifecycle state row for the given metric."""

        state = self._public_vector_index_state(metric=metric)
        return (
            state["name"],
            state["metric"],
            state["enabled"],
            state["state"],
            state["total_rows"],
            state["indexed_rows"],
            state["snapshot_rows"],
            state["delta_rows"],
            state["tombstone_rows"],
            state["ann_threshold_rows"],
            state["refresh_in_progress"],
            state["snapshot_table"],
            state["snapshot_generation"],
            state["maintenance_paused"],
        )

    def _vector_tombstones(self, *, metric: VectorMetric) -> set[tuple[str, str, int]]:
        """Return cached logical ids deleted since the metric's snapshot."""

        cached = self._vector_tombstone_cache
        if cached is not None:
            metric_cached = cached.get(metric)
            if metric_cached is not None:
                return metric_cached
        self._ensure_vector_schema()
        metric_cached = set(_load_vector_tombstones(self._sqlite, metric=metric))
        if cached is None:
            cached = {}
            self._vector_tombstone_cache = cached
        cached[metric] = metric_cached
        return metric_cached

    def _clear_vector_tombstone_cache(self) -> None:
        """Drop the cached tombstone set after vector lifecycle writes."""

        self._vector_tombstone_cache = None

    def _inspect_vector_runtime(
        self,
        *,
        metric: VectorMetric = "cosine",
    ) -> dict[str, Any]:
        """Return internal ANN snapshot runtime state for debugging and tests."""

        self._maybe_promote_snapshot_refresh(metric)
        if not self._has_vector_table():
            minimum_training_rows = (
                self._vector_runtime_config.lancedb.minimum_rows_for_training()
            )
            return {
                "metric": metric,
                "total_rows": 0,
                "indexed_rows": 0,
                "snapshot_rows": 0,
                "delta_rows": 0,
                "tombstone_rows": 0,
                "ann_threshold_rows": (
                    self._vector_runtime_config.ann_index_required_rows(
                        minimum_training_rows=minimum_training_rows,
                    )
                ),
                "refresh_in_progress": False,
                "snapshot_table": None,
                "snapshot_generation": None,
            }

        item_ids, _ = self._load_vector_matrix()
        cached = self._snapshot_vector_index_cache.get(metric)
        if cached is None:
            cached = self._load_persisted_snapshot_index(metric=metric)
        cached_item_indexes = self._snapshot_vector_item_index_cache.get(metric, {})
        tombstones = self._vector_tombstones(metric=metric)
        delta_rows = 0
        for item_id in item_ids.tolist():
            key = (str(item_id[0]), str(item_id[1]), int(item_id[2]))
            if key in tombstones or key not in cached_item_indexes:
                delta_rows += 1

        minimum_training_rows = (
            self._vector_runtime_config.lancedb.minimum_rows_for_training()
        )
        ann_threshold_rows = self._vector_runtime_config.ann_index_required_rows(
            minimum_training_rows=minimum_training_rows,
        )
        snapshot_rows = 0
        snapshot_table = None
        snapshot_generation: int | None = None
        tombstone_rows = 0
        if cached is not None:
            snapshot_rows = int(cached.item_ids.size)
            snapshot_table = cached.config.table_name
            metadata = _load_vector_index_snapshot_metadata(self._sqlite, metric=metric)
            if metadata is not None:
                snapshot_generation = metadata.generation
            tombstone_rows = sum(
                1 for item_id in tombstones if item_id in cached_item_indexes
            )
            ann_threshold_rows = (
                self._vector_runtime_config.ann_refresh_required_rows()
            )

        refresh_future = self._snapshot_refresh_futures.get(metric)
        maintenance_paused = self._vector_index_maintenance_paused(metric=metric)
        return {
            "metric": metric,
            "total_rows": int(item_ids.size),
            "indexed_rows": max(int(item_ids.size) - delta_rows, 0),
            "snapshot_rows": snapshot_rows,
            "delta_rows": delta_rows,
            "tombstone_rows": tombstone_rows,
            "ann_threshold_rows": ann_threshold_rows,
            "refresh_in_progress": (not maintenance_paused)
            and refresh_future is not None
            and not refresh_future.done(),
            "snapshot_table": snapshot_table,
            "snapshot_generation": snapshot_generation,
        }

    def _await_vector_runtime_snapshot_refresh(
        self,
        *,
        metric: VectorMetric = "cosine",
    ) -> bool:
        """Wait for one pending snapshot refresh and promote it when ready."""

        refresh_future = self._snapshot_refresh_futures.get(metric)
        if refresh_future is None:
            return False
        refresh_future.result()
        self._maybe_promote_snapshot_refresh(metric)
        return True

    def _known_snapshot_metrics(self) -> tuple[VectorMetric, ...]:
        """Return metrics that currently have a live or persisted snapshot."""

        self._ensure_vector_schema()
        metrics = set(self._snapshot_vector_index_cache)
        metrics.update(
            cast(VectorMetric, metadata.metric)
            for metadata in _list_vector_index_snapshot_metadata(self._sqlite)
        )
        return tuple(sorted(metrics))

    def _load_persisted_snapshot_index(
        self,
        *,
        metric: VectorMetric,
    ) -> _LanceDBVectorIndex | None:
        """Open one persisted ANN snapshot into the in-memory runtime cache."""

        if not self._vector_index_enabled(metric=metric):
            return None

        cached = self._snapshot_vector_index_cache.get(metric)
        if cached is not None:
            return cached

        metadata = _load_vector_index_snapshot_metadata(self._sqlite, metric=metric)
        if metadata is None:
            return None

        config = self._vector_runtime_config.lancedb.with_table_name(
            metadata.table_name
        )
        try:
            cached = _LanceDBVectorIndex.from_existing(
                metric=metric,
                lance_path=self._vector_lancedb_path(),
                config=config,
            )
        except _VECTOR_RUNTIME_BACKGROUND_ERRORS:
            logger.exception(
                "Failed to reopen persisted LanceDB snapshot metric=%s",
                metric,
            )
            _delete_vector_index_snapshot_metadata(self._sqlite, metric=metric)
            return None

        self._snapshot_generation_by_metric[metric] = max(
            self._snapshot_generation_by_metric.get(metric, 0),
            metadata.generation,
        )
        self._snapshot_vector_index_cache[metric] = cached
        self._snapshot_vector_item_index_cache[metric] = {
            (str(item_id[0]), str(item_id[1]), int(item_id[2])): index
            for index, item_id in enumerate(cached.item_ids.tolist())
        }
        return cached

    def _snapshot_refresh_drift(
        self,
        *,
        metric: VectorMetric,
    ) -> tuple[int, Any, Any] | None:
        """Return drift rows plus the current full matrix snapshot for one metric."""

        cached = self._snapshot_vector_index_cache.get(metric)
        if cached is None:
            cached = self._load_persisted_snapshot_index(metric=metric)
        if cached is None:
            return None

        item_ids, matrix = self._load_vector_matrix()
        cached_item_indexes = self._snapshot_vector_item_index_cache.get(metric, {})
        pending_insert_rows = sum(
            1
            for item_id in item_ids.tolist()
            if (str(item_id[0]), str(item_id[1]), int(item_id[2]))
            not in cached_item_indexes
        )
        tombstone_rows = sum(
            1
            for item_id in self._vector_tombstones(metric=metric)
            if item_id in cached_item_indexes
        )
        return (pending_insert_rows + tombstone_rows, item_ids, matrix)

    def _maybe_schedule_snapshot_refresh_after_write(self) -> None:
        """Schedule background refreshes for built snapshots after writes."""

        for metric in self._known_snapshot_metrics():
            if self._vector_index_maintenance_paused(metric=metric):
                continue
            drift = self._snapshot_refresh_drift(metric=metric)
            if drift is None:
                continue
            drift_rows, snapshot_item_ids, snapshot_matrix = drift
            cached = self._snapshot_vector_index_cache.get(metric)
            if cached is None:
                continue
            refresh_rows = self._vector_runtime_config.ann_refresh_required_rows()
            if drift_rows < refresh_rows:
                continue
            self._schedule_snapshot_refresh(
                metric=metric,
                item_ids=snapshot_item_ids.copy(),
                matrix=snapshot_matrix.copy(),
            )

    def _store_live_snapshot(
        self,
        *,
        metric: VectorMetric,
        index: _LanceDBVectorIndex,
    ) -> None:
        """Cache and persist one live ANN snapshot for the given metric."""

        self._snapshot_vector_index_cache[metric] = index
        self._snapshot_vector_item_index_cache[metric] = {
            (str(item_id[0]), str(item_id[1]), int(item_id[2])): item_index
            for item_index, item_id in enumerate(index.item_ids.tolist())
        }
        generation = self._snapshot_generation_by_metric.get(metric, 0)
        _upsert_vector_index_snapshot_metadata(
            self._sqlite,
            metric=metric,
            table_name=index.config.table_name,
            row_count=int(index.item_ids.size),
            generation=generation,
        )

    def _ensure_graph_schema(self) -> None:
        """Initialize the SQLite-backed graph tables on first Cypher use."""

        if self._graph_schema_ready:
            return

        logger.debug("Initializing graph schema on first Cypher use")
        _ensure_graph_schema(self._sqlite)
        self._graph_schema_ready = True

    def _ensure_vector_schema(self) -> None:
        """Initialize the SQLite-backed vector tables on first vector use."""

        if self._vector_schema_ready:
            return

        logger.debug("Initializing vector schema on first vector use")
        _ensure_vector_schema(self._sqlite)
        self._vector_schema_ready = True

    def _execute_vector_query(
        self,
        plan: _QueryExecutionPlan,
    ) -> QueryResult:
        """Execute one inferred language-level vector query."""

        if plan.route != "sqlite":
            raise ValueError(
                "HumemVector v0 currently runs only on route='sqlite'; "
                "SQLite is the canonical vector store and exact NumPy search path."
            )

        vector_plan = plan.vector_plan
        if vector_plan is None:
            raise ValueError(
                "HumemDB internal vector plans require a precomputed vector plan."
            )
        public_query_type: QueryType
        if isinstance(vector_plan, _SQLVectorQueryPlan):
            logger.debug("Routing SQL-backed vector query to exact SQLite/NumPy path")
            public_query_type = "sql"
            metric = vector_plan.metric
        else:
            logger.debug(
                "Routing Cypher-backed vector query to exact SQLite/NumPy path"
            )
            public_query_type = "cypher"
            metric = self._metric_for_cypher_vector_query_index(
                vector_plan.index_name
            )
        resolved_candidates = self._resolve_candidate_vector_query(
            vector_plan,
        )
        result = self._execute_vector_search(
            vector_plan.query,
            top_k=vector_plan.top_k,
            metric=metric,
            resolved_candidates=resolved_candidates,
            public_query_type=public_query_type,
        )
        if (
            isinstance(vector_plan, _CypherVectorQueryPlan)
            and vector_plan.result_mode == "queryNodes"
        ):
            ordered_rows = list(result.rows)
            for field, direction in reversed(vector_plan.order_items):
                ordered_rows.sort(
                    key=(
                        (lambda row: row[2])
                        if field == "node.id"
                        else (lambda row: row[3])
                    ),
                    reverse=direction == "desc",
                )
            rows = tuple(
                tuple(
                    row[2] if item == "node.id" else row[3]
                    for item in vector_plan.return_items
                )
                for row in ordered_rows
            )
            return QueryResult(
                rows=rows,
                columns=vector_plan.return_items,
                route=result.route,
                query_type="cypher",
                rowcount=len(rows),
            )
        return result

    def _execute_query_plan(self, plan: _QueryExecutionPlan) -> QueryResult:
        """Dispatch one normalized query plan onto the correct execution path."""

        logger.debug(
            (
                "Dispatching query_type=%s workload=%s preferred_route=%s "
                "actual_route=%s route_source=%s"
            ),
            plan.query_type,
            plan.workload.kind,
            plan.workload.preferred_route,
            plan.route,
            plan.route_decision.source,
        )

        if plan.vector_plan is not None:
            logger.debug(
                "Routing explicit %s vector plan to exact SQLite/NumPy path",
                type(plan.vector_plan).__name__,
            )
            return self._execute_vector_query(plan)

        if plan.query_type == "cypher":
            return self._execute_cypher_query_plan(plan)

        if plan.query_type == "sql":
            return self._execute_sql_query_plan(plan)

        logger.debug("Rejected unsupported query_type=%s", plan.query_type)
        raise NotImplementedError(
            "HumemDB currently supports query_type='sql' for HumemSQL v0, "
            "query_type='cypher' for HumemCypher v0; "
            f"got {plan.query_type!r}."
        )

    def _execute_cypher_query_plan(self, plan: _QueryExecutionPlan) -> QueryResult:
        """Execute one normalized Cypher query plan."""

        self._ensure_graph_schema()
        if plan.route == "duckdb" and not plan.workload.is_read_only:
            logger.debug(
                "Rejected graph write on DuckDB workload=%s",
                plan.workload.kind,
            )
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        logger.debug(
            "Routing Cypher query to graph path on route=%s workload=%s",
            plan.route,
            plan.workload.kind,
        )
        result = _execute_cypher(
            plan.text,
            route=plan.route,
            params=plan.params,
            sqlite=self._sqlite,
            duckdb=self._duckdb,
            plan=plan.cypher_plan,
        )
        if not plan.workload.is_read_only:
            bound_plan = (
                None
                if plan.cypher_plan is None
                else _bind_plan_values(
                    plan.cypher_plan,
                    _normalize_params(plan.params),
                )
            )
            invalidation = self._cypher_vector_invalidation_mode(bound_plan)
            if invalidation == "exact":
                self._invalidate_exact_vector_cache()
                self._clear_vector_tombstone_cache()
                self._maybe_schedule_snapshot_refresh_after_write()
            elif invalidation == "full":
                self._invalidate_vector_cache()
        return result

    def _execute_sql_query_plan(self, plan: _QueryExecutionPlan) -> QueryResult:
        """Execute one normalized SQL query plan."""

        translated_text = plan.translated_text
        if translated_text is None:
            raise ValueError("HumemDB internal SQL plans require translated SQL text.")

        if plan.route == "sqlite":
            logger.debug(
                "Routing SQL query to SQLite workload=%s",
                plan.workload.kind,
            )
            write_plan = _vectorrt_plan_sql_vector_write(plan.text, plan.params)
            result = self._sqlite.execute(
                translated_text,
                write_plan.normalized_params,
                query_type=plan.query_type,
            )
            if write_plan.deleted_item_ids:
                self._ensure_vector_schema()
                deleted = _delete_target_namespaced_vectors(
                    self._sqlite,
                    write_plan.deleted_item_ids,
                )
                if deleted:
                    self._invalidate_exact_vector_cache()
                    self._clear_vector_tombstone_cache()
                    self._maybe_schedule_snapshot_refresh_after_write()
            if write_plan.vector_rows:
                self._ensure_vector_schema()
                if write_plan.vector_mode == "insert":
                    resolved_vector_rows = _resolved_sql_vector_rows_after_insert(
                        self._sqlite,
                        write_plan.vector_rows,
                    )
                else:
                    resolved_vector_rows = cast(
                        list[_TargetNamespacedVectorRow],
                        write_plan.vector_rows,
                    )
                _write_target_namespaced_vector_rows(
                    self._sqlite,
                    resolved_vector_rows,
                    mode=write_plan.vector_mode or "insert",
                )
                if write_plan.vector_mode == "insert":
                    self._invalidate_exact_vector_cache()
                    self._clear_vector_tombstone_cache()
                    self._maybe_schedule_snapshot_refresh_after_write()
                else:
                    self._invalidate_vector_cache()
            self._invalidate_vector_cache_for_sql(plan.text, translated_text)
            return result

        if plan.route == "duckdb":
            if not plan.workload.is_read_only:
                logger.debug(
                    "Rejected direct write routed to DuckDB workload=%s",
                    plan.workload.kind,
                )
                raise ValueError(
                    "HumemDB does not allow direct writes to DuckDB; "
                    "SQLite is the source of truth."
                )
            logger.debug(
                "Routing read-only SQL query to DuckDB workload=%s",
                plan.workload.kind,
            )
            return self._duckdb.execute(
                translated_text,
                plan.params,
                query_type=plan.query_type,
            )

        raise ValueError(f"Unsupported route: {plan.route!r}")

    def _execute_batch_query_plan(self, plan: _BatchExecutionPlan) -> QueryResult:
        """Execute one normalized batched SQL query plan."""

        if plan.route == "sqlite":
            logger.debug("Routing batched SQL query to SQLite")
            batch_plan = _vectorrt_plan_sql_vector_write_batch(
                plan.text,
                plan.params_seq,
            )
            if batch_plan.requires_rowwise_execution:
                total_rowcount = 0
                resolved_vector_rows: list[_TargetNamespacedVectorRow] = []
                for normalized_params, pending_row in zip(
                    batch_plan.normalized_params_seq,
                    batch_plan.vector_rows,
                    strict=True,
                ):
                    result = self._sqlite.execute(
                        plan.translated_text,
                        normalized_params,
                        query_type="sql",
                    )
                    total_rowcount += result.rowcount
                    resolved_vector_rows.extend(
                        _resolved_sql_vector_rows_after_insert(
                            self._sqlite,
                            [pending_row],
                        )
                    )
                self._ensure_vector_schema()
                _write_target_namespaced_vector_rows(
                    self._sqlite,
                    resolved_vector_rows,
                    mode="insert",
                )
                self._invalidate_exact_vector_cache()
                self._clear_vector_tombstone_cache()
                self._maybe_schedule_snapshot_refresh_after_write()
                self._invalidate_vector_cache_for_sql(plan.text, plan.translated_text)
                return QueryResult(
                    rows=(),
                    columns=(),
                    route="sqlite",
                    query_type="sql",
                    rowcount=total_rowcount,
                )
            result = self._sqlite.executemany(
                plan.translated_text,
                batch_plan.normalized_params_seq,
                query_type="sql",
            )
            if batch_plan.vector_rows:
                self._ensure_vector_schema()
                _write_target_namespaced_vector_rows(
                    self._sqlite,
                    cast(list[_TargetNamespacedVectorRow], batch_plan.vector_rows),
                    mode="insert",
                )
                self._invalidate_exact_vector_cache()
                self._clear_vector_tombstone_cache()
                self._maybe_schedule_snapshot_refresh_after_write()
            self._invalidate_vector_cache_for_sql(plan.text, plan.translated_text)
            return result

        if plan.route == "duckdb":
            logger.debug("Rejected batched write routed to DuckDB")
            raise ValueError(
                "HumemDB does not allow direct batch writes to DuckDB; "
                "SQLite is the source of truth."
            )

        raise ValueError(f"Unsupported route: {plan.route!r}")

    def _execute_vector_search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        metric: VectorMetric,
        resolved_candidates: _ResolvedVectorCandidates,
        public_query_type: QueryType | None = None,
    ) -> QueryResult:
        """Execute one vector search using the current exact or indexed runtime."""

        if len(resolved_candidates.candidate_indexes) == 0:
            return QueryResult(
                rows=(),
                columns=_VECTOR_RESULT_COLUMNS,
                route="sqlite",
                query_type=public_query_type,
                rowcount=0,
            )

        snapshot_index = self._snapshot_vector_index_for(metric=metric)
        if snapshot_index is None:
            return self._execute_exact_vector_search(
                query,
                top_k=top_k,
                metric=metric,
                resolved_candidates=resolved_candidates,
                public_query_type=public_query_type,
            )

        logger.debug(
            (
                "Routing vector search to ANN snapshot runtime target=%s "
                "namespace=%s candidate_count=%s"
            ),
            resolved_candidates.target,
            resolved_candidates.namespace,
            resolved_candidates.candidate_count,
        )
        return self._execute_indexed_vector_search(
            query,
            top_k=top_k,
            metric=metric,
            resolved_candidates=resolved_candidates,
            snapshot_index=snapshot_index,
            public_query_type=public_query_type,
        )

    def _execute_indexed_vector_search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        metric: VectorMetric,
        resolved_candidates: _ResolvedVectorCandidates,
        snapshot_index: _LanceDBVectorIndex,
        public_query_type: QueryType | None = None,
    ) -> QueryResult:
        """Execute one ANN snapshot search plus exact delta rerank."""

        item_ids, _ = self._load_vector_matrix()
        item_id_rows = item_ids.tolist()
        snapshot_item_indexes = self._snapshot_vector_item_index_cache.get(metric, {})
        tombstones = self._vector_tombstones(metric=metric)
        delta_candidate_indexes: list[int] = []
        snapshot_candidate_indexes: list[int] = []
        snapshot_candidate_globals: dict[tuple[str, str, int], int] = {}
        for candidate_index in resolved_candidates.candidate_indexes:
            item_id = item_id_rows[int(candidate_index)]
            key = (str(item_id[0]), str(item_id[1]), int(item_id[2]))
            snapshot_candidate_index = snapshot_item_indexes.get(key)
            if key in tombstones or snapshot_candidate_index is None:
                delta_candidate_indexes.append(int(candidate_index))
            else:
                snapshot_candidate_indexes.append(snapshot_candidate_index)
                snapshot_candidate_globals[key] = int(candidate_index)

        buffered_top_k = self._vector_runtime_config.buffered_top_k(top_k)
        rerank_candidate_indexes = set(delta_candidate_indexes)
        if snapshot_candidate_indexes:
            snapshot_matches = snapshot_index.search(
                query,
                top_k=min(buffered_top_k, len(snapshot_candidate_indexes)),
                candidate_indexes=tuple(sorted(set(snapshot_candidate_indexes))),
            )
            for match in snapshot_matches:
                rerank_candidate_index = snapshot_candidate_globals.get(
                    (match.target, match.namespace, match.target_id)
                )
                if rerank_candidate_index is not None:
                    rerank_candidate_indexes.add(rerank_candidate_index)

        if not rerank_candidate_indexes:
            return QueryResult(
                rows=(),
                columns=_VECTOR_RESULT_COLUMNS,
                route="sqlite",
                query_type=public_query_type,
                rowcount=0,
            )

        exact_matches = self._vector_index_for(metric=metric).search(
            query,
            top_k=top_k,
            candidate_indexes=tuple(sorted(rerank_candidate_indexes)),
        )
        rows = tuple(
            (match.target, match.namespace, match.target_id, match.score)
            for match in exact_matches
        )
        return QueryResult(
            rows=rows,
            columns=_VECTOR_RESULT_COLUMNS,
            route="sqlite",
            query_type=public_query_type,
            rowcount=len(rows),
        )

    def _execute_exact_vector_search(
        self,
        query: Sequence[float],
        *,
        top_k: int,
        metric: VectorMetric,
        resolved_candidates: _ResolvedVectorCandidates,
        public_query_type: QueryType | None = None,
    ) -> QueryResult:
        """Execute one exact vector search against resolved candidate indexes."""

        if len(resolved_candidates.candidate_indexes) == 0:
            return QueryResult(
                rows=(),
                columns=_VECTOR_RESULT_COLUMNS,
                route="sqlite",
                query_type=public_query_type,
                rowcount=0,
            )

        if resolved_candidates.uses_full_namespace:
            logger.debug(
                "Using full namespace candidate set target=%s namespace=%s size=%s",
                resolved_candidates.target,
                resolved_candidates.namespace,
                resolved_candidates.namespace_size,
            )

        index = self._vector_index_for(metric=metric)
        matches = index.search(
            query,
            top_k=top_k,
            candidate_indexes=resolved_candidates.candidate_indexes,
        )
        rows = tuple(
            (match.target, match.namespace, match.target_id, match.score)
            for match in matches
        )
        return QueryResult(
            rows=rows,
            columns=_VECTOR_RESULT_COLUMNS,
            route="sqlite",
            query_type=public_query_type,
            rowcount=len(rows),
        )

    def _vector_index_for(
        self,
        *,
        metric: VectorMetric,
    ) -> _ExactVectorIndex:
        """Load and cache one exact vector index per metric."""

        cached = self._vector_index_cache.get(metric)
        if cached is not None:
            return cached

        item_ids, matrix = self._load_vector_matrix()
        cached = _ExactVectorIndex(item_ids=item_ids, matrix=matrix, metric=metric)
        self._vector_index_cache[metric] = cached
        return cached

    def _snapshot_vector_index_for(
        self,
        *,
        metric: VectorMetric,
    ) -> _LanceDBVectorIndex | None:
        """Load and cache one ANN snapshot LanceDB index per metric."""

        if not self._vector_index_enabled(metric=metric):
            return None

        self._maybe_promote_snapshot_refresh(metric)

        cached = self._snapshot_vector_index_cache.get(metric)
        if cached is not None:
            return cached
        cached = self._load_persisted_snapshot_index(metric=metric)
        if cached is not None:
            return cached
        if self._vector_index_maintenance_paused(metric=metric):
            return None

        item_ids, matrix = self._load_vector_matrix()
        if item_ids.size == 0:
            return None

        min_training_rows = (
            self._vector_runtime_config.lancedb.minimum_rows_for_training()
        )
        required_rows = self._vector_runtime_config.ann_index_required_rows(
            minimum_training_rows=min_training_rows,
        )
        if int(item_ids.size) < required_rows:
            logger.debug(
                (
                    "Skipping ANN snapshot LanceDB build metric=%s row_count=%s "
                    "required_rows=%s"
                ),
                metric,
                int(item_ids.size),
                required_rows,
            )
            return None

        config = self._vector_runtime_config.lancedb.with_table_name(
            self._snapshot_vector_table_name(metric=metric)
        )
        cached = _LanceDBVectorIndex.from_matrix(
            item_ids=item_ids,
            matrix=matrix,
            metric=metric,
            lance_path=self._vector_lancedb_path(),
            config=config,
        )
        self._store_live_snapshot(metric=metric, index=cached)
        _clear_vector_tombstones(self._sqlite, metric=metric)
        self._clear_vector_tombstone_cache()
        return cached

    def _vector_lancedb_path(self) -> Path:
        """Return the on-disk LanceDB path for the current SQLite database."""

        return Path(self._sqlite_path).with_suffix(".vectors.lancedb")

    def _load_vector_matrix(self) -> tuple[Any, Any]:
        """Load and cache the current vector set from SQLite."""

        if self._vector_matrix_cache is not None:
            return self._vector_matrix_cache

        self._ensure_vector_schema()
        cached = _load_vector_matrix(self._sqlite)
        self._vector_matrix_cache = cached
        self._prime_vector_lookup_caches(cached[0])
        return cached

    def _prime_vector_lookup_caches(self, item_ids: Any) -> None:
        """Build cached logical-id lookup tables for the loaded vector set."""

        item_index_cache: dict[tuple[str, str, int], int] = {}
        namespace_index_cache: dict[tuple[str, str], list[int]] = {}
        for index, item_id in enumerate(item_ids.tolist()):
            key = (str(item_id[0]), str(item_id[1]), int(item_id[2]))
            item_index_cache[key] = index
            namespace_index_cache.setdefault((key[0], key[1]), []).append(index)

        self._vector_item_index_cache = item_index_cache
        self._vector_namespace_index_cache = {
            namespace: tuple(indexes)
            for namespace, indexes in namespace_index_cache.items()
        }

    def _resolve_candidate_vector_query(
        self,
        plan: _CandidateVectorQueryPlan,
    ) -> _ResolvedVectorCandidates:
        """Resolve one candidate-query vector search into candidate metadata."""

        candidate_result = self._execute_candidate_vector_query(plan)

        return self._resolved_vector_candidates(
            target=candidate_result.target,
            namespace=candidate_result.namespace,
            candidate_keys=candidate_result.candidate_keys,
        )

    def _execute_candidate_vector_query(
        self,
        plan: _CandidateVectorQueryPlan,
    ) -> _CandidateVectorQueryResult:
        """Execute one candidate query and normalize it for vector resolution."""

        candidate_query = plan.candidate_query

        if isinstance(plan, _SQLVectorQueryPlan):
            if not translate_sql_plan(
                candidate_query.text,
                target=candidate_query.route,
            ).is_read_only:
                raise ValueError(
                    "HumemVector v0 SQL vector candidate query must be a read-only "
                    "SQL query."
                )
            candidate_query_type: Literal["sql", "cypher"] = "sql"
        else:
            normalized_candidate_query = candidate_query.text.lstrip()
            if not _CYPHER_MATCH_PREFIX.match(normalized_candidate_query):
                raise ValueError(
                    "HumemVector v0 Cypher vector candidate query must be a "
                    "MATCH query."
                )
            candidate_query_type = "cypher"

        candidate_result = self._execute_query_plan(
            _plan_query(
                candidate_query.text,
                route=candidate_query.route,
                query_type=candidate_query_type,
                params=candidate_query.params,
            )
        )
        return candidate_vector_result_from_query_result(
            candidate_result,
            target=candidate_query.target,
            namespace=candidate_query.namespace,
        )

    def _resolve_direct_vector_search(
        self,
        plan: _DirectVectorSearchPlan,
    ) -> _ResolvedVectorCandidates:
        """Resolve one direct-vector search into explicit candidate metadata."""

        if plan.filters is None:
            namespace_indexes = self._candidate_indexes_for_target(
                target="direct",
                namespace="",
            )
            return _ResolvedVectorCandidates(
                target="direct",
                namespace="",
                candidate_keys=(),
                candidate_indexes=namespace_indexes,
                candidate_count=len(namespace_indexes),
                namespace_size=len(namespace_indexes),
                uses_full_namespace=True,
            )

        self._ensure_vector_schema()
        candidate_keys = tuple(
            _load_filtered_vector_target_keys(
                self._sqlite,
                plan.filters,
                target="direct",
                namespace="",
            )
        )
        return self._resolved_vector_candidates(
            target="direct",
            namespace="",
            candidate_keys=candidate_keys,
        )

    def _resolved_vector_candidates(
        self,
        *,
        target: str,
        namespace: str,
        candidate_keys: tuple[tuple[str, str, int], ...],
    ) -> _ResolvedVectorCandidates:
        """Build one resolved vector-candidate object from logical keys."""

        if not candidate_keys:
            namespace_size = len(
                self._candidate_indexes_for_target(target=target, namespace=namespace)
            )
            return _ResolvedVectorCandidates(
                target=target,
                namespace=namespace,
                candidate_keys=(),
                candidate_indexes=(),
                candidate_count=0,
                namespace_size=namespace_size,
                uses_full_namespace=False,
            )

        namespace_indexes = self._candidate_indexes_for_target(
            target=target,
            namespace=namespace,
        )
        candidate_indexes = self._candidate_indexes_for_target_keys(candidate_keys)
        candidate_count = len(candidate_indexes)
        namespace_size = len(namespace_indexes)
        uses_full_namespace = candidate_count == namespace_size

        return _ResolvedVectorCandidates(
            target=target,
            namespace=namespace,
            candidate_keys=candidate_keys,
            candidate_indexes=(
                namespace_indexes if uses_full_namespace else candidate_indexes
            ),
            candidate_count=candidate_count,
            namespace_size=namespace_size,
            uses_full_namespace=uses_full_namespace,
        )

    def _candidate_indexes_for_target(
        self,
        *,
        target: str,
        namespace: str,
    ) -> Any:
        """Return cached matrix indexes for one logical vector namespace."""

        self._load_vector_matrix()
        if self._vector_namespace_index_cache is None:
            return ()
        return self._vector_namespace_index_cache.get((target, namespace), ())

    def _candidate_indexes_for_target_keys(
        self,
        item_ids: Sequence[tuple[str, str, int]],
    ) -> Any:
        """Map candidate logical vector identifiers onto cached matrix indexes."""

        self._load_vector_matrix()
        if self._vector_item_index_cache is None:
            return ()
        resolved_indexes = [
            self._vector_item_index_cache[item_id]
            for item_id in set(item_ids)
            if item_id in self._vector_item_index_cache
        ]
        resolved_indexes.sort()
        return tuple(resolved_indexes)

    def _has_vector_table(self) -> bool:
        """Return whether the current SQLite database already has vector storage."""

        result = self._sqlite.execute(
            (
                "SELECT name "
                "FROM sqlite_master "
                "WHERE type = 'table' AND name = 'vector_entries'"
            )
        )
        return bool(result.rows)

    def _invalidate_exact_vector_cache(self) -> None:
        """Drop exact-search caches while preserving any reusable snapshot."""

        self._vector_matrix_cache = None
        self._vector_item_index_cache = None
        self._vector_namespace_index_cache = None
        self._vector_index_cache.clear()

    def _invalidate_snapshot_vector_cache(self) -> None:
        """Drop cached snapshot indexed state."""

        for cached in self._snapshot_vector_index_cache.values():
            try:
                _drop_lancedb_table(
                    lance_path=self._vector_lancedb_path(),
                    table_name=cached.config.table_name,
                )
            except _VECTOR_RUNTIME_BACKGROUND_ERRORS:
                logger.debug(
                    "Ignoring failed LanceDB cleanup table=%s",
                    cached.config.table_name,
                    exc_info=True,
                )
        self._snapshot_vector_index_cache.clear()
        self._snapshot_vector_item_index_cache.clear()
        _delete_vector_index_snapshot_metadata(self._sqlite)
        _clear_vector_tombstones(self._sqlite)
        self._clear_vector_tombstone_cache()
        for metric in tuple(self._snapshot_refresh_futures):
            self._snapshot_refresh_epoch[metric] = (
                self._snapshot_refresh_epoch.get(metric, 0) + 1
            )

    def _invalidate_vector_cache(self) -> None:
        """Drop cached exact vector data after vector storage changes."""

        self._invalidate_exact_vector_cache()
        self._invalidate_snapshot_vector_cache()

    def _invalidate_vector_cache_for_sql(
        self,
        original_text: str,
        translated_text: str,
    ) -> None:
        """Drop cached exact indexes after raw SQL writes that touch vector tables."""

        if translate_sql_plan(original_text, target="sqlite").is_read_only:
            return
        if not _touches_vector_entries(translated_text):
            return

        logger.debug("Invalidating all vector cache entries after SQL write")
        self._invalidate_vector_cache()

    def begin(self) -> None:
        """Begin an explicit transaction on the canonical SQLite store."""

        logger.debug("Beginning transaction on route=sqlite")
        self._sqlite.begin()

    def commit(self) -> None:
        """Commit the active transaction on the canonical SQLite store."""

        logger.debug("Committing transaction on route=sqlite")
        self._sqlite.commit()

    def rollback(self) -> None:
        """Roll back the active transaction on the canonical SQLite store."""

        logger.debug("Rolling back transaction on route=sqlite")
        self._sqlite.rollback()

    def transaction(self) -> _TransactionContext:
        """Return a transaction context manager for the canonical SQLite store.

        Use this when multiple writes must succeed or fail as one unit, such as several
        dependent `query(...)` calls or one `executemany(...)` batch that should commit
        atomically. Single standalone writes already auto-commit when no explicit
        transaction is active, so wrapping one ordinary insert, update, or delete is
        usually unnecessary. Read-only queries do not need `transaction()`.

        A successful context commits on exit. An exception inside the context triggers a
        rollback before the exception continues to propagate. Explicit transactions are
        not nestable.

        Returns:
            A `_TransactionContext` bound to SQLite.
        """

        return _TransactionContext(self)

    def executemany(
        self,
        text: str,
        params_seq: BatchParameters,
    ) -> QueryResult:
        """Execute the same statement repeatedly for a batch of parameters.

        This method is intentionally limited to SQLite so HumemDB can support simple
        transactional batch writes without introducing a full ingestion framework.

        Args:
            text: SQL statement to execute repeatedly.
            params_seq: Sequence of mapping-style parameter sets.

        Returns:
            A normalized `QueryResult`.

        Raises:
            ValueError: If batch execution is directed to an unsupported internal
                route.
        """

        plan = _plan_batch_query(
            text,
            route="sqlite",
            params_seq=params_seq,
        )
        return self._execute_batch_query_plan(plan)

    def import_table(
        self,
        table: str,
        path: str | Path,
        *,
        columns: Sequence[str] | None = None,
        header: bool = True,
        delimiter: str = ",",
        chunk_size: int = 1000,
        encoding: str = "utf-8",
    ) -> int:
        """Import one CSV file into a relational table on the canonical SQLite store.

        Args:
            table: Destination relational table name.
            path: Path to the CSV file to import.
            columns: Optional destination column order. When omitted and `header=True`,
                the CSV header defines the imported columns.
            header: Whether the CSV file includes a header row.
            delimiter: Field delimiter used by the CSV file.
            chunk_size: Number of rows to batch per SQLite `executemany(...)` call.
            encoding: Text encoding used when reading the CSV file.

        Returns:
            Number of imported rows.

        Raises:
            ValueError: If configuration is invalid or the CSV rows do not match the
                expected column shape.
        """

        if not table:
            raise ValueError("import_table(...) requires a non-empty table name.")
        if chunk_size <= 0:
            raise ValueError("import_table(...) requires chunk_size >= 1.")
        if not header and columns is None:
            raise ValueError(
                "import_table(...) requires explicit columns when header=False."
            )

        normalized_columns = _normalize_import_columns(columns)
        import_path = Path(path)
        rows_imported = 0
        owns_transaction = not self._sqlite.in_transaction

        def import_rows() -> None:
            nonlocal rows_imported

            with import_path.open("r", encoding=encoding, newline="") as csv_file:
                resolved_columns, row_iter = _prepare_csv_import_reader(
                    csv_file,
                    columns=normalized_columns,
                    header=header,
                    delimiter=delimiter,
                )
                insert_sql = _build_import_insert_sql(
                    table=table,
                    columns=resolved_columns,
                )
                chunk: list[tuple[object, ...]] = []
                for row in row_iter:
                    chunk.append(row)
                    if len(chunk) >= chunk_size:
                        self._sqlite.executemany(insert_sql, chunk, query_type="sql")
                        rows_imported += len(chunk)
                        chunk = []
                if chunk:
                    self._sqlite.executemany(insert_sql, chunk, query_type="sql")
                    rows_imported += len(chunk)

        if owns_transaction:
            with self.transaction():
                import_rows()
        else:
            import_rows()

        return rows_imported

    def import_nodes(
        self,
        label: str,
        path: str | Path,
        *,
        id_column: str,
        property_columns: Sequence[str] | None = None,
        property_types: Mapping[str, _GraphImportPropertyType] | None = None,
        header: bool = True,
        delimiter: str = ",",
        chunk_size: int = 1000,
        encoding: str = "utf-8",
    ) -> int:
        """Import one CSV file into the SQLite-backed graph node store.

        Args:
            label: Label to assign to every imported node.
            path: Path to the CSV file to import.
            id_column: CSV column that provides the graph node id.
            property_columns: Optional node property columns. When omitted and
                `header=True`, all non-id columns are imported as properties.
            property_types: Optional per-property type mapping. Unspecified columns
                import as strings.
            header: Whether the CSV file includes a header row.
            delimiter: Field delimiter used by the CSV file.
            chunk_size: Number of rows to batch per SQLite write chunk.
            encoding: Text encoding used when reading the CSV file.

        Returns:
            Number of imported nodes.
        """

        if not label:
            raise ValueError("import_nodes(...) requires a non-empty label.")
        if not id_column:
            raise ValueError("import_nodes(...) requires a non-empty id_column.")
        if chunk_size <= 0:
            raise ValueError("import_nodes(...) requires chunk_size >= 1.")
        if not header and property_columns is None:
            raise ValueError(
                "import_nodes(...) requires explicit property_columns when "
                "header=False."
            )

        self._ensure_graph_schema()
        import_path = Path(path)
        normalized_property_columns = _normalize_graph_import_columns(
            property_columns
        )
        normalized_property_types = _normalize_graph_import_property_types(
            property_types
        )
        rows_imported = 0
        owns_transaction = not self._sqlite.in_transaction

        def import_rows() -> None:
            nonlocal rows_imported

            with import_path.open("r", encoding=encoding, newline="") as csv_file:
                resolved_columns, row_iter = _prepare_named_csv_import_reader(
                    csv_file,
                    columns=None if header else _node_import_columns(
                        id_column,
                        normalized_property_columns,
                    ),
                    header=header,
                    delimiter=delimiter,
                )
                resolved_property_columns = _resolve_graph_property_columns(
                    available_columns=resolved_columns,
                    required_columns=(id_column,),
                    property_columns=normalized_property_columns,
                    method_name="import_nodes",
                )

                node_rows: list[tuple[int, str]] = []
                property_rows: list[tuple[int, str, object, str]] = []
                for row in row_iter:
                    node_id = int(row[id_column])
                    node_rows.append((node_id, label))
                    property_rows.extend(
                        _build_graph_property_rows(
                            owner_id=node_id,
                            columns=resolved_property_columns,
                            row=row,
                            property_types=normalized_property_types,
                        )
                    )
                    if len(node_rows) >= chunk_size:
                        _write_imported_graph_nodes(
                            self._sqlite,
                            node_rows=node_rows,
                            property_rows=property_rows,
                        )
                        rows_imported += len(node_rows)
                        node_rows = []
                        property_rows = []

                if node_rows:
                    _write_imported_graph_nodes(
                        self._sqlite,
                        node_rows=node_rows,
                        property_rows=property_rows,
                    )
                    rows_imported += len(node_rows)

        if owns_transaction:
            with self.transaction():
                import_rows()
        else:
            import_rows()

        return rows_imported

    def import_edges(
        self,
        rel_type: str,
        path: str | Path,
        *,
        source_id_column: str,
        target_id_column: str,
        property_columns: Sequence[str] | None = None,
        property_types: Mapping[str, _GraphImportPropertyType] | None = None,
        header: bool = True,
        delimiter: str = ",",
        chunk_size: int = 1000,
        encoding: str = "utf-8",
    ) -> int:
        """Import one CSV file into the SQLite-backed graph edge store.

        Args:
            rel_type: Relationship type to assign to every imported edge.
            path: Path to the CSV file to import.
            source_id_column: CSV column that provides the source node id.
            target_id_column: CSV column that provides the target node id.
            property_columns: Optional edge property columns. When omitted and
                `header=True`, all non-endpoint columns are imported as properties.
            property_types: Optional per-property type mapping. Unspecified columns
                import as strings.
            header: Whether the CSV file includes a header row.
            delimiter: Field delimiter used by the CSV file.
            chunk_size: Number of rows to batch per SQLite write chunk.
            encoding: Text encoding used when reading the CSV file.

        Returns:
            Number of imported edges.
        """

        if not rel_type:
            raise ValueError("import_edges(...) requires a non-empty rel_type.")
        if not source_id_column or not target_id_column:
            raise ValueError(
                "import_edges(...) requires non-empty source and target id columns."
            )
        if chunk_size <= 0:
            raise ValueError("import_edges(...) requires chunk_size >= 1.")
        if not header and property_columns is None:
            raise ValueError(
                "import_edges(...) requires explicit property_columns when "
                "header=False."
            )

        self._ensure_graph_schema()
        import_path = Path(path)
        normalized_property_columns = _normalize_graph_import_columns(
            property_columns
        )
        normalized_property_types = _normalize_graph_import_property_types(
            property_types
        )
        rows_imported = 0
        owns_transaction = not self._sqlite.in_transaction

        def import_rows() -> None:
            nonlocal rows_imported

            next_edge_id = _next_graph_edge_id(self._sqlite)
            with import_path.open("r", encoding=encoding, newline="") as csv_file:
                resolved_columns, row_iter = _prepare_named_csv_import_reader(
                    csv_file,
                    columns=None if header else _edge_import_columns(
                        source_id_column,
                        target_id_column,
                        normalized_property_columns,
                    ),
                    header=header,
                    delimiter=delimiter,
                )
                resolved_property_columns = _resolve_graph_property_columns(
                    available_columns=resolved_columns,
                    required_columns=(source_id_column, target_id_column),
                    property_columns=normalized_property_columns,
                    method_name="import_edges",
                )

                edge_rows: list[tuple[int, str, int, int]] = []
                property_rows: list[tuple[int, str, object, str]] = []
                for row in row_iter:
                    edge_id = next_edge_id
                    next_edge_id += 1
                    edge_rows.append(
                        (
                            edge_id,
                            rel_type,
                            int(row[source_id_column]),
                            int(row[target_id_column]),
                        )
                    )
                    property_rows.extend(
                        _build_graph_property_rows(
                            owner_id=edge_id,
                            columns=resolved_property_columns,
                            row=row,
                            property_types=normalized_property_types,
                        )
                    )
                    if len(edge_rows) >= chunk_size:
                        _write_imported_graph_edges(
                            sqlite=self._sqlite,
                            edge_rows=edge_rows,
                            property_rows=property_rows,
                        )
                        rows_imported += len(edge_rows)
                        edge_rows = []
                        property_rows = []

                if edge_rows:
                    _write_imported_graph_edges(
                        sqlite=self._sqlite,
                        edge_rows=edge_rows,
                        property_rows=property_rows,
                    )
                    rows_imported += len(edge_rows)

        if owns_transaction:
            with self.transaction():
                import_rows()
        else:
            import_rows()

        return rows_imported

    def close(self) -> None:
        """Close both embedded database connections.

        Returns:
            `None`.
        """

        logger.debug("Closing HumemDB connections")
        if self._snapshot_refresh_executor is not None:
            self._snapshot_refresh_executor.shutdown(wait=True)
        self._sqlite.close()
        self._duckdb.close()

    def _cypher_vector_invalidation_mode(
        self,
        plan: _GraphPlan | None,
    ) -> Literal["none", "exact", "full"]:
        """Classify how one Cypher write affects the vector runtime caches."""

        if plan is None:
            return "full"
        if isinstance(plan, CreateNodePlan):
            return "exact" if _node_pattern_has_vector_property(plan.node) else "none"
        if isinstance(plan, CreateRelationshipPlan):
            return (
                "exact"
                if _node_pattern_has_vector_property(plan.left)
                or _node_pattern_has_vector_property(plan.right)
                else "none"
            )
        if isinstance(plan, CreateRelationshipFromSeparatePatternsPlan):
            return (
                "exact"
                if _node_pattern_has_vector_property(plan.first_node)
                or _node_pattern_has_vector_property(plan.second_node)
                else "none"
            )
        if isinstance(
            plan,
            (MatchCreateRelationshipPlan, MatchCreateRelationshipBetweenNodesPlan),
        ):
            return "none"
        if isinstance(plan, SetNodePlan):
            return "full" if _set_plan_has_vector_assignment(plan) else "none"
        if isinstance(plan, SetRelationshipPlan):
            return "none"
        if isinstance(plan, DeleteNodePlan):
            return "exact"
        if isinstance(plan, DeleteRelationshipPlan):
            return "none"
        return "full"

    def _snapshot_vector_table_name(self, *, metric: VectorMetric) -> str:
        """Return the next versioned table name for one snapshot build."""

        generation = self._snapshot_generation_by_metric.get(metric, 0) + 1
        self._snapshot_generation_by_metric[metric] = generation
        return f"snapshot_{metric}_v{generation}"

    def _schedule_snapshot_refresh(
        self,
        *,
        metric: VectorMetric,
        item_ids: Any,
        matrix: Any,
    ) -> None:
        """Start one background snapshot refresh when none is already active."""

        refresh_future = self._snapshot_refresh_futures.get(metric)
        if refresh_future is not None and not refresh_future.done():
            return
        if self._snapshot_refresh_executor is None:
            self._snapshot_refresh_executor = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="humemdb-vector-refresh",
            )
        epoch = self._snapshot_refresh_epoch.get(metric, 0)
        config = self._vector_runtime_config.lancedb.with_table_name(
            self._snapshot_vector_table_name(metric=metric)
        )
        self._snapshot_refresh_futures[metric] = (
            self._snapshot_refresh_executor.submit(
                _build_snapshot_refresh_result,
                metric=metric,
                epoch=epoch,
                item_ids=item_ids,
                matrix=matrix,
                lance_path=self._vector_lancedb_path(),
                config=config,
            )
        )

    def _maybe_promote_snapshot_refresh(self, metric: VectorMetric) -> None:
        """Promote one completed background snapshot refresh if it is still valid."""

        refresh_future = self._snapshot_refresh_futures.get(metric)
        if refresh_future is None or not refresh_future.done():
            return
        del self._snapshot_refresh_futures[metric]
        if refresh_future.cancelled():
            return
        try:
            refresh_result = refresh_future.result()
        except _VECTOR_RUNTIME_BACKGROUND_ERRORS:
            logger.exception("Snapshot LanceDB refresh failed metric=%s", metric)
            return
        if refresh_result.epoch != self._snapshot_refresh_epoch.get(metric, 0):
            try:
                _drop_lancedb_table(
                    lance_path=self._vector_lancedb_path(),
                    table_name=refresh_result.index.config.table_name,
                )
            except _VECTOR_RUNTIME_BACKGROUND_ERRORS:
                logger.debug(
                    "Ignoring failed cleanup for stale snapshot refresh table=%s",
                    refresh_result.index.config.table_name,
                    exc_info=True,
                )
            return
        previous = self._snapshot_vector_index_cache.get(metric)
        self._store_live_snapshot(metric=metric, index=refresh_result.index)
        _clear_vector_tombstones(self._sqlite, metric=metric)
        self._clear_vector_tombstone_cache()
        if previous is not None:
            try:
                _drop_lancedb_table(
                    lance_path=self._vector_lancedb_path(),
                    table_name=previous.config.table_name,
                )
            except _VECTOR_RUNTIME_BACKGROUND_ERRORS:
                logger.debug(
                    "Ignoring failed cleanup for prior snapshot table=%s",
                    previous.config.table_name,
                    exc_info=True,
                )

    def __enter__(self) -> HumemDB:
        """Return `self` for context-manager usage.

        Returns:
            The current `HumemDB` instance.
        """

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close connections when leaving a `with HumemDB(...)` block.

        Args:
            exc_type: Exception type raised inside the context, if any.
            exc: Exception instance raised inside the context, if any.
            tb: Traceback for the raised exception, if any.
        """

        self.close()

    def _engine_for_route(self, route: Route) -> _TransactionalEngine:
        """Resolve a route string into its backing engine object."""

        if route == "sqlite":
            return self._sqlite

        if route == "duckdb":
            return self._duckdb

        raise ValueError(f"Unsupported route: {route!r}")


class _TransactionContext:
    """SQLite transaction context manager used by `HumemDB`.

    This helper keeps the public transaction API ergonomic while making the
    commit-or-rollback behavior explicit and testable.

    Attributes:
        db: Owning `HumemDB` instance.
    """

    def __init__(self, db: HumemDB) -> None:
        """Bind the context manager to one database instance.

        Args:
            db: Owning database instance.
        """

        self.db = db

    def __enter__(self) -> HumemDB:
        """Begin the transaction and return the owning `HumemDB` instance.

        Returns:
            The owning `HumemDB` instance for use inside the context.
        """

        self.db.begin()
        return self.db

    def __exit__(self, exc_type, exc, tb) -> None:
        """Commit on success or roll back when an exception occurs.

        Args:
            exc_type: Exception type raised inside the context, if any.
            exc: Exception instance raised inside the context, if any.
            tb: Traceback for the raised exception, if any.
        """

        if exc_type is None:
            self.db.commit()
            return

        self.db.rollback()


def _unknown_vector_index_name_message(name: str) -> str:
    """Return the shared error message for unsupported public index names."""

    return (
        "HumemDB vector index admin commands require a created named vector "
        f"index; got {name!r}."
    )


def _create_vector_index_metric(metric_text: str | None) -> VectorMetric:
    """Return the metric requested by one CREATE VECTOR INDEX statement."""

    if metric_text is None:
        return "cosine"
    normalized = str(metric_text).lower()
    if normalized not in {"cosine", "dot", "l2"}:
        raise ValueError(
            "HumemDB vector indexes support only metrics 'cosine', 'dot', and 'l2'."
        )
    return cast(VectorMetric, normalized)


def _pgvector_operator_class_metric(operator_class_text: str) -> VectorMetric:
    """Return the metric implied by one pgvector operator class."""

    normalized = str(operator_class_text).lower()
    mapping: dict[str, VectorMetric] = {
        "vector_cosine_ops": "cosine",
        "vector_ip_ops": "dot",
        "vector_l2_ops": "l2",
    }
    if normalized not in mapping:
        raise ValueError(
            "HumemDB pgvector-like SQL index DDL currently supports only "
            "vector_cosine_ops, vector_ip_ops, and vector_l2_ops."
        )
    return mapping[normalized]


def _neo4j_vector_similarity_metric(options_text: str) -> VectorMetric:
    """Return the metric implied by one narrow Neo4j-like OPTIONS payload."""

    similarity_match = re.search(
        r"`?vector\.similarity_function`?\s*:\s*['\"](?P<metric>cosine|euclidean)['\"]",
        options_text,
        flags=re.IGNORECASE,
    )
    if similarity_match is None:
        return "cosine"
    normalized = str(similarity_match.group("metric")).lower()
    if normalized == "euclidean":
        return "l2"
    return cast(VectorMetric, normalized)


def _infer_query_type(text: str) -> InternalQueryType:
    """Infer the public query type for the current SQL/Cypher/vector subset.

    The inference stays intentionally narrow. Vector intent must be explicit in the
    query text itself, matching the current PostgreSQL-like SQL and Neo4j-like Cypher
    vector forms.
    """

    stripped = text.lstrip()
    if not stripped:
        return "sql"

    if _is_vector_query_text(stripped):
        return "vector"

    if (
        _CYPHER_MATCH_PREFIX.match(stripped)
        or _CYPHER_OPTIONAL_MATCH_PREFIX.match(stripped)
        or _CYPHER_CREATE_PATTERN.match(stripped)
        or _CYPHER_MERGE_PATTERN.match(stripped)
        or _CYPHER_UNWIND_PREFIX.match(stripped)
        or _CYPHER_CALL_PREFIX.match(stripped)
        or _CYPHER_RETURN_PREFIX.match(stripped)
        or _CYPHER_REMOVE_PREFIX.match(stripped)
    ):
        return "cypher"
    return "sql"


def _plan_query(
    text: str,
    *,
    route: Route | None,
    query_type: InternalQueryType | None = None,
    params: QueryParameters,
) -> _QueryExecutionPlan:
    """Build the thin internal dispatch plan for one `db.query(...)` call."""

    resolved_query_type = query_type or _infer_query_type(text)
    _validate_public_query_params(resolved_query_type, params)
    _validate_query_route(route)
    translated_text = None
    sql_plan = None
    cypher_plan = None
    cypher_shape = None
    vector_plan = None
    sql_is_read_only = None
    olap_thresholds = _resolve_sql_olap_thresholds()
    planning_route: Route = route or "sqlite"
    if resolved_query_type == "sql":
        sql_plan = translate_sql_plan(text, target=planning_route)
        sql_is_read_only = sql_plan.is_read_only
    elif resolved_query_type == "vector":
        vector_plan = _plan_candidate_vector_query(text, params)
    elif resolved_query_type == "cypher":
        cypher_plan, cypher_shape = _plan_cypher_query(text)
    workload = _classify_workload(
        resolved_query_type,
        sql_plan=sql_plan,
        cypher_shape=cypher_shape,
        olap_thresholds=olap_thresholds,
    )
    route_decision = _resolve_route_decision(route, workload)
    resolved_route = route_decision.selected_route
    if resolved_query_type == "sql":
        sql_plan = translate_sql_plan(text, target=resolved_route)
        translated_text = sql_plan.translated_text
        sql_is_read_only = sql_plan.is_read_only

    return _QueryExecutionPlan(
        text=text,
        route=resolved_route,
        route_decision=route_decision,
        query_type=resolved_query_type,
        params=params,
        workload=workload,
        translated_text=translated_text,
        sql_plan=sql_plan,
        cypher_plan=cypher_plan,
        cypher_shape=cypher_shape,
        vector_plan=vector_plan,
        sql_is_read_only=sql_is_read_only,
    )


@lru_cache(maxsize=256)
def _plan_cypher_query(text: str) -> tuple[_GraphPlan, _CypherPlanShape]:
    """Plan non-vector Cypher through the generated frontend boundary.

    Ordinary Cypher planning is now owned by the in-repo generated frontend.
    Unsupported or out-of-subset statements should fail at that boundary instead of
    silently falling back to the older handwritten parser.
    """

    plan = _lower_generated_cypher_text(text)
    return plan, _analyze_cypher_plan(plan)


def _resolve_route_decision(
    requested_route: Route | None,
    workload: _WorkloadProfile,
) -> _RouteDecision:
    """Resolve the selected route and record why that route won."""

    if requested_route is None:
        return _RouteDecision(
            selected_route=workload.preferred_route,
            source="automatic",
            reason=(
                f"Auto-selected {workload.preferred_route!r} from workload "
                f"classification: {workload.reason}"
            ),
        )

    if requested_route == workload.preferred_route:
        return _RouteDecision(
            selected_route=requested_route,
            source="explicit",
            reason=(
                f"Explicit route {requested_route!r} matches the workload "
                f"preference: {workload.reason}"
            ),
        )

    return _RouteDecision(
        selected_route=requested_route,
        source="explicit",
        reason=(
            f"Explicit route {requested_route!r} overrides the workload "
            f"preference {workload.preferred_route!r}: {workload.reason}"
        ),
    )


def _resolve_sql_olap_thresholds() -> _OlapRoutingThresholds:
    """Resolve SQL OLAP routing thresholds from env-backed benchmark output."""

    thresholds_path = os.environ.get(_SQL_OLAP_THRESHOLDS_PATH_ENV)
    if thresholds_path is None:
        return _DEFAULT_OLAP_ROUTING_THRESHOLDS
    return _load_sql_olap_thresholds_from_path(thresholds_path)


@lru_cache(maxsize=8)
def _load_sql_olap_thresholds_from_path(path_text: str) -> _OlapRoutingThresholds:
    """Load benchmark-calibrated SQL OLAP thresholds from one JSON file path."""

    path = Path(path_text)
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(
            "HumemDB SQL OLAP threshold file must contain a JSON object."
        )

    if "recommended_runtime" in payload:
        recommended_runtime = payload.get("recommended_runtime")
        if not isinstance(recommended_runtime, dict):
            raise ValueError(
                "HumemDB routing threshold report must expose object-valued "
                "recommended_runtime data."
            )
        threshold_payload = recommended_runtime.get("sql_olap_thresholds")
    else:
        threshold_payload = payload

    if not isinstance(threshold_payload, dict):
        raise ValueError(
            "HumemDB SQL OLAP thresholds must be a JSON object or appear under "
            "recommended_runtime.sql_olap_thresholds."
        )

    try:
        return _OlapRoutingThresholds(
            benchmark_calibrated=bool(threshold_payload["benchmark_calibrated"]),
            min_join_count=int(threshold_payload.get("min_join_count", 0)),
            min_aggregate_count=int(threshold_payload.get("min_aggregate_count", 0)),
            min_cte_count=int(threshold_payload.get("min_cte_count", 0)),
            min_window_count=int(threshold_payload.get("min_window_count", 0)),
            require_order_by_or_limit=bool(
                threshold_payload.get("require_order_by_or_limit", False)
            ),
            rules=tuple(
                _load_sql_olap_rule(rule_payload)
                for rule_payload in threshold_payload.get("rules", ())
            ),
        )
    except KeyError as exc:
        raise ValueError(
            "HumemDB SQL OLAP thresholds must define benchmark_calibrated."
        ) from exc
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "HumemDB SQL OLAP thresholds contain invalid scalar values."
        ) from exc


def _load_sql_olap_rule(rule_payload: object) -> _OlapRoutingRule:
    """Load one benchmark-calibrated SQL routing rule from JSON data."""

    if not isinstance(rule_payload, dict):
        raise ValueError("HumemDB SQL OLAP rules must be object-valued.")

    try:
        return _OlapRoutingRule(
            min_join_count=int(rule_payload.get("min_join_count", 0)),
            min_aggregate_count=int(rule_payload.get("min_aggregate_count", 0)),
            min_cte_count=int(rule_payload.get("min_cte_count", 0)),
            min_window_count=int(rule_payload.get("min_window_count", 0)),
            min_exists_count=int(rule_payload.get("min_exists_count", 0)),
            require_group_by=bool(rule_payload.get("require_group_by", False)),
            require_distinct=bool(rule_payload.get("require_distinct", False)),
            require_order_by_or_limit=bool(
                rule_payload.get("require_order_by_or_limit", False)
            ),
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "HumemDB SQL OLAP rules contain invalid scalar values."
        ) from exc


def _classify_workload(
    query_type: InternalQueryType,
    *,
    sql_plan: _SQLTranslationPlan | None,
    cypher_shape: _CypherPlanShape | None,
    olap_thresholds: _OlapRoutingThresholds = _DEFAULT_OLAP_ROUTING_THRESHOLDS,
) -> _WorkloadProfile:
    """Classify one parsed query into a small routing-oriented workload profile."""

    if query_type == "vector":
        return _WorkloadProfile(
            kind="vector_search",
            is_read_only=True,
            preferred_route="sqlite",
            reason="Vector search runs against the SQLite-backed exact NumPy index.",
        )

    if query_type == "cypher":
        if cypher_shape is None:
            raise ValueError("HumemDB internal Cypher plans require plan metadata.")
        if not cypher_shape.is_read_only:
            return _WorkloadProfile(
                kind="graph_write",
                is_read_only=False,
                preferred_route="sqlite",
                reason="Cypher CREATE and SET mutate the canonical SQLite graph store.",
            )
        return _WorkloadProfile(
            kind="graph_read",
            is_read_only=True,
            preferred_route="sqlite",
            reason=(
                "Current Cypher benchmark evidence is not broad enough to harden "
                "automatic DuckDB routing, so read-only graph queries stay on "
                "SQLite by default."
            ),
        )

    if sql_plan is None:
        raise ValueError("HumemDB internal SQL plans require translation metadata.")

    if not sql_plan.is_read_only:
        return _WorkloadProfile(
            kind="transactional_write",
            is_read_only=False,
            preferred_route="sqlite",
            reason="SQL writes target the canonical SQLite store.",
        )

    if _has_broad_sql_analytical_shape(sql_plan):
        if _matches_sql_olap_thresholds(sql_plan, olap_thresholds):
            return _WorkloadProfile(
                kind="analytical_read",
                is_read_only=True,
                preferred_route="duckdb",
                reason=(
                    "Read-only SQL crossed the current benchmark-calibrated OLAP "
                    "thresholds for DuckDB."
                ),
            )
        return _WorkloadProfile(
            kind="analytical_read",
            is_read_only=True,
            preferred_route="sqlite",
            reason=(
                "Read-only SQL is analytical, but DuckDB admission stays disabled "
                "until OLAP routing thresholds are benchmark-calibrated."
            ),
        )

    return _WorkloadProfile(
        kind="transactional_read",
        is_read_only=True,
        preferred_route="sqlite",
        reason="Simple read-only SQL stays classified as transactional.",
    )


def _validate_query_route(route: Route | None) -> None:
    """Reject unsupported explicit query routes before planning continues."""

    if route is None:
        return
    if route not in {"sqlite", "duckdb"}:
        raise ValueError(f"Unsupported route: {route!r}")


def _matches_sql_olap_thresholds(
    sql_plan: _SQLTranslationPlan,
    thresholds: _OlapRoutingThresholds,
) -> bool:
    """Return whether one SQL read crosses benchmark-calibrated DuckDB thresholds."""

    if not thresholds.benchmark_calibrated:
        return False

    if not _has_broad_sql_analytical_shape(sql_plan):
        return False

    if thresholds.rules:
        return any(
            _matches_sql_olap_rule(sql_plan, rule)
            for rule in thresholds.rules
        )

    if sql_plan.join_count < thresholds.min_join_count:
        return False
    if sql_plan.aggregate_count < thresholds.min_aggregate_count:
        return False
    if sql_plan.cte_count < thresholds.min_cte_count:
        return False
    if sql_plan.window_count < thresholds.min_window_count:
        return False
    if thresholds.require_order_by_or_limit and not (
        sql_plan.has_order_by or sql_plan.has_limit
    ):
        return False
    return True


def _matches_sql_olap_rule(
    sql_plan: _SQLTranslationPlan,
    rule: _OlapRoutingRule,
) -> bool:
    """Return whether one SQL read matches a calibrated DuckDB routing rule."""

    if sql_plan.join_count < rule.min_join_count:
        return False
    if sql_plan.aggregate_count < rule.min_aggregate_count:
        return False
    if sql_plan.cte_count < rule.min_cte_count:
        return False
    if sql_plan.window_count < rule.min_window_count:
        return False
    if sql_plan.exists_count < rule.min_exists_count:
        return False
    if rule.require_group_by and not sql_plan.has_group_by:
        return False
    if rule.require_distinct and not sql_plan.has_distinct:
        return False
    if rule.require_order_by_or_limit and not (
        sql_plan.has_order_by or sql_plan.has_limit
    ):
        return False
    return True


def _has_broad_sql_analytical_shape(sql_plan: _SQLTranslationPlan) -> bool:
    """Return whether one SQL read matches the current conservative DuckDB shape."""

    if sql_plan.window_count > 0:
        return True

    if sql_plan.exists_count > 0:
        return True

    if sql_plan.aggregate_count > 0:
        return (
            sql_plan.has_group_by
            or sql_plan.has_distinct
            or sql_plan.join_count > 0
            or sql_plan.cte_count > 0
            or sql_plan.has_order_by
            or sql_plan.has_limit
        )

    if sql_plan.has_distinct and sql_plan.join_count > 0:
        return True

    if sql_plan.cte_count > 0:
        return (
            sql_plan.join_count > 0
            or sql_plan.has_distinct
            or sql_plan.has_order_by
            or sql_plan.has_limit
        )

    if sql_plan.join_count > 1:
        return sql_plan.has_distinct or sql_plan.has_order_by or sql_plan.has_limit

    return False


def _plan_batch_query(
    text: str,
    *,
    route: Route,
    params_seq: BatchParameters,
) -> _BatchExecutionPlan:
    """Build the thin internal dispatch plan for one `executemany(...)` call."""

    _validate_public_batch_params(params_seq)

    return _BatchExecutionPlan(
        text=text,
        route=route,
        params_seq=params_seq,
        translated_text=translate_sql(text, target=route),
    )


def _touches_vector_entries(text: str) -> bool:
    """Return whether a SQL statement references the canonical vector table."""

    return "vector_entries" in text.casefold()


def _validate_public_query_params(
    query_type: InternalQueryType,
    params: QueryParameters,
) -> None:
    """Enforce the current public parameter conventions for each query surface."""

    if query_type != "sql" or params is None:
        return
    if isinstance(params, Mapping):
        return
    raise ValueError(
        "HumemDB SQL queries now require named mapping params with $placeholders; "
        "positional SQL params are no longer supported through db.query(...)."
    )


def _validate_public_batch_params(params_seq: BatchParameters) -> None:
    """Enforce mapping-style SQL batch params on the public executemany surface."""

    for params in params_seq:
        if isinstance(params, Mapping):
            continue
        raise ValueError(
            "HumemDB SQL batch writes now require mapping params with $placeholders; "
            "positional SQL params are no longer supported through executemany(...)."
        )


def _normalize_import_columns(
    columns: Sequence[str] | None,
) -> tuple[str, ...] | None:
    """Normalize public import columns into one validated identifier tuple."""

    if columns is None:
        return None
    normalized = tuple(str(column) for column in columns)
    if not normalized:
        raise ValueError("import_table(...) requires at least one column when set.")
    if any(not column for column in normalized):
        raise ValueError("import_table(...) does not allow empty column names.")
    if len(set(normalized)) != len(normalized):
        raise ValueError("import_table(...) does not allow duplicate column names.")
    return normalized


def _normalize_graph_import_columns(
    columns: Sequence[str] | None,
) -> tuple[str, ...] | None:
    """Normalize graph import property columns into one validated identifier tuple."""

    return _normalize_import_columns(columns)


def _normalize_graph_import_property_types(
    property_types: Mapping[str, _GraphImportPropertyType] | None,
) -> dict[str, _GraphImportPropertyType]:
    """Normalize graph import property type overrides."""

    if property_types is None:
        return {}

    normalized: dict[str, _GraphImportPropertyType] = {}
    for key, value in property_types.items():
        if not key:
            raise ValueError(
                "graph import property type mappings do not allow empty keys."
            )
        if value not in {"string", "integer", "real", "boolean"}:
            raise ValueError(
                "graph import property types must be one of string, integer, "
                "real, or boolean."
            )
        normalized[str(key)] = value
    return normalized


def _prepare_csv_import_reader(
    csv_file,
    *,
    columns: tuple[str, ...] | None,
    header: bool,
    delimiter: str,
) -> tuple[tuple[str, ...], Iterator[tuple[object, ...]]]:
    """Resolve CSV columns and return one normalized row iterator."""

    resolved_columns = columns

    if header:
        reader = csv.reader(csv_file, delimiter=delimiter)
        header_row = next(reader, None)
        if header_row is None:
            raise ValueError(
                "import_table(...) expected a CSV header row but the file is empty."
            )
        header_columns = tuple(header_row)
        if resolved_columns is None:
            resolved_columns = header_columns
        else:
            missing = [
                column for column in resolved_columns if column not in header_columns
            ]
            if missing:
                raise ValueError(
                    "import_table(...) CSV header is missing required columns: "
                    + ", ".join(missing)
                )

        assert resolved_columns is not None
        column_indexes = tuple(
            header_columns.index(column) for column in resolved_columns
        )

        def iter_header_rows() -> Iterator[tuple[object, ...]]:
            for row in reader:
                if len(row) > len(header_columns):
                    raise ValueError(
                        "import_table(...) found a CSV row with more fields "
                        "than the header."
                    )
                if _row_is_empty(row):
                    continue
                if len(row) < len(header_columns):
                    raise ValueError(
                        "import_table(...) found a CSV row with missing fields."
                    )
                yield tuple(row[index] for index in column_indexes)

        return resolved_columns, iter_header_rows()

    assert resolved_columns is not None
    reader = csv.reader(csv_file, delimiter=delimiter)

    def iter_body_rows() -> Iterator[tuple[object, ...]]:
        for row in reader:
            if _row_is_empty(row):
                continue
            if len(row) != len(resolved_columns):
                raise ValueError(
                    "import_table(...) found a CSV row whose field count does not "
                    "match the provided columns."
                )
            yield tuple(row)

    return resolved_columns, iter_body_rows()


def _prepare_named_csv_import_reader(
    csv_file,
    *,
    columns: tuple[str, ...] | None,
    header: bool,
    delimiter: str,
) -> tuple[tuple[str, ...], Iterator[dict[str, str]]]:
    """Resolve CSV columns and return rows keyed by their real column names."""

    resolved_columns = columns

    if header:
        reader = csv.DictReader(csv_file, delimiter=delimiter)
        if reader.fieldnames is None:
            raise ValueError(
                "CSV import expected a header row but the file is empty."
            )
        header_columns = tuple(reader.fieldnames)
        if resolved_columns is None:
            resolved_columns = header_columns
        else:
            missing = [
                column for column in resolved_columns if column not in header_columns
            ]
            if missing:
                raise ValueError(
                    "CSV header is missing required columns: " + ", ".join(missing)
                )

        assert resolved_columns is not None

        def iter_header_rows() -> Iterator[dict[str, str]]:
            for row in reader:
                if row is None:
                    continue
                if None in row:
                    raise ValueError(
                        "CSV import found a row with more fields than the header."
                    )
                if _row_is_empty(row.get(column, "") for column in resolved_columns):
                    continue
                normalized_row: dict[str, str] = {}
                for column in resolved_columns:
                    value = row[column]
                    if value is None:
                        raise ValueError(
                            "CSV import found a row with missing fields."
                        )
                    normalized_row[column] = value
                yield normalized_row

        return resolved_columns, iter_header_rows()

    assert resolved_columns is not None
    reader = csv.reader(csv_file, delimiter=delimiter)

    def iter_body_rows() -> Iterator[dict[str, str]]:
        for row in reader:
            if _row_is_empty(row):
                continue
            if len(row) != len(resolved_columns):
                raise ValueError(
                    "CSV import found a row whose field count does not match the "
                    "provided columns."
                )
            yield {column: value for column, value in zip(resolved_columns, row)}

    return resolved_columns, iter_body_rows()


def _node_import_columns(
    id_column: str,
    property_columns: tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    """Return the required headerless column order for node import."""

    if property_columns is None:
        return None
    return (id_column, *property_columns)


def _edge_import_columns(
    source_id_column: str,
    target_id_column: str,
    property_columns: tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    """Return the required headerless column order for edge import."""

    if property_columns is None:
        return None
    return (source_id_column, target_id_column, *property_columns)


def _resolve_graph_property_columns(
    *,
    available_columns: Sequence[str],
    required_columns: Sequence[str],
    property_columns: tuple[str, ...] | None,
    method_name: str,
) -> tuple[str, ...]:
    """Resolve graph property columns from CSV columns plus required id fields."""

    missing_required = [
        column for column in required_columns if column not in available_columns
    ]
    if missing_required:
        raise ValueError(
            f"{method_name}(...) CSV data is missing required columns: "
            + ", ".join(missing_required)
        )

    if property_columns is None:
        return tuple(
            column for column in available_columns if column not in required_columns
        )

    missing = [column for column in property_columns if column not in available_columns]
    if missing:
        raise ValueError(
            f"{method_name}(...) CSV data is missing property columns: "
            + ", ".join(missing)
        )
    disallowed = [column for column in property_columns if column in required_columns]
    if disallowed:
        raise ValueError(
            f"{method_name}(...) property columns cannot reuse required id columns: "
            + ", ".join(disallowed)
        )
    return property_columns


def _build_graph_property_rows(
    *,
    owner_id: int,
    columns: Sequence[str],
    row: Mapping[str, str],
    property_types: Mapping[str, _GraphImportPropertyType],
) -> list[tuple[int, str, object, str]]:
    """Encode one CSV row into graph property-table writes."""

    property_rows: list[tuple[int, str, object, str]] = []
    for column in columns:
        property_value = _coerce_graph_import_value(
            row[column],
            property_types.get(column, "string"),
        )
        encoded_value, value_type = _encode_property_value(property_value)
        property_rows.append((owner_id, column, encoded_value, value_type))
    return property_rows


def _coerce_graph_import_value(
    raw_value: str,
    value_type: _GraphImportPropertyType,
) -> str | int | float | bool | None:
    """Coerce one CSV field into the typed graph property value model."""

    if value_type == "string":
        return raw_value
    if value_type == "integer":
        return int(raw_value)
    if value_type == "real":
        return float(raw_value)
    lowered = raw_value.strip().casefold()
    if lowered in {"true", "1", "yes"}:
        return True
    if lowered in {"false", "0", "no"}:
        return False
    raise ValueError(
        "graph boolean CSV fields must be one of true/false/1/0/yes/no."
    )


def _write_imported_graph_nodes(
    sqlite: _SQLiteEngine,
    *,
    node_rows: Sequence[tuple[int, str]],
    property_rows: Sequence[tuple[int, str, object, str]],
) -> None:
    """Write one node-import batch into the graph node tables."""

    sqlite.executemany(
        "INSERT INTO graph_nodes (id, label) VALUES (?, ?)",
        node_rows,
        query_type="cypher",
    )
    if property_rows:
        sqlite.executemany(
            (
                "INSERT INTO graph_node_properties "
                "(node_id, key, value, value_type) VALUES (?, ?, ?, ?)"
            ),
            property_rows,
            query_type="cypher",
        )


def _next_graph_edge_id(sqlite: _SQLiteEngine) -> int:
    """Return the next explicit edge id for graph edge import."""

    row = sqlite.execute(
        "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM graph_edges",
        query_type="cypher",
    ).first()
    if row is None:
        return 1
    return int(row[0])


def _write_imported_graph_edges(
    sqlite: _SQLiteEngine,
    *,
    edge_rows: Sequence[tuple[int, str, int, int]],
    property_rows: Sequence[tuple[int, str, object, str]],
) -> None:
    """Write one edge-import batch into the graph edge tables."""

    sqlite.executemany(
        (
            "INSERT INTO graph_edges (id, type, from_node_id, to_node_id) "
            "VALUES (?, ?, ?, ?)"
        ),
        edge_rows,
        query_type="cypher",
    )
    if property_rows:
        sqlite.executemany(
            (
                "INSERT INTO graph_edge_properties "
                "(edge_id, key, value, value_type) VALUES (?, ?, ?, ?)"
            ),
            property_rows,
            query_type="cypher",
        )


def _build_import_insert_sql(
    *,
    table: str,
    columns: Sequence[str],
) -> str:
    """Build one identifier-safe SQL insert statement for CSV import batches."""

    quoted_columns = ", ".join(_quote_sql_identifier(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    return (
        f"INSERT INTO {_quote_sql_identifier(table)} ({quoted_columns}) "
        f"VALUES ({placeholders})"
    )


def _quote_sql_identifier(identifier: str) -> str:
    """Return one SQLite-safe quoted identifier for dynamic import SQL."""

    if not identifier:
        raise ValueError("import_table(...) does not allow empty identifiers.")
    return '"' + identifier.replace('"', '""') + '"'


def _row_is_empty(values: Iterable[object]) -> bool:
    """Return whether one parsed CSV row is effectively blank."""

    return all(str(value).strip() == "" for value in values)


def _write_target_namespaced_vector_rows(
    sqlite: _SQLiteEngine,
    vector_rows: Sequence[tuple[str, str, int, Sequence[float]]],
    *,
    mode: str,
) -> None:
    """Persist one batch of logical target/namespace vector rows."""

    grouped: dict[tuple[str, str], list[tuple[int, Sequence[float]]]] = {}
    for target, namespace, target_id, vector in vector_rows:
        grouped.setdefault((target, namespace), []).append((target_id, vector))

    for (target, namespace), rows in grouped.items():
        if mode == "insert":
            _insert_vectors(sqlite, rows, target=target, namespace=namespace)
        else:
            _upsert_vectors(sqlite, rows, target=target, namespace=namespace)


def _normalize_direct_vector_rows(
    sqlite: _SQLiteEngine,
    rows: Sequence[_DirectVectorRow],
) -> tuple[
    list[tuple[int, Sequence[float]]],
    tuple[int, ...],
    list[tuple[int, _DirectVectorMetadata]],
]:
    """Normalize direct vector inserts into stored rows plus optional metadata.

    Args:
        sqlite: Canonical SQLite engine used to discover the next direct id.
        rows: Direct-vector inputs in plain-embedding, explicit-id tuple, or
            mapping-record form.

    Returns:
        A tuple containing resolved `(target_id, embedding)` rows, the assigned ids in
        input order, and any metadata rows keyed by their resolved direct ids.
    """

    normalized: list[
        tuple[int | None, Sequence[float], _DirectVectorMetadata | None]
    ] = []
    explicit_ids: list[int] = []
    for row in rows:
        record = _direct_vector_record(row)
        if record is not None:
            target_id, vector, metadata = record
            if target_id is not None:
                explicit_ids.append(target_id)
            normalized.append((target_id, vector, metadata))
            continue

        if isinstance(row, Mapping):
            raise ValueError(
                "Direct vector record rows must include an 'embedding' key."
            )

        explicit = _explicit_direct_vector_row(row)
        if explicit is None:
            normalized.append((None, _plain_direct_vector(row), None))
            continue

        explicit_ids.append(explicit[0])
        normalized.append((explicit[0], explicit[1], None))

    next_id = _next_direct_target_id(sqlite, floor=max(explicit_ids, default=0))
    assigned_ids: list[int] = []
    resolved_rows: list[tuple[int, Sequence[float]]] = []
    metadata_rows: list[tuple[int, _DirectVectorMetadata]] = []
    for target_id, vector, metadata in normalized:
        if target_id is None:
            target_id = next_id
            next_id += 1
        assigned_ids.append(int(target_id))
        resolved_rows.append((int(target_id), vector))
        if metadata:
            metadata_rows.append((int(target_id), metadata))

    return resolved_rows, tuple(assigned_ids), metadata_rows


def _direct_vector_record(
    row: _DirectVectorRow,
) -> tuple[int | None, Sequence[float], _DirectVectorMetadata | None] | None:
    """Return one direct-vector record mapping if the row uses record syntax.

    Args:
        row: Candidate direct-vector input row.

    Returns:
        `(target_id, embedding, metadata)` when the row uses mapping-record syntax, or
        `None` when the row should be handled by the plain embedding or tuple path.

    Raises:
        ValueError: If the mapping omits `embedding`, uses a non-integral `target_id`,
            or provides non-mapping metadata.
    """

    if not isinstance(row, Mapping):
        return None

    if "embedding" not in row:
        raise ValueError(
            "Direct vector record rows must include an 'embedding' key."
        )

    embedding = row["embedding"]
    if isinstance(embedding, (str, bytes, bytearray)) or not isinstance(
        embedding, Sequence
    ):
        raise ValueError(
            "Direct vector record 'embedding' values must be numeric sequences."
        )

    raw_target_id = row.get("target_id")
    target_id: int | None
    if raw_target_id is None:
        target_id = None
    elif isinstance(raw_target_id, Integral):
        target_id = int(raw_target_id)
    else:
        raise ValueError(
            "Direct vector record 'target_id' values must be integers when provided."
        )

    raw_metadata = row.get("metadata")
    metadata: _DirectVectorMetadata | None
    if raw_metadata is None:
        metadata = None
    elif isinstance(raw_metadata, Mapping):
        metadata = dict(raw_metadata)
    else:
        raise ValueError(
            "Direct vector record 'metadata' values must be mappings when provided."
        )

    return target_id, embedding, metadata


def _explicit_direct_vector_row(
    row: Sequence[float] | tuple[int, Sequence[float]],
) -> tuple[int, Sequence[float]] | None:
    """Return one explicit `(target_id, embedding)` direct vector row if present."""

    if len(row) != 2:
        return None

    target_id, vector = row
    if not isinstance(target_id, Integral):
        return None
    if isinstance(vector, (str, bytes, bytearray)):
        return None
    if not isinstance(vector, Sequence):
        return None
    return (int(target_id), vector)


def _plain_direct_vector(
    row: Sequence[float] | tuple[int, Sequence[float]],
) -> Sequence[float]:
    """Return the embedding sequence for a plain direct-vector input row.

    Args:
        row: Candidate non-record direct-vector input row.

    Returns:
        The embedding sequence for plain direct inserts.

    Raises:
        ValueError: If the row looks like an explicit-id tuple or otherwise does not
            represent a plain embedding sequence.
    """

    if _explicit_direct_vector_row(row) is not None:
        raise ValueError(
            "Plain direct vector rows cannot use explicit `(target_id, embedding)` "
            "shape."
        )

    return cast(Sequence[float], row)


def _next_direct_target_id(sqlite: _SQLiteEngine, *, floor: int) -> int:
    """Return the next auto-assigned direct target id, starting from 1."""

    result = sqlite.execute(
        (
            "SELECT COALESCE(MAX(target_id), 0) "
            "FROM vector_entries "
            "WHERE target = 'direct' AND namespace = ''"
        ),
        query_type="vector",
    )
    first_row = result.first()
    current_max = int(first_row[0]) if first_row is not None else 0
    return max(current_max, floor) + 1


def _resolved_sql_vector_rows_after_insert(
    sqlite: _SQLiteEngine,
    vector_rows: Sequence[_PendingTargetNamespacedVectorRow],
) -> list[_TargetNamespacedVectorRow]:
    """Resolve SQLite-assigned row ids for freshly inserted SQL-owned vectors."""

    resolved: list[_TargetNamespacedVectorRow] = []
    for target, namespace, target_id, vector in vector_rows:
        if target_id is None:
            first_row = sqlite.execute(
                "SELECT last_insert_rowid() AS row_id",
                query_type="sql",
            ).first()
            if first_row is None:
                raise ValueError(
                    "HumemVector v0 SQL inserts could not resolve the assigned row "
                    "id."
                )
            target_id = int(first_row[0])
        resolved.append((target, namespace, int(target_id), vector))

    return resolved


def _node_pattern_has_vector_property(node: Any) -> bool:
    """Return whether one bound node pattern contains a vector property value."""

    return any(
        _encode_property_value(cast(Any, value))[1] == "vector"
        for _, value in node.properties
    )


def _set_plan_has_vector_assignment(plan: SetNodePlan) -> bool:
    """Return whether one Cypher SET node plan writes any vector property."""

    return any(
        _encode_property_value(cast(Any, assignment.value))[1] == "vector"
        for assignment in plan.assignments
    )
