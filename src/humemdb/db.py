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

from dataclasses import dataclass
from functools import lru_cache
import json
import logging
import os
import re
from numbers import Integral
from pathlib import Path
from typing import Any, Literal, Mapping, Protocol, Sequence, TypeAlias, cast

from ._vector_runtime import (
    DirectVectorSearchPlan as _DirectVectorSearchPlan,
    PendingTargetNamespacedVectorRow as _PendingTargetNamespacedVectorRow,
    ResolvedVectorCandidates as _ResolvedVectorCandidates,
    CandidateVectorQueryResult as _CandidateVectorQueryResult,
    CandidateVectorQueryPlan as _CandidateVectorQueryPlan,
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
    CypherPlanShape as _CypherPlanShape,
    GraphPlan as _GraphPlan,
    analyze_cypher_plan,
    ensure_graph_schema,
    execute_cypher,
)
from .cypher_frontend import lower_cypher_text as _lower_generated_cypher_text
from .engines import DuckDBEngine, SQLiteEngine
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
    ExactVectorIndex,
    VectorMetric,
    ensure_vector_schema,
    insert_vectors as insert_vector_rows,
    load_filtered_vector_target_keys,
    load_vector_matrix,
    upsert_vectors,
    upsert_vector_metadata,
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

DirectVectorMetadata: TypeAlias = Mapping[str, str | int | float | bool | None]
DirectVectorRow: TypeAlias = (
    Sequence[float]
    | tuple[int, Sequence[float]]
    | Mapping[str, object]
)
WorkloadKind: TypeAlias = Literal[
    "transactional_read",
    "transactional_write",
    "analytical_read",
    "graph_read",
    "graph_write",
    "vector_search",
]


@dataclass(frozen=True, slots=True)
class _WorkloadProfile:
    """Explainable workload classification derived from validated query structure."""

    kind: WorkloadKind
    is_read_only: bool
    preferred_route: Route
    reason: str


@dataclass(frozen=True, slots=True)
class _RouteDecision:
    """Internal record of how one query route was selected."""

    selected_route: Route
    source: Literal["automatic", "explicit"]
    reason: str


@dataclass(frozen=True, slots=True)
class _OlapRoutingRule:
    """One benchmark-calibrated SQL shape family that should route to DuckDB."""

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
    """Benchmark-calibrated thresholds for admitting SQL OLAP reads to DuckDB."""

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
    """Thin internal plan for one public `db.query(...)` call."""

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
    """Thin internal plan for one public `executemany(...)` call."""

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
        sqlite_path: Path to the canonical SQLite database.
        duckdb_path: Optional path to a DuckDB database file. If omitted, DuckDB uses
            an in-memory database.
        preload_vectors: Optional eager vector preload flag. Use `False` to keep the
            exact vector set lazy-loaded or `True` to warm it on open when vector data
            already exists.

    Notes:
        Instantiating `HumemDB` opens both embedded database connections. Use the object
        as a context manager or call `close()` explicitly to release them.
    """

    def __init__(
        self,
        sqlite_path: str,
        duckdb_path: str | None = None,
        *,
        preload_vectors: bool = False,
    ) -> None:
        """Open the embedded engines and initialize lazy runtime state.

        Args:
            sqlite_path: Path to the canonical SQLite database file.
            duckdb_path: Optional path to a DuckDB file. When omitted, DuckDB uses an
                in-memory database.
            preload_vectors: Whether to warm the exact vector cache immediately when
                vector storage already exists.
        """

        self.sqlite_path = sqlite_path
        self.duckdb_path = duckdb_path
        self._graph_schema_ready = False
        self._vector_schema_ready = False
        self._vector_matrix_cache: tuple[Any, Any] | None = None
        self._vector_item_index_cache: dict[tuple[str, str, int], int] | None = None
        self._vector_namespace_index_cache: (
            dict[tuple[str, str], tuple[int, ...]] | None
        ) = None
        self._vector_index_cache: dict[VectorMetric, ExactVectorIndex] = {}

        sqlite_path_obj = Path(self.sqlite_path)
        sqlite_path_obj.parent.mkdir(parents=True, exist_ok=True)

        self._sqlite = SQLiteEngine(str(sqlite_path_obj))
        self._duckdb = DuckDBEngine(self.duckdb_path)
        self._duckdb.attach_sqlite(str(sqlite_path_obj))
        logger.debug(
            "HumemDB initialized with sqlite_path=%s duckdb_path=%s",
            self.sqlite_path,
            self.duckdb_path,
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
                `SEARCH ... VECTOR INDEX embedding FOR $query LIMIT ...` forms.

        Returns:
            A normalized `QueryResult`.

        Raises:
            NotImplementedError: If an unsupported query type is requested.
            ValueError: If execution reaches an unsupported internal route or a write
                is planned for DuckDB.
        """

        plan = _plan_query(
            text,
            route=None,
            params=params,
        )
        return self._execute_query_plan(plan)

    def insert_vectors(
        self,
        rows: Sequence[DirectVectorRow],
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
        insert_vector_rows(
            self._sqlite,
            normalized_rows,
            target="direct",
            namespace="",
        )
        if metadata_rows:
            upsert_vector_metadata(
                self._sqlite,
                metadata_rows,
                target="direct",
                namespace="",
            )
        self._invalidate_vector_cache()
        return assigned_ids

    def search_vectors(
        self,
        query: Sequence[float],
        *,
        top_k: int = 10,
        metric: VectorMetric = "cosine",
        filters: Mapping[str, str | int | float | bool | None] | None = None,
    ) -> QueryResult:
        """Search the current SQLite-backed vector set with exact NumPy search.

        Args:
            query: Query embedding to rank against the current vector set.
            top_k: Maximum number of nearest matches to return.
            metric: Similarity metric to use for ranking.
            filters: Optional equality metadata filters for direct-vector rows.

        Returns:
            A normalized `QueryResult` with
            `(target, namespace, target_id, score)` rows.

        Notes:
            `search_vectors(...)` is the public direct-vector search surface. Vector
            search over SQL rows or graph nodes now lives on language-level
            SQL and Cypher query syntax through `query(...)`.
        """

        direct_plan = _plan_direct_vector_search(
            query,
            top_k=top_k,
            metric=metric,
            filters=filters,
        )
        resolved_candidates = self._resolve_direct_vector_search(direct_plan)
        return self._execute_exact_vector_search(
            query=direct_plan.query,
            top_k=direct_plan.top_k,
            metric=direct_plan.metric,
            resolved_candidates=resolved_candidates,
        )

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
        upsert_vector_metadata(
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

    def _ensure_graph_schema(self) -> None:
        """Initialize the SQLite-backed graph tables on first Cypher use."""

        if self._graph_schema_ready:
            return

        logger.debug("Initializing graph schema on first Cypher use")
        ensure_graph_schema(self._sqlite)
        self._graph_schema_ready = True

    def _ensure_vector_schema(self) -> None:
        """Initialize the SQLite-backed vector tables on first vector use."""

        if self._vector_schema_ready:
            return

        logger.debug("Initializing vector schema on first vector use")
        ensure_vector_schema(self._sqlite)
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
        else:
            logger.debug(
                "Routing Cypher-backed vector query to exact SQLite/NumPy path"
            )
            public_query_type = "cypher"
        resolved_candidates = self._resolve_candidate_vector_query(
            vector_plan,
        )
        return self._execute_exact_vector_search(
            vector_plan.query,
            top_k=vector_plan.top_k,
            metric=vector_plan.metric,
            resolved_candidates=resolved_candidates,
            public_query_type=public_query_type,
        )

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
        result = execute_cypher(
            plan.text,
            route=plan.route,
            params=plan.params,
            sqlite=self._sqlite,
            duckdb=self._duckdb,
            plan=plan.cypher_plan,
        )
        if not plan.workload.is_read_only:
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
                self._invalidate_vector_cache()
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
                self._invalidate_vector_cache()
            self._invalidate_vector_cache_for_sql(plan.text, plan.translated_text)
            return result

        if plan.route == "duckdb":
            logger.debug("Rejected batched write routed to DuckDB")
            raise ValueError(
                "HumemDB does not allow direct batch writes to DuckDB; "
                "SQLite is the source of truth."
            )

        raise ValueError(f"Unsupported route: {plan.route!r}")

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
    ) -> ExactVectorIndex:
        """Load and cache one exact vector index per metric."""

        cached = self._vector_index_cache.get(metric)
        if cached is not None:
            return cached

        item_ids, matrix = self._load_vector_matrix()
        cached = ExactVectorIndex(item_ids=item_ids, matrix=matrix, metric=metric)
        self._vector_index_cache[metric] = cached
        return cached

    def _load_vector_matrix(self) -> tuple[Any, Any]:
        """Load and cache the current vector set from SQLite."""

        if self._vector_matrix_cache is not None:
            return self._vector_matrix_cache

        self._ensure_vector_schema()
        cached = load_vector_matrix(self._sqlite)
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
            load_filtered_vector_target_keys(
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

    def _invalidate_vector_cache(self) -> None:
        """Drop cached exact vector data after vector storage changes."""

        self._vector_matrix_cache = None
        self._vector_item_index_cache = None
        self._vector_namespace_index_cache = None
        self._vector_index_cache.clear()

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

        A successful context commits on exit. An exception inside the context triggers a
        rollback before the exception continues to propagate.

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

    def close(self) -> None:
        """Close both embedded database connections.

        Returns:
            `None`.
        """

        logger.debug("Closing HumemDB connections")
        self._sqlite.close()
        self._duckdb.close()

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
    return plan, analyze_cypher_plan(plan)


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


def _write_target_namespaced_vector_rows(
    sqlite: SQLiteEngine,
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
            insert_vector_rows(sqlite, rows, target=target, namespace=namespace)
        else:
            upsert_vectors(sqlite, rows, target=target, namespace=namespace)


def _normalize_direct_vector_rows(
    sqlite: SQLiteEngine,
    rows: Sequence[DirectVectorRow],
) -> tuple[
    list[tuple[int, Sequence[float]]],
    tuple[int, ...],
    list[tuple[int, DirectVectorMetadata]],
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
        tuple[int | None, Sequence[float], DirectVectorMetadata | None]
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
    metadata_rows: list[tuple[int, DirectVectorMetadata]] = []
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
    row: DirectVectorRow,
) -> tuple[int | None, Sequence[float], DirectVectorMetadata | None] | None:
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
    metadata: DirectVectorMetadata | None
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


def _next_direct_target_id(sqlite: SQLiteEngine, *, floor: int) -> int:
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
    sqlite: SQLiteEngine,
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
