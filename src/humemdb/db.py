"""High-level embedded database interface for HumemDB.

The `HumemDB` class is the public entry point for the current runtime. It owns the
SQLite and DuckDB engine wrappers, exposes an explicit routing API, and defines the
current lifecycle semantics for queries and transactions.

The current public surface is intentionally conservative:

- `query_type="sql"` maps to `HumemSQL v0`
- `query_type="cypher"` maps to `HumemCypher v0`
- `query_type="vector"` maps to the exact `HumemVector v0` search path on SQLite
- SQLite is the canonical public write target, including vectors
- DuckDB is read-only through the public API
- PostgreSQL-like SQL is translated into backend SQL before execution
- transaction control is explicit and route-scoped

As the project grows, this module is where routing, query validation, and the portable
frontend surfaces will expand.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from sqlglot import parse_one
from sqlglot import errors as sqlglot_errors

from .cypher import ensure_graph_schema, execute_cypher
from .engines import DuckDBEngine, SQLiteEngine
from .sql import translate_sql
from .types import BatchParameters, QueryParameters, QueryResult, QueryType, Route
from .vector import (
    ExactVectorIndex,
    VectorMetric,
    encode_vector_blob,
    ensure_vector_schema,
    insert_vectors as insert_vector_rows,
    load_filtered_vector_item_ids,
    load_vector_matrix,
    upsert_vectors,
    upsert_vector_metadata,
)

_READ_ONLY_KEYWORDS = {
    "select",
    "show",
    "describe",
    "explain",
    "with",
    "pragma",
}

logger = logging.getLogger(__name__)

_VECTOR_RESULT_COLUMNS = ("item_id", "score")


class _TransactionalEngine(Protocol):
    """Protocol for engine objects that support explicit transactions."""

    def begin(self) -> None:
        """Start one explicit transaction on the engine."""

        raise NotImplementedError

    def commit(self) -> None:
        """Commit the current explicit transaction on the engine."""

        raise NotImplementedError

    def rollback(self) -> None:
        """Roll back the current explicit transaction on the engine."""

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
        """Open the embedded engines and initialize lazy runtime state."""

        self.sqlite_path = sqlite_path
        self.duckdb_path = duckdb_path
        self._graph_schema_ready = False
        self._vector_schema_ready = False
        self._vector_matrix_cache: tuple[Any, Any] | None = None
        self._vector_index_cache: dict[VectorMetric, ExactVectorIndex] = {}

        sqlite_path_obj = Path(self.sqlite_path)
        sqlite_path_obj.parent.mkdir(parents=True, exist_ok=True)

        self.sqlite = SQLiteEngine(str(sqlite_path_obj))
        self.duckdb = DuckDBEngine(self.duckdb_path)
        self.duckdb.attach_sqlite(str(sqlite_path_obj))
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
        route: Route,
        query_type: QueryType = "sql",
        params: QueryParameters = None,
    ) -> QueryResult:
        """Execute a query against the explicitly selected engine.

        Args:
            text: Query text to execute. For `query_type="vector"`, this value is
                currently ignored and the request is defined by `params`.
            route: Engine route. This must be `sqlite` or `duckdb`.
            query_type: Logical query type. `sql` maps to `HumemSQL v0` and `cypher`
                maps to `HumemCypher v0`.
            params: Optional DB-API parameters.

        Returns:
            A normalized `QueryResult`.

        Raises:
            NotImplementedError: If an unsupported query type is requested.
            ValueError: If the route is unsupported or a public write is sent to DuckDB.
        """

        if query_type == "vector":
            logger.debug("Routing vector query to exact SQLite/NumPy path")
            return self._execute_vector_query(text, route=route, params=params)

        if query_type == "cypher":
            self._ensure_graph_schema()
            logger.debug("Routing Cypher query to graph path on route=%s", route)
            return execute_cypher(
                text,
                route=route,
                params=params,
                sqlite=self.sqlite,
                duckdb=self.duckdb,
            )

        if query_type != "sql":
            logger.debug("Rejected unsupported query_type=%s", query_type)
            raise NotImplementedError(
                "HumemDB currently supports query_type='sql' for HumemSQL v0, "
                "query_type='cypher' for HumemCypher v0, and "
                "query_type='vector' for exact HumemVector v0 search; "
                f"got {query_type!r}."
            )

        translated_text = translate_sql(text, target=route)

        if route == "sqlite":
            logger.debug("Routing SQL query to SQLite")
            normalized_params, vector_rows, vector_mode = _prepare_sql_vector_write(
                text,
                params,
            )
            result = self.sqlite.execute(
                translated_text,
                normalized_params,
                query_type=query_type,
            )
            if vector_rows:
                self._ensure_vector_schema()
                if vector_mode == "insert":
                    insert_vector_rows(self.sqlite, vector_rows)
                else:
                    upsert_vectors(self.sqlite, vector_rows)
                self._invalidate_vector_cache()
            self._invalidate_vector_cache_for_sql(text, translated_text)
            return result

        if route == "duckdb":
            if not _is_read_only_query(text):
                logger.debug("Rejected direct write routed to DuckDB")
                raise ValueError(
                    "HumemDB does not allow direct writes to DuckDB; "
                    "SQLite is the source of truth."
                )
            logger.debug("Routing read-only SQL query to DuckDB")
            return self.duckdb.execute(
                translated_text,
                params,
                query_type=query_type,
            )

        raise ValueError(f"Unsupported route: {route!r}")

    def insert_vectors(
        self,
        rows: Sequence[tuple[int, Sequence[float]]],
    ) -> int:
        """Insert vector rows into the canonical SQLite store.

        Args:
            rows: Sequence of `(item_id, embedding)` tuples.

        Returns:
            The number of inserted rows.
        """

        if not rows:
            return 0

        self._ensure_vector_schema()
        insert_vector_rows(self.sqlite, rows)
        self._invalidate_vector_cache()
        return len(rows)

    def search_vectors(
        self,
        query: Sequence[float],
        *,
        top_k: int = 10,
        metric: VectorMetric = "cosine",
        filters: Mapping[str, str | int | float | bool | None] | None = None,
    ) -> QueryResult:
        """Search the current SQLite-backed vector set with exact NumPy search."""

        params: dict[str, Any] = {
            "query": query,
            "top_k": top_k,
            "metric": metric,
        }
        if filters is not None:
            params["filters"] = dict(filters)

        return self.query(
            "",
            route="sqlite",
            query_type="vector",
            params=params,
        )

    def set_vector_metadata(
        self,
        rows: Sequence[tuple[int, Mapping[str, str | int | float | bool | None]]],
    ) -> int:
        """Insert or replace narrow equality-filterable vector metadata."""

        if not rows:
            return 0

        self._ensure_vector_schema()
        upsert_vector_metadata(self.sqlite, rows)
        return sum(len(metadata) for _, metadata in rows)

    def preload_vectors(self) -> bool:
        """Eagerly load the current vector set into the in-memory exact-search cache."""

        if not self._has_vector_table():
            return False

        self._load_vector_matrix()
        return True

    def vectors_cached(self) -> bool:
        """Return whether the current vector set is loaded in memory."""

        return self._vector_matrix_cache is not None

    def _ensure_graph_schema(self) -> None:
        """Initialize the SQLite-backed graph tables on first Cypher use."""

        if self._graph_schema_ready:
            return

        logger.debug("Initializing graph schema on first Cypher use")
        ensure_graph_schema(self.sqlite)
        self._graph_schema_ready = True

    def _ensure_vector_schema(self) -> None:
        """Initialize the SQLite-backed vector tables on first vector use."""

        if self._vector_schema_ready:
            return

        logger.debug("Initializing vector schema on first vector use")
        ensure_vector_schema(self.sqlite)
        self._vector_schema_ready = True

    def _execute_vector_query(
        self,
        _text: str,
        *,
        route: Route,
        params: QueryParameters,
    ) -> QueryResult:
        """Execute the exact `HumemVector v0` search path.

        The vector frontend uses a mapping-style `params` object with the following
        keys:

        - `query`: required query embedding
        - `top_k`: optional positive integer, default `10`
        - `metric`: optional `cosine`, `dot`, or `l2`, default `cosine`
        - `filters`: optional equality metadata filters for direct vector search
        - `scope_query_type`: optional `sql` or `cypher` candidate scope query type
        - `scope_route`: optional route for the candidate scope query, default `sqlite`
        - `scope_params`: optional parameters for the candidate scope query
        """

        if route != "sqlite":
            raise ValueError(
                "HumemVector v0 currently runs only on route='sqlite'; "
                "SQLite is the canonical vector store and exact NumPy search path."
            )

        if not isinstance(params, Mapping):
            raise ValueError(
                "HumemVector v0 expects mapping-style params containing at least "
                "a 'query' vector."
            )

        if "query" not in params:
            raise ValueError("HumemVector v0 requires params['query'].")

        top_k = int(params.get("top_k", 10))
        metric = params.get("metric", "cosine")
        if metric not in {"cosine", "dot", "l2"}:
            raise ValueError(
                "HumemVector v0 metric must be one of 'cosine', 'dot', or 'l2'."
            )

        filters = params.get("filters")
        if filters is not None and not isinstance(filters, Mapping):
            raise ValueError(
                "HumemVector v0 expects params['filters'] to be a mapping "
                "when provided."
            )

        scope_query_type = params.get("scope_query_type")
        scope_route = params.get("scope_route", "sqlite")
        scope_params = params.get("scope_params")

        index = self._vector_index_for(metric=metric)
        candidate_indexes = self._candidate_indexes_for_vector_query(
            scope_text=_text,
            scope_query_type=scope_query_type,
            scope_route=scope_route,
            scope_params=scope_params,
            filters=filters,
        )
        if candidate_indexes is not None and len(candidate_indexes) == 0:
            return QueryResult(
                rows=(),
                columns=_VECTOR_RESULT_COLUMNS,
                route="sqlite",
                query_type="vector",
                rowcount=0,
            )

        matches = index.search(
            params["query"],
            top_k=top_k,
            candidate_indexes=candidate_indexes,
        )
        rows = tuple((match.item_id, match.score) for match in matches)
        return QueryResult(
            rows=rows,
            columns=_VECTOR_RESULT_COLUMNS,
            route="sqlite",
            query_type="vector",
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
        cached = load_vector_matrix(self.sqlite)
        self._vector_matrix_cache = cached
        return cached

    def _candidate_indexes_for_vector_query(
        self,
        *,
        scope_text: str,
        scope_query_type: Any,
        scope_route: Any,
        scope_params: QueryParameters,
        filters: Mapping[str, str | int | float | bool | None] | None,
    ) -> Any:
        """Resolve optional SQL/Cypher/vector-metadata scope into matrix indexes."""

        candidate_item_ids: set[int] | None = None

        if filters:
            self._ensure_vector_schema()
            candidate_item_ids = set(
                load_filtered_vector_item_ids(self.sqlite, filters)
            )

        if scope_query_type is not None:
            if scope_query_type not in {"sql", "cypher"}:
                raise ValueError(
                    "HumemVector v0 scope_query_type must be 'sql' or 'cypher'."
                )
            if not scope_text.strip():
                raise ValueError(
                    "HumemVector v0 scoped vector queries require scope SQL "
                    "or Cypher text."
                )
            if scope_route not in {"sqlite", "duckdb"}:
                raise ValueError(
                    "HumemVector v0 scope_route must be 'sqlite' or 'duckdb'."
                )
            if scope_query_type == "sql" and not _is_read_only_query(scope_text):
                raise ValueError(
                    "HumemVector v0 SQL vector scope must be a read-only SQL query."
                )
            normalized_scope_text = scope_text.lstrip().casefold()
            if scope_query_type == "cypher" and not normalized_scope_text.startswith(
                "match "
            ):
                raise ValueError(
                    "HumemVector v0 Cypher vector scope must be a MATCH query."
                )

            scoped_result = self.query(
                scope_text,
                route=scope_route,
                query_type=scope_query_type,
                params=scope_params,
            )
            scoped_item_ids = set(
                self._vector_candidate_item_ids_from_result(scoped_result)
            )
            if candidate_item_ids is None:
                candidate_item_ids = scoped_item_ids
            else:
                candidate_item_ids &= scoped_item_ids

        if candidate_item_ids is None:
            return None

        if not candidate_item_ids:
            return ()

        return self._candidate_indexes_for_item_ids(candidate_item_ids)

    def _candidate_indexes_for_item_ids(self, item_ids: set[int]) -> Any:
        """Map candidate item ids onto cached matrix row indexes."""

        vector_item_ids, _matrix = self._load_vector_matrix()
        return tuple(
            index
            for index, item_id in enumerate(vector_item_ids.tolist())
            if int(item_id) in item_ids
        )

    def _vector_candidate_item_ids_from_result(
        self,
        result: QueryResult,
    ) -> tuple[int, ...]:
        """Extract vector candidate item ids from a SQL or Cypher scope result."""

        if not result.columns:
            raise ValueError(
                "HumemVector v0 candidate scope queries must return at least "
                "one column."
            )

        lowered_columns = tuple(column.casefold() for column in result.columns)
        if "item_id" in lowered_columns:
            id_index = lowered_columns.index("item_id")
        elif "id" in lowered_columns:
            id_index = lowered_columns.index("id")
        else:
            matching_indexes = [
                index
                for index, column in enumerate(lowered_columns)
                if column.endswith(".id")
            ]
            if len(matching_indexes) == 1:
                id_index = matching_indexes[0]
            elif len(result.columns) == 1:
                id_index = 0
            else:
                raise ValueError(
                    "HumemVector v0 candidate scope queries must return one id column "
                    "named 'item_id', 'id', or '*.id'."
                )

        item_ids: list[int] = []
        for row in result.rows:
            item_ids.append(int(row[id_index]))
        return tuple(item_ids)

    def _has_vector_table(self) -> bool:
        """Return whether the current SQLite database already has vector storage."""

        result = self.sqlite.execute(
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
        self._vector_index_cache.clear()

    def _invalidate_vector_cache_for_sql(
        self,
        original_text: str,
        translated_text: str,
    ) -> None:
        """Drop cached exact indexes after raw SQL writes that touch vector tables."""

        if _is_read_only_query(original_text):
            return
        if not _touches_vector_entries(translated_text):
            return

        logger.debug("Invalidating all vector cache entries after SQL write")
        self._invalidate_vector_cache()

    def begin(self, *, route: Route) -> None:
        """Begin an explicit transaction on the selected route."""

        logger.debug("Beginning transaction on route=%s", route)
        self._engine_for_route(route).begin()

    def commit(self, *, route: Route) -> None:
        """Commit the active transaction on the selected route."""

        logger.debug("Committing transaction on route=%s", route)
        self._engine_for_route(route).commit()

    def rollback(self, *, route: Route) -> None:
        """Roll back the active transaction on the selected route."""

        logger.debug("Rolling back transaction on route=%s", route)
        self._engine_for_route(route).rollback()

    def transaction(self, *, route: Route) -> _TransactionContext:
        """Return a transaction context manager for the selected route.

        A successful context commits on exit. An exception inside the context triggers a
        rollback before the exception continues to propagate.
        """

        return _TransactionContext(self, route)

    def executemany(
        self,
        text: str,
        params_seq: BatchParameters,
        *,
        route: Route,
        query_type: QueryType = "sql",
    ) -> QueryResult:
        """Execute the same statement repeatedly for a batch of parameters.

        This method is intentionally limited to SQLite so HumemDB can support simple
        transactional batch writes without introducing a full ingestion framework.

        Args:
            text: SQL statement to execute repeatedly.
            params_seq: Sequence of DB-API parameter sets.
            route: Execution route. Must be `sqlite`.
            query_type: Logical query type. Batch execution currently supports only
                `sql` / `HumemSQL v0`.

        Returns:
            A normalized `QueryResult`.

        Raises:
            NotImplementedError: If a non-SQL query type is requested.
            ValueError: If the route is unsupported or batch writes are directed
                to DuckDB.
        """

        if query_type != "sql":
            logger.debug("Rejected unsupported batch query_type=%s", query_type)
            raise NotImplementedError(
                "HumemDB batch execution currently supports only query_type='sql' "
                f"for HumemSQL v0; got {query_type!r}."
            )

        translated_text = translate_sql(text, target=route)

        if route == "sqlite":
            logger.debug("Routing batched SQL query to SQLite")
            normalized_params_seq, vector_rows = _prepare_sql_vector_write_batch(
                text,
                params_seq,
            )
            result = self.sqlite.executemany(
                translated_text,
                normalized_params_seq,
                query_type=query_type,
            )
            if vector_rows:
                self._ensure_vector_schema()
                insert_vector_rows(self.sqlite, vector_rows)
                self._invalidate_vector_cache()
            self._invalidate_vector_cache_for_sql(text, translated_text)
            return result

        if route == "duckdb":
            logger.debug("Rejected batched write routed to DuckDB")
            raise ValueError(
                "HumemDB does not allow direct batch writes to DuckDB; "
                "SQLite is the source of truth."
            )

        raise ValueError(f"Unsupported route: {route!r}")

    def close(self) -> None:
        """Close both embedded database connections."""

        logger.debug("Closing HumemDB connections")
        self.sqlite.close()
        self.duckdb.close()

    def __enter__(self) -> HumemDB:
        """Return `self` for context-manager usage."""

        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """Close connections when leaving a `with HumemDB(...)` block."""

        self.close()

    def _engine_for_route(self, route: Route) -> _TransactionalEngine:
        """Resolve a route string into its backing engine object."""

        if route == "sqlite":
            return self.sqlite

        if route == "duckdb":
            return self.duckdb

        raise ValueError(f"Unsupported route: {route!r}")


class _TransactionContext:
    """Route-scoped transaction context manager used by `HumemDB`.

    This helper keeps the public transaction API ergonomic while making the
    commit-or-rollback behavior explicit and testable.
    """

    def __init__(self, db: HumemDB, route: Route) -> None:
        """Bind the context manager to one database instance and route."""

        self.db = db
        self.route: Route = route

    def __enter__(self) -> HumemDB:
        """Begin the transaction and return the owning `HumemDB` instance."""

        self.db.begin(route=self.route)
        return self.db

    def __exit__(self, exc_type, exc, tb) -> None:
        """Commit on success or roll back when an exception occurs."""

        if exc_type is None:
            self.db.commit(route=self.route)
            return

        self.db.rollback(route=self.route)


def _is_read_only_query(text: str) -> bool:
    """Return whether a SQL statement is treated as read-only.

    This is intentionally lightweight and only supports the small amount of policy
    enforcement needed to keep DuckDB read-only through the public API.
    """

    stripped = text.lstrip()
    if not stripped:
        return False

    keyword = stripped.split(None, 1)[0].lower()
    return keyword in _READ_ONLY_KEYWORDS


def _touches_vector_entries(text: str) -> bool:
    """Return whether a SQL statement references the canonical vector table."""

    return "vector_entries" in text.casefold()


def _prepare_sql_vector_write(
    text: str,
    params: QueryParameters,
) -> tuple[QueryParameters, list[tuple[int, Sequence[float]]], str | None]:
    """Convert vector-bearing SQL INSERT params into SQLite blobs plus vector rows."""

    if params is None or isinstance(params, Mapping):
        return params, [], None

    analysis = _analyze_sql_vector_insert(text)
    if analysis is not None:
        normalized_params, vector_row = _normalize_sql_vector_row(
            params,
            id_index=analysis["id_index"],
            vector_index=analysis["vector_index"],
        )
        return normalized_params, [vector_row], "insert"

    update_analysis = _analyze_sql_vector_update(text)
    if update_analysis is not None:
        vector_index = update_analysis["vector_index"]
        id_index = update_analysis["id_index"]
        if vector_index is None or id_index is None:
            raise ValueError(
                "HumemVector v0 SQL updates require both an embedding target and "
                "an id predicate."
            )
        normalized_params, vector_row = _normalize_sql_vector_update(
            params,
            vector_index=vector_index,
            id_index=id_index,
            id_literal=update_analysis["id_literal"],
        )
        return normalized_params, [vector_row], "upsert"

    return params, [], None


def _prepare_sql_vector_write_batch(
    text: str,
    params_seq: BatchParameters,
) -> tuple[BatchParameters, list[tuple[int, Sequence[float]]]]:
    """Convert batched vector-bearing SQL INSERT params into blobs plus vector rows."""

    analysis = _analyze_sql_vector_insert(text)
    if analysis is None:
        return params_seq, []

    normalized_params_seq: list[Sequence[Any]] = []
    vector_rows: list[tuple[int, Sequence[float]]] = []
    for params in params_seq:
        if isinstance(params, Mapping):
            raise ValueError(
                "HumemVector v0 SQL vector inserts currently require positional params."
            )
        normalized_params, vector_row = _normalize_sql_vector_row(
            params,
            id_index=analysis["id_index"],
            vector_index=analysis["vector_index"],
        )
        normalized_params_seq.append(normalized_params)
        vector_rows.append(vector_row)

    return normalized_params_seq, vector_rows


def _analyze_sql_vector_insert(text: str) -> dict[str, int] | None:
    """Return id/vector param indexes for narrow vector-bearing SQL INSERTs."""

    try:
        expression = parse_one(text, read="postgres")
    except sqlglot_errors.ParseError:
        return None

    if type(expression).__name__ != "Insert":
        return None

    target = getattr(getattr(expression, "this", None), "this", None)
    target_name = getattr(target, "name", None) or str(target or "")
    if target_name.casefold() == "vector_entries":
        return None

    columns = [
        getattr(column, "name", None)
        or getattr(getattr(column, "this", None), "name", None)
        for column in expression.this.expressions
    ]
    normalized_columns = [str(column).casefold() for column in columns]
    vector_indexes = [
        index
        for index, column in enumerate(normalized_columns)
        if column == "embedding"
    ]
    if not vector_indexes:
        return None
    if len(vector_indexes) > 1:
        raise ValueError(
            "HumemVector v0 SQL inserts support only one embedding column."
        )

    if "id" in normalized_columns:
        id_index = normalized_columns.index("id")
    elif "item_id" in normalized_columns:
        id_index = normalized_columns.index("item_id")
    else:
        raise ValueError(
            "HumemVector v0 SQL inserts require an 'id' or 'item_id' column."
        )

    values = expression.args.get("expression")
    row_count = len(getattr(values, "expressions", [])) if values is not None else 0
    if row_count > 1:
        raise ValueError(
            "HumemVector v0 SQL vector inserts support one VALUES row per statement; "
            "use executemany for batches."
        )

    return {"id_index": id_index, "vector_index": vector_indexes[0]}


def _analyze_sql_vector_update(text: str) -> dict[str, int | None] | None:
    """Return id/vector param indexes for narrow vector-bearing SQL UPDATEs."""

    try:
        expression = parse_one(text, read="postgres")
    except sqlglot_errors.ParseError:
        return None

    if type(expression).__name__ != "Update":
        return None

    target = getattr(expression, "this", None)
    target_name = getattr(target, "name", None) or str(target or "")
    if target_name.casefold() == "vector_entries":
        return None

    vector_indexes = []
    param_position = 0
    for assignment in expression.expressions:
        column_name = getattr(getattr(assignment, "this", None), "name", None)
        rhs = getattr(assignment, "expression", None)
        rhs_args = getattr(rhs, "args", {}) if rhs is not None else {}
        if type(rhs).__name__ == "Placeholder" or rhs_args.get("jdbc"):
            current_param = param_position
            param_position += 1
        else:
            current_param = None

        if str(column_name).casefold() == "embedding":
            if current_param is None:
                raise ValueError(
                    "HumemVector v0 SQL updates currently require embedding = ? "
                    "with positional params."
                )
            vector_indexes.append(current_param)

    if not vector_indexes:
        return None
    if len(vector_indexes) > 1:
        raise ValueError(
            "HumemVector v0 SQL updates support only one embedding assignment."
        )

    where = expression.args.get("where")
    if where is None or getattr(where, "this", None) is None:
        raise ValueError(
            "HumemVector v0 SQL vector updates currently require WHERE id = ... "
            "or WHERE item_id = ...."
        )

    predicate = where.this
    if type(predicate).__name__ != "EQ":
        raise ValueError(
            "HumemVector v0 SQL vector updates currently require a simple id "
            "equality predicate."
        )

    left = getattr(predicate, "this", None)
    id_name = getattr(left, "name", None)
    if str(id_name).casefold() not in {"id", "item_id"}:
        raise ValueError(
            "HumemVector v0 SQL vector updates currently require WHERE id = ... "
            "or WHERE item_id = ...."
        )

    right = getattr(predicate, "expression", None)
    right_args = getattr(right, "args", {}) if right is not None else {}
    if type(right).__name__ == "Placeholder" or right_args.get("jdbc"):
        id_index = param_position
        id_literal = None
    else:
        id_index = None
        id_literal = int(str(getattr(right, "this", right)))

    return {
        "vector_index": vector_indexes[0],
        "id_index": id_index,
        "id_literal": id_literal,
    }


def _normalize_sql_vector_row(
    params: Sequence[Any],
    *,
    id_index: int,
    vector_index: int,
) -> tuple[tuple[Any, ...], tuple[int, Sequence[float]]]:
    """Encode one SQL row's embedding param and extract its canonical vector row."""

    bound = list(params)
    if max(id_index, vector_index) >= len(bound):
        raise ValueError(
            "HumemVector v0 SQL vector inserts did not receive enough "
            "positional params."
        )

    vector_value = _coerce_vector_param(bound[vector_index], context="SQL")
    row_id = int(bound[id_index])
    bound[vector_index] = encode_vector_blob(vector_value)
    return tuple(bound), (row_id, vector_value)


def _normalize_sql_vector_update(
    params: Sequence[Any],
    *,
    vector_index: int,
    id_index: int | None,
    id_literal: int | None,
) -> tuple[tuple[Any, ...], tuple[int, Sequence[float]]]:
    """Encode one SQL UPDATE embedding param and extract its canonical vector row."""

    bound = list(params)
    if vector_index >= len(bound):
        raise ValueError(
            "HumemVector v0 SQL vector updates did not receive enough positional "
            "params."
        )

    vector_value = _coerce_vector_param(bound[vector_index], context="SQL")
    if id_index is not None:
        if id_index >= len(bound):
            raise ValueError(
                "HumemVector v0 SQL vector updates did not receive enough positional "
                "params."
            )
        row_id = int(bound[id_index])
    elif id_literal is not None:
        row_id = int(id_literal)
    else:
        raise ValueError("HumemVector v0 SQL vector updates could not resolve an id.")

    bound[vector_index] = encode_vector_blob(vector_value)
    return tuple(bound), (row_id, vector_value)


def _coerce_vector_param(value: Any, *, context: str) -> Sequence[float]:
    """Validate one vector-bearing SQL or Cypher parameter value."""

    if isinstance(value, (str, bytes, bytearray, memoryview)):
        raise ValueError(
            f"HumemVector v0 {context} vector values must be sequences of numbers."
        )
    if not isinstance(value, Sequence):
        raise ValueError(
            f"HumemVector v0 {context} vector values must be sequences of numbers."
        )
    if not value:
        raise ValueError(f"HumemVector v0 {context} vector values cannot be empty.")

    normalized = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ValueError(
                f"HumemVector v0 {context} vector values must contain only numbers."
            )
        normalized.append(float(item))
    return tuple(normalized)
