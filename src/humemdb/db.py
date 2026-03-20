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

from .cypher import ensure_graph_schema, execute_cypher
from .engines import DuckDBEngine, SQLiteEngine
from .sql import translate_sql
from .types import BatchParameters, QueryParameters, QueryResult, QueryType, Route
from .vector import (
    ExactVectorIndex,
    VectorMetric,
    ensure_vector_schema,
    insert_vectors as insert_vector_rows,
    load_vector_matrix,
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

    def begin(self) -> None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...


class HumemDB:
    """Main in-process entry point for HumemDB.

    Args:
        sqlite_path: Path to the canonical SQLite database.
        duckdb_path: Optional path to a DuckDB database file. If omitted, DuckDB uses
            an in-memory database.
        preload_vector_collections: Optional eager vector preload policy. Use `False`
            to keep vector collections lazy-loaded, `True` to preload all existing
            collections on open, or a sequence of collection names to preload a specific
            subset.

    Notes:
        Instantiating `HumemDB` opens both embedded database connections. Use the object
        as a context manager or call `close()` explicitly to release them.
    """

    def __init__(
        self,
        sqlite_path: str,
        duckdb_path: str | None = None,
        *,
        preload_vector_collections: bool | Sequence[str] = False,
    ) -> None:
        self.sqlite_path = sqlite_path
        self.duckdb_path = duckdb_path
        self._graph_schema_ready = False
        self._vector_schema_ready = False
        self._vector_collection_cache: dict[str, tuple[Any, Any, Any]] = {}
        self._vector_index_cache: dict[
            tuple[str, VectorMetric], tuple[Any, ExactVectorIndex]
        ] = {}

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
        if preload_vector_collections:
            self.preload_vector_collections(preload_vector_collections)

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
            text: Query text to execute.
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
            result = self.sqlite.execute(
                translated_text,
                params,
                query_type=query_type,
            )
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
        rows: Sequence[tuple[int, str, int, Sequence[float]]],
    ) -> int:
        """Insert vector rows into the canonical SQLite store.

        Args:
            rows: Sequence of `(item_id, collection, bucket, embedding)` tuples.

        Returns:
            The number of inserted rows.
        """

        if not rows:
            return 0

        self._ensure_vector_schema()
        insert_vector_rows(self.sqlite, rows)
        self._invalidate_vector_cache(collection for _, collection, _, _ in rows)
        return len(rows)

    def search_vectors(
        self,
        collection: str,
        query: Sequence[float],
        *,
        top_k: int = 10,
        metric: VectorMetric = "cosine",
        bucket: int | Sequence[int] | None = None,
    ) -> QueryResult:
        """Search one SQLite-backed vector collection with exact NumPy search."""

        params: dict[str, Any] = {
            "query": query,
            "top_k": top_k,
            "metric": metric,
        }
        if bucket is not None:
            params["bucket"] = bucket

        return self.query(
            collection,
            route="sqlite",
            query_type="vector",
            params=params,
        )

    def preload_vector_collections(
        self,
        collections: bool | Sequence[str] = True,
    ) -> tuple[str, ...]:
        """Eagerly load vector collections into the in-memory exact-search cache."""

        if collections is False:
            return ()

        if collections is True:
            collection_names = self._list_vector_collections()
        else:
            collection_names = tuple(collections)

        loaded: list[str] = []
        for collection in collection_names:
            self._load_vector_collection(collection)
            loaded.append(collection)
        return tuple(loaded)

    def cached_vector_collections(self) -> tuple[str, ...]:
        """Return the collection names currently loaded in the vector cache."""

        return tuple(sorted(self._vector_collection_cache))

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
        text: str,
        *,
        route: Route,
        params: QueryParameters,
    ) -> QueryResult:
        """Execute the exact `HumemVector v0` search path.

        The vector frontend uses the `text` argument as the collection name and a
        mapping-style `params` object with the following keys:

        - `query`: required query embedding
        - `top_k`: optional positive integer, default `10`
        - `metric`: optional `cosine`, `dot`, or `l2`, default `cosine`
        - `bucket`: optional integer or sequence of integers to restrict candidates
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

        collection = text.strip()
        if not collection:
            raise ValueError("HumemVector v0 collection names cannot be empty.")

        if "query" not in params:
            raise ValueError("HumemVector v0 requires params['query'].")

        top_k = int(params.get("top_k", 10))
        metric = params.get("metric", "cosine")
        if metric not in {"cosine", "dot", "l2"}:
            raise ValueError(
                "HumemVector v0 metric must be one of 'cosine', 'dot', or 'l2'."
            )

        index_buckets, index = self._vector_index_for(
            collection=collection,
            metric=metric,
        )
        candidate_indexes = _candidate_indexes_for_bucket_filter(
            index_buckets,
            params.get("bucket"),
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
        collection: str,
        metric: VectorMetric,
    ) -> tuple[Any, ExactVectorIndex]:
        """Load and cache one exact vector index per collection and metric."""

        cache_key = (collection, metric)
        cached = self._vector_index_cache.get(cache_key)
        if cached is not None:
            return cached

        item_ids, buckets, matrix = self._load_vector_collection(collection)
        cached = (
            buckets,
            ExactVectorIndex(item_ids=item_ids, matrix=matrix, metric=metric),
        )
        self._vector_index_cache[cache_key] = cached
        return cached

    def _load_vector_collection(self, collection: str) -> tuple[Any, Any, Any]:
        """Load and cache one vector collection from SQLite."""

        cached = self._vector_collection_cache.get(collection)
        if cached is not None:
            return cached

        self._ensure_vector_schema()
        cached = load_vector_matrix(self.sqlite, collection=collection)
        self._vector_collection_cache[collection] = cached
        return cached

    def _list_vector_collections(self) -> tuple[str, ...]:
        """Return the existing vector collection names without creating schema."""

        if not self._has_vector_table():
            return ()

        result = self.sqlite.execute(
            (
                "SELECT DISTINCT collection "
                "FROM vector_entries "
                "ORDER BY collection"
            )
        )
        return tuple(str(row[0]) for row in result.rows)

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

    def _invalidate_vector_cache(self, collections: Sequence[str]) -> None:
        """Drop cached exact indexes for any collection that changed."""

        collection_names = set(collections)
        if not collection_names:
            return

        stale_keys = [
            cache_key
            for cache_key in self._vector_index_cache
            if cache_key[0] in collection_names
        ]
        for collection in collection_names:
            self._vector_collection_cache.pop(collection, None)
        for cache_key in stale_keys:
            del self._vector_index_cache[cache_key]

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
        self._vector_collection_cache.clear()
        self._vector_index_cache.clear()

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
            result = self.sqlite.executemany(
                translated_text,
                params_seq,
                query_type=query_type,
            )
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


def _candidate_indexes_for_bucket_filter(
    buckets: Sequence[int],
    bucket_filter: int | Sequence[int] | None,
) -> list[int] | None:
    """Resolve an optional bucket filter into row indexes for vector search."""

    if bucket_filter is None:
        return None

    if isinstance(bucket_filter, Sequence) and not isinstance(
        bucket_filter,
        (str, bytes),
    ):
        allowed = {int(bucket) for bucket in bucket_filter}
    else:
        allowed = {int(bucket_filter)}

    return [index for index, bucket in enumerate(buckets) if int(bucket) in allowed]


def _touches_vector_entries(text: str) -> bool:
    """Return whether a SQL statement references the canonical vector table."""

    return "vector_entries" in text.casefold()
