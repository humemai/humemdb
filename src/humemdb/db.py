"""High-level embedded database interface for HumemDB.

The `HumemDB` class is the public entry point for the Phase 1 runtime. It owns the
SQLite and DuckDB engine wrappers, exposes an explicit routing API, and defines the
current lifecycle semantics for queries and transactions.

The current contract is intentionally conservative:

- only `query_type="sql"` is implemented
- SQLite is the canonical public write target
- DuckDB is read-only through the public API
- transaction control is explicit and route-scoped

As the project grows, this module is where routing, query validation, and the portable
HumemDB SQL layer will expand.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Protocol

from .engines import DuckDBEngine, SQLiteEngine
from .types import BatchParameters, QueryParameters, QueryResult, QueryType, Route

_READ_ONLY_KEYWORDS = {
    "select",
    "show",
    "describe",
    "explain",
    "with",
    "pragma",
}

logger = logging.getLogger(__name__)


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

    Notes:
        Instantiating `HumemDB` opens both embedded database connections. Use the object
        as a context manager or call `close()` explicitly to release them.
    """

    def __init__(self, sqlite_path: str, duckdb_path: str | None = None) -> None:
        self.sqlite_path = sqlite_path
        self.duckdb_path = duckdb_path

        sqlite_path_obj = Path(self.sqlite_path)
        sqlite_path_obj.parent.mkdir(parents=True, exist_ok=True)

        self.sqlite = SQLiteEngine(str(sqlite_path_obj))
        self.duckdb = DuckDBEngine(self.duckdb_path)
        logger.debug(
            "HumemDB initialized with sqlite_path=%s duckdb_path=%s",
            self.sqlite_path,
            self.duckdb_path,
        )

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
            route: Engine route. In Phase 1 this must be `sqlite` or `duckdb`.
            query_type: Logical query type. Only `sql` is implemented in Phase 1.
            params: Optional DB-API parameters.

        Returns:
            A normalized `QueryResult`.

        Raises:
            NotImplementedError: If a non-SQL query type is requested.
            ValueError: If the route is unsupported or a public write is sent to DuckDB.
        """

        if query_type != "sql":
            logger.debug("Rejected unsupported query_type=%s", query_type)
            raise NotImplementedError(
                f"Phase 1 only supports query_type='sql'; got {query_type!r}."
            )

        if route == "sqlite":
            logger.debug("Routing SQL query to SQLite")
            return self.sqlite.execute(text, params, query_type=query_type)

        if route == "duckdb":
            if not _is_read_only_query(text):
                logger.debug("Rejected direct write routed to DuckDB")
                raise ValueError(
                    "HumemDB does not allow direct writes to DuckDB; "
                    "SQLite is the source of truth."
                )
            logger.debug("Routing read-only SQL query to DuckDB")
            return self.duckdb.execute(text, params, query_type=query_type)

        raise ValueError(f"Unsupported route: {route!r}")

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

        In Phase 1, this method is intentionally limited to SQLite so HumemDB can
        support simple transactional batch writes without introducing a full ingestion
        framework.

        Args:
            text: SQL statement to execute repeatedly.
            params_seq: Sequence of DB-API parameter sets.
            route: Execution route. Must be `sqlite` in Phase 1.
            query_type: Logical query type. Only `sql` is implemented.

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
                f"Phase 1 only supports query_type='sql'; got {query_type!r}."
            )

        if route == "sqlite":
            logger.debug("Routing batched SQL query to SQLite")
            return self.sqlite.executemany(text, params_seq, query_type=query_type)

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
    """Return whether a SQL statement is treated as read-only in Phase 1.

    This is intentionally lightweight and only supports the small amount of policy
    enforcement needed to keep DuckDB read-only through the public API.
    """

    stripped = text.lstrip()
    if not stripped:
        return False

    keyword = stripped.split(None, 1)[0].lower()
    return keyword in _READ_ONLY_KEYWORDS
