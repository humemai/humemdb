"""Low-level embedded engine wrappers for SQLite and DuckDB.

The classes in this module are intentionally small. They provide a thin runtime boundary
around the Python database bindings while normalizing results into the `QueryResult`
object used by the rest of HumemDB.

Two design decisions are important in Phase 1:

- both engines expose a similar execute/begin/commit/rollback/close surface
- the public `HumemDB` API, not this module, enforces architecture rules such as DuckDB
    being read-only for canonical application writes

That split keeps these wrappers useful for internal implementation details without
overloading them with product-level routing policy.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence, TypeAlias

import duckdb

from .types import BatchParameters, QueryParameters, QueryResult, QueryType

_AUTO_COMMIT_KEYWORDS = {
    "alter",
    "attach",
    "create",
    "delete",
    "detach",
    "drop",
    "insert",
    "replace",
    "update",
}

logger = logging.getLogger(__name__)

_BoundParameters: TypeAlias = Mapping[str, Any] | Sequence[Any]


@dataclass(slots=True)
class SQLiteEngine:
    """Thin wrapper around an embedded `sqlite3` connection.

    This wrapper is responsible for:

    - opening the SQLite database file
    - executing SQL with DB-API style parameters
    - auto-committing write statements when no explicit transaction is active
    - exposing a small transaction lifecycle surface to `HumemDB`
    """

    path: str
    connection: sqlite3.Connection = field(init=False)
    _in_transaction: bool = field(init=False, default=False)

    def __post_init__(self) -> None:
        """Open the SQLite connection for the configured database path."""

        self.connection = sqlite3.connect(self.path)
        logger.debug("Opened SQLite connection path=%s", self.path)

    def execute(
        self,
        text: str,
        params: QueryParameters = None,
        *,
        query_type: QueryType = "sql",
    ) -> QueryResult:
        """Execute SQL on SQLite and return a normalized result.

        Args:
            text: SQL text to execute.
            params: Optional DB-API parameters.
            query_type: Logical query type label carried into the result.

        Returns:
            A normalized `QueryResult`.
        """

        cursor = self.connection.execute(text, _bound_params(params))
        if not self._in_transaction and _should_auto_commit(text):
            self.connection.commit()
        rows, columns = _collect_rows(cursor)
        return QueryResult(
            rows=rows,
            columns=columns,
            route="sqlite",
            query_type=query_type,
            rowcount=cursor.rowcount,
        )

    def executemany(
        self,
        text: str,
        params_seq: BatchParameters,
        *,
        query_type: QueryType = "sql",
    ) -> QueryResult:
        """Execute the same SQL statement for multiple parameter sets.

        This is intended for small to moderate SQLite batch writes in the early HumemDB
        implementation. Large ingestion strategies are intentionally deferred to later
        phases.

        Args:
            text: SQL statement to execute repeatedly.
            params_seq: Sequence of DB-API parameter sets.
            query_type: Logical query type label carried into the result.

        Returns:
            A normalized `QueryResult`.
        """

        bound_params = [_bound_params(params) for params in params_seq]
        cursor = self.connection.executemany(text, bound_params)
        if not self._in_transaction and _should_auto_commit(text):
            self.connection.commit()
        rows, columns = _collect_rows(cursor)
        return QueryResult(
            rows=rows,
            columns=columns,
            route="sqlite",
            query_type=query_type,
            rowcount=cursor.rowcount,
        )

    def begin(self) -> None:
        """Begin an explicit SQLite transaction.

        Raises:
            RuntimeError: If a transaction is already active on this engine.
        """

        if self._in_transaction:
            raise RuntimeError("SQLite transaction already active.")

        self.connection.execute("BEGIN")
        self._in_transaction = True
        logger.debug("SQLite transaction started")

    def commit(self) -> None:
        """Commit the current SQLite transaction."""

        self.connection.commit()
        self._in_transaction = False
        logger.debug("SQLite transaction committed")

    def rollback(self) -> None:
        """Roll back the current SQLite transaction."""

        self.connection.rollback()
        self._in_transaction = False
        logger.debug("SQLite transaction rolled back")

    def close(self) -> None:
        """Close the SQLite connection."""

        self.connection.close()
        logger.debug("Closed SQLite connection")


@dataclass(slots=True)
class DuckDBEngine:
    """Thin wrapper around an embedded DuckDB connection.

    This engine provides the same lifecycle surface as `SQLiteEngine` so the
    higher-level `HumemDB` object can manage both engines consistently.
    """

    path: str | None = None
    connection: duckdb.DuckDBPyConnection = field(init=False)
    _in_transaction: bool = field(init=False, default=False)
    _sqlite_alias: str = field(init=False, default="sqlite_db")

    def __post_init__(self) -> None:
        """Open the DuckDB connection for a file path or in-memory database."""

        database = self.path or ":memory:"
        self.connection = duckdb.connect(database=database)
        logger.debug("Opened DuckDB connection path=%s", database)

    def attach_sqlite(self, path: str) -> None:
        """Attach a SQLite database so DuckDB can read it directly.

        The attached SQLite database is placed first in DuckDB's search path so
        unqualified read queries resolve to SQLite tables before falling back to
        DuckDB's local `main` schema.
        """

        self.connection.execute("INSTALL sqlite")
        self.connection.execute("LOAD sqlite")
        self.connection.execute(
            f"ATTACH '{path}' AS {self._sqlite_alias} (TYPE sqlite)"
        )
        self.connection.execute(
            f"SET search_path='{self._sqlite_alias},main'"
        )
        logger.debug(
            "Attached SQLite database path=%s into DuckDB alias=%s",
            path,
            self._sqlite_alias,
        )

    def execute(
        self,
        text: str,
        params: QueryParameters = None,
        *,
        query_type: QueryType = "sql",
    ) -> QueryResult:
        """Execute SQL on DuckDB and return a normalized result.

        Args:
            text: SQL text to execute.
            params: Optional DB-API parameters.
            query_type: Logical query type label carried into the result.

        Returns:
            A normalized `QueryResult`.
        """

        cursor = self.connection.execute(text, _bound_params(params))
        if not self._in_transaction and _should_auto_commit(text):
            self.connection.commit()
        rows, columns = _collect_rows(cursor)
        return QueryResult(
            rows=rows,
            columns=columns,
            route="duckdb",
            query_type=query_type,
            rowcount=cursor.rowcount,
        )

    def begin(self) -> None:
        """Begin an explicit DuckDB transaction.

        Raises:
            RuntimeError: If a transaction is already active on this engine.
        """

        if self._in_transaction:
            raise RuntimeError("DuckDB transaction already active.")

        self.connection.execute("BEGIN")
        self._in_transaction = True
        logger.debug("DuckDB transaction started")

    def commit(self) -> None:
        """Commit the current DuckDB transaction."""

        self.connection.commit()
        self._in_transaction = False
        logger.debug("DuckDB transaction committed")

    def rollback(self) -> None:
        """Roll back the current DuckDB transaction."""

        self.connection.rollback()
        self._in_transaction = False
        logger.debug("DuckDB transaction rolled back")

    def close(self) -> None:
        """Close the DuckDB connection."""

        self.connection.close()
        logger.debug("Closed DuckDB connection")


def _collect_rows(
    cursor: sqlite3.Cursor | duckdb.DuckDBPyConnection,
) -> tuple[tuple[tuple[object, ...], ...], tuple[str, ...]]:
    """Materialize rows and column names from a driver cursor.

    Args:
        cursor: A SQLite or DuckDB cursor-like object after execution.

    Returns:
        A pair of `(rows, columns)` where rows are fully materialized tuples.
    """

    description = cursor.description
    if description is None:
        return (), ()

    rows = tuple(tuple(row) for row in cursor.fetchall())
    columns = tuple(str(column[0]) for column in description)
    return rows, columns


def _bound_params(params: QueryParameters) -> _BoundParameters:
    """Normalize optional query parameters into a DB-API compatible form."""

    if params is None:
        return ()
    return params


def _should_auto_commit(text: str) -> bool:
    """Return whether the SQL statement should auto-commit outside transactions."""

    return _statement_keyword(text) in _AUTO_COMMIT_KEYWORDS


def _statement_keyword(text: str) -> str:
    """Extract the first SQL keyword from a statement.

    This helper is intentionally lightweight and only supports the small amount of
    statement classification needed in Phase 1.
    """

    stripped = text.lstrip()
    if not stripped:
        return ""

    return stripped.split(None, 1)[0].lower()
