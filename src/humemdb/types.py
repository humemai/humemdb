"""Shared public types for the HumemDB runtime.

This module intentionally stays small. It defines the public result object returned by
queries and the core type aliases used across the package.

The current API surface is deliberately conservative:

- routes are explicit and limited to SQLite or DuckDB
- query types are explicit; `sql`, `cypher`, and exact `vector` search are implemented
- public query params use mapping-style named bindings across SQL, Cypher, and the
    vector frontend

As HumemDB grows, this module is the natural place for additional request, result, and
configuration data objects that should remain lightweight and stable across the rest of
the package.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping, Sequence, TypeAlias

Route: TypeAlias = Literal["sqlite", "duckdb"]
QueryType: TypeAlias = Literal["sql", "cypher", "vector"]
QueryParameters: TypeAlias = Mapping[str, Any] | Sequence[Any] | None
BatchParameters: TypeAlias = Sequence[Mapping[str, Any] | Sequence[Any]]


@dataclass(slots=True, frozen=True)
class QueryResult:
    """Normalized result returned by `HumemDB.query`.

    The goal of `QueryResult` is to hide driver-level result differences and give the
    rest of the library a stable, predictable shape.

    Attributes:
        rows: Fully materialized query rows as tuples.
        columns: Column names in result order.
        route: The engine that executed the query.
        query_type: The logical query type requested by the caller.
        rowcount: The driver-reported affected row count for write statements, or the
            driver-specific value for read statements.
    """

    rows: tuple[tuple[Any, ...], ...]
    columns: tuple[str, ...]
    route: Route
    query_type: QueryType
    rowcount: int

    def first(self) -> tuple[Any, ...] | None:
        """Return the first row in the result set, if one exists.

        Returns:
            The first row as a tuple, or `None` when the result set is empty.
        """

        if not self.rows:
            return None
        return self.rows[0]
