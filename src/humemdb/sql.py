"""SQL parsing and translation helpers for HumemDB.

Phase 2 introduces a PostgreSQL-like public SQL surface for HumemDB. This module keeps
the translation boundary isolated from the runtime so later work can add validation,
rewrites, and eventually a thin plan layer without pushing parser details into the rest
of the package.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from sqlglot import parse_one
from sqlglot import errors as sqlglot_errors

from .types import Route

_SUPPORTED_STATEMENT_NAMES = {
    "Select",
    "Insert",
    "Update",
    "Delete",
    "Create",
}


def translate_sql(text: str, *, target: Route) -> str:
    """Translate PostgreSQL-like SQL into backend-specific SQL.

    Args:
        text: User-facing HumemSQL text.
        target: Backend route whose SQL dialect should be emitted.

    Returns:
        SQL text ready for the selected backend.

    Raises:
        ValueError: If the SQL cannot be parsed as PostgreSQL-like input.
    """

    try:
        return _translate_sql_cached(text, target)
    except sqlglot_errors.ParseError as exc:
        raise ValueError(
            "HumemDB could not parse the SQL as PostgreSQL-like HumemSQL."
        ) from exc


@lru_cache(maxsize=512)
def _translate_sql_cached(text: str, target: Route) -> str:
    """Cache SQL translations for repeated query shapes."""

    expression = parse_one(text, read="postgres")
    _validate_humemsql_v0(expression)
    return expression.sql(dialect=target)


def _validate_humemsql_v0(expression: Any) -> None:
    """Validate the initial HumemSQL v0 statement subset.

    Phase 2 intentionally supports a small PostgreSQL-like subset rather than
    pretending to accept arbitrary PostgreSQL syntax.
    """

    statement_name = type(expression).__name__
    if statement_name not in _SUPPORTED_STATEMENT_NAMES:
        raise ValueError(
            "HumemDB HumemSQL v0 only supports SELECT, INSERT, UPDATE, DELETE, "
            "and CREATE statements."
        )

    expression_args = getattr(expression, "args", {})
    with_clause = expression_args.get("with_")
    with_args = getattr(with_clause, "args", {}) if with_clause is not None else {}
    if with_args.get("recursive"):
        raise ValueError(
            "HumemDB HumemSQL v0 does not support recursive CTEs."
        )
