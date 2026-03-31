"""SQL parsing and translation helpers for HumemDB.

This module keeps the translation boundary isolated from the runtime so later work can
add validation, rewrites, and eventually a thin plan layer without pushing parser
details into the rest of the package.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import logging
import re
from typing import Any

from sqlglot import parse_one
from sqlglot import errors as sqlglot_errors

from .types import Route

logger = logging.getLogger(__name__)

_CREATE_INDEX_PREFIX = re.compile(r"^\s*CREATE\s+(UNIQUE\s+)?INDEX\b", re.IGNORECASE)

_SUPPORTED_STATEMENT_NAMES = {
    "Select",
    "Insert",
    "Update",
    "Delete",
    "Create",
}


@dataclass(frozen=True, slots=True)
class SQLTranslationPlan:
    """Validated SQL translation plus lightweight planning metadata.

    Attributes:
        translated_text: Backend-specific SQL emitted by translation.
        statement_name: Parsed sqlglot statement class name.
        is_read_only: Whether the translated statement is read-only.
        cte_count: Number of common table expressions in the statement.
        join_count: Number of joins in the statement.
        aggregate_count: Number of aggregate expressions in the statement.
        window_count: Number of window expressions in the statement.
        exists_count: Number of EXISTS subqueries in the statement.
        has_order_by: Whether the statement includes ORDER BY.
        has_limit: Whether the statement includes LIMIT.
        has_group_by: Whether the statement includes GROUP BY.
        has_distinct: Whether the statement includes DISTINCT.
    """

    translated_text: str
    statement_name: str
    is_read_only: bool
    cte_count: int
    join_count: int
    aggregate_count: int
    window_count: int
    exists_count: int
    has_order_by: bool
    has_limit: bool
    has_group_by: bool
    has_distinct: bool


def _expr_args(expression: Any) -> dict[str, Any]:
    """Return parsed sqlglot args or an empty dict."""

    return dict(getattr(expression, "args", {}))


def _expr_arg(expression: Any, name: str) -> Any:
    """Return one parsed sqlglot arg when present."""

    return _expr_args(expression).get(name)


def _expr_expressions(expression: Any) -> tuple[Any, ...]:
    """Return parsed child expressions as a tuple."""

    return tuple(getattr(expression, "expressions", ()))


def _expr_this(expression: Any) -> Any:
    """Return the parsed `this` child when present."""

    return getattr(expression, "this", None)


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

    logger.debug("Translating SQL for target=%s", target)

    try:
        return _translate_sql_plan_cached(text, target).translated_text
    except sqlglot_errors.ParseError as exc:
        logger.debug("Failed to parse SQL for target=%s", target)
        raise ValueError(
            "HumemDB could not parse the SQL as PostgreSQL-like HumemSQL."
        ) from exc


def translate_sql_plan(text: str, *, target: Route) -> SQLTranslationPlan:
    """Return validated SQL translation plus lightweight planning metadata."""

    logger.debug("Planning SQL translation for target=%s", target)

    try:
        return _translate_sql_plan_cached(text, target)
    except sqlglot_errors.ParseError as exc:
        logger.debug("Failed to parse SQL plan for target=%s", target)
        raise ValueError(
            "HumemDB could not parse the SQL as PostgreSQL-like HumemSQL."
        ) from exc


@lru_cache(maxsize=512)
def _translate_sql_plan_cached(text: str, target: Route) -> SQLTranslationPlan:
    """Cache SQL translations and lightweight planning metadata."""

    expression = parse_one(text, read="postgres")
    _validate_humemsql_v0(expression)
    translated = expression.sql(dialect=target)
    translated = _normalize_translated_sql(text, translated, expression, target=target)
    statement_name = type(expression).__name__
    logger.debug(
        "Translated SQL statement kind=%s target=%s",
        statement_name,
        target,
    )
    return SQLTranslationPlan(
        translated_text=translated,
        statement_name=statement_name,
        is_read_only=_expression_is_read_only(expression),
        cte_count=_expression_cte_count(expression),
        join_count=_expression_join_count(expression),
        aggregate_count=_expression_aggregate_count(expression),
        window_count=_expression_window_count(expression),
        exists_count=_expression_exists_count(expression),
        has_order_by=_expression_has_order_by(expression),
        has_limit=_expression_has_limit(expression),
        has_group_by=_expression_has_group_by(expression),
        has_distinct=_expression_has_distinct(expression),
    )


def _validate_humemsql_v0(expression: Any) -> None:
    """Validate the initial HumemSQL v0 statement subset.

    HumemSQL v0 intentionally supports a small PostgreSQL-like subset rather than
    pretending to accept arbitrary PostgreSQL syntax.
    """

    statement_name = type(expression).__name__
    if statement_name not in _SUPPORTED_STATEMENT_NAMES:
        logger.debug("Rejected unsupported HumemSQL statement kind=%s", statement_name)
        raise ValueError(
            "HumemDB HumemSQL v0 only supports SELECT, INSERT, UPDATE, DELETE, "
            "and CREATE statements."
        )

    expression_args = _expr_args(expression)
    with_clause = expression_args.get("with_")
    with_args = _expr_args(with_clause) if with_clause is not None else {}
    if with_args.get("recursive"):
        logger.debug("Rejected recursive CTE in HumemSQL v0")
        raise ValueError(
            "HumemDB HumemSQL v0 does not support recursive CTEs."
        )


def _normalize_translated_sql(
    original_text: str,
    translated_text: str,
    expression: Any,
    *,
    target: Route,
) -> str:
    """Apply narrow backend-safe fixes after sqlglot translation.

    HumemSQL should let ordinary SQLite index DDL flow through `db.query(...)`.
    sqlglot currently emits `NULLS LAST` inside SQLite `CREATE INDEX` column lists,
    which SQLite rejects. Keep the fix narrow to SQLite-targeted index DDL.
    """

    if target != "sqlite":
        return translated_text

    if type(expression).__name__ != "Create":
        return translated_text

    if not _CREATE_INDEX_PREFIX.match(original_text):
        return translated_text

    return translated_text.replace(" NULLS LAST", "")


def _expression_is_read_only(expression: Any) -> bool:
    """Return whether one parsed HumemSQL expression is read-only."""

    if type(expression).__name__ != "Select":
        return False

    with_clause = _expr_arg(expression, "with_")
    ctes = list(_expr_expressions(with_clause)) if with_clause else []
    return all(_expression_is_read_only(_expr_this(cte)) for cte in ctes)


def _expression_cte_count(expression: Any) -> int:
    """Return the number of CTE bindings attached to one expression."""

    with_clause = _expr_arg(expression, "with_")
    if with_clause is None:
        return 0
    return len(_expr_expressions(with_clause))


def _expression_join_count(expression: Any) -> int:
    """Return the number of JOIN nodes present in one parsed expression."""

    return sum(1 for node in expression.walk() if type(node).__name__ == "Join")


def _expression_aggregate_count(expression: Any) -> int:
    """Return the number of aggregate nodes present in one parsed expression."""

    return sum(
        1
        for node in expression.walk()
        if type(node).__name__
        in {"Count", "Sum", "Avg", "Min", "Max", "Stddev", "Variance"}
    )


def _expression_window_count(expression: Any) -> int:
    """Return the number of window-function nodes present in one expression."""

    return sum(1 for node in expression.walk() if type(node).__name__ == "Window")


def _expression_exists_count(expression: Any) -> int:
    """Return the number of EXISTS nodes present in one expression."""

    return sum(1 for node in expression.walk() if type(node).__name__ == "Exists")


def _expression_has_order_by(expression: Any) -> bool:
    """Return whether one parsed expression contains ORDER BY."""

    return _expr_arg(expression, "order") is not None


def _expression_has_limit(expression: Any) -> bool:
    """Return whether one parsed expression contains LIMIT."""

    return _expr_arg(expression, "limit") is not None


def _expression_has_group_by(expression: Any) -> bool:
    """Return whether one parsed expression contains GROUP BY."""

    return _expr_arg(expression, "group") is not None


def _expression_has_distinct(expression: Any) -> bool:
    """Return whether one parsed expression contains DISTINCT."""

    return _expr_arg(expression, "distinct") is not None
