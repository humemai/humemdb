"""Internal vector runtime plans and pure helpers for HumemDB.

This module keeps vector-specific planning and normalization logic separate from the
top-level `HumemDB` orchestration code in `db.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping, Sequence, TypeAlias, TypedDict, cast

from sqlglot import parse_one
from sqlglot import errors as sqlglot_errors

from .cypher import analyze_cypher_vector_query
from .types import BatchParameters, QueryParameters, QueryResult, Route
from .vector import VectorMetric, encode_vector_blob

VECTOR_RESULT_COLUMNS = ("target", "namespace", "target_id", "score")
_CYPHER_MATCH_PREFIX = re.compile(r"^MATCH\b")
_NAMED_SQL_PARAM_RE = re.compile(r"@([A-Za-z_]\w*)")
_SQL_VECTOR_QUERY_RE = re.compile(
    r"^(?P<candidate_query>.+?)\s+ORDER\s+BY\s+"
    r"(?P<embedding>[A-Za-z_][\w.]*)\s*"
    r"(?P<operator><->|<=>|<#>)\s*"
    r"(?P<query>\$[A-Za-z_]\w*)\s+"
    r"LIMIT\s+(?P<limit>\$[A-Za-z_]\w*|\d+)\s*;?\s*$",
    flags=re.IGNORECASE | re.DOTALL,
)
_SQL_VECTOR_OPERATOR_TO_METRIC: dict[str, VectorMetric] = {
    "<->": "l2",
    "<=>": "cosine",
    "<#>": "dot",
}

CandidateQueryType: TypeAlias = Literal["sql", "cypher"]
SQLParamRef: TypeAlias = int | str
TargetNamespacedVectorRow: TypeAlias = tuple[str, str, int, Sequence[float]]
PendingTargetNamespacedVectorRow: TypeAlias = tuple[
    str,
    str,
    int | None,
    Sequence[float],
]


@dataclass(frozen=True, slots=True)
class CandidateQueryPlan:
    """Thin internal plan for the SQL/Cypher candidate query in vector search."""

    text: str
    query_type: CandidateQueryType
    route: Route
    target: str
    namespace: str
    params: QueryParameters


@dataclass(frozen=True, slots=True)
class CandidateVectorQueryPlan:
    """Thin internal plan for one candidate-query vector execution."""

    candidate_query: CandidateQueryPlan
    query: Sequence[float]
    top_k: int
    metric: VectorMetric


@dataclass(frozen=True, slots=True)
class CandidateVectorQueryResult:
    """Normalized candidate-query result for one vector search."""

    target: str
    namespace: str
    candidate_keys: tuple[tuple[str, str, int], ...]
    candidate_count: int


@dataclass(frozen=True, slots=True)
class DirectVectorSearchPlan:
    """Thin internal plan for one direct-vector search execution."""

    query: Sequence[float]
    top_k: int
    metric: VectorMetric
    filters: Mapping[str, str | int | float | bool | None] | None


@dataclass(frozen=True, slots=True)
class ResolvedVectorCandidates:
    """Resolved candidate metadata for one exact vector search execution."""

    target: str
    namespace: str
    candidate_keys: tuple[tuple[str, str, int], ...]
    candidate_indexes: tuple[int, ...]
    candidate_count: int
    namespace_size: int
    uses_full_namespace: bool = False


@dataclass(frozen=True, slots=True)
class SQLVectorWritePlan:
    """Thin internal plan for one SQL write that may also touch vector storage."""

    normalized_params: QueryParameters
    vector_rows: tuple[PendingTargetNamespacedVectorRow, ...]
    vector_mode: Literal["insert", "upsert"] | None


@dataclass(frozen=True, slots=True)
class SQLVectorBatchWritePlan:
    """Thin internal plan for one batched SQL write over SQLite."""

    normalized_params_seq: BatchParameters
    vector_rows: tuple[PendingTargetNamespacedVectorRow, ...]
    requires_rowwise_execution: bool


class SQLVectorInsertAnalysis(TypedDict):
    """Typed analysis payload for narrow vector-bearing SQL INSERT statements."""

    target_name: str
    id_param_ref: SQLParamRef | None
    id_literal: int | None
    vector_param_ref: SQLParamRef


class SQLVectorUpdateAnalysis(TypedDict):
    """Typed analysis payload for narrow vector-bearing SQL UPDATE statements."""

    target_name: str
    vector_param_ref: SQLParamRef
    id_param_ref: SQLParamRef | None
    id_literal: int | None


@dataclass(frozen=True, slots=True)
class SQLCandidateVectorQueryAnalysis:
    """Parsed SQL vector-query analysis for one narrow language-level query."""

    candidate_query_text: str
    metric: VectorMetric
    query_param_name: str
    limit_ref: SQLParamRef


def plan_candidate_vector_query(
    text: str,
    params: QueryParameters,
) -> CandidateVectorQueryPlan:
    """Lower one language-level vector query into an internal candidate plan."""

    mapping_params = _require_vector_mapping_params(params)
    stripped = text.strip()
    if not stripped:
        raise ValueError(
            "HumemVector v0 queries require SQL or Cypher text with explicit "
            "vector syntax."
        )

    sql_analysis = _analyze_sql_candidate_vector_query(stripped)
    if sql_analysis is not None:
        excluded_params = {sql_analysis.query_param_name}
        if isinstance(sql_analysis.limit_ref, str):
            excluded_params.add(sql_analysis.limit_ref)
        target, namespace = target_namespace_for_vector_query(
            sql_analysis.candidate_query_text,
            candidate_query_type="sql",
        )

        return CandidateVectorQueryPlan(
            candidate_query=CandidateQueryPlan(
                text=sql_analysis.candidate_query_text,
                query_type="sql",
                route="sqlite",
                target=target,
                namespace=namespace,
                params=_candidate_query_params(mapping_params, excluded_params),
            ),
            query=_required_mapping_value(
                mapping_params,
                sql_analysis.query_param_name,
            ),
            top_k=_resolve_vector_limit_ref(sql_analysis.limit_ref, mapping_params),
            metric=sql_analysis.metric,
        )

    cypher_analysis = analyze_cypher_vector_query(stripped)
    if cypher_analysis is not None:
        excluded_params = {cypher_analysis.query_param_name}
        if isinstance(cypher_analysis.limit_ref, str):
            excluded_params.add(cypher_analysis.limit_ref)
        target, namespace = target_namespace_for_vector_query(
            cypher_analysis.candidate_query_text,
            candidate_query_type="cypher",
        )

        return CandidateVectorQueryPlan(
            candidate_query=CandidateQueryPlan(
                text=cypher_analysis.candidate_query_text,
                query_type="cypher",
                route="sqlite",
                target=target,
                namespace=namespace,
                params=_candidate_query_params(mapping_params, excluded_params),
            ),
            query=_required_mapping_value(
                mapping_params,
                cypher_analysis.query_param_name,
            ),
            top_k=_resolve_vector_limit_ref(
                cypher_analysis.limit_ref,
                mapping_params,
            ),
            metric="cosine",
        )

    raise ValueError(
        "HumemVector v0 now requires PostgreSQL-like SQL vector syntax "
        "(ORDER BY embedding <->|<=>|<#> $query LIMIT ...) or Neo4j-like "
        "Cypher SEARCH ... VECTOR INDEX embedding FOR $query LIMIT ...."
    )


def is_vector_query_text(text: str) -> bool:
    """Return whether the text uses one supported language-level vector form."""

    stripped = text.strip()
    return bool(
        _analyze_sql_candidate_vector_query(stripped) is not None
        or analyze_cypher_vector_query(stripped) is not None
    )


def _require_vector_mapping_params(
    params: QueryParameters,
) -> Mapping[str, Any]:
    """Return validated mapping params for one language-level vector query."""

    if not isinstance(params, Mapping):
        raise ValueError(
            "HumemVector v0 expects mapping-style params for language-level "
            "vector queries."
        )
    return params


def _param_name(token: str) -> str:
    """Return the mapping key referenced by one `$name` parameter token."""

    return token[1:]


def _required_mapping_value(
    params: Mapping[str, Any],
    key: str,
) -> Any:
    """Return one required mapping value referenced by vector syntax."""

    if key not in params:
        raise ValueError(f"HumemVector v0 requires params['{key}'].")
    return params[key]


def _resolve_vector_limit(
    token: str,
    params: Mapping[str, Any],
) -> int:
    """Resolve one vector LIMIT literal or `$name` parameter into an integer."""

    if token.startswith("$"):
        return int(_required_mapping_value(params, _param_name(token)))
    return int(token)


def _resolve_vector_limit_ref(
    limit_ref: SQLParamRef,
    params: Mapping[str, Any],
) -> int:
    """Resolve one parsed SQL vector LIMIT reference into an integer."""

    if isinstance(limit_ref, str):
        return int(_required_mapping_value(params, limit_ref))
    return int(limit_ref)


def _candidate_query_params(
    params: Mapping[str, Any],
    excluded_keys: set[str],
) -> QueryParameters:
    """Drop vector-only params before executing the candidate query."""

    candidate_params = {
        key: value
        for key, value in params.items()
        if key not in excluded_keys
    }
    return candidate_params or None


def _looks_like_embedding_ref(identifier: str) -> bool:
    """Return whether one vector syntax identifier points at `embedding`."""

    return identifier.split(".")[-1].casefold() == "embedding"


def _analyze_sql_candidate_vector_query(
    text: str,
) -> SQLCandidateVectorQueryAnalysis | None:
    """Return parsed analysis for one supported SQL vector query, if any."""

    if not _looks_like_sql_vector_query_text(text):
        return None

    try:
        expression = parse_one(text, read="postgres")
    except sqlglot_errors.ParseError:
        return _analyze_sql_candidate_vector_query_regex(text)

    if expression.key != "select":
        return None

    order = expression.args.get("order")
    ordered_expressions = list(getattr(order, "expressions", ()))
    if len(ordered_expressions) != 1:
        return None

    ordered = ordered_expressions[0]
    metric = _sql_vector_metric(getattr(ordered, "this", None))
    if metric is None:
        return None

    vector_expression = getattr(ordered, "this", None)
    embedding_expr = getattr(vector_expression, "args", {}).get("this")
    embedding_ref = _sql_identifier_text(embedding_expr)
    if not embedding_ref or not _looks_like_embedding_ref(embedding_ref):
        raise ValueError(
            "HumemVector v0 SQL vector queries currently require ORDER BY "
            "an embedding column."
        )

    query_param_name = _named_sql_param_name(
        getattr(vector_expression, "args", {}).get("expression")
    )
    if query_param_name is None:
        raise ValueError(
            "HumemVector v0 SQL vector queries currently require the query "
            "vector to come from a named parameter."
        )

    limit_ref = _sql_limit_ref(expression.args.get("limit"))
    if limit_ref is None:
        return None

    candidate_expression = expression.copy()
    candidate_expression.set("order", None)
    candidate_expression.set("limit", None)
    _sql_select_table_name(candidate_expression)

    return SQLCandidateVectorQueryAnalysis(
        candidate_query_text=_restore_named_sql_params(
            candidate_expression.sql(dialect="postgres")
        ),
        metric=metric,
        query_param_name=query_param_name,
        limit_ref=limit_ref,
    )


def _analyze_sql_candidate_vector_query_regex(
    text: str,
) -> SQLCandidateVectorQueryAnalysis | None:
    """Fallback SQL vector-query analysis for operators sqlglot does not parse."""

    sql_match = _SQL_VECTOR_QUERY_RE.match(text)
    if sql_match is None:
        return None

    operator = sql_match.group("operator")
    if operator != "<#>":
        return None

    embedding_ref = sql_match.group("embedding")
    if not _looks_like_embedding_ref(embedding_ref):
        raise ValueError(
            "HumemVector v0 SQL vector queries currently require ORDER BY "
            "an embedding column."
        )

    candidate_query_text = _strip_trailing_semicolon(
        sql_match.group("candidate_query")
    )
    try:
        candidate_expression = parse_one(candidate_query_text, read="postgres")
    except sqlglot_errors.ParseError as exc:
        raise ValueError(
            "HumemVector v0 SQL vector candidate query must be valid HumemSQL v0."
        ) from exc
    _sql_select_table_name(candidate_expression)

    limit_token = sql_match.group("limit")
    limit_ref: SQLParamRef
    if limit_token.startswith("$"):
        limit_ref = _param_name(limit_token)
    else:
        limit_ref = int(limit_token)

    return SQLCandidateVectorQueryAnalysis(
        candidate_query_text=candidate_query_text,
        metric=_SQL_VECTOR_OPERATOR_TO_METRIC[operator],
        query_param_name=_param_name(sql_match.group("query")),
        limit_ref=limit_ref,
    )


def _looks_like_sql_vector_query_text(text: str) -> bool:
    """Return whether text is worth attempting to parse as SQL vector syntax."""

    lowered = text.casefold()
    return (
        "order by" in lowered
        and "limit" in lowered
        and any(operator in text for operator in ("<->", "<=>", "<#>"))
    )


def _sql_vector_metric(expression: Any) -> VectorMetric | None:
    """Return the vector metric represented by one parsed SQL ORDER BY node."""

    if expression is None:
        return None

    expression_name = type(expression).__name__
    if expression_name == "Distance":
        return "l2"
    if expression_name == "NullSafeEQ":
        return "cosine"
    if expression_name == "Dot":
        return "dot"
    return None


def _named_sql_param_name(expression: Any) -> str | None:
    """Return the `$name` param referenced by one parsed SQL expression."""

    if expression is None or type(expression).__name__ != "Parameter":
        return None

    parameter_var = getattr(expression, "args", {}).get("this")
    parameter_name = getattr(parameter_var, "this", None)
    if parameter_name is None:
        return None
    return str(parameter_name)


def _sql_limit_ref(limit_expression: Any) -> SQLParamRef | None:
    """Return the parsed SQL LIMIT reference for one candidate vector query."""

    limit_value = getattr(limit_expression, "expression", None)
    if limit_value is None:
        return None

    named_param = _named_sql_param_name(limit_value)
    if named_param is not None:
        return named_param

    if type(limit_value).__name__ == "Literal":
        return int(str(getattr(limit_value, "this", limit_value)))

    return None


def _restore_named_sql_params(text: str) -> str:
    """Convert sqlglot named parameter rendering back into `$name` form."""

    return _NAMED_SQL_PARAM_RE.sub(r"$\1", text)


def _sql_identifier_text(expression: Any) -> str:
    """Return one parsed identifier as text suitable for embedding checks."""

    table_name = getattr(getattr(expression, "args", {}).get("table"), "name", None)
    column_name = getattr(expression, "name", None)
    if table_name and column_name:
        return f"{table_name}.{column_name}"
    if column_name:
        return str(column_name)
    return str(expression or "")


def _strip_trailing_semicolon(text: str) -> str:
    """Trim whitespace and one optional trailing semicolon from query text."""

    return text.strip().removesuffix(";").rstrip()


def infer_candidate_query_type(text: str) -> CandidateQueryType:
    """Infer whether one vector candidate query is SQL or Cypher.

    Candidate-query vector search currently supports only read-only SQL queries and
    Cypher `MATCH` queries. That means the inference can stay intentionally narrow.
    """

    stripped = text.lstrip()
    if _CYPHER_MATCH_PREFIX.match(stripped):
        return "cypher"
    return "sql"


def plan_direct_vector_search(
    query: Sequence[float],
    *,
    top_k: int,
    metric: VectorMetric,
    filters: Mapping[str, str | int | float | bool | None] | None,
) -> DirectVectorSearchPlan:
    """Normalize and validate one direct-vector search into an internal plan."""

    if metric not in {"cosine", "dot", "l2"}:
        raise ValueError(
            "HumemVector v0 metric must be one of 'cosine', 'dot', or 'l2'."
        )

    if filters is not None and not isinstance(filters, Mapping):
        raise ValueError(
            "HumemVector v0 filters must be a mapping when provided."
        )

    return DirectVectorSearchPlan(
        query=query,
        top_k=top_k,
        metric=metric,
        filters=filters,
    )


def target_namespace_for_vector_query(
    text: str,
    *,
    candidate_query_type: CandidateQueryType,
) -> tuple[str, str]:
    """Resolve vector target metadata for one SQL or Cypher candidate query."""

    if candidate_query_type == "cypher":
        return "graph_node", ""

    table_name = sql_candidate_query_table_name(text)
    return "sql_row", table_name


def sql_candidate_query_table_name(text: str) -> str:
    """Return the single table name referenced by a narrow SQL vector query."""

    try:
        expression = parse_one(text, read="postgres")
    except sqlglot_errors.ParseError as exc:
        raise ValueError(
            "HumemVector v0 SQL vector candidate query must be valid HumemSQL v0."
        ) from exc

    return _sql_select_table_name(expression)


def _sql_select_table_name(expression: Any) -> str:
    """Return the single base table name for one narrow parsed SQL SELECT."""

    if type(expression).__name__ != "Select":
        raise ValueError(
            "HumemVector v0 SQL vector candidate query must be a SELECT over one table."
        )

    joins = expression.args.get("joins") or []
    if joins:
        raise ValueError(
            "HumemVector v0 SQL vector candidate queries currently support one "
            "base table, "
            "not joins."
        )

    from_clause = expression.args.get("from") or expression.args.get("from_")
    source = getattr(from_clause, "this", None)
    table_name = (
        getattr(source, "name", None)
        or getattr(getattr(source, "this", None), "name", None)
        or str(source or "")
    )
    if not table_name:
        raise ValueError(
            "HumemVector v0 SQL vector candidate queries currently require a "
            "concrete table "
            "name in FROM."
        )
    return table_name


def vector_candidate_keys_from_result(
    result: QueryResult,
    *,
    target: str,
    namespace: str,
) -> tuple[tuple[str, str, int], ...]:
    """Extract logical vector identifiers from a SQL or Cypher candidate result."""

    if not result.columns:
        raise ValueError(
            "HumemVector v0 candidate queries must return at least "
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
                "HumemVector v0 candidate queries must return one id column "
                "named 'item_id', 'id', or '*.id'."
            )

    item_ids: list[tuple[str, str, int]] = []
    for row in result.rows:
        item_ids.append((target, namespace, int(row[id_index])))
    return tuple(item_ids)


def candidate_vector_result_from_query_result(
    result: QueryResult,
    *,
    target: str,
    namespace: str,
) -> CandidateVectorQueryResult:
    """Normalize one SQL/Cypher candidate query into vector candidate keys."""

    candidate_keys = vector_candidate_keys_from_result(
        result,
        target=target,
        namespace=namespace,
    )
    return CandidateVectorQueryResult(
        target=target,
        namespace=namespace,
        candidate_keys=candidate_keys,
        candidate_count=len(candidate_keys),
    )


def plan_sql_vector_write(
    text: str,
    params: QueryParameters,
) -> SQLVectorWritePlan:
    """Normalize one SQL write into a thin internal vector-write plan."""

    if params is None:
        return SQLVectorWritePlan(
            normalized_params=params,
            vector_rows=(),
            vector_mode=None,
        )

    analysis = analyze_sql_vector_insert(text)
    if analysis is not None:
        normalized_params, vector_row = normalize_sql_vector_row(
            params,
            id_param_ref=analysis["id_param_ref"],
            id_literal=analysis["id_literal"],
            vector_param_ref=analysis["vector_param_ref"],
        )
        return SQLVectorWritePlan(
            normalized_params=normalized_params,
            vector_rows=(
                ("sql_row", analysis["target_name"], vector_row[0], vector_row[1]),
            ),
            vector_mode="insert",
        )

    update_analysis = analyze_sql_vector_update(text)
    if update_analysis is not None:
        if update_analysis["vector_param_ref"] is None:
            raise ValueError(
                "HumemVector v0 SQL updates require both an embedding target and "
                "an id predicate."
            )
        normalized_params, vector_row = normalize_sql_vector_update(
            params,
            vector_param_ref=update_analysis["vector_param_ref"],
            id_param_ref=update_analysis["id_param_ref"],
            id_literal=update_analysis["id_literal"],
        )
        return SQLVectorWritePlan(
            normalized_params=normalized_params,
            vector_rows=(
                (
                    "sql_row",
                    update_analysis["target_name"],
                    vector_row[0],
                    vector_row[1],
                ),
            ),
            vector_mode="upsert",
        )

    return SQLVectorWritePlan(
        normalized_params=params,
        vector_rows=(),
        vector_mode=None,
    )


def plan_sql_vector_write_batch(
    text: str,
    params_seq: BatchParameters,
) -> SQLVectorBatchWritePlan:
    """Normalize one batched SQL write into a thin internal vector-write plan."""

    analysis = analyze_sql_vector_insert(text)
    if analysis is None:
        return SQLVectorBatchWritePlan(
            normalized_params_seq=params_seq,
            vector_rows=(),
            requires_rowwise_execution=False,
        )

    normalized_params_seq: list[Sequence[Any]] = []
    normalized_mapping_params_seq: list[Mapping[str, Any]] = []
    vector_rows: list[PendingTargetNamespacedVectorRow] = []
    saw_mapping_params = False
    for params in params_seq:
        normalized_params, vector_row = normalize_sql_vector_row(
            params,
            id_param_ref=analysis["id_param_ref"],
            id_literal=analysis["id_literal"],
            vector_param_ref=analysis["vector_param_ref"],
        )
        if isinstance(normalized_params, Mapping):
            saw_mapping_params = True
            normalized_mapping_params_seq.append(normalized_params)
        else:
            normalized_params_seq.append(cast(Sequence[Any], normalized_params))
        vector_rows.append(
            ("sql_row", analysis["target_name"], vector_row[0], vector_row[1])
        )

    if saw_mapping_params and normalized_params_seq:
        raise ValueError(
            "HumemDB batch SQL params must not mix positional and named styles."
        )

    return SQLVectorBatchWritePlan(
        normalized_params_seq=cast(
            BatchParameters,
            tuple(normalized_mapping_params_seq)
            if saw_mapping_params
            else tuple(normalized_params_seq),
        ),
        vector_rows=tuple(vector_rows),
        requires_rowwise_execution=any(row[2] is None for row in vector_rows),
    )


def analyze_sql_vector_insert(text: str) -> SQLVectorInsertAnalysis | None:
    """Return id/vector param references for narrow vector-bearing SQL INSERTs."""

    try:
        expression = parse_one(text, read="postgres")
    except sqlglot_errors.ParseError:
        return None

    if expression.key != "insert":
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
        id_column_index = normalized_columns.index("id")
    elif "item_id" in normalized_columns:
        id_column_index = normalized_columns.index("item_id")
    else:
        id_column_index = None

    values = expression.args.get("expression")
    row_values = list(getattr(values, "expressions", [])) if values is not None else []
    row_count = len(row_values)
    if row_count > 1:
        raise ValueError(
            "HumemVector v0 SQL vector inserts support one VALUES row per statement; "
            "use executemany for batches."
        )
    if row_count == 0:
        return None

    first_row = row_values[0]
    row_expressions = list(getattr(first_row, "expressions", []))
    if len(row_expressions) != len(normalized_columns):
        raise ValueError(
            "HumemVector v0 SQL vector inserts require one parameter or literal per "
            "declared column in VALUES."
        )

    row_param_refs = _row_sql_param_refs(row_expressions)
    vector_param_ref = row_param_refs[vector_indexes[0]]
    if vector_param_ref is None:
        raise ValueError(
            "HumemVector v0 SQL inserts require embedding values to come from a "
            "parameter."
        )

    id_param_ref: SQLParamRef | None = None
    id_literal: int | None = None
    if id_column_index is not None:
        id_expression = row_expressions[id_column_index]
        id_param_ref = row_param_refs[id_column_index]
        if id_param_ref is None:
            id_literal = int(str(getattr(id_expression, "this", id_expression)))

    return {
        "target_name": target_name,
        "id_param_ref": id_param_ref,
        "id_literal": id_literal,
        "vector_param_ref": vector_param_ref,
    }


def analyze_sql_vector_update(text: str) -> SQLVectorUpdateAnalysis | None:
    """Return id/vector param references for narrow vector-bearing SQL UPDATEs."""

    try:
        expression = parse_one(text, read="postgres")
    except sqlglot_errors.ParseError:
        return None

    if expression.key != "update":
        return None

    target = getattr(expression, "this", None)
    target_name = getattr(target, "name", None) or str(target or "")
    if target_name.casefold() == "vector_entries":
        return None

    vector_param_refs: list[SQLParamRef] = []
    positional_index = 0
    for assignment in expression.expressions:
        column_name = getattr(getattr(assignment, "this", None), "name", None)
        rhs = getattr(assignment, "expression", None)
        current_param, positional_index = _next_sql_param_ref(rhs, positional_index)

        if str(column_name).casefold() == "embedding":
            if current_param is None:
                raise ValueError(
                    "HumemVector v0 SQL updates currently require embedding to be "
                    "set from a parameter."
                )
            vector_param_refs.append(current_param)

    if not vector_param_refs:
        return None
    if len(vector_param_refs) > 1:
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
    id_param_ref, positional_index = _next_sql_param_ref(right, positional_index)
    if id_param_ref is not None:
        id_literal = None
    else:
        id_literal = int(str(getattr(right, "this", right)))

    return {
        "target_name": target_name,
        "vector_param_ref": vector_param_refs[0],
        "id_param_ref": id_param_ref,
        "id_literal": id_literal,
    }


def normalize_sql_vector_row(
    params: QueryParameters,
    *,
    id_param_ref: SQLParamRef | None,
    id_literal: int | None,
    vector_param_ref: SQLParamRef,
) -> tuple[QueryParameters, tuple[int | None, Sequence[float]]]:
    """Encode one SQL row's embedding param and extract its canonical vector row."""

    bound, vector_value = _replace_sql_param(
        params,
        vector_param_ref,
        context="HumemVector v0 SQL vector inserts",
    )
    row_id = _resolve_sql_row_id(
        bound,
        id_param_ref=id_param_ref,
        id_literal=id_literal,
        context="HumemVector v0 SQL vector inserts",
    )
    return bound, (row_id, vector_value)


def normalize_sql_vector_update(
    params: QueryParameters,
    *,
    vector_param_ref: SQLParamRef,
    id_param_ref: SQLParamRef | None,
    id_literal: int | None,
) -> tuple[QueryParameters, tuple[int, Sequence[float]]]:
    """Encode one SQL UPDATE embedding param and extract its canonical vector row."""

    bound, vector_value = _replace_sql_param(
        params,
        vector_param_ref,
        context="HumemVector v0 SQL vector updates",
    )
    row_id = _resolve_sql_row_id(
        bound,
        id_param_ref=id_param_ref,
        id_literal=id_literal,
        context="HumemVector v0 SQL vector updates",
    )
    if row_id is None:
        raise ValueError("HumemVector v0 SQL vector updates could not resolve an id.")
    return bound, (row_id, vector_value)


def _row_sql_param_refs(
    expressions: Sequence[Any],
) -> tuple[SQLParamRef | None, ...]:
    """Return parameter refs for one VALUES row, aligned with its expressions."""

    positional_index = 0
    refs: list[SQLParamRef | None] = []
    for expression in expressions:
        param_ref, positional_index = _next_sql_param_ref(expression, positional_index)
        refs.append(param_ref)
    return tuple(refs)


def _next_sql_param_ref(
    expression: Any,
    positional_index: int,
) -> tuple[SQLParamRef | None, int]:
    """Return one SQL placeholder reference plus the next positional index."""

    if expression is None:
        return None, positional_index

    expression_args = getattr(expression, "args", {})
    if type(expression).__name__ == "Placeholder" or expression_args.get("jdbc"):
        return positional_index, positional_index + 1
    if type(expression).__name__ == "Parameter":
        parameter_var = expression_args.get("this")
        parameter_name = getattr(parameter_var, "this", None)
        if parameter_name is None:
            return None, positional_index
        return str(parameter_name), positional_index
    return None, positional_index


def _replace_sql_param(
    params: QueryParameters,
    param_ref: SQLParamRef,
    *,
    context: str,
) -> tuple[QueryParameters, Sequence[float]]:
    """Replace one SQL embedding param with its encoded blob form."""

    if isinstance(param_ref, str):
        if not isinstance(params, Mapping):
            raise ValueError(f"{context} require named params for named placeholders.")
        if param_ref not in params:
            raise ValueError(f"{context} did not receive param ${param_ref}.")
        vector_value = coerce_vector_param(params[param_ref], context="SQL")
        bound = dict(params)
        bound[param_ref] = encode_vector_blob(vector_value)
        return bound, vector_value

    if not isinstance(params, Sequence) or isinstance(
        params, (str, bytes, bytearray, memoryview)
    ):
        raise ValueError(
            f"{context} require positional params for positional placeholders."
        )

    bound = list(params)
    if param_ref >= len(bound):
        raise ValueError(f"{context} did not receive enough positional params.")
    vector_value = coerce_vector_param(bound[param_ref], context="SQL")
    bound[param_ref] = encode_vector_blob(vector_value)
    return tuple(bound), vector_value


def _resolve_sql_row_id(
    params: QueryParameters,
    *,
    id_param_ref: SQLParamRef | None,
    id_literal: int | None,
    context: str,
) -> int | None:
    """Resolve one SQL row id from named, positional, or literal input."""

    if id_param_ref is None:
        return int(id_literal) if id_literal is not None else None

    if isinstance(id_param_ref, str):
        if not isinstance(params, Mapping):
            raise ValueError(
                f"{context} require named params for named id placeholders."
            )
        if id_param_ref not in params:
            raise ValueError(f"{context} did not receive param ${id_param_ref}.")
        return int(params[id_param_ref])

    if not isinstance(params, Sequence) or isinstance(
        params, (str, bytes, bytearray, memoryview)
    ):
        raise ValueError(
            f"{context} require positional params for positional id placeholders."
        )
    if id_param_ref >= len(params):
        raise ValueError(f"{context} did not receive enough positional params.")
    return int(params[id_param_ref])


def coerce_vector_param(value: Any, *, context: str) -> Sequence[float]:
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
