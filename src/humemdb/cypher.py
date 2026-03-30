"""Minimal HumemCypher v0 support for HumemDB.

This module implements a deliberately small `HumemCypher v0` surface so HumemDB can
start storing graph data in SQLite and reading it back through SQLite or DuckDB.

The current subset is intentionally narrow:

- `CREATE` for labeled nodes with inline properties
- `CREATE` for a single directed relationship between two labeled nodes
- `MATCH` for labeled nodes and single directed relationships
- simple `WHERE` predicates joined by `AND`
- `ORDER BY` and `LIMIT` for `MATCH` queries
- named Cypher parameters such as `$name`
- `RETURN` for node and relationship `id`, graph labels/types, and stored properties

This is not a general Cypher engine. The goal is to keep `HumemCypher v0` small,
explicit, and easy to evolve into a richer graph plan later.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Callable, Mapping, Sequence
from typing import Literal, TypeGuard, cast

import numpy as np

from .engines import _DuckDBEngine, _SQLiteEngine
from .types import QueryParameters, QueryResult, Route
from .vector import (
    _GRAPH_NODE_VECTOR_DELETE_TRIGGER_SQL,
    _ensure_vector_schema,
    _insert_vectors,
    _upsert_vectors,
)

logger = logging.getLogger(__name__)

_CYPHER_CREATE_PREFIX = re.compile(r"^CREATE\b")
_CYPHER_MATCH_PREFIX = re.compile(r"^MATCH\b")

_ScalarPropertyValue = str | int | float | bool | None
_VectorPropertyValue = tuple[float, ...]
PropertyValue = _ScalarPropertyValue | _VectorPropertyValue


@dataclass(frozen=True, slots=True)
class _ParameterRef:
    """Named Cypher parameter reference such as `$name`.

    Attributes:
        name: Parameter name without the leading `$` prefix.
    """

    name: str


CypherValue = PropertyValue | _ParameterRef
PropertyItems = tuple[tuple[str, CypherValue], ...]
_ScalarQueryParam = str | int | float | bool | None
_CypherVectorLimitRef = int | str


@dataclass(frozen=True, slots=True)
class _EncodedNodePropertyWrite:
    """Encoded graph node property write plus optional vector payload.

    Attributes:
        key: Graph property key to persist.
        encoded_value: SQLite-storable scalar representation for the property.
        value_type: Narrow type tag used by the graph property tables.
        vector_value: Optional detached vector payload when the property also feeds
            graph-node vector storage.
    """

    key: str
    encoded_value: object
    value_type: str
    vector_value: _VectorPropertyValue | None = None


@dataclass(frozen=True, slots=True)
class _CypherVectorQueryAnalysis:
    """Parsed analysis for one narrow Cypher vector procedure query.

    Attributes:
        candidate_query_text: Cypher candidate query text synthesized from the
            post-procedure filtering clauses.
        index_name: Referenced vector index identifier.
        query_param_name: Mapping key that supplies the query embedding.
        limit_ref: Integer limit literal or parameter name referenced by the
            procedure call.
        result_mode: Whether execution should keep the current normalized vector
            result shape or project the narrow `queryNodes` return subset.
        return_items: Requested return items for the narrow `queryNodes` subset.
        order_items: Requested order items for the narrow `queryNodes` subset.
    """

    candidate_query_text: str
    index_name: str
    query_param_name: str
    limit_ref: _CypherVectorLimitRef
    result_mode: Literal["normalized", "queryNodes"] = "normalized"
    return_items: tuple[str, ...] = ()
    order_items: tuple[tuple[str, Literal["asc", "desc"]], ...] = ()


@dataclass(frozen=True, slots=True)
class Predicate:
    """Simple predicate used by the current Cypher `WHERE` subset.

    Attributes:
        alias: Bound node or relationship alias the predicate targets.
        field: Property name being compared.
        operator: Comparison operator applied to the field.
        disjunct_index: Zero-based OR-clause index for this predicate.
        value: Literal or parameter-backed Cypher value to compare against.
    """

    alias: str
    field: str
    operator: Literal[
        "=",
        "<",
        "<=",
        ">",
        ">=",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "IS NULL",
        "IS NOT NULL",
    ]
    value: CypherValue
    disjunct_index: int = 0


@dataclass(frozen=True, slots=True)
class PropertyConstraint:
    """Bound property equality constraint used during Cypher compilation.

    Attributes:
        alias: Bound node or relationship alias the constraint targets.
        field: Property name constrained during compilation.
        value: Bound scalar or vector property value.
    """

    alias: str
    field: str
    value: PropertyValue


@dataclass(frozen=True, slots=True)
class NodePattern:
    """Parsed node pattern for the initial Cypher subset.

    Attributes:
        alias: Pattern alias used by the query.
        label: Optional node label required by the narrow v0 surface.
        properties: Inline property items attached to the pattern.
    """

    alias: str
    label: str | None
    properties: PropertyItems = ()


@dataclass(frozen=True, slots=True)
class RelationshipPattern:
    """Parsed directed relationship pattern for the initial Cypher subset.

    Attributes:
        alias: Optional relationship alias bound by the query.
        type_name: Optional relationship type token, optionally containing narrow
            `|` alternation for MATCH patterns.
        direction: Relationship direction relative to the left node.
        properties: Inline property items attached to the relationship pattern.
    """

    alias: str | None
    type_name: str | None
    direction: Literal["out", "in"]
    properties: PropertyItems = ()


@dataclass(frozen=True, slots=True)
class ReturnItem:
    """Requested return binding for a Cypher `RETURN` clause.

    Attributes:
        alias: Alias being projected.
        field: Field requested from the alias.
    """

    alias: str
    field: str

    @property
    def column_name(self) -> str:
        """Return the public result column name for this item.

        Returns:
            A stable `alias.field` column name.
        """

        return f"{self.alias}.{self.field}"


@dataclass(frozen=True, slots=True)
class OrderItem:
    """Requested sort binding for a Cypher `ORDER BY` clause.

    Attributes:
        alias: Alias being ordered.
        field: Field requested for ordering.
        direction: Sort direction.
    """

    alias: str
    field: str
    direction: Literal["asc", "desc"] = "asc"


@dataclass(frozen=True, slots=True)
class CreateNodePlan:
    """Plan for creating a single labeled node.

    Attributes:
        node: Parsed node pattern to create.
    """

    node: NodePattern


@dataclass(frozen=True, slots=True)
class CreateRelationshipPlan:
    """Plan for creating two nodes and one directed edge between them.

    Attributes:
        left: Left node pattern.
        relationship: Relationship pattern to create.
        right: Right node pattern.
    """

    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class CreateRelationshipFromSeparatePatternsPlan:
    """Plan for a narrow multi-pattern CREATE with two nodes and one edge.

    Attributes:
        first_node: First standalone node pattern created by the statement.
        second_node: Second standalone node pattern created by the statement.
        left: Left endpoint node referenced by the relationship pattern.
        relationship: Relationship pattern to create between the endpoints.
        right: Right endpoint node referenced by the relationship pattern.
    """

    first_node: NodePattern
    second_node: NodePattern
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class MatchCreateRelationshipPlan:
    """Plan for matching one node binding and creating one relationship pattern.

    Attributes:
        match_node: Existing node pattern that anchors the match step.
        predicates: Additional predicates that narrow the matched node set.
        left: Left endpoint node pattern used by the relationship creation.
        relationship: Relationship pattern to create.
        right: Right endpoint node pattern used by the relationship creation.
    """

    match_node: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class MatchCreateRelationshipBetweenNodesPlan:
    """Plan for matching two existing nodes and creating one relationship.

    Attributes:
        left_match: Existing node pattern that resolves the left endpoint.
        right_match: Existing node pattern that resolves the right endpoint.
        predicates: Additional predicates that narrow the matched endpoint set.
        left: Left endpoint node pattern used during relationship creation.
        relationship: Relationship pattern to create between the endpoints.
        right: Right endpoint node pattern used during relationship creation.
    """

    left_match: NodePattern
    right_match: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class MatchNodePlan:
    """Plan for matching labeled nodes and projecting bound values.

    Attributes:
        node: Node pattern to match.
        predicates: Additional equality predicates from `WHERE`.
        returns: Requested projections.
        order_by: Optional sort items.
        limit: Optional row limit.
    """

    node: NodePattern
    predicates: tuple[Predicate, ...]
    returns: tuple[ReturnItem, ...]
    order_by: tuple[OrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class MatchRelationshipPlan:
    """Plan for matching one directed relationship and projecting node values.

    Attributes:
        left: Left node pattern.
        relationship: Relationship pattern to match.
        right: Right node pattern.
        predicates: Additional equality predicates from `WHERE`.
        returns: Requested projections.
        order_by: Optional sort items.
        limit: Optional row limit.
    """

    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]
    returns: tuple[ReturnItem, ...]
    order_by: tuple[OrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class SetItem:
    """Requested property assignment for a narrow Cypher `SET` clause.

    Attributes:
        alias: Alias whose property is being updated.
        field: Property name to assign.
        value: Literal or parameter-backed Cypher value to store.
    """

    alias: str
    field: str
    value: CypherValue


@dataclass(frozen=True, slots=True)
class SetNodePlan:
    """Plan for matching labeled nodes and updating node properties.

    Attributes:
        node: Node pattern that anchors the update.
        predicates: Equality predicates that select target nodes.
        assignments: Property assignments to apply.
    """

    node: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class SetRelationshipPlan:
    """Plan for matching one relationship and updating relationship properties.

    Attributes:
        left: Left node pattern.
        relationship: Relationship pattern that anchors the update.
        right: Right node pattern.
        predicates: Equality predicates that select target relationships.
        assignments: Property assignments to apply.
    """

    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class DeleteNodePlan:
    """Plan for matching labeled nodes and deleting them with detach semantics.

    Attributes:
        node: Node pattern used to select nodes for deletion.
        predicates: Additional predicates that narrow the target node set.
        detach: Whether connected relationships should also be removed.
    """

    node: NodePattern
    predicates: tuple[Predicate, ...]
    detach: bool = True


@dataclass(frozen=True, slots=True)
class DeleteRelationshipPlan:
    """Plan for matching one relationship and deleting it.

    Attributes:
        left: Left endpoint node pattern used to anchor the match.
        relationship: Relationship pattern to delete.
        right: Right endpoint node pattern used to anchor the match.
        predicates: Additional predicates that narrow the target relationship set.
    """

    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]


@dataclass(frozen=True, slots=True)
class _CypherPlanShape:
    """Lightweight metadata derived from one parsed Cypher plan.

    Attributes:
        plan_name: Concrete parsed plan class name.
        is_read_only: Whether the parsed plan performs reads only.
        pattern_kind: Whether the plan centers on node or relationship patterns.
        predicate_count: Number of explicit `WHERE` predicates in the plan.
        has_inline_properties: Whether the plan includes inline node or
            relationship property maps.
        has_order_by: Whether the plan contains an `ORDER BY` clause.
        has_limit: Whether the plan contains a `LIMIT` clause.
    """

    plan_name: str
    is_read_only: bool
    pattern_kind: Literal["node", "relationship"]
    predicate_count: int
    has_inline_properties: bool
    has_order_by: bool
    has_limit: bool


GraphPlan = (
    CreateNodePlan
    | CreateRelationshipPlan
    | CreateRelationshipFromSeparatePatternsPlan
    | MatchCreateRelationshipPlan
    | MatchCreateRelationshipBetweenNodesPlan
    | MatchNodePlan
    | MatchRelationshipPlan
    | SetNodePlan
    | SetRelationshipPlan
    | DeleteNodePlan
    | DeleteRelationshipPlan
)


@dataclass(frozen=True, slots=True)
class _CompiledReturnItem:
    """Compiled return metadata used to decode SQL result rows.

    Attributes:
        column_name: Public column name to expose in the decoded result.
        kind: Whether the column maps to a raw graph field or typed property.
    """

    column_name: str
    kind: Literal["raw", "property"]


@dataclass(frozen=True, slots=True)
class _CompiledMatchQuery:
    """Backend SQL plus result decoding metadata for a Cypher match.

    Attributes:
        sql: Backend SQL statement to execute.
        params: Positional parameters for `sql`.
        returns: Compiled return descriptors used during decoding.
    """

    sql: str
    params: tuple[object, ...]
    returns: tuple[_CompiledReturnItem, ...]


_IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_]*"
_NODE_PATTERN_RE = re.compile(
    rf"^(?:(?P<alias>{_IDENTIFIER}))?(?::(?P<label>{_IDENTIFIER}))?"
    r"(?:\s*\{\s*(?P<properties>.*)\s*\})?$"
)
_REL_PATTERN_RE = re.compile(
    rf"^(?:(?P<alias>{_IDENTIFIER})\s*)?"
    rf"(?::(?P<type>{_IDENTIFIER}(?:\|{_IDENTIFIER})*))?"
    r"(?:\s*\{\s*(?P<properties>.*)\s*\})?$"
)
_RETURN_ITEM_RE = re.compile(rf"^(?P<alias>{_IDENTIFIER})\.(?P<field>{_IDENTIFIER})$")

_UNSUPPORTED_KEYWORD_PATTERNS = {
    "with": re.compile(r"(?<!starts )(?<!ends )\bwith\b"),
    "optional": re.compile(r"\boptional\b"),
    "merge": re.compile(r"\bmerge\b"),
    "unwind": re.compile(r"\bunwind\b"),
}

_GRAPH_SCHEMA_SQL = (
    "CREATE TABLE IF NOT EXISTS graph_nodes ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "label TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS graph_node_properties ("
    "node_id INTEGER NOT NULL, "
    "key TEXT NOT NULL, "
    "value TEXT, "
    "value_type TEXT NOT NULL, "
    "FOREIGN KEY (node_id) REFERENCES graph_nodes(id) ON DELETE CASCADE, "
    "PRIMARY KEY (node_id, key))",
    "CREATE TABLE IF NOT EXISTS graph_edges ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "type TEXT NOT NULL, "
    "from_node_id INTEGER NOT NULL, "
    "to_node_id INTEGER NOT NULL, "
    "FOREIGN KEY (from_node_id) REFERENCES graph_nodes(id) ON DELETE CASCADE, "
    "FOREIGN KEY (to_node_id) REFERENCES graph_nodes(id) ON DELETE CASCADE)",
    "CREATE TABLE IF NOT EXISTS graph_edge_properties ("
    "edge_id INTEGER NOT NULL, "
    "key TEXT NOT NULL, "
    "value TEXT, "
    "value_type TEXT NOT NULL, "
    "FOREIGN KEY (edge_id) REFERENCES graph_edges(id) ON DELETE CASCADE, "
    "PRIMARY KEY (edge_id, key))",
    "CREATE INDEX IF NOT EXISTS idx_graph_nodes_label_id "
    "ON graph_nodes(label, id)",
    "CREATE INDEX IF NOT EXISTS idx_graph_edges_from_type_to "
    "ON graph_edges(from_node_id, type, to_node_id)",
    "CREATE INDEX IF NOT EXISTS idx_graph_edges_to_type_from "
    "ON graph_edges(to_node_id, type, from_node_id)",
    "CREATE INDEX IF NOT EXISTS idx_graph_node_props_lookup "
    "ON graph_node_properties(key, value_type, value, node_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_graph_node_single_vector_prop "
    "ON graph_node_properties(node_id) WHERE value_type = 'vector'",
    "CREATE INDEX IF NOT EXISTS idx_graph_edge_props_lookup "
    "ON graph_edge_properties(key, value_type, value, edge_id)",
)


def _ensure_graph_schema(sqlite: _SQLiteEngine) -> None:
    """Create the SQLite-backed graph storage tables if they do not exist yet.

    Args:
        sqlite: Canonical SQLite engine that owns graph storage.
    """

    logger.debug("Ensuring SQLite-backed graph schema exists")
    for statement in _GRAPH_SCHEMA_SQL:
        sqlite.execute(statement)

    if sqlite.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        ("vector_entries",),
        query_type="cypher",
    ).first() is not None:
        sqlite.execute(_GRAPH_NODE_VECTOR_DELETE_TRIGGER_SQL, query_type="cypher")


def _execute_cypher(
    text: str,
    *,
    route: Route,
    params: QueryParameters,
    sqlite: _SQLiteEngine,
    duckdb: _DuckDBEngine,
    plan: GraphPlan | None = None,
) -> QueryResult:
    """Execute a minimal Cypher statement through the HumemDB graph path.

    Args:
        text: Cypher statement to execute.
        route: Backend route selected by the caller.
        params: Optional named or positional Cypher parameters.
        sqlite: Canonical SQLite engine that owns graph storage.
        duckdb: DuckDB engine used for read-only graph queries.
        plan: Optional pre-parsed Cypher plan to reuse instead of reparsing `text`.

    Returns:
        A normalized `QueryResult`.

    Raises:
        ValueError: If the route is unsupported or a Cypher write is directed to
            DuckDB.
    """

    parsed_plan = plan or parse_cypher(text)
    plan = _bind_plan_values(parsed_plan, _normalize_params(params))
    logger.debug(
        "Executing Cypher plan kind=%s route=%s",
        type(plan).__name__,
        route,
    )

    if isinstance(
        plan,
        (
            CreateNodePlan,
            CreateRelationshipPlan,
            CreateRelationshipFromSeparatePatternsPlan,
        ),
    ):
        if route != "sqlite":
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        return _execute_cypher_write_atomically(
            sqlite,
            lambda: _execute_create_plan(plan, sqlite),
        )

    if isinstance(plan, MatchCreateRelationshipPlan):
        if route != "sqlite":
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        return _execute_cypher_write_atomically(
            sqlite,
            lambda: _execute_match_create_relationship_plan(plan, sqlite),
        )

    if isinstance(plan, MatchCreateRelationshipBetweenNodesPlan):
        if route != "sqlite":
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        return _execute_cypher_write_atomically(
            sqlite,
            lambda: _execute_match_create_relationship_between_nodes_plan(
                plan,
                sqlite,
            ),
        )

    if isinstance(plan, (SetNodePlan, SetRelationshipPlan)):
        if route != "sqlite":
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        if isinstance(plan, SetNodePlan):
            return _execute_cypher_write_atomically(
                sqlite,
                lambda: _execute_set_node_plan(plan, sqlite),
            )
        return _execute_cypher_write_atomically(
            sqlite,
            lambda: _execute_set_relationship_plan(plan, sqlite),
        )

    if isinstance(plan, (DeleteNodePlan, DeleteRelationshipPlan)):
        if route != "sqlite":
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        if isinstance(plan, DeleteNodePlan):
            return _execute_cypher_write_atomically(
                sqlite,
                lambda: _execute_delete_node_plan(plan, sqlite),
            )
        return _execute_cypher_write_atomically(
            sqlite,
            lambda: _execute_delete_relationship_plan(plan, sqlite),
        )

    compiled = _compile_match_plan(plan)
    logger.debug(
        "Compiled Cypher match plan kind=%s route=%s params=%s",
        type(plan).__name__,
        route,
        len(compiled.params),
    )

    if route == "sqlite":
        sql_result = sqlite.execute(compiled.sql, compiled.params, query_type="cypher")
        return _decode_match_result(sql_result, compiled.returns)

    if route == "duckdb":
        sql_result = duckdb.execute(compiled.sql, compiled.params, query_type="cypher")
        return _decode_match_result(sql_result, compiled.returns)

    raise ValueError(f"Unsupported route: {route!r}")


def _execute_cypher_write_atomically(
    sqlite: _SQLiteEngine,
    execute_write: Callable[[], QueryResult],
) -> QueryResult:
    """Execute one logical Cypher write in a single SQLite transaction."""

    owns_transaction = not sqlite.in_transaction
    if owns_transaction:
        sqlite.begin()

    try:
        result = execute_write()
    except Exception:
        if owns_transaction:
            sqlite.rollback()
        raise

    if owns_transaction:
        sqlite.commit()
    return result


def parse_cypher(text: str) -> GraphPlan:
    """Parse a `HumemCypher v0` statement into a small internal graph plan.

    Args:
        text: Cypher statement text.

    Returns:
        Parsed `GraphPlan` for the supported Cypher subset.

    Raises:
        ValueError: If the statement is empty or outside the supported v0 subset.
    """

    statement = text.strip().rstrip(";").strip()
    if not statement:
        raise ValueError("HumemCypher v0 requires a non-empty statement.")

    lowered_statement = statement.lower()
    for keyword, pattern in _UNSUPPORTED_KEYWORD_PATTERNS.items():
        if pattern.search(lowered_statement):
            raise ValueError(
                "HumemCypher v0 only supports CREATE and MATCH with simple RETURN "
                f"clauses; found unsupported keyword {keyword!r}."
            )

    create_match = _CYPHER_CREATE_PREFIX.match(statement)
    if create_match is not None:
        plan = _parse_create(statement[create_match.end():].strip())
        logger.debug("Parsed Cypher statement kind=%s", type(plan).__name__)
        return plan

    match_match = _CYPHER_MATCH_PREFIX.match(statement)
    if match_match is not None:
        plan = _parse_match(statement[match_match.end():].strip())
        logger.debug("Parsed Cypher statement kind=%s", type(plan).__name__)
        return plan

    raise ValueError(
        "HumemCypher v0 only supports CREATE and MATCH statements."
    )


def analyze_cypher_vector_query(text: str) -> _CypherVectorQueryAnalysis | None:
    """Return parsed metadata for one narrow Cypher vector query."""

    statement = text.strip().rstrip(";").strip()
    if not statement:
        return None
    return _analyze_cypher_query_nodes_call(statement)


def _analyze_cypher_query_nodes_call(
    statement: str,
) -> _CypherVectorQueryAnalysis | None:
    """Return parsed metadata for narrow `CALL db.index.vector.queryNodes(...)`."""

    lowered = statement.casefold()
    if not lowered.startswith("call db.index.vector.querynodes"):
        return None

    prefix_match = re.match(
        r"^CALL\s+db\.index\.vector\.queryNodes\s*\(",
        statement,
        flags=re.IGNORECASE,
    )
    if prefix_match is None:
        return None

    args_text, remainder = _consume_balanced_parenthesized(
        statement[prefix_match.end() - 1:]
    )
    if args_text is None:
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require "
            "CALL db.index.vector.queryNodes('user_embedding_idx', 10, $query) "
            "YIELD node, score RETURN node.id, score."
        )

    args = _split_top_level_commas(args_text)
    if len(args) != 3:
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require "
            "CALL db.index.vector.queryNodes('user_embedding_idx', 10, $query) "
            "YIELD node, score RETURN node.id, score."
        )

    index_name = _parse_cypher_string_literal(args[0].strip(), "index name")
    limit_ref = _parse_cypher_vector_limit_ref(args[1].strip())
    query_token = args[2].strip()
    if not re.fullmatch(rf"\${_IDENTIFIER}", query_token):
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require the query "
            "vector to come from a named parameter."
        )

    remainder = remainder.strip()
    yield_match = re.match(r"^YIELD\b", remainder, flags=re.IGNORECASE)
    if yield_match is None:
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require "
            "YIELD node, score RETURN node.id, score."
        )
    return_index = _find_keyword(remainder, "return")
    if return_index is None:
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require "
            "YIELD node, score RETURN node.id, score."
        )

    yield_tail = remainder[yield_match.end():return_index].strip()
    yielded_match = re.match(
        r"^node\s*,\s*score\b(?P<tail>.*)$",
        yield_tail,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if yielded_match is None:
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require "
            "YIELD node, score in that order."
        )
    post_yield_text = yielded_match.group("tail").strip()
    return_text = remainder[return_index + len("return"):].strip()

    candidate_query_text = _query_nodes_candidate_query_text(post_yield_text)
    candidate_plan = parse_cypher(candidate_query_text)
    if "node" not in _match_plan_node_aliases(candidate_plan):
        raise ValueError(
            "HumemCypher v0 vector procedure queries must keep the yielded "
            "node alias bound as `node` when adding MATCH filters."
        )

    return_text, order_text = _split_query_nodes_return_and_order(return_text)

    return_items = tuple(
        item.strip() for item in return_text.split(",") if item.strip()
    )
    if not return_items:
        raise ValueError(
            "HumemCypher v0 vector procedure queries currently require a RETURN "
            "clause over node.id and/or score."
        )
    for item in return_items:
        if item not in {"node.id", "score"}:
            raise ValueError(
                "HumemCypher v0 vector procedure queries currently support only "
                "RETURN node.id and score."
            )

    order_items = _parse_query_nodes_order_items(order_text)

    return _CypherVectorQueryAnalysis(
        candidate_query_text=candidate_query_text,
        index_name=index_name,
        query_param_name=query_token[1:],
        limit_ref=limit_ref,
        result_mode="queryNodes",
        return_items=return_items,
        order_items=order_items,
    )


def _query_nodes_candidate_query_text(post_yield_text: str) -> str:
    """Build one candidate Cypher query from post-procedure filter clauses."""

    filter_text = post_yield_text.strip()
    if not filter_text:
        return "MATCH (node) RETURN node.id"
    if filter_text.upper().startswith("WHERE "):
        return f"MATCH (node) WHERE {filter_text[6:]} RETURN node.id"
    if filter_text.upper().startswith("MATCH "):
        return f"MATCH {filter_text[6:]} RETURN node.id"
    raise ValueError(
        "HumemCypher v0 vector procedure queries currently support only "
        "optional WHERE ... or MATCH ... [WHERE ...] between YIELD and RETURN."
    )


def _split_query_nodes_return_and_order(text: str) -> tuple[str, str | None]:
    """Split one `RETURN ... [ORDER BY ...]` tail for vector procedure queries."""

    match = re.search(r"\bORDER\s+BY\b", text, flags=re.IGNORECASE)
    if match is None:
        return text.strip(), None
    return text[:match.start()].strip(), text[match.end():].strip()


def _parse_query_nodes_order_items(
    text: str | None,
) -> tuple[tuple[str, Literal["asc", "desc"]], ...]:
    """Parse one narrow `ORDER BY` clause for vector procedure queries."""

    if text is None:
        return ()
    if not text:
        raise ValueError("HumemCypher v0 ORDER BY clauses cannot be empty.")

    items: list[tuple[str, Literal["asc", "desc"]]] = []
    for raw_item in _split_comma_separated(text):
        item_text = raw_item.strip()
        parts = item_text.rsplit(None, 1)
        direction: Literal["asc", "desc"] = "asc"
        target = item_text
        if len(parts) == 2 and parts[1].lower() in {"asc", "desc"}:
            target = parts[0]
            direction = cast(Literal["asc", "desc"], parts[1].lower())
        if target not in {"node.id", "score"}:
            raise ValueError(
                "HumemCypher v0 vector procedure ORDER BY currently supports only "
                "node.id and score."
            )
        items.append((target, direction))
    return tuple(items)


def _consume_balanced_parenthesized(text: str) -> tuple[str | None, str]:
    """Return text inside one balanced leading parenthesized group."""

    if not text.startswith("("):
        return None, text
    depth = 0
    in_string = False
    string_quote = ""
    for index, char in enumerate(text):
        if in_string:
            if char == string_quote:
                in_string = False
            continue
        if char in {"'", '"'}:
            in_string = True
            string_quote = char
            continue
        if char == "(":
            depth += 1
            continue
        if char == ")":
            depth -= 1
            if depth == 0:
                return text[1:index], text[index + 1:]
    return None, text


def _split_top_level_commas(text: str) -> tuple[str, ...]:
    """Split one comma-delimited argument list while respecting strings."""

    parts: list[str] = []
    current: list[str] = []
    in_string = False
    string_quote = ""
    depth = 0
    for char in text:
        if in_string:
            current.append(char)
            if char == string_quote:
                in_string = False
            continue
        if char in {"'", '"'}:
            in_string = True
            string_quote = char
            current.append(char)
            continue
        if char in "([{":
            depth += 1
            current.append(char)
            continue
        if char in ")]}":
            depth -= 1
            current.append(char)
            continue
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    parts.append("".join(current).strip())
    return tuple(part for part in parts if part)


def _parse_cypher_string_literal(token: str, label: str) -> str:
    """Parse one narrow quoted Cypher string literal."""

    if len(token) < 2 or token[0] not in {"'", '"'} or token[-1] != token[0]:
        raise ValueError(
            f"HumemCypher v0 vector procedure queries require one quoted {label}."
        )
    value = token[1:-1]
    if not re.fullmatch(_IDENTIFIER, value):
        raise ValueError(
            "HumemCypher v0 vector procedure queries require one "
            f"identifier-like {label}."
        )
    return value


def _analyze_cypher_plan(plan: GraphPlan) -> _CypherPlanShape:
    """Return lightweight structural metadata for one parsed Cypher plan."""

    if isinstance(plan, CreateNodePlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="node",
            predicate_count=0,
            has_inline_properties=bool(plan.node.properties),
            has_order_by=False,
            has_limit=False,
        )

    if isinstance(plan, CreateRelationshipPlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="relationship",
            predicate_count=0,
            has_inline_properties=bool(
                plan.left.properties
                or plan.relationship.properties
                or plan.right.properties
            ),
            has_order_by=False,
            has_limit=False,
        )

    if isinstance(plan, CreateRelationshipFromSeparatePatternsPlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="relationship",
            predicate_count=0,
            has_inline_properties=bool(
                plan.first_node.properties
                or plan.second_node.properties
                or plan.relationship.properties
            ),
            has_order_by=False,
            has_limit=False,
        )

    if isinstance(plan, MatchCreateRelationshipPlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="relationship",
            predicate_count=len(plan.predicates),
            has_inline_properties=bool(
                plan.match_node.properties
                or plan.left.properties
                or plan.relationship.properties
                or plan.right.properties
            ),
            has_order_by=False,
            has_limit=False,
        )

    if isinstance(plan, MatchCreateRelationshipBetweenNodesPlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="relationship",
            predicate_count=len(plan.predicates),
            has_inline_properties=bool(
                plan.left_match.properties
                or plan.right_match.properties
                or plan.left.properties
                or plan.relationship.properties
                or plan.right.properties
            ),
            has_order_by=False,
            has_limit=False,
        )

    if isinstance(plan, MatchNodePlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=True,
            pattern_kind="node",
            predicate_count=len(plan.predicates),
            has_inline_properties=bool(plan.node.properties),
            has_order_by=bool(plan.order_by),
            has_limit=plan.limit is not None,
        )

    if isinstance(plan, MatchRelationshipPlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=True,
            pattern_kind="relationship",
            predicate_count=len(plan.predicates),
            has_inline_properties=bool(
                plan.left.properties
                or plan.relationship.properties
                or plan.right.properties
            ),
            has_order_by=bool(plan.order_by),
            has_limit=plan.limit is not None,
        )

    if isinstance(plan, SetRelationshipPlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="relationship",
            predicate_count=len(plan.predicates),
            has_inline_properties=bool(
                plan.left.properties
                or plan.relationship.properties
                or plan.right.properties
            ),
            has_order_by=False,
            has_limit=False,
        )

    if isinstance(plan, DeleteRelationshipPlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="relationship",
            predicate_count=len(plan.predicates),
            has_inline_properties=bool(
                plan.left.properties
                or plan.relationship.properties
                or plan.right.properties
            ),
            has_order_by=False,
            has_limit=False,
        )

    if isinstance(plan, DeleteNodePlan):
        return _CypherPlanShape(
            plan_name=type(plan).__name__,
            is_read_only=False,
            pattern_kind="node",
            predicate_count=len(plan.predicates),
            has_inline_properties=bool(plan.node.properties),
            has_order_by=False,
            has_limit=False,
        )

    return _CypherPlanShape(
        plan_name=type(plan).__name__,
        is_read_only=False,
        pattern_kind="node",
        predicate_count=len(plan.predicates),
        has_inline_properties=bool(plan.node.properties),
        has_order_by=False,
        has_limit=False,
    )


def _parse_create(body: str) -> GraphPlan:
    """Parse one CREATE body into a node or single-edge creation plan."""

    create_patterns = _split_comma_separated(body)
    if len(create_patterns) == 3:
        if any(
            _looks_like_relationship_pattern(pattern)
            for pattern in create_patterns[:2]
        ) or not _looks_like_relationship_pattern(create_patterns[2]):
            raise ValueError(
                "HumemCypher v0 CREATE currently supports either one node pattern, "
                "one directed relationship pattern, or the narrow three-pattern "
                "form with two node patterns followed by one relationship pattern."
            )

        first_node = _parse_node_pattern(
            _unwrap_node_pattern(create_patterns[0]),
            require_label=True,
            default_alias="__humem_create_first_node",
        )
        second_node = _parse_node_pattern(
            _unwrap_node_pattern(create_patterns[1]),
            require_label=True,
            default_alias="__humem_create_second_node",
        )
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(create_patterns[2])
        )
        left = _parse_node_pattern(
            left_text,
            default_alias="__humem_create_left_node",
        )
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            default_alias="__humem_create_right_node",
        )
        _validate_create_relationship_separate_patterns(
            first_node,
            second_node,
            left,
            right,
        )
        return CreateRelationshipFromSeparatePatternsPlan(
            first_node=first_node,
            second_node=second_node,
            left=left,
            relationship=relationship,
            right=right,
        )

    if len(create_patterns) != 1:
        raise ValueError(
            "HumemCypher v0 CREATE currently supports either one node pattern, "
            "one directed relationship pattern, or the narrow three-pattern form "
            "with two node patterns followed by one relationship pattern."
        )

    if _looks_like_relationship_pattern(body):
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(body)
        )
        left = _parse_node_pattern(
            left_text,
            require_label=True,
            default_alias="__humem_create_left_node",
        )
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            require_label=True,
            default_alias="__humem_create_right_node",
        )
        return CreateRelationshipPlan(left, relationship, right)

    node = _parse_node_pattern(
        _unwrap_node_pattern(body),
        require_label=True,
        default_alias="__humem_create_node",
    )
    return CreateNodePlan(node)


def _parse_match(body: str) -> GraphPlan:
    """Parse a narrow MATCH statement into a node or relationship plan.

    The supported shape is intentionally small: one node pattern or one directed
    relationship pattern, an optional simple WHERE clause, a required RETURN clause,
    and optional DISTINCT, ORDER BY, SKIP, and LIMIT clauses.
    """

    set_index = _find_keyword(body, "set")
    create_index = _find_keyword(body, "create")
    delete_index = _find_keyword(body, "delete")
    return_index = _find_keyword(body, "return")

    if set_index is not None and return_index is None:
        return _parse_match_set(body)
    if set_index is not None and return_index is not None and set_index < return_index:
        return _parse_match_set(body)

    if create_index is not None and return_index is None:
        return _parse_match_create(body)
    if (
        create_index is not None
        and return_index is not None
        and create_index < return_index
    ):
        return _parse_match_create(body)

    if delete_index is not None and return_index is None:
        return _parse_match_delete(body)
    if (
        delete_index is not None
        and return_index is not None
        and delete_index < return_index
    ):
        return _parse_match_delete(body)

    if return_index is None:
        raise ValueError("HumemCypher v0 MATCH statements require a RETURN clause.")

    match_body = body[:return_index].strip()
    return_clause = body[return_index + len("return"):].strip()
    if not return_clause:
        raise ValueError("HumemCypher v0 RETURN clauses cannot be empty.")

    where_index = _find_keyword(match_body, "where")
    predicates: tuple[Predicate, ...] = ()
    if where_index is None:
        pattern_text = match_body
    else:
        pattern_text = match_body[:where_index].strip()
        where_text = match_body[where_index + len("where"):].strip()
        if not where_text:
            raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")
        predicates = _parse_predicates(where_text)

    return_text, order_by, limit, distinct, skip = _split_return_clause(
        return_clause
    )
    returns = _parse_return_items(return_text)

    if _looks_like_relationship_pattern(pattern_text):
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(pattern_text)
        )
        left = _parse_node_pattern(
            left_text,
            default_alias="__humem_match_left_node",
        )
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            default_alias="__humem_match_right_node",
        )
        _validate_match_predicates(
            predicates,
            alias_kinds={
                left.alias: "node",
                right.alias: "node",
                **(
                    {relationship.alias: "relationship"}
                    if relationship.alias is not None
                    else {}
                ),
            },
        )
        return MatchRelationshipPlan(
            left=left,
            relationship=relationship,
            right=right,
            predicates=predicates,
            returns=returns,
            order_by=order_by,
            limit=limit,
            distinct=distinct,
            skip=skip,
        )

    node = _parse_node_pattern(
        _unwrap_node_pattern(pattern_text),
        default_alias="__humem_match_node",
    )
    _validate_match_predicates(
        predicates,
        alias_kinds={node.alias: "node"},
    )
    return MatchNodePlan(
        node=node,
        predicates=predicates,
        returns=returns,
        order_by=order_by,
        limit=limit,
        distinct=distinct,
        skip=skip,
    )


def _match_plan_node_aliases(
    plan: GraphPlan,
) -> tuple[str, ...]:
    """Return node aliases that remain available inside one Cypher match plan."""

    if isinstance(plan, MatchNodePlan):
        return (plan.node.alias,)
    if isinstance(plan, MatchRelationshipPlan):
        return (plan.left.alias, plan.right.alias)
    return ()


def _parse_match_set(body: str) -> SetNodePlan | SetRelationshipPlan:
    """Parse a narrow MATCH ... SET property update statement."""

    set_index = _find_keyword(body, "set")
    if set_index is None:
        raise ValueError("HumemCypher v0 MATCH ... SET statements require SET.")

    match_body = body[:set_index].strip()
    set_text = body[set_index + len("set"):].strip()
    if not set_text:
        raise ValueError("HumemCypher v0 SET clauses cannot be empty.")

    where_index = _find_keyword(match_body, "where")
    predicates: tuple[Predicate, ...] = ()
    if where_index is None:
        pattern_text = match_body
    else:
        pattern_text = match_body[:where_index].strip()
        where_text = match_body[where_index + len("where"):].strip()
        if not where_text:
            raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")
        predicates = _parse_predicates(where_text)

    assignments = _parse_set_items(set_text)
    if _looks_like_relationship_pattern(pattern_text):
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(pattern_text)
        )
        left = _parse_node_pattern(
            left_text,
            default_alias="__humem_set_left_node",
        )
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            default_alias="__humem_set_right_node",
        )
        _validate_match_predicates(
            predicates,
            alias_kinds={
                left.alias: "node",
                right.alias: "node",
                **(
                    {relationship.alias: "relationship"}
                    if relationship.alias is not None
                    else {}
                ),
            },
        )
        _validate_match_set_assignments(
            assignments,
            target_alias=relationship.alias,
            target_kind="relationship",
        )
        return SetRelationshipPlan(
            left,
            relationship,
            right,
            predicates,
            assignments,
        )

    node = _parse_node_pattern(
        _unwrap_node_pattern(pattern_text),
        default_alias="__humem_set_node",
    )
    _validate_match_predicates(
        predicates,
        alias_kinds={node.alias: "node"},
    )
    _validate_match_set_assignments(
        assignments,
        target_alias=node.alias,
        target_kind="node",
    )
    return SetNodePlan(node, predicates, assignments)


def _parse_match_create(
    body: str,
) -> MatchCreateRelationshipPlan | MatchCreateRelationshipBetweenNodesPlan:
    """Parse a narrow MATCH ... CREATE relationship statement."""

    create_index = _find_keyword(body, "create")
    if create_index is None:
        raise ValueError("HumemCypher v0 MATCH ... CREATE statements require CREATE.")

    match_body = body[:create_index].strip()
    create_text = body[create_index + len("create"):].strip()
    if not create_text:
        raise ValueError("HumemCypher v0 CREATE clauses cannot be empty.")

    where_index = _find_keyword(match_body, "where")
    predicates: tuple[Predicate, ...] = ()
    if where_index is None:
        pattern_text = match_body
    else:
        pattern_text = match_body[:where_index].strip()
        where_text = match_body[where_index + len("where"):].strip()
        if not where_text:
            raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")
        predicates = _parse_predicates(where_text)

    if not _looks_like_relationship_pattern(create_text):
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE currently supports only one directed "
            "relationship pattern in the CREATE clause."
        )

    match_patterns = _split_comma_separated(pattern_text)
    if len(match_patterns) == 1:
        if _looks_like_relationship_pattern(match_patterns[0]):
            raise ValueError(
                "HumemCypher v0 MATCH ... CREATE currently supports only matched "
                "node patterns before CREATE."
            )

        match_node = _parse_node_pattern(
            _unwrap_node_pattern(match_patterns[0]),
            default_alias="__humem_match_create_node",
        )
        _validate_match_predicates(
            predicates,
            alias_kinds={match_node.alias: "node"},
        )

        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(create_text)
        )
        left = _parse_node_pattern(
            left_text,
            default_alias="__humem_match_create_left_node",
        )
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            default_alias="__humem_match_create_right_node",
        )
        _validate_match_create_relationship_endpoints(match_node, left, right)
        return MatchCreateRelationshipPlan(
            match_node=match_node,
            predicates=predicates,
            left=left,
            relationship=relationship,
            right=right,
        )

    if len(match_patterns) != 2:
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE currently supports one matched node "
            "pattern, or two disconnected matched node patterns, before CREATE."
        )

    if any(_looks_like_relationship_pattern(pattern) for pattern in match_patterns):
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE currently supports only matched node "
            "patterns before CREATE."
        )

    left_match = _parse_node_pattern(
        _unwrap_node_pattern(match_patterns[0]),
        default_alias="__humem_match_create_left_match_node",
    )
    right_match = _parse_node_pattern(
        _unwrap_node_pattern(match_patterns[1]),
        default_alias="__humem_match_create_right_match_node",
    )
    _validate_match_predicates(
        predicates,
        alias_kinds={
            left_match.alias: "node",
            right_match.alias: "node",
        },
    )

    left_text, relationship_text, right_text, direction = _split_relationship_pattern(
        create_text
    )
    left = _parse_node_pattern(
        left_text,
        default_alias="__humem_match_create_left_node",
    )
    relationship = _parse_relationship_pattern(relationship_text, direction)
    right = _parse_node_pattern(
        right_text,
        default_alias="__humem_match_create_right_node",
    )
    _validate_match_create_relationship_between_nodes_endpoints(
        left_match,
        right_match,
        left,
        right,
    )
    return MatchCreateRelationshipBetweenNodesPlan(
        left_match=left_match,
        right_match=right_match,
        predicates=predicates,
        left=left,
        relationship=relationship,
        right=right,
    )


def _parse_match_delete(body: str) -> DeleteNodePlan | DeleteRelationshipPlan:
    """Parse a narrow MATCH ... DELETE statement."""

    delete_index = _find_keyword(body, "delete")
    if delete_index is None:
        raise ValueError(
            "HumemCypher v0 MATCH ... DELETE statements require DELETE."
        )

    match_body = body[:delete_index].strip()
    delete_text = body[delete_index + len("delete"):].strip()
    if not delete_text:
        raise ValueError("HumemCypher v0 DELETE clauses cannot be empty.")

    detach = False
    detach_index = _find_keyword(match_body, "detach")
    if detach_index is not None:
        if match_body[detach_index + len("detach"):].strip():
            raise ValueError(
                "HumemCypher v0 DETACH DELETE must place DETACH immediately "
                "before DELETE."
            )
        match_body = match_body[:detach_index].strip()
        detach = True

    where_index = _find_keyword(match_body, "where")
    predicates: tuple[Predicate, ...] = ()
    if where_index is None:
        pattern_text = match_body
    else:
        pattern_text = match_body[:where_index].strip()
        where_text = match_body[where_index + len("where"):].strip()
        if not where_text:
            raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")
        predicates = _parse_predicates(where_text)

    target_alias = _parse_cypher_identifier_token(delete_text, "DELETE target")

    if _looks_like_relationship_pattern(pattern_text):
        if detach:
            raise ValueError(
                "HumemCypher v0 currently supports DETACH DELETE only for "
                "matched node aliases."
            )
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(pattern_text)
        )
        left = _parse_node_pattern(
            left_text,
            default_alias="__humem_delete_left_node",
        )
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(
            right_text,
            default_alias="__humem_delete_right_node",
        )
        _validate_match_predicates(
            predicates,
            alias_kinds={
                left.alias: "node",
                right.alias: "node",
                **(
                    {relationship.alias: "relationship"}
                    if relationship.alias is not None
                    else {}
                ),
            },
        )
        if relationship.alias is None or target_alias != relationship.alias:
            raise ValueError(
                "HumemCypher v0 MATCH ... DELETE relationship statements must "
                "delete the matched relationship alias."
            )
        return DeleteRelationshipPlan(
            left=left,
            relationship=relationship,
            right=right,
            predicates=predicates,
        )

    node = _parse_node_pattern(
        _unwrap_node_pattern(pattern_text),
        default_alias="__humem_delete_node",
    )
    _validate_match_predicates(
        predicates,
        alias_kinds={node.alias: "node"},
    )
    if target_alias != node.alias:
        raise ValueError(
            "HumemCypher v0 MATCH ... DELETE node statements must delete the "
            "matched node alias."
        )
    if not detach:
        raise ValueError(
            "HumemCypher v0 currently supports node deletion only through "
            "DETACH DELETE."
        )
    return DeleteNodePlan(node=node, predicates=predicates, detach=True)


def _execute_create_plan(
    plan: (
        CreateNodePlan
        | CreateRelationshipPlan
        | CreateRelationshipFromSeparatePatternsPlan
    ),
    sqlite: _SQLiteEngine,
) -> QueryResult:
    """Execute one CREATE plan against the SQLite-backed graph tables."""

    if isinstance(plan, CreateNodePlan):
        node_id = _insert_node(sqlite, plan.node)
        return QueryResult(
            rows=((node_id,),),
            columns=("node_id",),
            route="sqlite",
            query_type="cypher",
            rowcount=1,
        )

    if isinstance(plan, CreateRelationshipFromSeparatePatternsPlan):
        first_node_id = _insert_node(sqlite, plan.first_node)
        second_node_id = _insert_node(sqlite, plan.second_node)
        node_ids = {
            plan.first_node.alias: first_node_id,
            plan.second_node.alias: second_node_id,
        }
        left_id = node_ids[plan.left.alias]
        right_id = node_ids[plan.right.alias]
        edge_id = _insert_edge(sqlite, plan.relationship, left_id, right_id)
        from_id = left_id
        to_id = right_id
        if plan.relationship.direction == "in":
            from_id = right_id
            to_id = left_id
        return QueryResult(
            rows=((from_id, edge_id, to_id),),
            columns=("from_id", "edge_id", "to_id"),
            route="sqlite",
            query_type="cypher",
            rowcount=1,
        )

    left_id = _insert_node(sqlite, plan.left)
    right_id = left_id
    if _create_relationship_uses_distinct_nodes(plan):
        right_id = _insert_node(sqlite, plan.right)
    edge_id = _insert_edge(sqlite, plan.relationship, left_id, right_id)
    from_id = left_id
    to_id = right_id
    if plan.relationship.direction == "in":
        from_id = right_id
        to_id = left_id
    return QueryResult(
        rows=((from_id, edge_id, to_id),),
        columns=("from_id", "edge_id", "to_id"),
        route="sqlite",
        query_type="cypher",
        rowcount=1,
    )


def _create_relationship_uses_distinct_nodes(plan: CreateRelationshipPlan) -> bool:
    """Return whether a CREATE relationship plan needs two inserted nodes."""

    if plan.left.alias != plan.right.alias:
        return True

    if plan.left.label != plan.right.label:
        raise ValueError(
            "HumemCypher v0 CREATE self-loop patterns require the repeated node "
            "alias to use the same label on both sides."
        )

    if plan.left.properties != plan.right.properties:
        raise ValueError(
            "HumemCypher v0 CREATE self-loop patterns require the repeated node "
            "alias to use the same inline properties on both sides."
        )

    return False


def _execute_set_node_plan(
    plan: SetNodePlan,
    sqlite: _SQLiteEngine,
) -> QueryResult:
    """Execute a narrow MATCH ... SET node-property update."""

    for assignment in plan.assignments:
        if assignment.alias != plan.node.alias:
            raise ValueError(
                "HumemCypher v0 MATCH ... SET assignments must target the "
                "matched node alias."
            )

    match_plan = MatchNodePlan(
        node=plan.node,
        predicates=plan.predicates,
        returns=(ReturnItem(plan.node.alias, "id"),),
    )
    compiled = _compile_match_node_plan(match_plan)
    matched = sqlite.execute(compiled.sql, compiled.params, query_type="cypher")
    node_ids = tuple(int(row[0]) for row in matched.rows)
    property_writes = _plan_node_property_writes(
        tuple(
            (assignment.field, _require_property_value(assignment.value))
            for assignment in plan.assignments
        )
    )

    for node_id in node_ids:
        _persist_node_property_writes(
            sqlite,
            node_id,
            property_writes,
            mode="upsert",
        )

    return QueryResult(
        rows=(),
        columns=(),
        route="sqlite",
        query_type="cypher",
        rowcount=len(node_ids),
    )


def _execute_set_relationship_plan(
    plan: SetRelationshipPlan,
    sqlite: _SQLiteEngine,
) -> QueryResult:
    """Execute a narrow MATCH ... SET relationship-property update."""

    relationship_alias = plan.relationship.alias
    if relationship_alias is None:
        raise ValueError(
            "HumemCypher v0 MATCH ... SET relationship updates require a "
            "relationship alias."
        )

    for assignment in plan.assignments:
        if assignment.alias != relationship_alias:
            raise ValueError(
                "HumemCypher v0 MATCH ... SET assignments must target the "
                "matched relationship alias."
            )

    match_plan = MatchRelationshipPlan(
        left=plan.left,
        relationship=plan.relationship,
        right=plan.right,
        predicates=plan.predicates,
        returns=(ReturnItem(relationship_alias, "id"),),
    )
    compiled = _compile_match_relationship_plan(match_plan)
    matched = sqlite.execute(compiled.sql, compiled.params, query_type="cypher")
    edge_ids = tuple(int(row[0]) for row in matched.rows)

    for edge_id in edge_ids:
        for assignment in plan.assignments:
            property_value = _require_property_value(assignment.value)
            encoded_value, value_type = _encode_property_value(property_value)
            sqlite.execute(
                "INSERT INTO graph_edge_properties (edge_id, key, value, value_type) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(edge_id, key) DO UPDATE SET "
                "value = excluded.value, value_type = excluded.value_type",
                (edge_id, assignment.field, encoded_value, value_type),
                query_type="cypher",
            )

    return QueryResult(
        rows=(),
        columns=(),
        route="sqlite",
        query_type="cypher",
        rowcount=len(edge_ids),
    )


def _execute_delete_node_plan(
    plan: DeleteNodePlan,
    sqlite: _SQLiteEngine,
) -> QueryResult:
    """Execute a narrow MATCH ... DETACH DELETE node statement."""

    match_plan = MatchNodePlan(
        node=plan.node,
        predicates=plan.predicates,
        returns=(ReturnItem(plan.node.alias, "id"),),
        distinct=True,
    )
    compiled = _compile_match_node_plan(match_plan)
    matched = sqlite.execute(compiled.sql, compiled.params, query_type="cypher")
    node_ids = tuple(dict.fromkeys(int(row[0]) for row in matched.rows))

    if node_ids:
        sqlite.executemany(
            "DELETE FROM graph_nodes WHERE id = ?",
            tuple((node_id,) for node_id in node_ids),
            query_type="cypher",
        )

    return QueryResult(
        rows=(),
        columns=(),
        route="sqlite",
        query_type="cypher",
        rowcount=len(node_ids),
    )


def _execute_delete_relationship_plan(
    plan: DeleteRelationshipPlan,
    sqlite: _SQLiteEngine,
) -> QueryResult:
    """Execute a narrow MATCH ... DELETE relationship statement."""

    relationship_alias = plan.relationship.alias
    if relationship_alias is None:
        raise ValueError(
            "HumemCypher v0 MATCH ... DELETE relationship statements require a "
            "relationship alias."
        )

    match_plan = MatchRelationshipPlan(
        left=plan.left,
        relationship=plan.relationship,
        right=plan.right,
        predicates=plan.predicates,
        returns=(ReturnItem(relationship_alias, "id"),),
        distinct=True,
    )
    compiled = _compile_match_relationship_plan(match_plan)
    matched = sqlite.execute(compiled.sql, compiled.params, query_type="cypher")
    edge_ids = tuple(dict.fromkeys(int(row[0]) for row in matched.rows))

    if edge_ids:
        sqlite.executemany(
            "DELETE FROM graph_edges WHERE id = ?",
            tuple((edge_id,) for edge_id in edge_ids),
            query_type="cypher",
        )

    return QueryResult(
        rows=(),
        columns=(),
        route="sqlite",
        query_type="cypher",
        rowcount=len(edge_ids),
    )


def _execute_match_create_relationship_plan(
    plan: MatchCreateRelationshipPlan,
    sqlite: _SQLiteEngine,
) -> QueryResult:
    """Execute a narrow MATCH single-node ... CREATE relationship statement."""

    match_plan = MatchNodePlan(
        node=plan.match_node,
        predicates=plan.predicates,
        returns=(ReturnItem(plan.match_node.alias, "id"),),
    )
    compiled = _compile_match_node_plan(match_plan)
    matched = sqlite.execute(compiled.sql, compiled.params, query_type="cypher")
    matched_node_ids = tuple(int(row[0]) for row in matched.rows)

    for matched_node_id in matched_node_ids:
        left_id = _resolve_match_create_endpoint_node_id(
            sqlite,
            endpoint=plan.left,
            match_node=plan.match_node,
            matched_node_id=matched_node_id,
        )
        right_id = _resolve_match_create_endpoint_node_id(
            sqlite,
            endpoint=plan.right,
            match_node=plan.match_node,
            matched_node_id=matched_node_id,
        )
        _insert_edge(sqlite, plan.relationship, left_id, right_id)

    return QueryResult(
        rows=(),
        columns=(),
        route="sqlite",
        query_type="cypher",
        rowcount=len(matched_node_ids),
    )


def _execute_match_create_relationship_between_nodes_plan(
    plan: MatchCreateRelationshipBetweenNodesPlan,
    sqlite: _SQLiteEngine,
) -> QueryResult:
    """Execute a narrow MATCH two-node ... CREATE relationship statement."""

    left_alias = plan.left_match.alias
    right_alias = plan.right_match.alias
    select_parts = [f"{left_alias}.id", f"{right_alias}.id"]
    joins: list[str] = [f"CROSS JOIN graph_nodes AS {right_alias}"]
    where_parts: list[str] = []
    from_params: list[object] = []
    join_params: list[object] = []
    where_params: list[object] = []

    left_constraints = _node_property_constraints(plan.left_match, plan.predicates)
    left_anchor_constraint = _pick_anchor_constraint(left_constraints)
    from_clause = _compile_node_from_clause(
        left_alias,
        left_anchor_constraint,
        from_params,
    )
    joins.extend(
        _compile_property_constraints(
            left_alias,
            _without_anchor_constraint(left_constraints, left_anchor_constraint),
            join_params,
        )
    )
    joins.extend(
        _compile_property_constraints(
            right_alias,
            _node_property_constraints(plan.right_match, plan.predicates),
            join_params,
        )
    )

    if plan.left_match.label is not None:
        where_parts.append(f"{left_alias}.label = ?")
        where_params.append(plan.left_match.label)
    if plan.right_match.label is not None:
        where_parts.append(f"{right_alias}.label = ?")
        where_params.append(plan.right_match.label)

    _compile_predicates(
        alias_map={
            plan.left_match.alias: left_alias,
            plan.right_match.alias: right_alias,
        },
        alias_kinds={
            plan.left_match.alias: "node",
            plan.right_match.alias: "node",
        },
        predicates=plan.predicates,
        where_parts=where_parts,
        where_params=where_params,
    )

    sql = [f"SELECT {', '.join(select_parts)}", from_clause]
    sql.extend(joins)
    if where_parts:
        sql.append(f"WHERE {' AND '.join(where_parts)}")

    matched = sqlite.execute(
        " ".join(sql),
        tuple(from_params + join_params + where_params),
        query_type="cypher",
    )

    rowcount = 0
    for row in matched.rows:
        left_id = int(row[0])
        right_id = int(row[1])
        _insert_edge(sqlite, plan.relationship, left_id, right_id)
        rowcount += 1

    return QueryResult(
        rows=(),
        columns=(),
        route="sqlite",
        query_type="cypher",
        rowcount=rowcount,
    )


def _resolve_match_create_endpoint_node_id(
    sqlite: _SQLiteEngine,
    *,
    endpoint: NodePattern,
    match_node: NodePattern,
    matched_node_id: int,
) -> int:
    """Return the node id to use for one MATCH ... CREATE endpoint."""

    if endpoint.alias == match_node.alias:
        return matched_node_id
    return _insert_node(sqlite, endpoint)


def _compile_match_plan(
    plan: MatchNodePlan | MatchRelationshipPlan,
) -> _CompiledMatchQuery:
    """Dispatch one MATCH plan to the node or relationship compiler."""

    logger.debug("Compiling Cypher match plan kind=%s", type(plan).__name__)
    if isinstance(plan, MatchNodePlan):
        return _compile_match_node_plan(plan)
    return _compile_match_relationship_plan(plan)


def _compile_match_node_plan(
    plan: MatchNodePlan,
) -> _CompiledMatchQuery:
    """Compile a node MATCH plan into relational SQL over graph tables.

    This compiler tries to anchor the scan from the most selective property equality
    constraint when possible, then layers on remaining property joins, projection,
    explicit ordering, and an optional LIMIT.
    """

    alias = plan.node.alias
    select_parts: list[str] = []
    joins: list[str] = []
    order_joins: list[str] = []
    where_parts: list[str] = []
    order_parts: list[str] = []
    from_params: list[object] = []
    join_params: list[object] = []
    order_params: list[object] = []
    where_params: list[object] = []
    returns: list[_CompiledReturnItem] = []
    property_join_aliases: dict[tuple[str, str, str], str] = {}

    property_constraints = _node_property_constraints(plan.node, plan.predicates)
    anchor_constraint = _pick_anchor_constraint(property_constraints)
    from_clause = _compile_node_from_clause(alias, anchor_constraint, from_params)
    remaining_constraints = _without_anchor_constraint(
        property_constraints,
        anchor_constraint,
    )

    if plan.node.label is not None:
        where_parts.append(f"{alias}.label = ?")
        where_params.append(plan.node.label)

    _compile_predicates(
        alias_map={plan.node.alias: alias},
        alias_kinds={plan.node.alias: "node"},
        predicates=plan.predicates,
        where_parts=where_parts,
        where_params=where_params,
    )

    joins.extend(
        _compile_property_constraints(alias, remaining_constraints, join_params)
    )
    _compile_return_items(
        alias_map={plan.node.alias: alias},
        alias_kinds={plan.node.alias: "node"},
        returns_to_compile=plan.returns,
        joins=joins,
        params=join_params,
        select_parts=select_parts,
        returns=returns,
        property_join_aliases=property_join_aliases,
    )
    _compile_order_items(
        alias_map={plan.node.alias: alias},
        alias_kinds={plan.node.alias: "node"},
        order_to_compile=plan.order_by,
        joins=order_joins,
        params=order_params,
        order_parts=order_parts,
        property_join_aliases=property_join_aliases,
    )

    select_keyword = "SELECT DISTINCT" if plan.distinct else "SELECT"
    sql = [f"{select_keyword} {', '.join(select_parts)}", from_clause]
    sql.extend(joins)
    sql.extend(order_joins)
    if where_parts:
        sql.append(f"WHERE {' AND '.join(where_parts)}")
    if order_parts:
        sql.append(f"ORDER BY {', '.join(order_parts)}")
    elif plan.distinct:
        order_by_projection = ", ".join(
            str(index) for index in range(1, len(select_parts) + 1)
        )
        sql.append(f"ORDER BY {order_by_projection}")
    if plan.limit is not None:
        sql.append(f"LIMIT {plan.limit}")
    if plan.skip is not None:
        if plan.limit is None:
            sql.append("LIMIT -1")
        sql.append(f"OFFSET {plan.skip}")

    return _CompiledMatchQuery(
        sql=" ".join(sql),
        params=tuple(from_params + join_params + order_params + where_params),
        returns=tuple(returns),
    )


def _compile_match_relationship_plan(
    plan: MatchRelationshipPlan,
) -> _CompiledMatchQuery:
    """Compile a single-edge MATCH plan into relational SQL over graph tables.

    The compiler chooses an anchor side based on available property constraints, joins
    through the edge table in the requested direction, and then applies projection,
    explicit ordering, and an optional LIMIT.
    """

    left_alias = plan.left.alias
    edge_alias = plan.relationship.alias or "edge_rel"
    right_alias = plan.right.alias
    relationship_type_names = _relationship_type_names(plan.relationship)
    select_parts: list[str] = []
    joins: list[str] = []
    order_joins: list[str] = []
    where_parts: list[str] = []
    order_parts: list[str] = []
    from_params: list[object] = []
    join_params: list[object] = []
    order_params: list[object] = []
    where_params: list[object] = []
    returns: list[_CompiledReturnItem] = []
    property_join_aliases: dict[tuple[str, str, str], str] = {}
    alias_map = {
        plan.left.alias: left_alias,
        plan.right.alias: right_alias,
        **(
            {plan.relationship.alias: edge_alias}
            if plan.relationship.alias is not None
            else {}
        ),
    }
    alias_kinds: dict[str, Literal["node", "relationship"]] = {
        plan.left.alias: "node",
        plan.right.alias: "node",
        **(
            {plan.relationship.alias: "relationship"}
            if plan.relationship.alias is not None
            else {}
        ),
    }

    if _can_use_relationship_disjunction_identity_union(plan):
        return _compile_match_relationship_disjunction_plan(
            plan,
            alias_map=alias_map,
            alias_kinds=alias_kinds,
            left_alias=left_alias,
            edge_alias=edge_alias,
            right_alias=right_alias,
        )

    left_constraints = _node_property_constraints(plan.left, plan.predicates)
    right_constraints = _node_property_constraints(plan.right, plan.predicates)
    relationship_constraints = _relationship_property_constraints(
        plan.relationship,
        plan.predicates,
    )
    anchor_alias = _pick_relationship_anchor_alias(
        left_alias,
        left_constraints,
        right_alias,
        right_constraints,
    )

    if anchor_alias == right_alias:
        anchor_constraint = _pick_anchor_constraint(right_constraints)
        from_clause = _compile_node_from_clause(
            right_alias,
            anchor_constraint,
            from_params,
        )
        joins.extend(
            _compile_relationship_joins(
                direction=plan.relationship.direction,
                edge_alias=edge_alias,
                left_alias=left_alias,
                right_alias=right_alias,
                anchor_alias=right_alias,
            )
        )
    else:
        anchor_constraint = _pick_anchor_constraint(left_constraints)
        from_clause = _compile_node_from_clause(
            left_alias,
            anchor_constraint,
            from_params,
        )
        joins.extend(
            _compile_relationship_joins(
                direction=plan.relationship.direction,
                edge_alias=edge_alias,
                left_alias=left_alias,
                right_alias=right_alias,
                anchor_alias=left_alias,
            )
        )

    left_remaining_constraints = _without_anchor_constraint(
        left_constraints,
        anchor_constraint if anchor_alias == left_alias else None,
    )
    right_remaining_constraints = _without_anchor_constraint(
        right_constraints,
        anchor_constraint if anchor_alias == right_alias else None,
    )

    if plan.left.label is not None:
        where_parts.append(f"{left_alias}.label = ?")
        where_params.append(plan.left.label)
    if plan.right.label is not None:
        where_parts.append(f"{right_alias}.label = ?")
        where_params.append(plan.right.label)
    if relationship_type_names:
        where_parts.append(
            _compile_relationship_type_filter(edge_alias, relationship_type_names)
        )
        where_params.extend(relationship_type_names)

    _compile_predicates(
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        predicates=plan.predicates,
        where_parts=where_parts,
        where_params=where_params,
    )

    joins.extend(
        _compile_property_constraints(
            left_alias,
            left_remaining_constraints,
            join_params,
        )
    )
    joins.extend(
        _compile_property_constraints(
            right_alias,
            right_remaining_constraints,
            join_params,
        )
    )
    joins.extend(
        _compile_property_constraints(
            edge_alias,
            relationship_constraints,
            join_params,
            node_table="graph_edge_properties",
            id_column="edge_id",
        )
    )
    order_bindings = _compile_order_bindings(
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        order_to_compile=plan.order_by,
        joins=order_joins,
        params=order_params,
        property_join_aliases=property_join_aliases,
    )
    order_parts.extend(
        f"{expression} {direction}" for expression, direction in order_bindings
    )

    if _can_use_order_limit_projection_fastpath(plan, order_bindings):
        return_aliases = {item.alias for item in plan.returns}
        narrowed_select_parts: list[str] = []
        outer_alias_id_expressions: dict[str, str] = {}
        outer_projected_properties: dict[tuple[str, str], tuple[str, str]] = {}
        if plan.left.alias in return_aliases:
            narrowed_select_parts.append(f"{left_alias}.id AS __left_id")
            outer_alias_id_expressions[plan.left.alias] = "narrowed.__left_id"
        if (
            plan.relationship.alias is not None
            and plan.relationship.alias in return_aliases
        ):
            narrowed_select_parts.append(f"{edge_alias}.id AS __edge_id")
            outer_alias_id_expressions[plan.relationship.alias] = "narrowed.__edge_id"
        if plan.right.alias in return_aliases:
            narrowed_select_parts.append(f"{right_alias}.id AS __right_id")
            outer_alias_id_expressions[plan.right.alias] = "narrowed.__right_id"
        narrowed_select_parts.extend(
            f"{expression} AS __order_{index}"
            for index, (expression, _) in enumerate(order_bindings)
        )
        order_item = plan.order_by[0]
        if not _is_raw_return_field(
            alias_kind=alias_kinds[order_item.alias],
            field=order_item.field,
        ):
            order_property_alias = property_join_aliases.get(
                (
                    alias_map[order_item.alias],
                    alias_kinds[order_item.alias],
                    order_item.field,
                )
            )
            if order_property_alias is not None:
                narrowed_select_parts.extend(
                    [
                        f"{order_property_alias}.value AS __order_value_0",
                        (
                            f"{order_property_alias}.value_type "
                            "AS __order_value_type_0"
                        ),
                    ]
                )
                outer_projected_properties[(order_item.alias, order_item.field)] = (
                    "narrowed.__order_value_0",
                    "narrowed.__order_value_type_0",
                )
        narrowed_sql = [
            f"SELECT {', '.join(narrowed_select_parts)}",
            from_clause,
        ]
        narrowed_sql.extend(joins)
        narrowed_sql.extend(order_joins)
        narrowed_sql.append(f"WHERE {' AND '.join(where_parts)}")
        narrowed_sql.append(f"ORDER BY {', '.join(order_parts)}")
        assert plan.limit is not None
        narrowed_sql.append(f"LIMIT {plan.limit}")

        outer_select_parts: list[str] = []
        outer_joins: list[str] = []
        outer_alias_map: dict[str, str] = {}
        for item in plan.returns:
            alias_kind = alias_kinds[item.alias]
            if not _is_raw_return_field(alias_kind=alias_kind, field=item.field):
                continue
            if item.alias in outer_alias_map:
                continue
            if item.alias == plan.left.alias:
                outer_joins.append(
                    (
                        f"JOIN graph_nodes AS {left_alias} "
                        f"ON {left_alias}.id = narrowed.__left_id"
                    )
                )
                outer_alias_map[item.alias] = left_alias
                continue
            if item.alias == plan.right.alias:
                outer_joins.append(
                    (
                        f"JOIN graph_nodes AS {right_alias} "
                        f"ON {right_alias}.id = narrowed.__right_id"
                    )
                )
                outer_alias_map[item.alias] = right_alias
                continue
            if (
                plan.relationship.alias is not None
                and item.alias == plan.relationship.alias
            ):
                outer_joins.append(
                    (
                        f"JOIN graph_edges AS {edge_alias} "
                        f"ON {edge_alias}.id = narrowed.__edge_id"
                    )
                )
                outer_alias_map[item.alias] = edge_alias
        outer_join_params: list[object] = []
        outer_returns: list[_CompiledReturnItem] = []
        outer_property_join_aliases: dict[tuple[str, str, str], str] = {}
        _compile_return_items(
            alias_map=outer_alias_map,
            alias_kinds=alias_kinds,
            returns_to_compile=plan.returns,
            joins=outer_joins,
            params=outer_join_params,
            select_parts=outer_select_parts,
            returns=outer_returns,
            property_join_aliases=outer_property_join_aliases,
            alias_id_expressions=outer_alias_id_expressions,
            projected_property_expressions=outer_projected_properties,
        )
        outer_sql = [
            f"SELECT {', '.join(outer_select_parts)}",
            f"FROM ({' '.join(narrowed_sql)}) AS narrowed",
        ]
        outer_sql.extend(outer_joins)
        outer_sql.append(
            "ORDER BY "
            + ", ".join(
                f"narrowed.__order_{index} {direction}"
                for index, (_, direction) in enumerate(order_bindings)
            )
        )
        return _CompiledMatchQuery(
            sql=" ".join(outer_sql),
            params=tuple(
                from_params
                + join_params
                + order_params
                + where_params
                + outer_join_params
            ),
            returns=tuple(outer_returns),
        )

    _compile_return_items(
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        returns_to_compile=plan.returns,
        joins=joins,
        params=join_params,
        select_parts=select_parts,
        returns=returns,
        property_join_aliases=property_join_aliases,
    )

    select_keyword = "SELECT DISTINCT" if plan.distinct else "SELECT"
    sql = [f"{select_keyword} {', '.join(select_parts)}", from_clause]
    sql.extend(joins)
    sql.extend(order_joins)
    sql.append(f"WHERE {' AND '.join(where_parts)}")
    if order_parts:
        sql.append(f"ORDER BY {', '.join(order_parts)}")
    elif plan.distinct:
        order_by_projection = ", ".join(
            str(index) for index in range(1, len(select_parts) + 1)
        )
        sql.append(f"ORDER BY {order_by_projection}")
    if plan.limit is not None:
        sql.append(f"LIMIT {plan.limit}")
    if plan.skip is not None:
        if plan.limit is None:
            sql.append("LIMIT -1")
        sql.append(f"OFFSET {plan.skip}")

    return _CompiledMatchQuery(
        sql=" ".join(sql),
        params=tuple(from_params + join_params + order_params + where_params),
        returns=tuple(returns),
    )


def _can_use_relationship_disjunction_identity_union(
    plan: MatchRelationshipPlan,
) -> bool:
    """Return whether one relationship MATCH can union disjunct identities first."""

    return (
        not _predicates_are_simple_conjunction(plan.predicates)
        and plan.relationship.alias is not None
    )


def _compile_match_relationship_disjunction_plan(
    plan: MatchRelationshipPlan,
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    left_alias: str,
    edge_alias: str,
    right_alias: str,
) -> _CompiledMatchQuery:
    """Compile a disjunctive relationship MATCH by unioning matched identities."""

    relationship_alias = plan.relationship.alias
    assert relationship_alias is not None

    identity_selects: list[str] = []
    identity_params: list[object] = []
    identity_returns = (
        ReturnItem(plan.left.alias, "id"),
        ReturnItem(relationship_alias, "id"),
        ReturnItem(plan.right.alias, "id"),
    )
    left_id_column = identity_returns[0].column_name
    edge_id_column = identity_returns[1].column_name
    right_id_column = identity_returns[2].column_name

    for disjunct_predicates in _group_predicates_by_disjunct(plan.predicates):
        branch_plan = MatchRelationshipPlan(
            left=plan.left,
            relationship=plan.relationship,
            right=plan.right,
            predicates=disjunct_predicates,
            returns=identity_returns,
            order_by=(),
            limit=None,
            distinct=False,
            skip=None,
        )
        branch_compiled = _compile_match_relationship_plan(branch_plan)
        identity_selects.append(
            (
                f'SELECT "{left_id_column}" AS __left_id, '
                f'"{edge_id_column}" AS __edge_id, '
                f'"{right_id_column}" AS __right_id '
                f"FROM ({branch_compiled.sql})"
            )
        )
        identity_params.extend(branch_compiled.params)

    select_parts: list[str] = []
    joins = [
        f"JOIN graph_nodes AS {left_alias} ON {left_alias}.id = matched.__left_id",
        f"JOIN graph_edges AS {edge_alias} ON {edge_alias}.id = matched.__edge_id",
        (
            f"JOIN graph_nodes AS {right_alias} "
            f"ON {right_alias}.id = matched.__right_id"
        ),
    ]
    order_joins: list[str] = []
    join_params: list[object] = []
    order_params: list[object] = []
    order_parts: list[str] = []
    returns: list[_CompiledReturnItem] = []
    property_join_aliases: dict[tuple[str, str, str], str] = {}

    _compile_return_items(
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        returns_to_compile=plan.returns,
        joins=joins,
        params=join_params,
        select_parts=select_parts,
        returns=returns,
        property_join_aliases=property_join_aliases,
    )
    _compile_order_items(
        alias_map=alias_map,
        alias_kinds=alias_kinds,
        order_to_compile=plan.order_by,
        joins=order_joins,
        params=order_params,
        order_parts=order_parts,
        property_join_aliases=property_join_aliases,
    )

    select_keyword = "SELECT DISTINCT" if plan.distinct else "SELECT"
    sql = [
        f"{select_keyword} {', '.join(select_parts)}",
        f"FROM ({' UNION '.join(identity_selects)}) AS matched",
    ]
    sql.extend(joins)
    sql.extend(order_joins)
    if order_parts:
        sql.append(f"ORDER BY {', '.join(order_parts)}")
    elif plan.distinct:
        order_by_projection = ", ".join(
            str(index) for index in range(1, len(select_parts) + 1)
        )
        sql.append(f"ORDER BY {order_by_projection}")
    if plan.limit is not None:
        sql.append(f"LIMIT {plan.limit}")
    if plan.skip is not None:
        if plan.limit is None:
            sql.append("LIMIT -1")
        sql.append(f"OFFSET {plan.skip}")

    return _CompiledMatchQuery(
        sql=" ".join(sql),
        params=tuple(identity_params + join_params + order_params),
        returns=tuple(returns),
    )


def _compile_property_constraints(
    alias: str,
    constraints: list[PropertyConstraint],
    params: list[object],
    *,
    node_table: str = "graph_node_properties",
    id_column: str = "node_id",
) -> list[str]:
    """Compile bound property equality constraints into filtered property joins."""

    joins: list[str] = []

    for index, constraint in enumerate(constraints):
        filter_alias = f"{alias}_filter_{index}"
        encoded_value, value_type = _encode_property_value(constraint.value)
        if encoded_value is None:
            joins.append(
                f"JOIN {node_table} AS {filter_alias} "
                f"ON {filter_alias}.{id_column} = {alias}.id "
                f"AND {filter_alias}.key = ? "
                f"AND {filter_alias}.value IS NULL "
                f"AND {filter_alias}.value_type = ?"
            )
            params.extend((constraint.field, value_type))
            continue

        joins.append(
            f"JOIN {node_table} AS {filter_alias} "
            f"ON {filter_alias}.{id_column} = {alias}.id "
            f"AND {filter_alias}.key = ? "
            f"AND {filter_alias}.value = ? "
            f"AND {filter_alias}.value_type = ?"
        )
        params.extend((constraint.field, encoded_value, value_type))

    return joins


def _compile_node_from_clause(
    alias: str,
    anchor_constraint: PropertyConstraint | None,
    params: list[object],
) -> str:
    """Build the FROM clause for a node match, optionally anchored by one property.

    When an anchor constraint is present, the query starts from the property table so
    the database can narrow candidate node ids before joining back to graph_nodes.
    """

    if anchor_constraint is None:
        return f"FROM graph_nodes AS {alias}"

    anchor_alias = f"{alias}_anchor"
    encoded_value, value_type = _encode_property_value(anchor_constraint.value)
    if encoded_value is None:
        params.extend((anchor_constraint.field, value_type))
        return (
            f"FROM graph_node_properties AS {anchor_alias} "
            f"JOIN graph_nodes AS {alias} ON {alias}.id = {anchor_alias}.node_id "
            f"AND {anchor_alias}.key = ? "
            f"AND {anchor_alias}.value IS NULL "
            f"AND {anchor_alias}.value_type = ?"
        )

    params.extend((anchor_constraint.field, encoded_value, value_type))
    return (
        f"FROM graph_node_properties AS {anchor_alias} "
        f"JOIN graph_nodes AS {alias} ON {alias}.id = {anchor_alias}.node_id "
        f"AND {anchor_alias}.key = ? "
        f"AND {anchor_alias}.value = ? "
        f"AND {anchor_alias}.value_type = ?"
    )


def _node_property_constraints(
    node: NodePattern,
    predicates: tuple[Predicate, ...],
) -> list[PropertyConstraint]:
    """Collect one node pattern's property-equality constraints."""

    constraints: list[PropertyConstraint] = []
    use_predicate_constraints = _predicates_are_simple_conjunction(predicates)

    for predicate in predicates:
        if (
            predicate.alias != node.alias
            or not use_predicate_constraints
            or predicate.operator != "="
            or predicate.field in {"id", "label"}
        ):
            continue
        constraints.append(
            PropertyConstraint(
                predicate.alias,
                predicate.field,
                _require_property_value(predicate.value),
            )
        )

    for key, value in node.properties:
        constraints.append(
            PropertyConstraint(node.alias, key, _require_property_value(value))
        )

    return constraints


def _relationship_property_constraints(
    relationship: RelationshipPattern,
    predicates: tuple[Predicate, ...],
) -> list[PropertyConstraint]:
    """Collect one relationship pattern's property-equality constraints."""

    constraints = [
        PropertyConstraint(
            relationship.alias or "edge_rel",
            key,
            _require_property_value(value),
        )
        for key, value in relationship.properties
    ]

    if relationship.alias is None:
        return constraints

    use_predicate_constraints = _predicates_are_simple_conjunction(predicates)

    for predicate in predicates:
        if (
            predicate.alias != relationship.alias
            or not use_predicate_constraints
            or predicate.operator != "="
            or predicate.field in {"id", "type"}
        ):
            continue
        constraints.append(
            PropertyConstraint(
                relationship.alias,
                predicate.field,
                _require_property_value(predicate.value),
            )
        )

    return constraints


def _pick_anchor_constraint(
    constraints: list[PropertyConstraint],
) -> PropertyConstraint | None:
    """Choose the best property constraint to anchor a compiled match query.

    The heuristic is intentionally simple: prefer strings, then numeric values, then
    booleans, and only use null equality as a last resort.
    """

    if not constraints:
        return None

    ranked_constraints = sorted(
        enumerate(constraints),
        key=lambda item: (_constraint_rank(item[1]), item[0]),
        reverse=True,
    )
    return ranked_constraints[0][1]


def _pick_relationship_anchor_alias(
    left_alias: str,
    left_constraints: list[PropertyConstraint],
    right_alias: str,
    right_constraints: list[PropertyConstraint],
) -> str:
    """Choose which side of a relationship pattern should anchor compilation."""

    left_anchor = _pick_anchor_constraint(left_constraints)
    right_anchor = _pick_anchor_constraint(right_constraints)

    if left_anchor is None and right_anchor is None:
        return left_alias
    if right_anchor is None:
        return left_alias
    if left_anchor is None:
        return right_alias
    if _constraint_rank(right_anchor) > _constraint_rank(left_anchor):
        return right_alias
    return left_alias


def _without_anchor_constraint(
    constraints: list[PropertyConstraint],
    anchor_constraint: PropertyConstraint | None,
) -> list[PropertyConstraint]:
    """Return the remaining constraints after removing the chosen anchor once."""

    if anchor_constraint is None:
        return list(constraints)

    remaining = list(constraints)
    for index, constraint in enumerate(remaining):
        if constraint == anchor_constraint:
            del remaining[index]
            break
    return remaining


def _constraint_rank(constraint: PropertyConstraint) -> int:
    """Score one constraint for anchor selection heuristics."""

    if constraint.value is None:
        return 0
    if isinstance(constraint.value, bool):
        return 1
    if isinstance(constraint.value, (int, float)):
        return 2
    return 3


def _is_raw_return_field(
    *,
    alias_kind: Literal["node", "relationship"],
    field: str,
) -> bool:
    """Return whether one RETURN field comes from the base graph row directly."""

    return field == "id" or (
        alias_kind == "node" and field == "label"
    ) or (alias_kind == "relationship" and field == "type")


def _compile_return_items(
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    returns_to_compile: tuple[ReturnItem, ...],
    joins: list[str],
    params: list[object],
    select_parts: list[str],
    returns: list[_CompiledReturnItem],
    property_join_aliases: dict[tuple[str, str, str], str],
    alias_id_expressions: dict[str, str] | None = None,
    projected_property_expressions: dict[
        tuple[str, str], tuple[str, str]
    ] | None = None,
) -> None:
    """Compile RETURN items into projections and supporting property joins."""

    if alias_id_expressions is None:
        alias_id_expressions = {}
    if projected_property_expressions is None:
        projected_property_expressions = {}

    for index, item in enumerate(returns_to_compile):
        if item.alias not in alias_kinds:
            raise ValueError(
                f"HumemCypher v0 cannot RETURN unknown alias {item.alias!r}."
            )

        table_alias = alias_map.get(item.alias)
        alias_kind = alias_kinds[item.alias]
        if _is_raw_return_field(alias_kind=alias_kind, field=item.field):
            if table_alias is None:
                raise ValueError(
                    f"HumemCypher v0 cannot RETURN raw field {item.alias}.{item.field}."
                )

        if item.field == "id":
            select_parts.append(f"{table_alias}.id AS \"{item.column_name}\"")
            returns.append(_CompiledReturnItem(item.column_name, "raw"))
            continue

        if alias_kind == "node" and item.field == "label":
            select_parts.append(f"{table_alias}.label AS \"{item.column_name}\"")
            returns.append(_CompiledReturnItem(item.column_name, "raw"))
            continue

        if alias_kind == "relationship" and item.field == "type":
            select_parts.append(f"{table_alias}.type AS \"{item.column_name}\"")
            returns.append(_CompiledReturnItem(item.column_name, "raw"))
            continue

        projected_property = projected_property_expressions.get(
            (item.alias, item.field)
        )
        if projected_property is not None:
            value_expression, value_type_expression = projected_property
            select_parts.append(f"{value_expression} AS \"__value_{index}\"")
            select_parts.append(
                f"{value_type_expression} AS \"__value_type_{index}\""
            )
            returns.append(_CompiledReturnItem(item.column_name, "property"))
            continue

        property_alias = _ensure_property_join(
            table_alias=table_alias or item.alias,
            alias_kind=alias_kind,
            field=item.field,
            join_alias=f"{(table_alias or item.alias)}_return_{index}",
            joins=joins,
            params=params,
            property_join_aliases=property_join_aliases,
            id_expression=alias_id_expressions.get(item.alias),
        )
        select_parts.append(f"{property_alias}.value AS \"__value_{index}\"")
        select_parts.append(
            f"{property_alias}.value_type AS \"__value_type_{index}\""
        )
        returns.append(_CompiledReturnItem(item.column_name, "property"))


def _ensure_property_join(
    *,
    table_alias: str,
    alias_kind: Literal["node", "relationship"],
    field: str,
    join_alias: str,
    joins: list[str],
    params: list[object],
    property_join_aliases: dict[tuple[str, str, str], str],
    id_expression: str | None = None,
) -> str:
    """Return one shared property join alias for one alias/field lookup."""

    join_key = (table_alias, alias_kind, field)
    existing_alias = property_join_aliases.get(join_key)
    if existing_alias is not None:
        return existing_alias

    property_table = (
        "graph_node_properties" if alias_kind == "node" else "graph_edge_properties"
    )
    id_column = "node_id" if alias_kind == "node" else "edge_id"
    join_id_expression = id_expression or f"{table_alias}.id"
    joins.append(
        f"LEFT JOIN {property_table} AS {join_alias} "
        f"ON {join_alias}.{id_column} = {join_id_expression} "
        f"AND {join_alias}.key = ?"
    )
    params.append(field)
    property_join_aliases[join_key] = join_alias
    return join_alias


def _can_use_order_limit_projection_fastpath(
    plan: MatchRelationshipPlan,
    order_bindings: list[tuple[str, str]],
) -> bool:
    """Return whether one relationship MATCH can narrow rows before projection."""

    return (
        plan.limit is not None
        and plan.skip is None
        and not plan.distinct
        and bool(order_bindings)
        and len(plan.order_by) == 1
        and plan.order_by[0].direction.lower() == "desc"
    )


def _compile_predicates(
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    predicates: tuple[Predicate, ...],
    where_parts: list[str],
    where_params: list[object],
) -> None:
    """Compile supported WHERE predicates into SQL fragments and params."""

    if not predicates:
        return

    if not _predicates_are_simple_conjunction(predicates):
        grouped_parts: dict[int, list[str]] = {}
        grouped_params: dict[int, list[object]] = {}
        disjunct_order: list[int] = []

        for predicate in predicates:
            if predicate.disjunct_index not in grouped_parts:
                grouped_parts[predicate.disjunct_index] = []
                grouped_params[predicate.disjunct_index] = []
                disjunct_order.append(predicate.disjunct_index)

            sql, params = _compile_single_predicate(
                predicate=predicate,
                alias_map=alias_map,
                alias_kinds=alias_kinds,
                filter_index=(
                    len(grouped_parts[predicate.disjunct_index])
                    + predicate.disjunct_index * 100
                ),
            )
            grouped_parts[predicate.disjunct_index].append(sql)
            grouped_params[predicate.disjunct_index].extend(params)

        disjunct_sql: list[str] = []
        disjunct_params: list[object] = []
        for disjunct_index in disjunct_order:
            disjunct_sql.append(
                "(" + " AND ".join(grouped_parts[disjunct_index]) + ")"
            )
            disjunct_params.extend(grouped_params[disjunct_index])

        where_parts.append("(" + " OR ".join(disjunct_sql) + ")")
        where_params.extend(disjunct_params)
        return

    for predicate in predicates:
        sql, params = _compile_single_predicate(
            predicate=predicate,
            alias_map=alias_map,
            alias_kinds=alias_kinds,
            filter_index=len(where_parts),
        )
        if predicate.operator == "=" and _is_property_predicate(predicate, alias_kinds):
            continue
        where_parts.append(sql)
        where_params.extend(params)


def _compile_single_predicate(
    *,
    predicate: Predicate,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    filter_index: int,
) -> tuple[str, tuple[object, ...]]:
    """Compile one predicate into SQL plus positional params."""

    if predicate.alias not in alias_map:
        raise ValueError(
            f"HumemCypher v0 cannot filter on unknown alias {predicate.alias!r}."
        )

    table_alias = alias_map[predicate.alias]
    alias_kind = alias_kinds[predicate.alias]
    predicate_value = _require_scalar_query_param(predicate.value)

    if predicate.field == "id":
        _validate_direct_predicate_operator(
            predicate.operator,
            field="id",
            alias_kind=alias_kind,
        )
        return f"{table_alias}.id {predicate.operator} ?", (predicate_value,)

    if alias_kind == "node" and predicate.field == "label":
        _validate_direct_predicate_operator(
            predicate.operator,
            field="label",
            alias_kind=alias_kind,
        )
        return f"{table_alias}.label {predicate.operator} ?", (predicate_value,)

    if alias_kind == "relationship" and predicate.field == "type":
        _validate_direct_predicate_operator(
            predicate.operator,
            field="type",
            alias_kind=alias_kind,
        )
        return f"{table_alias}.type {predicate.operator} ?", (predicate_value,)

    return _compile_property_predicate_filter(
        table_alias=table_alias,
        alias_kind=alias_kind,
        field=predicate.field,
        operator=predicate.operator,
        value=predicate_value,
        filter_index=filter_index,
    )


def _is_property_predicate(
    predicate: Predicate,
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> bool:
    """Return whether one predicate targets a stored property table."""

    alias_kind = alias_kinds[predicate.alias]
    if predicate.field == "id":
        return False
    if alias_kind == "node" and predicate.field == "label":
        return False
    if alias_kind == "relationship" and predicate.field == "type":
        return False
    return True


def _predicates_are_simple_conjunction(predicates: tuple[Predicate, ...]) -> bool:
    """Return whether all predicates belong to the single default AND clause."""

    return all(predicate.disjunct_index == 0 for predicate in predicates)


def _group_predicates_by_disjunct(
    predicates: tuple[Predicate, ...],
) -> tuple[tuple[Predicate, ...], ...]:
    """Group parsed predicates by their disjunct index while preserving order."""

    grouped: dict[int, list[Predicate]] = {}
    disjunct_order: list[int] = []

    for predicate in predicates:
        if predicate.disjunct_index not in grouped:
            grouped[predicate.disjunct_index] = []
            disjunct_order.append(predicate.disjunct_index)
        grouped[predicate.disjunct_index].append(
            Predicate(
                alias=predicate.alias,
                field=predicate.field,
                operator=predicate.operator,
                value=predicate.value,
                disjunct_index=0,
            )
        )

    return tuple(tuple(grouped[index]) for index in disjunct_order)


def _validate_direct_predicate_operator(
    operator: Literal[
        "=",
        "<",
        "<=",
        ">",
        ">=",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "IS NULL",
        "IS NOT NULL",
    ],
    *,
    field: str,
    alias_kind: Literal["node", "relationship"],
) -> None:
    """Reject unsupported comparison operators for direct graph fields."""

    if field == "id":
        if operator in {"IS NULL", "IS NOT NULL"}:
            raise ValueError(
                "HumemCypher v0 currently does not support null predicates for "
                f"{alias_kind} field {field!r}."
            )
        return
    if operator != "=":
        raise ValueError(
            "HumemCypher v0 currently supports only equality predicates for "
            f"{alias_kind} field {field!r}."
        )


def _compile_property_predicate_filter(
    *,
    table_alias: str,
    alias_kind: Literal["node", "relationship"],
    field: str,
    operator: Literal[
        "=",
        "<",
        "<=",
        ">",
        ">=",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "IS NULL",
        "IS NOT NULL",
    ],
    value: _ScalarQueryParam,
    filter_index: int,
) -> tuple[str, tuple[object, ...]]:
    """Compile one property predicate into a typed EXISTS filter."""

    property_table = (
        "graph_node_properties" if alias_kind == "node" else "graph_edge_properties"
    )
    property_alias = f"pf{filter_index}"
    id_column = "node_id" if alias_kind == "node" else "edge_id"

    if operator == "IS NULL":
        return (
            "NOT EXISTS ("
            f"SELECT 1 FROM {property_table} AS {property_alias} "
            f"WHERE {property_alias}.{id_column} = {table_alias}.id "
            f"AND {property_alias}.key = ? "
            f"AND {property_alias}.value_type != 'null'"
            ")",
            (field,),
        )

    if operator == "IS NOT NULL":
        return (
            "EXISTS ("
            f"SELECT 1 FROM {property_table} AS {property_alias} "
            f"WHERE {property_alias}.{id_column} = {table_alias}.id "
            f"AND {property_alias}.key = ? "
            f"AND {property_alias}.value_type != 'null'"
            ")",
            (field,),
        )

    if value is None:
        if operator != "=":
            raise ValueError(
                "HumemCypher v0 currently supports only equality and null "
                "predicates for null values."
            )
        return (
            "EXISTS ("
            f"SELECT 1 FROM {property_table} AS {property_alias} "
            f"WHERE {property_alias}.{id_column} = {table_alias}.id "
            f"AND {property_alias}.key = ? "
            f"AND {property_alias}.value IS NULL "
            f"AND {property_alias}.value_type = 'null'"
            ")",
            (field,),
        )

    if isinstance(value, str):
        if operator == "=":
            return (
                "EXISTS ("
                f"SELECT 1 FROM {property_table} AS {property_alias} "
                f"WHERE {property_alias}.{id_column} = {table_alias}.id "
                f"AND {property_alias}.key = ? "
                f"AND {property_alias}.value_type = 'string' "
                f"AND {property_alias}.value = ?"
                ")",
                (field, value),
            )
        if operator == "STARTS WITH":
            return (
                "EXISTS ("
                f"SELECT 1 FROM {property_table} AS {property_alias} "
                f"WHERE {property_alias}.{id_column} = {table_alias}.id "
                f"AND {property_alias}.key = ? "
                f"AND {property_alias}.value_type = 'string' "
                f"AND substr({property_alias}.value, 1, length(?)) = ?"
                ")",
                (field, value, value),
            )
        if operator == "ENDS WITH":
            return (
                "EXISTS ("
                f"SELECT 1 FROM {property_table} AS {property_alias} "
                f"WHERE {property_alias}.{id_column} = {table_alias}.id "
                f"AND {property_alias}.key = ? "
                f"AND {property_alias}.value_type = 'string' "
                f"AND length({property_alias}.value) >= length(?) "
                f"AND substr({property_alias}.value, "
                f"length({property_alias}.value) - length(?) + 1"
                f") = ?"
                ")",
                (field, value, value, value),
            )
        if operator == "CONTAINS":
            return (
                "EXISTS ("
                f"SELECT 1 FROM {property_table} AS {property_alias} "
                f"WHERE {property_alias}.{id_column} = {table_alias}.id "
                f"AND {property_alias}.key = ? "
                f"AND {property_alias}.value_type = 'string' "
                f"AND instr({property_alias}.value, ?) > 0"
                ")",
                (field, value),
            )
        raise ValueError(
            "HumemCypher v0 currently supports only equality, STARTS WITH, "
            "ENDS WITH, and CONTAINS for string non-property-system predicates."
        )

    numeric_value = _coerce_numeric_predicate_value(value)
    return (
        "EXISTS ("
        f"SELECT 1 FROM {property_table} AS {property_alias} "
        f"WHERE {property_alias}.{id_column} = {table_alias}.id "
        f"AND {property_alias}.key = ? "
        f"AND {property_alias}.value_type IN ('integer', 'real', 'boolean') "
        "AND CASE "
        f"WHEN {property_alias}.value_type = 'integer' "
        f"THEN CAST({property_alias}.value AS INTEGER) "
        f"WHEN {property_alias}.value_type = 'real' "
        f"THEN CAST({property_alias}.value AS REAL) "
        f"WHEN {property_alias}.value_type = 'boolean' "
        f"THEN CASE WHEN {property_alias}.value = 'true' THEN 1 ELSE 0 END "
        f"END {operator} ?"
        ")",
        (field, numeric_value),
    )


def _coerce_numeric_predicate_value(value: _ScalarQueryParam) -> int | float:
    """Convert one scalar predicate value into a numeric comparison literal."""

    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return value
    raise ValueError(
        "HumemCypher v0 currently supports only numeric, boolean, or string values "
        "for non-equality predicates."
    )


def _compile_order_bindings(
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    order_to_compile: tuple[OrderItem, ...],
    joins: list[str],
    params: list[object],
    property_join_aliases: dict[tuple[str, str, str], str],

) -> list[tuple[str, str]]:
    """Compile ORDER BY items into reusable expressions and join requirements.

    Property ordering is expressed in a type-aware way so integers, reals, booleans,
    and strings can be sorted without losing their stored scalar semantics.
    """

    order_bindings: list[tuple[str, str]] = []

    for index, item in enumerate(order_to_compile):
        if item.alias not in alias_map:
            raise ValueError(
                f"HumemCypher v0 cannot ORDER BY unknown alias {item.alias!r}."
            )

        table_alias = alias_map[item.alias]
        alias_kind = alias_kinds[item.alias]
        direction = item.direction.upper()

        if item.field == "id":
            order_bindings.append((f"{table_alias}.id", direction))
            continue

        if alias_kind == "node" and item.field == "label":
            order_bindings.append((f"{table_alias}.label", direction))
            continue

        if alias_kind == "relationship" and item.field == "type":
            order_bindings.append((f"{table_alias}.type", direction))
            continue

        property_alias = _ensure_property_join(
            table_alias=table_alias,
            alias_kind=alias_kind,
            field=item.field,
            join_alias=f"{table_alias}_order_{index}",
            joins=joins,
            params=params,
            property_join_aliases=property_join_aliases,
        )
        order_bindings.extend(
            [
                (_compile_numeric_order_expression(property_alias), direction),
                (_compile_string_order_expression(property_alias), direction),
            ]
        )

    return order_bindings


def _compile_order_items(
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    order_to_compile: tuple[OrderItem, ...],
    joins: list[str],
    params: list[object],
    order_parts: list[str],
    property_join_aliases: dict[tuple[str, str, str], str],
) -> None:
    """Compile ORDER BY items into SQL expressions and any needed property joins."""

    order_parts.extend(
        f"{expression} {direction}"
        for expression, direction in _compile_order_bindings(
            alias_map=alias_map,
            alias_kinds=alias_kinds,
            order_to_compile=order_to_compile,
            joins=joins,
            params=params,
            property_join_aliases=property_join_aliases,
        )
    )


def _compile_numeric_order_expression(property_alias: str) -> str:
    """Build one numeric ORDER BY expression for a typed property join."""

    return (
        "CASE "
        f"WHEN {property_alias}.value_type = 'integer' "
        f"THEN CAST({property_alias}.value AS INTEGER) "
        f"WHEN {property_alias}.value_type = 'real' "
        f"THEN CAST({property_alias}.value AS REAL) "
        f"WHEN {property_alias}.value_type = 'boolean' "
        f"THEN CASE WHEN {property_alias}.value = 'true' THEN 1 ELSE 0 END "
        "END "
    )


def _compile_string_order_expression(property_alias: str) -> str:
    """Build one string/null ORDER BY expression for a typed property join."""

    return (
        "CASE "
        f"WHEN {property_alias}.value_type IN ('string', 'null') "
        f"THEN {property_alias}.value "
        "END "
    )


def _decode_match_result(
    result: QueryResult,
    returns: tuple[_CompiledReturnItem, ...],
) -> QueryResult:
    """Decode one raw relational MATCH result into public Cypher values."""

    decoded_rows: list[tuple[object, ...]] = []

    for row in result.rows:
        decoded_row: list[object] = []
        index = 0

        for item in returns:
            if item.kind == "raw":
                decoded_row.append(row[index])
                index += 1
                continue

            decoded_row.append(_decode_property_value(row[index], row[index + 1]))
            index += 2

        decoded_rows.append(tuple(decoded_row))

    return QueryResult(
        rows=tuple(decoded_rows),
        columns=tuple(item.column_name for item in returns),
        route=result.route,
        query_type="cypher",
        rowcount=result.rowcount,
    )


def _insert_node(sqlite: _SQLiteEngine, node: NodePattern) -> int:
    """Insert one labeled node plus its properties into the graph store."""

    if node.label is None:
        raise ValueError("HumemCypher v0 CREATE node patterns require a label.")

    sqlite.execute(
        "INSERT INTO graph_nodes (label) VALUES (?)",
        (node.label,),
        query_type="cypher",
    )
    node_id_row = sqlite.execute(
        "SELECT last_insert_rowid() AS node_id",
        query_type="cypher",
    ).first()
    if node_id_row is None:
        raise ValueError("HumemCypher v0 could not resolve the created node id.")
    node_id = node_id_row[0]

    property_writes = _plan_node_property_writes(node.properties)
    _persist_node_property_writes(
        sqlite,
        int(node_id),
        property_writes,
        mode="insert",
    )

    return int(node_id)


def _insert_edge(
    sqlite: _SQLiteEngine,
    relationship: RelationshipPattern,
    left_id: int,
    right_id: int,
) -> int:
    """Insert one directed edge plus its properties into the graph store."""

    relationship_type_names = _relationship_type_names(relationship)
    if len(relationship_type_names) != 1:
        raise ValueError(
            "HumemCypher v0 CREATE relationship patterns require exactly one "
            "relationship type."
        )

    from_node_id = left_id
    to_node_id = right_id
    if relationship.direction == "in":
        from_node_id = right_id
        to_node_id = left_id

    sqlite.execute(
        "INSERT INTO graph_edges (type, from_node_id, to_node_id) VALUES (?, ?, ?)",
        (relationship_type_names[0], from_node_id, to_node_id),
        query_type="cypher",
    )
    edge_id_row = sqlite.execute(
        "SELECT last_insert_rowid() AS edge_id",
        query_type="cypher",
    ).first()
    if edge_id_row is None:
        raise ValueError("HumemCypher v0 could not resolve the created edge id.")
    edge_id = edge_id_row[0]

    for key, value in relationship.properties:
        encoded_value, value_type = _encode_property_value(
            _require_property_value(value)
        )
        sqlite.execute(
            "INSERT INTO graph_edge_properties (edge_id, key, value, value_type) "
            "VALUES (?, ?, ?, ?)",
            (edge_id, key, encoded_value, value_type),
            query_type="cypher",
        )

    return int(edge_id)


def _upsert_node_property(
    sqlite: _SQLiteEngine,
    node_id: int,
    key: str,
    value: PropertyValue,
) -> None:
    """Insert or replace one graph node property and sync vector storage if needed."""

    property_writes = _plan_node_property_writes(((key, value),))
    _persist_node_property_writes(
        sqlite,
        node_id,
        property_writes,
        mode="upsert",
    )


def _plan_node_property_writes(
    properties: PropertyItems | tuple[tuple[str, PropertyValue], ...],
) -> tuple[_EncodedNodePropertyWrite, ...]:
    """Encode one node-property batch into explicit property write objects."""

    writes: list[_EncodedNodePropertyWrite] = []
    for key, value in properties:
        property_value = cast(PropertyValue, value)
        encoded_value, value_type = _encode_property_value(property_value)
        writes.append(
            _EncodedNodePropertyWrite(
                key=key,
                encoded_value=encoded_value,
                value_type=value_type,
                vector_value=(
                    cast(_VectorPropertyValue, property_value)
                    if value_type == "vector"
                    else None
                ),
            )
        )

    return tuple(writes)


def _persist_node_property_writes(
    sqlite: _SQLiteEngine,
    node_id: int,
    property_writes: tuple[_EncodedNodePropertyWrite, ...],
    *,
    mode: Literal["insert", "upsert"],
) -> None:
    """Persist encoded node-property writes and sync graph-node vectors."""

    _validate_node_vector_property_writes(
        sqlite,
        node_id,
        property_writes,
        mode=mode,
    )

    vector_rows: list[tuple[int, Sequence[float]]] = []
    for property_write in property_writes:
        if mode == "insert":
            sqlite.execute(
                "INSERT INTO graph_node_properties (node_id, key, value, value_type) "
                "VALUES (?, ?, ?, ?)",
                (
                    node_id,
                    property_write.key,
                    property_write.encoded_value,
                    property_write.value_type,
                ),
                query_type="cypher",
            )
        else:
            sqlite.execute(
                "INSERT INTO graph_node_properties (node_id, key, value, value_type) "
                "VALUES (?, ?, ?, ?) "
                "ON CONFLICT(node_id, key) DO UPDATE SET "
                "value = excluded.value, value_type = excluded.value_type",
                (
                    node_id,
                    property_write.key,
                    property_write.encoded_value,
                    property_write.value_type,
                ),
                query_type="cypher",
            )

        if property_write.vector_value is not None:
            vector_rows.append((int(node_id), property_write.vector_value))

    if len(vector_rows) > 1:
        raise ValueError(
            "HumemCypher v0 currently supports at most one vector-valued property "
            "per node write batch."
        )
    if vector_rows:
        _sync_graph_node_vectors(sqlite, vector_rows, mode=mode)


def _validate_node_vector_property_writes(
    sqlite: _SQLiteEngine,
    node_id: int,
    property_writes: tuple[_EncodedNodePropertyWrite, ...],
    *,
    mode: Literal["insert", "upsert"],
) -> None:
    """Reject node-property writes that would store multiple vectors per node."""

    vector_keys = tuple(
        property_write.key
        for property_write in property_writes
        if property_write.vector_value is not None
    )
    if not vector_keys:
        return

    if len(vector_keys) > 1:
        raise ValueError(
            "HumemCypher v0 currently supports at most one vector-valued property "
            "per node write batch."
        )

    sql = (
        "SELECT key FROM graph_node_properties "
        "WHERE node_id = ? AND value_type = 'vector'"
    )
    params: list[object] = [node_id]
    if mode == "upsert":
        assert vector_keys
        sql += " AND key <> ?"
        params.append(vector_keys[0])

    existing_row = sqlite.execute(
        sql,
        tuple(params),
        query_type="cypher",
    ).first()
    if existing_row is not None:
        raise ValueError(
            "HumemCypher v0 currently supports only one vector-valued property "
            "per node."
        )


def _sync_graph_node_vectors(
    sqlite: _SQLiteEngine,
    vector_rows: Sequence[tuple[int, Sequence[float]]],
    *,
    mode: Literal["insert", "upsert"],
) -> None:
    """Persist graph-node vectors for encoded Cypher node-property writes."""

    _ensure_vector_schema(sqlite)
    if mode == "insert":
        _insert_vectors(sqlite, vector_rows, target="graph_node", namespace="")
        return

    _upsert_vectors(
        sqlite,
        vector_rows,
        target="graph_node",
        namespace="",
    )


def _parse_node_pattern(
    text: str,
    *,
    require_label: bool = False,
    default_alias: str | None = None,
) -> NodePattern:
    """Parse one node-pattern body into the narrow NodePattern structure."""

    match = _NODE_PATTERN_RE.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"HumemCypher v0 could not parse node pattern: {text!r}")

    label = match.group("label")
    if require_label and label is None:
        raise ValueError("HumemCypher v0 CREATE patterns require labeled nodes.")

    alias = match.group("alias") or default_alias
    if alias is None:
        raise ValueError(
            "HumemCypher v0 currently requires a node alias unless the pattern "
            "position admits an anonymous node."
        )

    return NodePattern(
        alias=alias,
        label=label,
        properties=_parse_properties(match.group("properties")),
    )


def _parse_relationship_pattern(
    text: str,
    direction: Literal["out", "in"],
) -> RelationshipPattern:
    """Parse one relationship-pattern body into the narrow edge structure."""

    match = _REL_PATTERN_RE.fullmatch(text.strip())
    if match is None:
        raise ValueError(
            f"HumemCypher v0 could not parse relationship pattern: {text!r}"
        )

    return RelationshipPattern(
        alias=match.group("alias"),
        type_name=match.group("type"),
        direction=direction,
        properties=_parse_properties(match.group("properties")),
    )


def _relationship_type_names(relationship: RelationshipPattern) -> tuple[str, ...]:
    """Return the admitted relationship type names for one relationship pattern."""

    if relationship.type_name is None:
        return ()

    return tuple(part.strip() for part in relationship.type_name.split("|"))


def _compile_relationship_type_filter(
    edge_alias: str,
    relationship_type_names: tuple[str, ...],
) -> str:
    """Compile one admitted relationship type token into SQL filter text."""

    if len(relationship_type_names) == 1:
        return f"{edge_alias}.type = ?"

    placeholders = ", ".join("?" for _ in relationship_type_names)
    return f"{edge_alias}.type IN ({placeholders})"


def _parse_return_items(text: str) -> tuple[ReturnItem, ...]:
    """Parse one RETURN clause into ordered alias.field items."""

    items: list[ReturnItem] = []

    for raw_item in _split_comma_separated(text):
        match = _RETURN_ITEM_RE.fullmatch(raw_item.strip())
        if match is None:
            raise ValueError(
                "HumemCypher v0 RETURN items must look like alias.field."
            )
        items.append(ReturnItem(match.group("alias"), match.group("field")))

    if not items:
        raise ValueError("HumemCypher v0 RETURN clauses cannot be empty.")

    return tuple(items)


def _parse_order_items(text: str) -> tuple[OrderItem, ...]:
    """Parse one ORDER BY clause into ordered sort items."""

    items: list[OrderItem] = []

    for raw_item in _split_comma_separated(text):
        item_text = raw_item.strip()
        parts = item_text.rsplit(None, 1)
        direction = "asc"
        direction: Literal["asc", "desc"] = "asc"
        target = item_text

        if len(parts) == 2 and parts[1].lower() in {"asc", "desc"}:
            target = parts[0]
            direction = cast(Literal["asc", "desc"], parts[1].lower())

        match = _RETURN_ITEM_RE.fullmatch(target.strip())
        if match is None:
            raise ValueError(
                "HumemCypher v0 ORDER BY items must look like alias.field "
                "optionally followed by ASC or DESC."
            )
        items.append(
            OrderItem(
                alias=match.group("alias"),
                field=match.group("field"),
                direction=direction,
            )
        )

    if not items:
        raise ValueError("HumemCypher v0 ORDER BY clauses cannot be empty.")

    return tuple(items)


def _split_return_clause(
    text: str,
) -> tuple[str, tuple[OrderItem, ...], int | None, bool, int | None]:
    """Split a RETURN clause into projection, ordering, limit, distinct, and skip."""

    order_by_match = re.search(r"\border\s+by\b", text, flags=re.IGNORECASE)
    skip_match = re.search(r"\b(skip|offset)\b", text, flags=re.IGNORECASE)
    limit_match = re.search(r"\blimit\b", text, flags=re.IGNORECASE)

    if order_by_match is None and skip_match is None and limit_match is None:
        return_text, distinct = _parse_return_projection(text)
        return return_text, (), None, distinct, None

    clause_positions = [
        match.start()
        for match in (order_by_match, skip_match, limit_match)
        if match is not None
    ]
    returns_text = text[: min(clause_positions)].strip()

    if order_by_match is not None and (
        (skip_match is not None and skip_match.start() < order_by_match.start())
        or (limit_match is not None and limit_match.start() < order_by_match.start())
    ):
        raise ValueError(
            "HumemCypher v0 requires ORDER BY to appear before SKIP/OFFSET and LIMIT."
        )
    if (
        skip_match is not None
        and limit_match is not None
        and limit_match.start() < skip_match.start()
    ):
        raise ValueError(
            "HumemCypher v0 requires SKIP/OFFSET to appear before LIMIT."
        )

    order_by: tuple[OrderItem, ...] = ()
    if order_by_match is not None:
        order_end = len(text)
        if skip_match is not None:
            order_end = skip_match.start()
        elif limit_match is not None:
            order_end = limit_match.start()
        order_text = text[order_by_match.end():order_end].strip()
        order_by = _parse_order_items(order_text)

    skip: int | None = None
    if skip_match is not None:
        skip_end = limit_match.start() if limit_match is not None else len(text)
        skip = _parse_skip_clause(
            text[skip_match.end():skip_end].strip(),
            clause_name=skip_match.group(1).upper(),
        )

    limit: int | None = None
    if limit_match is not None:
        limit = _parse_limit_clause(text[limit_match.end():].strip())

    return_text, distinct = _parse_return_projection(returns_text)
    return return_text, order_by, limit, distinct, skip


def _parse_return_projection(text: str) -> tuple[str, bool]:
    """Parse a RETURN projection and whether it is DISTINCT."""

    projection_text = text.strip()
    distinct_match = re.match(r"distinct\b", projection_text, flags=re.IGNORECASE)
    if distinct_match is None:
        return projection_text, False

    projection_text = projection_text[distinct_match.end():].strip()
    if not projection_text:
        raise ValueError("HumemCypher v0 RETURN DISTINCT clauses cannot be empty.")
    return projection_text, True


def _parse_skip_clause(text: str, *, clause_name: str = "SKIP") -> int:
    """Parse the small SKIP/OFFSET subset accepted by HumemCypher v0."""

    if not text:
        raise ValueError(f"HumemCypher v0 {clause_name} clauses cannot be empty.")
    if not re.fullmatch(r"\d+", text):
        raise ValueError(
            f"HumemCypher v0 {clause_name} currently requires an integer literal."
        )

    skip = int(text)
    if skip < 0:
        raise ValueError(f"HumemCypher v0 {clause_name} must be at least 0.")
    return skip


def _parse_limit_clause(text: str) -> int:
    """Parse the small LIMIT subset accepted by HumemCypher v0."""

    if not text:
        raise ValueError("HumemCypher v0 LIMIT clauses cannot be empty.")
    if not re.fullmatch(r"\d+", text):
        raise ValueError("HumemCypher v0 LIMIT currently requires an integer literal.")

    limit = int(text)
    if limit < 1:
        raise ValueError("HumemCypher v0 LIMIT must be at least 1.")
    return limit


def _parse_predicates(text: str) -> tuple[Predicate, ...]:
    """Parse one WHERE clause into `AND`/`OR` comparison predicates."""

    predicates: list[Predicate] = []

    for disjunct_index, disjunct in enumerate(
        _parse_boolean_predicate_groups(text)
    ):
        for item in disjunct:
            try:
                left_text, operator, value_text = _split_predicate_comparison(item)
            except ValueError as exc:
                raise ValueError(
                    "HumemCypher v0 WHERE items must look like alias.field OP value."
                ) from exc
            match = _RETURN_ITEM_RE.fullmatch(left_text.strip())
            if match is None:
                raise ValueError(
                    "HumemCypher v0 WHERE items must look like alias.field OP value."
                )
            parsed_value: CypherValue
            if operator in {"IS NULL", "IS NOT NULL"}:
                if value_text.strip():
                    raise ValueError(
                        "HumemCypher v0 null predicates cannot include a trailing "
                        "literal value."
                    )
                parsed_value = None
            else:
                parsed_value = _parse_literal(value_text.strip())
            predicates.append(
                Predicate(
                    alias=match.group("alias"),
                    field=match.group("field"),
                    operator=operator,
                    disjunct_index=disjunct_index,
                    value=parsed_value,
                )
            )

    if not predicates:
        raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")

    return tuple(predicates)


def _parse_boolean_predicate_groups(text: str) -> list[list[str]]:
    """Parse a boolean predicate expression into disjunctive normal form."""

    tokens = _tokenize_boolean_expression(text)
    if not tokens:
        raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")

    parser = _BooleanPredicateParser(tokens)
    groups = parser.parse_expression()
    if parser.has_more_tokens():
        raise ValueError("HumemCypher v0 could not parse the full WHERE clause.")
    return groups


class _BooleanPredicateParser:
    """Small parser for parenthesized boolean predicate expressions."""

    def __init__(self, tokens: list[str]) -> None:
        self._tokens = tokens
        self._index = 0

    def has_more_tokens(self) -> bool:
        """Return whether unconsumed boolean-expression tokens remain."""

        return self._index < len(self._tokens)

    def parse_expression(self) -> list[list[str]]:
        """Parse the full token stream into OR-of-AND predicate groups.

        Returns:
            A disjunctive-normal-form list where each inner list is one AND group of
            comparison expressions.

        Raises:
            ValueError: If the token stream does not contain any valid predicates.
        """

        groups = self._parse_or_expression()
        if not groups:
            raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")
        return groups

    def _parse_or_expression(self) -> list[list[str]]:
        groups = self._parse_and_expression()
        while self._matches_keyword("OR"):
            self._index += 1
            groups.extend(self._parse_and_expression())
        return groups

    def _parse_and_expression(self) -> list[list[str]]:
        groups = self._parse_primary_expression()
        while self._matches_keyword("AND"):
            self._index += 1
            right_groups = self._parse_primary_expression()
            groups = [
                left_group + right_group
                for left_group in groups
                for right_group in right_groups
            ]
        return groups

    def _parse_primary_expression(self) -> list[list[str]]:
        token = self._peek()
        if token is None:
            raise ValueError("HumemCypher v0 WHERE clauses cannot end abruptly.")

        if token == "(":
            self._index += 1
            groups = self._parse_or_expression()
            if self._peek() != ")":
                raise ValueError(
                    "HumemCypher v0 found an unmatched '(' in WHERE clause."
                )
            self._index += 1
            return groups

        comparison_tokens: list[str] = []
        while self.has_more_tokens():
            current = self._peek()
            assert current is not None
            if current in ("(", ")") or current.upper() in ("AND", "OR"):
                break
            comparison_tokens.append(current)
            self._index += 1

        if not comparison_tokens:
            raise ValueError(
                "HumemCypher v0 WHERE items must look like alias.field OP value."
            )
        return [[" ".join(comparison_tokens)]]

    def _peek(self) -> str | None:
        if not self.has_more_tokens():
            return None
        return self._tokens[self._index]

    def _matches_keyword(self, keyword: str) -> bool:
        token = self._peek()
        return token is not None and token.upper() == keyword


def _tokenize_boolean_expression(text: str) -> list[str]:
    """Tokenize a WHERE boolean expression while respecting quoted strings."""

    tokens: list[str] = []
    current: list[str] = []
    in_string = False
    escape = False

    for character in text:
        if escape:
            current.append(character)
            escape = False
            continue

        if character == "\\":
            current.append(character)
            escape = True
            continue

        if character == "'":
            current.append(character)
            in_string = not in_string
            continue

        if not in_string and character in "()":
            token = "".join(current).strip()
            if token:
                tokens.append(token)
            tokens.append(character)
            current = []
            continue

        if not in_string and character.isspace():
            token = "".join(current).strip()
            if token:
                tokens.append(token)
                current = []
            continue

        current.append(character)

    final_token = "".join(current).strip()
    if in_string:
        raise ValueError("HumemCypher v0 found an unterminated string literal.")
    if final_token:
        tokens.append(final_token)
    return tokens


def _parse_set_items(text: str) -> tuple[SetItem, ...]:
    """Parse the small SET subset accepted by HumemCypher v0."""

    assignments: list[SetItem] = []
    for item in _split_comma_separated(text):
        left_text, value_text = _split_outside_string(item, "=")
        match = _RETURN_ITEM_RE.fullmatch(left_text.strip())
        if match is None:
            raise ValueError(
                "HumemCypher v0 SET items must look like alias.field = value."
            )
        assignments.append(
            SetItem(
                alias=match.group("alias"),
                field=match.group("field"),
                value=_parse_literal(value_text.strip()),
            )
        )

    if not assignments:
        raise ValueError("HumemCypher v0 SET clauses cannot be empty.")

    return tuple(assignments)


def _parse_properties(text: str | None) -> PropertyItems:
    """Parse one inline property map into ordered key/value pairs."""

    if text is None or not text.strip():
        return ()

    properties: list[tuple[str, CypherValue]] = []
    for item in _split_comma_separated(text):
        key_text, value_text = _split_outside_string(item, ":")
        key = key_text.strip()
        if not re.fullmatch(_IDENTIFIER, key):
            raise ValueError(
                f"HumemCypher v0 property keys must be simple identifiers; got {key!r}."
            )
        properties.append((key, _parse_literal(value_text.strip())))

    return tuple(properties)


def _parse_literal(text: str) -> CypherValue:
    """Parse one supported inline Cypher literal or parameter reference."""

    if text.startswith("$"):
        parameter_name = text[1:]
        if not re.fullmatch(_IDENTIFIER, parameter_name):
            raise ValueError(
                f"HumemCypher v0 parameter names must be identifiers; got {text!r}."
            )
        return _ParameterRef(parameter_name)

    if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
        return text[1:-1].replace("\\'", "'")

    lowered = text.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if lowered == "null":
        return None

    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)

    raise ValueError(
        f"HumemCypher v0 only supports inline string, integer, float, boolean, "
        f"and null literals; got {text!r}."
    )


def _encode_property_value(value: PropertyValue) -> tuple[object, str]:
    """Encode one graph property into the typed SQLite storage representation."""

    if value is None:
        return None, "null"
    if isinstance(value, tuple):
        array = np.asarray(value, dtype=np.float32)
        if array.ndim != 1 or array.size == 0:
            raise ValueError(
                "HumemCypher v0 vector properties must be one-dimensional "
                "and non-empty."
            )
        return array.astype(np.float32, copy=False).tobytes(), "vector"
    if isinstance(value, bool):
        return ("true" if value else "false"), "boolean"
    if isinstance(value, int):
        return str(value), "integer"
    if isinstance(value, float):
        return repr(value), "real"
    return value, "string"


def _decode_property_value(value: object, value_type: object) -> PropertyValue:
    """Decode one typed SQLite graph property back into a public Cypher value."""

    if value_type is None or value_type == "null":
        return None
    if value_type == "vector":
        raw = value
        if isinstance(raw, memoryview):
            raw = raw.tobytes()
        if isinstance(raw, bytearray):
            raw = bytes(raw)
        if not isinstance(raw, (bytes, bytearray)):
            raise ValueError("HumemCypher v0 stored an invalid vector property value.")
        return tuple(np.frombuffer(raw, dtype=np.float32).astype(float).tolist())
    if value_type == "boolean":
        return value == "true"
    if value_type == "integer":
        return int(str(value))
    if value_type == "real":
        return float(str(value))
    return value if value is None else str(value)


def _normalize_params(
    params: QueryParameters,
) -> Mapping[str, PropertyValue]:
    """Normalize Cypher params into the named-mapping form supported by v0."""

    if params is None:
        return {}

    if isinstance(params, Mapping):
        return cast(Mapping[str, PropertyValue], params)

    raise NotImplementedError(
        "HumemCypher v0 only supports named parameter mappings."
    )


def _bind_plan_values(
    plan: GraphPlan,
    params: Mapping[str, PropertyValue],
) -> GraphPlan:
    """Resolve named parameter references throughout a parsed Cypher plan."""

    if isinstance(plan, CreateNodePlan):
        return CreateNodePlan(_bind_node_pattern(plan.node, params))

    if isinstance(plan, CreateRelationshipPlan):
        return CreateRelationshipPlan(
            _bind_node_pattern(plan.left, params),
            _bind_relationship_pattern(plan.relationship, params),
            _bind_node_pattern(plan.right, params),
        )

    if isinstance(plan, CreateRelationshipFromSeparatePatternsPlan):
        return CreateRelationshipFromSeparatePatternsPlan(
            first_node=_bind_node_pattern(plan.first_node, params),
            second_node=_bind_node_pattern(plan.second_node, params),
            left=_bind_node_pattern(plan.left, params),
            relationship=_bind_relationship_pattern(plan.relationship, params),
            right=_bind_node_pattern(plan.right, params),
        )

    if isinstance(plan, MatchCreateRelationshipPlan):
        return MatchCreateRelationshipPlan(
            match_node=_bind_node_pattern(plan.match_node, params),
            predicates=_bind_predicates(plan.predicates, params),
            left=_bind_node_pattern(plan.left, params),
            relationship=_bind_relationship_pattern(plan.relationship, params),
            right=_bind_node_pattern(plan.right, params),
        )

    if isinstance(plan, MatchCreateRelationshipBetweenNodesPlan):
        return MatchCreateRelationshipBetweenNodesPlan(
            left_match=_bind_node_pattern(plan.left_match, params),
            right_match=_bind_node_pattern(plan.right_match, params),
            predicates=_bind_predicates(plan.predicates, params),
            left=_bind_node_pattern(plan.left, params),
            relationship=_bind_relationship_pattern(plan.relationship, params),
            right=_bind_node_pattern(plan.right, params),
        )

    if isinstance(plan, MatchNodePlan):
        return MatchNodePlan(
            _bind_node_pattern(plan.node, params),
            _bind_predicates(plan.predicates, params),
            plan.returns,
            plan.order_by,
            plan.limit,
            plan.distinct,
            plan.skip,
        )

    if isinstance(plan, SetNodePlan):
        return SetNodePlan(
            _bind_node_pattern(plan.node, params),
            _bind_predicates(plan.predicates, params),
            _bind_set_items(plan.assignments, params),
        )

    if isinstance(plan, SetRelationshipPlan):
        return SetRelationshipPlan(
            _bind_node_pattern(plan.left, params),
            _bind_relationship_pattern(plan.relationship, params),
            _bind_node_pattern(plan.right, params),
            _bind_predicates(plan.predicates, params),
            _bind_set_items(plan.assignments, params),
        )

    if isinstance(plan, DeleteNodePlan):
        return DeleteNodePlan(
            node=_bind_node_pattern(plan.node, params),
            predicates=_bind_predicates(plan.predicates, params),
            detach=plan.detach,
        )

    if isinstance(plan, DeleteRelationshipPlan):
        return DeleteRelationshipPlan(
            left=_bind_node_pattern(plan.left, params),
            relationship=_bind_relationship_pattern(plan.relationship, params),
            right=_bind_node_pattern(plan.right, params),
            predicates=_bind_predicates(plan.predicates, params),
        )

    return MatchRelationshipPlan(
        _bind_node_pattern(plan.left, params),
        _bind_relationship_pattern(plan.relationship, params),
        _bind_node_pattern(plan.right, params),
        _bind_predicates(plan.predicates, params),
        plan.returns,
        plan.order_by,
        plan.limit,
        plan.distinct,
        plan.skip,
    )


def _bind_node_pattern(
    node: NodePattern,
    params: Mapping[str, PropertyValue],
) -> NodePattern:
    """Resolve parameter references inside one parsed node pattern."""

    return NodePattern(
        alias=node.alias,
        label=node.label,
        properties=_bind_properties(node.properties, params),
    )


def _bind_relationship_pattern(
    relationship: RelationshipPattern,
    params: Mapping[str, PropertyValue],
) -> RelationshipPattern:
    """Resolve parameter references inside one parsed relationship pattern."""

    return RelationshipPattern(
        alias=relationship.alias,
        type_name=relationship.type_name,
        direction=relationship.direction,
        properties=_bind_properties(relationship.properties, params),
    )


def _bind_predicates(
    predicates: tuple[Predicate, ...],
    params: Mapping[str, PropertyValue],
) -> tuple[Predicate, ...]:
    """Resolve parameter references across one predicate tuple."""

    return tuple(
        Predicate(
            alias=predicate.alias,
            field=predicate.field,
            operator=predicate.operator,
            disjunct_index=predicate.disjunct_index,
            value=_resolve_cypher_value(predicate.value, params),
        )
        for predicate in predicates
    )


def _bind_properties(
    properties: PropertyItems,
    params: Mapping[str, PropertyValue],
) -> tuple[tuple[str, PropertyValue], ...]:
    """Resolve parameter references across one property item tuple."""

    return tuple(
        (key, _resolve_cypher_value(value, params))
        for key, value in properties
    )


def _bind_set_items(
    assignments: tuple[SetItem, ...],
    params: Mapping[str, PropertyValue],
) -> tuple[SetItem, ...]:
    """Resolve parameter references across one SET assignment tuple."""

    return tuple(
        SetItem(
            alias=assignment.alias,
            field=assignment.field,
            value=_resolve_cypher_value(assignment.value, params),
        )
        for assignment in assignments
    )


def _resolve_cypher_value(
    value: CypherValue,
    params: Mapping[str, PropertyValue],
) -> PropertyValue:
    """Resolve one Cypher literal-or-parameter into a concrete bound value."""

    if not isinstance(value, _ParameterRef):
        return value

    if value.name not in params:
        raise ValueError(
            f"HumemCypher v0 did not receive a value for parameter ${value.name}."
        )

    resolved = params[value.name]
    if _is_vector_param_value(resolved):
        return tuple(float(item) for item in resolved)
    if isinstance(resolved, (str, int, float, bool)) or resolved is None:
        return resolved

    raise ValueError(
        "HumemCypher v0 parameters must resolve to string, integer, float, boolean, "
        "null, or one vector-valued numeric sequence; "
        f"got {resolved!r}."
    )


def _require_property_value(value: CypherValue) -> PropertyValue:
    """Return one already-bound Cypher value as a concrete property value."""

    if isinstance(value, _ParameterRef):
        raise ValueError("HumemCypher v0 encountered an unbound parameter value.")
    return value


def _require_scalar_query_param(value: CypherValue) -> _ScalarQueryParam:
    """Return one already-bound Cypher value that is valid as a scalar SQL param."""

    resolved = _require_property_value(value)
    if isinstance(resolved, tuple):
        raise ValueError(
            "HumemCypher v0 does not allow vector values in scalar predicates."
        )
    return resolved


def _is_vector_param_value(value: object) -> TypeGuard[Sequence[int | float]]:
    """Return whether one parameter value should be treated as a vector sequence."""

    if isinstance(value, (str, bytes, bytearray, memoryview)):
        return False
    if not isinstance(value, Sequence):
        return False
    if len(value) == 0:
        return False
    return all(
        not isinstance(item, bool) and isinstance(item, (int, float))
        for item in value
    )


def _looks_like_relationship_pattern(text: str) -> bool:
    """Return whether one pattern text appears to contain a directed edge."""

    return ("-[" in text and "]->" in text) or ("<-[" in text and "]-" in text)


def _split_relationship_pattern(
    text: str,
) -> tuple[str, str, str, Literal["out", "in"]]:
    """Split a single-edge relationship pattern into left, edge, right, direction."""

    outbound = re.fullmatch(
        r"\((?P<left>[^)]*)\)\s*-\[\s*(?P<rel>[^\]]+)\s*\]\s*->\s*\((?P<right>[^)]*)\)",
        text.strip(),
    )
    if outbound is not None:
        return (
            outbound.group("left"),
            outbound.group("rel"),
            outbound.group("right"),
            "out",
        )

    inbound = re.fullmatch(
        r"\((?P<left>[^)]*)\)\s*<-\[\s*(?P<rel>[^\]]+)\s*\]\s*-\s*\((?P<right>[^)]*)\)",
        text.strip(),
    )
    if inbound is not None:
        return (
            inbound.group("left"),
            inbound.group("rel"),
            inbound.group("right"),
            "in",
        )

    raise ValueError(
        "HumemCypher v0 only supports a single directed relationship pattern."
    )


def _compile_relationship_joins(
    *,
    direction: Literal["out", "in"],
    edge_alias: str,
    left_alias: str,
    right_alias: str,
    anchor_alias: str,
) -> list[str]:
    """Generate the edge and opposite-node joins for one directed relationship."""

    if direction == "out":
        if anchor_alias == right_alias:
            return [
                "JOIN graph_edges AS "
                f"{edge_alias} ON {edge_alias}.to_node_id = {right_alias}.id",
                "JOIN graph_nodes AS "
                f"{left_alias} ON {left_alias}.id = {edge_alias}.from_node_id",
            ]
        return [
            "JOIN graph_edges AS "
            f"{edge_alias} ON {edge_alias}.from_node_id = {left_alias}.id",
            "JOIN graph_nodes AS "
            f"{right_alias} ON {right_alias}.id = {edge_alias}.to_node_id",
        ]

    if anchor_alias == right_alias:
        return [
            "JOIN graph_edges AS "
            f"{edge_alias} ON {edge_alias}.from_node_id = {right_alias}.id",
            "JOIN graph_nodes AS "
            f"{left_alias} ON {left_alias}.id = {edge_alias}.to_node_id",
        ]
    return [
        "JOIN graph_edges AS "
        f"{edge_alias} ON {edge_alias}.to_node_id = {left_alias}.id",
        "JOIN graph_nodes AS "
        f"{right_alias} ON {right_alias}.id = {edge_alias}.from_node_id",
    ]


def _unwrap_node_pattern(text: str) -> str:
    """Remove the outer parentheses from one single-node pattern string."""

    match = re.fullmatch(r"\((?P<node>[^)]*)\)", text.strip())
    if match is None:
        raise ValueError(
            "HumemCypher v0 only supports a single node pattern or one directed edge."
        )
    return match.group("node")


def _find_keyword(text: str, keyword: str) -> int | None:
    """Find one keyword outside of any parsing policy beyond word boundaries."""

    match = re.search(rf"\b{keyword}\b", text, flags=re.IGNORECASE)
    if match is None:
        return None
    return match.start()


def _consume_keyword_prefix(text: str, keyword: str) -> str:
    """Remove one required leading keyword and return the remaining text."""

    stripped = text.strip()
    match = re.match(rf"^{keyword}\b", stripped, flags=re.IGNORECASE)
    if match is None:
        raise ValueError(f"HumemCypher v0 expected leading keyword {keyword!r}.")
    return stripped[match.end():].strip()


def _parse_cypher_identifier_token(text: str, label: str) -> str:
    """Validate one narrow Cypher identifier token."""

    if not re.fullmatch(_IDENTIFIER, text):
        raise ValueError(f"HumemCypher v0 {label} must be an identifier.")
    return text


def _parse_cypher_vector_limit_ref(text: str) -> _CypherVectorLimitRef:
    """Parse one Cypher vector limit token into a literal or param ref."""

    if not text:
        raise ValueError("HumemCypher v0 vector queries currently require a LIMIT value.")
    if re.fullmatch(r"\d+", text):
        return int(text)
    if re.fullmatch(rf"\${_IDENTIFIER}", text):
        return text[1:]
    raise ValueError(
        "HumemCypher v0 vector queries currently require literal or "
        "parameterized LIMIT."
    )


def _split_comma_separated(text: str) -> list[str]:
    """Split one comma-separated clause at top level only."""

    items: list[str] = []
    current: list[str] = []
    in_string = False
    escape = False
    paren_depth = 0
    bracket_depth = 0
    brace_depth = 0

    for character in text:
        if escape:
            current.append(character)
            escape = False
            continue

        if character == "\\":
            current.append(character)
            escape = True
            continue

        if character == "'":
            in_string = not in_string
            current.append(character)
            continue

        if not in_string:
            if character == "(":
                paren_depth += 1
            elif character == ")":
                paren_depth -= 1
            elif character == "[":
                bracket_depth += 1
            elif character == "]":
                bracket_depth -= 1
            elif character == "{":
                brace_depth += 1
            elif character == "}":
                brace_depth -= 1

            if min(paren_depth, bracket_depth, brace_depth) < 0:
                raise ValueError("HumemCypher v0 found an unbalanced pattern clause.")

        if (
            character == ","
            and not in_string
            and paren_depth == 0
            and bracket_depth == 0
            and brace_depth == 0
        ):
            item = "".join(current).strip()
            if not item:
                raise ValueError("HumemCypher v0 does not allow empty list items.")
            items.append(item)
            current = []
            continue

        current.append(character)

    final_item = "".join(current).strip()
    if in_string:
        raise ValueError("HumemCypher v0 found an unterminated string literal.")
    if paren_depth or bracket_depth or brace_depth:
        raise ValueError("HumemCypher v0 found an unbalanced pattern clause.")
    if not final_item:
        raise ValueError("HumemCypher v0 does not allow empty list items.")
    items.append(final_item)
    return items


def _validate_create_relationship_separate_patterns(
    first_node: NodePattern,
    second_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
) -> None:
    """Reject unsupported narrow multi-pattern CREATE endpoint shapes."""

    if first_node.alias == second_node.alias:
        raise ValueError(
            "HumemCypher v0 CREATE with separate node patterns currently requires "
            "two distinct created node aliases."
        )

    created_nodes = {
        first_node.alias: first_node,
        second_node.alias: second_node,
    }
    if {left.alias, right.alias} != set(created_nodes):
        raise ValueError(
            "HumemCypher v0 CREATE with separate node patterns currently requires "
            "the relationship pattern to reuse exactly those two created aliases."
        )

    for endpoint in (left, right):
        created_node = created_nodes[endpoint.alias]
        if endpoint.label is not None and endpoint.label != created_node.label:
            raise ValueError(
                "HumemCypher v0 CREATE reused-node endpoints must use the same "
                "label as the created node alias."
            )
        if endpoint.properties:
            raise ValueError(
                "HumemCypher v0 CREATE reused-node endpoints in separate-pattern "
                "creates cannot redeclare inline properties."
            )


def _validate_match_set_assignments(
    assignments: tuple[SetItem, ...],
    *,
    target_alias: str | None,
    target_kind: Literal["node", "relationship"],
) -> None:
    """Reject MATCH ... SET assignments that target a different bound alias."""

    for assignment in assignments:
        if assignment.alias != target_alias:
            raise ValueError(
                "HumemCypher v0 MATCH ... SET assignments must target the "
                f"matched {target_kind} alias."
            )


def _validate_match_predicates(
    predicates: tuple[Predicate, ...],
    *,
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> None:
    """Reject unsupported MATCH predicate aliases and direct-field operators."""

    for predicate in predicates:
        alias_kind = alias_kinds.get(predicate.alias)
        if alias_kind is None:
            raise ValueError(
                f"HumemCypher v0 cannot filter on unknown alias {predicate.alias!r}."
            )

        if (
            alias_kind == "node"
            and predicate.field == "label"
            and predicate.operator != "="
        ):
            raise ValueError(
                "HumemCypher v0 currently supports only equality predicates for "
                "node field 'label'."
            )

        if (
            alias_kind == "relationship"
            and predicate.field == "type"
            and predicate.operator != "="
        ):
            raise ValueError(
                "HumemCypher v0 currently supports only equality predicates for "
                "relationship field 'type'."
            )


def _validate_match_create_relationship_endpoints(
    match_node: NodePattern,
    left: NodePattern,
    right: NodePattern,
) -> None:
    """Reject unsupported endpoint shapes in narrow MATCH ... CREATE patterns."""

    if left.alias != match_node.alias and right.alias != match_node.alias:
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE currently requires the CREATE "
            "relationship pattern to reuse the matched node alias on at least one "
            "endpoint."
        )

    for endpoint in (left, right):
        if endpoint.alias == match_node.alias:
            if endpoint.label is not None and endpoint.label != match_node.label:
                raise ValueError(
                    "HumemCypher v0 MATCH ... CREATE reused-node endpoints must use "
                    "the same label as the matched node alias."
                )
            if endpoint.properties:
                raise ValueError(
                    "HumemCypher v0 MATCH ... CREATE reused-node endpoints cannot "
                    "redeclare inline properties for the matched node alias."
                )
            continue

        if endpoint.label is None:
            raise ValueError(
                "HumemCypher v0 MATCH ... CREATE new endpoint nodes currently "
                "require a label unless they reuse the matched node alias."
            )


def _validate_match_create_relationship_between_nodes_endpoints(
    left_match: NodePattern,
    right_match: NodePattern,
    left: NodePattern,
    right: NodePattern,
) -> None:
    """Reject unsupported endpoints in two-node MATCH ... CREATE patterns."""

    if left_match.alias == right_match.alias:
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE with two matched node patterns "
            "currently requires two distinct matched aliases."
        )

    matched_aliases = {left_match.alias, right_match.alias}
    endpoint_aliases = {left.alias, right.alias}
    if endpoint_aliases != matched_aliases:
        raise ValueError(
            "HumemCypher v0 MATCH ... CREATE with two matched node patterns "
            "currently requires the CREATE relationship endpoints to reuse those "
            "two matched aliases exactly."
        )

    for matched_node, endpoint in ((left_match, left), (right_match, right)):
        if endpoint.alias != matched_node.alias:
            continue
        if endpoint.label is not None and endpoint.label != matched_node.label:
            raise ValueError(
                "HumemCypher v0 MATCH ... CREATE reused-node endpoints must use "
                "the same label as the matched node alias."
            )
        if endpoint.properties:
            raise ValueError(
                "HumemCypher v0 MATCH ... CREATE reused-node endpoints cannot "
                "redeclare inline properties for matched node aliases."
            )


def _split_keyword_separated(text: str, keyword: str) -> list[str]:
    """Split one clause on a keyword while respecting quoted strings."""

    items: list[str] = []
    current: list[str] = []
    in_string = False
    escape = False
    index = 0
    keyword_upper = keyword.upper()

    while index < len(text):
        character = text[index]

        if escape:
            current.append(character)
            escape = False
            index += 1
            continue

        if character == "\\":
            current.append(character)
            escape = True
            index += 1
            continue

        if character == "'":
            in_string = not in_string
            current.append(character)
            index += 1
            continue

        current_window = text[index:index + len(keyword)].upper()
        if not in_string and current_window == keyword_upper:
            before_ok = index == 0 or text[index - 1].isspace()
            after_index = index + len(keyword)
            after_ok = after_index == len(text) or text[after_index].isspace()
            if before_ok and after_ok:
                item = "".join(current).strip()
                if not item:
                    raise ValueError(
                        "HumemCypher v0 does not allow empty boolean clauses."
                    )
                items.append(item)
                current = []
                index = after_index
                continue

        current.append(character)
        index += 1

    final_item = "".join(current).strip()
    if in_string:
        raise ValueError("HumemCypher v0 found an unterminated string literal.")
    if not final_item:
        raise ValueError("HumemCypher v0 does not allow empty boolean clauses.")
    items.append(final_item)
    return items


def _split_outside_string(text: str, delimiter: str) -> tuple[str, str]:
    """Split on the first delimiter occurrence that is not inside a string."""

    in_string = False
    escape = False

    for index, character in enumerate(text):
        if escape:
            escape = False
            continue

        if character == "\\":
            escape = True
            continue

        if character == "'":
            in_string = not in_string
            continue

        if character == delimiter and not in_string:
            return text[:index], text[index + 1:]

    raise ValueError(
        f"HumemCypher v0 expected {delimiter!r} in {text!r}."
    )


def _split_predicate_comparison(
    text: str,
) -> tuple[
    str,
    Literal[
        "=",
        "<",
        "<=",
        ">",
        ">=",
        "STARTS WITH",
        "ENDS WITH",
        "CONTAINS",
        "IS NULL",
        "IS NOT NULL",
    ],
    str,
]:
    """Split one predicate item into left side, operator, and right side."""

    in_string = False
    escape = False

    for index, character in enumerate(text):
        if escape:
            escape = False
            continue

        if character == "\\":
            escape = True
            continue

        if character == "'":
            in_string = not in_string
            continue

        if in_string:
            continue

        remaining = text[index:].upper()
        if remaining.startswith(" IS NOT NULL"):
            return text[:index], "IS NOT NULL", text[index + len(" IS NOT NULL"):]
        if remaining.startswith(" IS NULL"):
            return text[:index], "IS NULL", text[index + len(" IS NULL"):]
        if remaining.startswith(" STARTS WITH "):
            return text[:index], "STARTS WITH", text[index + len(" STARTS WITH "):]
        if remaining.startswith(" ENDS WITH "):
            return text[:index], "ENDS WITH", text[index + len(" ENDS WITH "):]
        if remaining.startswith(" CONTAINS "):
            return text[:index], "CONTAINS", text[index + len(" CONTAINS "):]

        if text.startswith("<=", index) or text.startswith(">=", index):
            operator = cast(Literal["<=", ">="], text[index:index + 2])
            return text[:index], operator, text[index + 2:]
        if character in "=<>":
            operator = cast(Literal["=", "<", ">"], character)
            return text[:index], operator, text[index + 1:]

    raise ValueError(
        f"HumemCypher v0 expected a comparison operator in {text!r}."
    )
