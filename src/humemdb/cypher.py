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
from typing import Mapping, Sequence
from typing import Literal, TypeGuard, cast

import numpy as np

from .engines import DuckDBEngine, SQLiteEngine
from .types import QueryParameters, QueryResult, Route
from .vector import (
    ensure_vector_schema,
    insert_vectors as insert_vector_rows,
    upsert_vectors,
)

logger = logging.getLogger(__name__)

_CYPHER_CREATE_PREFIX = re.compile(r"^CREATE\b")
_CYPHER_MATCH_PREFIX = re.compile(r"^MATCH\b")

ScalarPropertyValue = str | int | float | bool | None
VectorPropertyValue = tuple[float, ...]
PropertyValue = ScalarPropertyValue | VectorPropertyValue


@dataclass(frozen=True, slots=True)
class ParameterRef:
    """Named Cypher parameter reference such as `$name`.

    Attributes:
        name: Parameter name without the leading `$` prefix.
    """

    name: str


CypherValue = PropertyValue | ParameterRef
PropertyItems = tuple[tuple[str, CypherValue], ...]
ScalarQueryParam = str | int | float | bool | None


@dataclass(frozen=True, slots=True)
class _EncodedNodePropertyWrite:
    """Encoded graph node property write plus optional vector payload."""

    key: str
    encoded_value: object
    value_type: str
    vector_value: VectorPropertyValue | None = None


@dataclass(frozen=True, slots=True)
class Predicate:
    """Simple equality predicate used by the initial Cypher `WHERE` subset.

    Attributes:
        alias: Bound node or relationship alias the predicate targets.
        field: Property name being compared.
        value: Literal or parameter-backed Cypher value to compare against.
    """

    alias: str
    field: str
    value: CypherValue


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
        type_name: Relationship type name.
        direction: Relationship direction relative to the left node.
        properties: Inline property items attached to the relationship pattern.
    """

    alias: str | None
    type_name: str
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


GraphPlan = (
    CreateNodePlan
    | CreateRelationshipPlan
    | MatchNodePlan
    | MatchRelationshipPlan
    | SetNodePlan
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
    rf"^(?P<alias>{_IDENTIFIER})(?::(?P<label>{_IDENTIFIER}))?"
    r"(?:\s*\{\s*(?P<properties>.*)\s*\})?$"
)
_REL_PATTERN_RE = re.compile(
    rf"^(?:(?P<alias>{_IDENTIFIER})\s*)?:(?P<type>{_IDENTIFIER})"
    r"(?:\s*\{\s*(?P<properties>.*)\s*\})?$"
)
_RETURN_ITEM_RE = re.compile(rf"^(?P<alias>{_IDENTIFIER})\.(?P<field>{_IDENTIFIER})$")

_UNSUPPORTED_KEYWORDS = {
    "with",
    "optional",
    "merge",
    "delete",
    "skip",
    "unwind",
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
    "PRIMARY KEY (node_id, key))",
    "CREATE TABLE IF NOT EXISTS graph_edges ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "type TEXT NOT NULL, "
    "from_node_id INTEGER NOT NULL, "
    "to_node_id INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS graph_edge_properties ("
    "edge_id INTEGER NOT NULL, "
    "key TEXT NOT NULL, "
    "value TEXT, "
    "value_type TEXT NOT NULL, "
    "PRIMARY KEY (edge_id, key))",
    "CREATE INDEX IF NOT EXISTS idx_graph_nodes_label_id "
    "ON graph_nodes(label, id)",
    "CREATE INDEX IF NOT EXISTS idx_graph_edges_from_type_to "
    "ON graph_edges(from_node_id, type, to_node_id)",
    "CREATE INDEX IF NOT EXISTS idx_graph_edges_to_type_from "
    "ON graph_edges(to_node_id, type, from_node_id)",
    "CREATE INDEX IF NOT EXISTS idx_graph_node_props_lookup "
    "ON graph_node_properties(key, value_type, value, node_id)",
    "CREATE INDEX IF NOT EXISTS idx_graph_edge_props_lookup "
    "ON graph_edge_properties(key, value_type, value, edge_id)",
)


def ensure_graph_schema(sqlite: SQLiteEngine) -> None:
    """Create the SQLite-backed graph storage tables if they do not exist yet.

    Args:
        sqlite: Canonical SQLite engine that owns graph storage.
    """

    logger.debug("Ensuring SQLite-backed graph schema exists")
    for statement in _GRAPH_SCHEMA_SQL:
        sqlite.execute(statement)


def execute_cypher(
    text: str,
    *,
    route: Route,
    params: QueryParameters,
    sqlite: SQLiteEngine,
    duckdb: DuckDBEngine,
) -> QueryResult:
    """Execute a minimal Cypher statement through the HumemDB graph path.

    Args:
        text: Cypher statement to execute.
        route: Backend route selected by the caller.
        params: Optional named or positional Cypher parameters.
        sqlite: Canonical SQLite engine that owns graph storage.
        duckdb: DuckDB engine used for read-only graph queries.

    Returns:
        A normalized `QueryResult`.

    Raises:
        ValueError: If the route is unsupported or a Cypher write is directed to
            DuckDB.
    """

    plan = _bind_plan_values(parse_cypher(text), _normalize_params(params))
    logger.debug(
        "Executing Cypher plan kind=%s route=%s",
        type(plan).__name__,
        route,
    )

    if isinstance(plan, (CreateNodePlan, CreateRelationshipPlan)):
        if route != "sqlite":
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        return _execute_create_plan(plan, sqlite)

    if isinstance(plan, SetNodePlan):
        if route != "sqlite":
            raise ValueError(
                "HumemDB does not allow direct Cypher writes to DuckDB; SQLite is "
                "the source of truth."
            )
        return _execute_set_node_plan(plan, sqlite)

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
    for keyword in _UNSUPPORTED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", lowered_statement):
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


def _parse_create(body: str) -> GraphPlan:
    """Parse one CREATE body into a node or single-edge creation plan."""

    if _looks_like_relationship_pattern(body):
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(body)
        )
        left = _parse_node_pattern(left_text, require_label=True)
        relationship = _parse_relationship_pattern(relationship_text, direction)
        right = _parse_node_pattern(right_text, require_label=True)
        return CreateRelationshipPlan(left, relationship, right)

    node = _parse_node_pattern(_unwrap_node_pattern(body), require_label=True)
    return CreateNodePlan(node)


def _parse_match(body: str) -> GraphPlan:
    """Parse a narrow MATCH statement into a node or relationship plan.

    The supported shape is intentionally small: one node pattern or one directed
    relationship pattern, an optional simple WHERE clause, a required RETURN clause,
    and optional ORDER BY and LIMIT clauses.
    """

    set_index = _find_keyword(body, "set")
    return_index = _find_keyword(body, "return")

    if set_index is not None and return_index is None:
        return _parse_match_set(body)
    if set_index is not None and return_index is not None and set_index < return_index:
        return _parse_match_set(body)

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

    return_text, order_by, limit = _split_return_clause(return_clause)
    returns = _parse_return_items(return_text)

    if _looks_like_relationship_pattern(pattern_text):
        left_text, relationship_text, right_text, direction = (
            _split_relationship_pattern(pattern_text)
        )
        return MatchRelationshipPlan(
            _parse_node_pattern(left_text),
            _parse_relationship_pattern(relationship_text, direction),
            _parse_node_pattern(right_text),
            predicates,
            returns,
            order_by,
            limit,
        )

    return MatchNodePlan(
        _parse_node_pattern(_unwrap_node_pattern(pattern_text)),
        predicates,
        returns,
        order_by,
        limit,
    )


def _parse_match_set(body: str) -> SetNodePlan:
    """Parse a narrow MATCH ... SET node-property update statement."""

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

    if _looks_like_relationship_pattern(pattern_text):
        raise ValueError("HumemCypher v0 MATCH ... SET currently supports only nodes.")

    node = _parse_node_pattern(_unwrap_node_pattern(pattern_text))
    assignments = _parse_set_items(set_text)
    return SetNodePlan(node, predicates, assignments)


def _execute_create_plan(
    plan: CreateNodePlan | CreateRelationshipPlan,
    sqlite: SQLiteEngine,
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

    left_id = _insert_node(sqlite, plan.left)
    right_id = _insert_node(sqlite, plan.right)
    edge_id = _insert_edge(sqlite, plan.relationship, left_id, right_id)
    return QueryResult(
        rows=((left_id, edge_id, right_id),),
        columns=("from_id", "edge_id", "to_id"),
        route="sqlite",
        query_type="cypher",
        rowcount=1,
    )


def _execute_set_node_plan(
    plan: SetNodePlan,
    sqlite: SQLiteEngine,
) -> QueryResult:
    """Execute a narrow MATCH ... SET node-property update."""

    if len(plan.assignments) != 1:
        raise ValueError(
            "HumemCypher v0 MATCH ... SET currently supports one property assignment."
        )

    assignment = plan.assignments[0]
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

    assignment_value = _require_property_value(assignment.value)
    for node_id in node_ids:
        _upsert_node_property(sqlite, node_id, assignment.field, assignment_value)

    return QueryResult(
        rows=(),
        columns=(),
        route="sqlite",
        query_type="cypher",
        rowcount=len(node_ids),
    )


def _compile_match_plan(
    plan: MatchNodePlan | MatchRelationshipPlan,
) -> _CompiledMatchQuery:
    """Dispatch one MATCH plan to the node or relationship compiler."""

    logger.debug("Compiling Cypher match plan kind=%s", type(plan).__name__)
    if isinstance(plan, MatchNodePlan):
        return _compile_match_node_plan(plan)
    return _compile_match_relationship_plan(plan)


def _compile_match_node_plan(plan: MatchNodePlan) -> _CompiledMatchQuery:
    """Compile a node MATCH plan into relational SQL over graph tables.

    This compiler tries to anchor the scan from the most selective property equality
    constraint when possible, then layers on remaining property joins, projection,
    ordering, and an optional LIMIT.
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
    )
    _compile_order_items(
        alias_map={plan.node.alias: alias},
        alias_kinds={plan.node.alias: "node"},
        order_to_compile=plan.order_by,
        joins=order_joins,
        params=order_params,
        order_parts=order_parts,
    )

    sql = [f"SELECT {', '.join(select_parts)}", from_clause]
    sql.extend(joins)
    sql.extend(order_joins)
    if where_parts:
        sql.append(f"WHERE {' AND '.join(where_parts)}")
    if order_parts:
        sql.append(f"ORDER BY {', '.join(order_parts)}")
    else:
        sql.append(f"ORDER BY {alias}.id")
    if plan.limit is not None:
        sql.append(f"LIMIT {plan.limit}")

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
    ordering, and an optional LIMIT.
    """

    left_alias = plan.left.alias
    edge_alias = plan.relationship.alias or "edge_rel"
    right_alias = plan.right.alias
    select_parts: list[str] = []
    joins: list[str] = []
    order_joins: list[str] = []
    where_parts: list[str] = [f"{edge_alias}.type = ?"]
    order_parts: list[str] = []
    from_params: list[object] = []
    join_params: list[object] = []
    order_params: list[object] = []
    where_params: list[object] = [plan.relationship.type_name]
    returns: list[_CompiledReturnItem] = []

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

    _compile_predicates(
        alias_map={
            plan.left.alias: left_alias,
            plan.right.alias: right_alias,
            **(
                {plan.relationship.alias: edge_alias}
                if plan.relationship.alias is not None
                else {}
            ),
        },
        alias_kinds={
            plan.left.alias: "node",
            plan.right.alias: "node",
            **(
                {plan.relationship.alias: "relationship"}
                if plan.relationship.alias is not None
                else {}
            ),
        },
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
    _compile_return_items(
        alias_map={
            plan.left.alias: left_alias,
            plan.right.alias: right_alias,
            **(
                {plan.relationship.alias: edge_alias}
                if plan.relationship.alias is not None
                else {}
            ),
        },
        alias_kinds={
            plan.left.alias: "node",
            plan.right.alias: "node",
            **(
                {plan.relationship.alias: "relationship"}
                if plan.relationship.alias is not None
                else {}
            ),
        },
        returns_to_compile=plan.returns,
        joins=joins,
        params=join_params,
        select_parts=select_parts,
        returns=returns,
    )
    _compile_order_items(
        alias_map={
            plan.left.alias: left_alias,
            plan.right.alias: right_alias,
            **(
                {plan.relationship.alias: edge_alias}
                if plan.relationship.alias is not None
                else {}
            ),
        },
        alias_kinds={
            plan.left.alias: "node",
            plan.right.alias: "node",
            **(
                {plan.relationship.alias: "relationship"}
                if plan.relationship.alias is not None
                else {}
            ),
        },
        order_to_compile=plan.order_by,
        joins=order_joins,
        params=order_params,
        order_parts=order_parts,
    )

    sql = [f"SELECT {', '.join(select_parts)}", from_clause]
    sql.extend(joins)
    sql.extend(order_joins)
    sql.append(f"WHERE {' AND '.join(where_parts)}")
    if order_parts:
        sql.append(f"ORDER BY {', '.join(order_parts)}")
    else:
        sql.append(f"ORDER BY {left_alias}.id, {edge_alias}.id, {right_alias}.id")
    if plan.limit is not None:
        sql.append(f"LIMIT {plan.limit}")

    return _CompiledMatchQuery(
        sql=" ".join(sql),
        params=tuple(from_params + join_params + order_params + where_params),
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

    for predicate in predicates:
        if predicate.alias != node.alias or predicate.field in {"id", "label"}:
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

    for predicate in predicates:
        if predicate.alias != relationship.alias or predicate.field in {"id", "type"}:
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


def _compile_return_items(
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    returns_to_compile: tuple[ReturnItem, ...],
    joins: list[str],
    params: list[object],
    select_parts: list[str],
    returns: list[_CompiledReturnItem],
) -> None:
    """Compile RETURN items into projections and supporting property joins."""

    for index, item in enumerate(returns_to_compile):
        if item.alias not in alias_map:
            raise ValueError(
                f"HumemCypher v0 cannot RETURN unknown alias {item.alias!r}."
            )

        table_alias = alias_map[item.alias]
        alias_kind = alias_kinds[item.alias]
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

        property_alias = f"{table_alias}_return_{index}"
        property_table = (
            "graph_node_properties"
            if alias_kind == "node"
            else "graph_edge_properties"
        )
        id_column = "node_id" if alias_kind == "node" else "edge_id"
        joins.append(
            f"LEFT JOIN {property_table} AS {property_alias} "
            f"ON {property_alias}.{id_column} = {table_alias}.id "
            f"AND {property_alias}.key = ?"
        )
        params.append(item.field)
        select_parts.append(f"{property_alias}.value AS \"__value_{index}\"")
        select_parts.append(
            f"{property_alias}.value_type AS \"__value_type_{index}\""
        )
        returns.append(_CompiledReturnItem(item.column_name, "property"))


def _compile_predicates(
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    predicates: tuple[Predicate, ...],
    where_parts: list[str],
    where_params: list[object],
) -> None:
    """Compile supported WHERE predicates into SQL fragments and params."""

    for predicate in predicates:
        if predicate.alias not in alias_map:
            raise ValueError(
                f"HumemCypher v0 cannot filter on unknown alias {predicate.alias!r}."
            )

        table_alias = alias_map[predicate.alias]
        alias_kind = alias_kinds[predicate.alias]
        predicate_value = _require_scalar_query_param(predicate.value)
        if predicate.field == "id":
            where_parts.append(f"{table_alias}.id = ?")
            where_params.append(predicate_value)
            continue

        if alias_kind == "node" and predicate.field == "label":
            where_parts.append(f"{table_alias}.label = ?")
            where_params.append(predicate_value)
            continue

        if alias_kind == "relationship" and predicate.field == "type":
            where_parts.append(f"{table_alias}.type = ?")
            where_params.append(predicate_value)
            continue


def _compile_order_items(
    *,
    alias_map: dict[str, str],
    alias_kinds: dict[str, Literal["node", "relationship"]],
    order_to_compile: tuple[OrderItem, ...],
    joins: list[str],
    params: list[object],
    order_parts: list[str],
) -> None:
    """Compile ORDER BY items into SQL expressions and any needed property joins.

    Property ordering is expressed in a type-aware way so integers, reals, booleans,
    and strings can be sorted without losing their stored scalar semantics.
    """

    for index, item in enumerate(order_to_compile):
        if item.alias not in alias_map:
            raise ValueError(
                f"HumemCypher v0 cannot ORDER BY unknown alias {item.alias!r}."
            )

        table_alias = alias_map[item.alias]
        alias_kind = alias_kinds[item.alias]
        direction = item.direction.upper()

        if item.field == "id":
            order_parts.append(f"{table_alias}.id {direction}")
            continue

        if alias_kind == "node" and item.field == "label":
            order_parts.append(f"{table_alias}.label {direction}")
            continue

        if alias_kind == "relationship" and item.field == "type":
            order_parts.append(f"{table_alias}.type {direction}")
            continue

        property_alias = f"{table_alias}_order_{index}"
        property_table = (
            "graph_node_properties"
            if alias_kind == "node"
            else "graph_edge_properties"
        )
        id_column = "node_id" if alias_kind == "node" else "edge_id"
        joins.append(
            f"LEFT JOIN {property_table} AS {property_alias} "
            f"ON {property_alias}.{id_column} = {table_alias}.id "
            f"AND {property_alias}.key = ?"
        )
        params.append(item.field)
        order_parts.extend(
            [
                _compile_numeric_order_expression(property_alias, direction),
                _compile_string_order_expression(property_alias, direction),
            ]
        )


def _compile_numeric_order_expression(property_alias: str, direction: str) -> str:
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
        f"{direction}"
    )


def _compile_string_order_expression(property_alias: str, direction: str) -> str:
    """Build one string/null ORDER BY expression for a typed property join."""

    return (
        "CASE "
        f"WHEN {property_alias}.value_type IN ('string', 'null') "
        f"THEN {property_alias}.value "
        "END "
        f"{direction}"
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


def _insert_node(sqlite: SQLiteEngine, node: NodePattern) -> int:
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
    sqlite: SQLiteEngine,
    relationship: RelationshipPattern,
    left_id: int,
    right_id: int,
) -> int:
    """Insert one directed edge plus its properties into the graph store."""

    sqlite.execute(
        "INSERT INTO graph_edges (type, from_node_id, to_node_id) VALUES (?, ?, ?)",
        (relationship.type_name, left_id, right_id),
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
    sqlite: SQLiteEngine,
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
                    cast(VectorPropertyValue, property_value)
                    if value_type == "vector"
                    else None
                ),
            )
        )

    return tuple(writes)


def _persist_node_property_writes(
    sqlite: SQLiteEngine,
    node_id: int,
    property_writes: tuple[_EncodedNodePropertyWrite, ...],
    *,
    mode: Literal["insert", "upsert"],
) -> None:
    """Persist encoded node-property writes and sync graph-node vectors."""

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
            "per created node."
        )
    if vector_rows:
        _sync_graph_node_vectors(sqlite, vector_rows, mode=mode)


def _sync_graph_node_vectors(
    sqlite: SQLiteEngine,
    vector_rows: Sequence[tuple[int, Sequence[float]]],
    *,
    mode: Literal["insert", "upsert"],
) -> None:
    """Persist graph-node vectors for encoded Cypher node-property writes."""

    ensure_vector_schema(sqlite)
    if mode == "insert":
        insert_vector_rows(sqlite, vector_rows, target="graph_node", namespace="")
        return

    upsert_vectors(
        sqlite,
        vector_rows,
        target="graph_node",
        namespace="",
    )


def _parse_node_pattern(text: str, *, require_label: bool = False) -> NodePattern:
    """Parse one node-pattern body into the narrow NodePattern structure."""

    match = _NODE_PATTERN_RE.fullmatch(text.strip())
    if match is None:
        raise ValueError(f"HumemCypher v0 could not parse node pattern: {text!r}")

    label = match.group("label")
    if require_label and label is None:
        raise ValueError("HumemCypher v0 CREATE patterns require labeled nodes.")

    return NodePattern(
        alias=match.group("alias"),
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
) -> tuple[str, tuple[OrderItem, ...], int | None]:
    """Split a RETURN clause into projection, ordering, and limit components."""

    order_by_match = re.search(r"\border\s+by\b", text, flags=re.IGNORECASE)
    limit_match = re.search(r"\blimit\b", text, flags=re.IGNORECASE)

    if order_by_match is None and limit_match is None:
        return text.strip(), (), None

    if order_by_match is None:
        assert limit_match is not None
        returns_text = text[:limit_match.start()].strip()
        limit = _parse_limit_clause(text[limit_match.end():].strip())
        return returns_text, (), limit

    if limit_match is not None and limit_match.start() < order_by_match.start():
        raise ValueError("HumemCypher v0 requires ORDER BY to appear before LIMIT.")

    returns_text = text[:order_by_match.start()].strip()
    if limit_match is None:
        order_text = text[order_by_match.end():].strip()
        return returns_text, _parse_order_items(order_text), None

    order_text = text[order_by_match.end():limit_match.start()].strip()
    limit_text = text[limit_match.end():].strip()
    return returns_text, _parse_order_items(order_text), _parse_limit_clause(limit_text)


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
    """Parse one WHERE clause into simple equality predicates."""

    predicates: list[Predicate] = []

    for item in _split_keyword_separated(text, "and"):
        try:
            left_text, value_text = _split_outside_string(item, "=")
        except ValueError as exc:
            raise ValueError(
                "HumemCypher v0 WHERE items must look like alias.field = value."
            ) from exc
        match = _RETURN_ITEM_RE.fullmatch(left_text.strip())
        if match is None:
            raise ValueError(
                "HumemCypher v0 WHERE items must look like alias.field = value."
            )
        predicates.append(
            Predicate(
                alias=match.group("alias"),
                field=match.group("field"),
                value=_parse_literal(value_text.strip()),
            )
        )

    if not predicates:
        raise ValueError("HumemCypher v0 WHERE clauses cannot be empty.")

    return tuple(predicates)


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
        return ParameterRef(parameter_name)

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

    if isinstance(plan, MatchNodePlan):
        return MatchNodePlan(
            _bind_node_pattern(plan.node, params),
            _bind_predicates(plan.predicates, params),
            plan.returns,
            plan.order_by,
            plan.limit,
        )

    if isinstance(plan, SetNodePlan):
        return SetNodePlan(
            _bind_node_pattern(plan.node, params),
            _bind_predicates(plan.predicates, params),
            _bind_set_items(plan.assignments, params),
        )

    return MatchRelationshipPlan(
        _bind_node_pattern(plan.left, params),
        _bind_relationship_pattern(plan.relationship, params),
        _bind_node_pattern(plan.right, params),
        _bind_predicates(plan.predicates, params),
        plan.returns,
        plan.order_by,
        plan.limit,
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

    if not isinstance(value, ParameterRef):
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

    if isinstance(value, ParameterRef):
        raise ValueError("HumemCypher v0 encountered an unbound parameter value.")
    return value


def _require_scalar_query_param(value: CypherValue) -> ScalarQueryParam:
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


def _split_comma_separated(text: str) -> list[str]:
    """Split one comma-separated clause while respecting quoted strings."""

    items: list[str] = []
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
            in_string = not in_string
            current.append(character)
            continue

        if character == "," and not in_string:
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
    if not final_item:
        raise ValueError("HumemCypher v0 does not allow empty list items.")
    items.append(final_item)
    return items


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
