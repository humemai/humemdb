"""Lower validated Cypher frontend structures into HumemDB internal plans."""

from __future__ import annotations

from ..cypher import (
    CreateNodePlan,
    CreateRelationshipPlan,
    CreateRelationshipFromSeparatePatternsPlan,
    DeleteNodePlan,
    DeleteRelationshipPlan,
    GraphPlan,
    MatchCreateRelationshipPlan,
    MatchCreateRelationshipBetweenNodesPlan,
    MatchNodePlan,
    MatchRelationshipPlan,
    SetNodePlan,
    SetRelationshipPlan,
)
from .normalize import (
    NormalizedCreateNode,
    NormalizedCreateRelationship,
    NormalizedCreateRelationshipFromSeparatePatterns,
    NormalizedCypherStatement,
    NormalizedDeleteNode,
    NormalizedDeleteRelationship,
    NormalizedMatchCreateRelationship,
    NormalizedMatchCreateRelationshipBetweenNodes,
    NormalizedMatchNode,
    NormalizedMatchRelationship,
    NormalizedSetNode,
    NormalizedSetRelationship,
    normalize_cypher_parse_result,
)
from .parser import CypherParseResult, parse_cypher_text


def lower_cypher_text(text: str) -> GraphPlan:
    """Parse, normalize, and lower one admitted Cypher statement."""

    return lower_cypher_parse_result(parse_cypher_text(text))


def lower_cypher_parse_result(result: CypherParseResult) -> GraphPlan:
    """Lower one parse result into the current handwritten GraphPlan types."""

    return lower_normalized_cypher_statement(normalize_cypher_parse_result(result))


def lower_normalized_cypher_statement(
    statement: NormalizedCypherStatement,
) -> GraphPlan:

    """Lower one normalized generated-frontend statement into a GraphPlan."""

    if isinstance(statement, NormalizedCreateNode):
        return CreateNodePlan(node=statement.node)

    if isinstance(statement, NormalizedCreateRelationship):
        return CreateRelationshipPlan(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )

    if isinstance(statement, NormalizedCreateRelationshipFromSeparatePatterns):
        return CreateRelationshipFromSeparatePatternsPlan(
            first_node=statement.first_node,
            second_node=statement.second_node,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )

    if isinstance(statement, NormalizedMatchCreateRelationship):
        return MatchCreateRelationshipPlan(
            match_node=statement.match_node,
            predicates=statement.predicates,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )

    if isinstance(statement, NormalizedMatchCreateRelationshipBetweenNodes):
        return MatchCreateRelationshipBetweenNodesPlan(
            left_match=statement.left_match,
            right_match=statement.right_match,
            predicates=statement.predicates,
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
        )

    if isinstance(statement, NormalizedMatchNode):
        return MatchNodePlan(
            node=statement.node,
            predicates=statement.predicates,
            returns=statement.returns,
            order_by=statement.order_by,
            limit=statement.limit,
            distinct=statement.distinct,
            skip=statement.skip,
        )

    if isinstance(statement, NormalizedMatchRelationship):
        return MatchRelationshipPlan(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
            predicates=statement.predicates,
            returns=statement.returns,
            order_by=statement.order_by,
            limit=statement.limit,
            distinct=statement.distinct,
            skip=statement.skip,
        )

    if isinstance(statement, NormalizedSetNode):
        return SetNodePlan(
            node=statement.node,
            predicates=statement.predicates,
            assignments=statement.assignments,
        )

    if isinstance(statement, NormalizedSetRelationship):
        return SetRelationshipPlan(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
            predicates=statement.predicates,
            assignments=statement.assignments,
        )

    if isinstance(statement, NormalizedDeleteNode):
        return DeleteNodePlan(
            node=statement.node,
            predicates=statement.predicates,
            detach=statement.detach,
        )

    if isinstance(statement, NormalizedDeleteRelationship):
        return DeleteRelationshipPlan(
            left=statement.left,
            relationship=statement.relationship,
            right=statement.right,
            predicates=statement.predicates,
        )

    raise TypeError(f"Unsupported normalized Cypher statement: {type(statement)!r}")
