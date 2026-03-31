"""Normalize generated Cypher parse output into HumemDB-facing structures."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Literal

from ..cypher import (
    NodePattern,
    OrderItem,
    Predicate,
    RelationshipPattern,
    ReturnItem,
    SetItem,
    _looks_like_relationship_pattern,
    _parse_node_pattern,
    _parse_predicates,
    _parse_relationship_pattern,
    _parse_return_items,
    _parse_set_items,
    _split_comma_separated,
    _split_relationship_pattern,
    _split_return_clause,
    _unwrap_node_pattern,
    _validate_create_relationship_separate_patterns,
    _validate_match_create_relationship_between_nodes_endpoints,
    _validate_match_create_relationship_endpoints,
)
from .parser import CypherParseResult, parse_cypher_text
from .validate import validate_cypher_parse_result


@dataclass(frozen=True, slots=True)
class NormalizedCreateNode:
    """Normalized generated-parser representation of one node CREATE.

    Attributes:
        kind: Statement family discriminator for CREATE normalization.
        pattern_kind: Pattern discriminator indicating a node pattern.
        node: Parsed node pattern to create.
    """

    kind: Literal["create"]
    pattern_kind: Literal["node"]
    node: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedCreateRelationship:
    """Normalized generated-parser representation of one relationship CREATE.

    Attributes:
        kind: Statement family discriminator for CREATE normalization.
        pattern_kind: Pattern discriminator indicating a relationship pattern.
        left: Left endpoint node pattern.
        relationship: Relationship pattern to create.
        right: Right endpoint node pattern.
    """

    kind: Literal["create"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedCreateRelationshipFromSeparatePatterns:
    """Normalized generated-parser representation of narrow multi-pattern CREATE.

    Attributes:
        kind: Statement family discriminator for CREATE normalization.
        pattern_kind: Pattern discriminator indicating a relationship pattern.
        first_node: First standalone node pattern from the original CREATE clause.
        second_node: Second standalone node pattern from the original CREATE clause.
        left: Left endpoint node pattern after normalization.
        relationship: Relationship pattern to create.
        right: Right endpoint node pattern after normalization.
    """

    kind: Literal["create"]
    pattern_kind: Literal["relationship"]
    first_node: NodePattern
    second_node: NodePattern
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchNode:
    """Normalized generated-parser representation of one node MATCH query.

    Attributes:
        kind: Statement family discriminator for MATCH normalization.
        pattern_kind: Pattern discriminator indicating a node pattern.
        node: Matched node pattern.
        predicates: Normalized predicates applied to the match.
        returns: Normalized return items.
        order_by: Optional ORDER BY items.
        limit: Optional LIMIT value.
        distinct: Whether the return projection is DISTINCT.
        skip: Optional SKIP/OFFSET value.
    """

    kind: Literal["match"]
    pattern_kind: Literal["node"]
    node: NodePattern
    predicates: tuple[Predicate, ...]
    returns: tuple[ReturnItem, ...]
    order_by: tuple[OrderItem, ...] = ()
    limit: int | None = None
    distinct: bool = False
    skip: int | None = None


@dataclass(frozen=True, slots=True)
class NormalizedMatchRelationship:
    """Normalized generated-parser representation of one relationship MATCH query.

    Attributes:
        kind: Statement family discriminator for MATCH normalization.
        pattern_kind: Pattern discriminator indicating a relationship pattern.
        left: Left endpoint node pattern.
        relationship: Matched relationship pattern.
        right: Right endpoint node pattern.
        predicates: Normalized predicates applied to the match.
        returns: Normalized return items.
        order_by: Optional ORDER BY items.
        limit: Optional LIMIT value.
        distinct: Whether the return projection is DISTINCT.
        skip: Optional SKIP/OFFSET value.
    """

    kind: Literal["match"]
    pattern_kind: Literal["relationship"]
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
class NormalizedSetNode:
    """Normalized generated-parser representation of one MATCH ... SET statement.

    Attributes:
        kind: Statement family discriminator for SET normalization.
        pattern_kind: Pattern discriminator indicating a node pattern.
        node: Matched node pattern receiving assignments.
        predicates: Normalized predicates applied before the SET.
        assignments: Normalized property assignments to apply.
    """

    kind: Literal["set"]
    pattern_kind: Literal["node"]
    node: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class NormalizedSetRelationship:
    """Normalized generated-parser representation of one relationship MATCH ... SET.

    Attributes:
        kind: Statement family discriminator for SET normalization.
        pattern_kind: Pattern discriminator indicating a relationship pattern.
        left: Left endpoint node pattern.
        relationship: Matched relationship pattern receiving assignments.
        right: Right endpoint node pattern.
        predicates: Normalized predicates applied before the SET.
        assignments: Normalized property assignments to apply.
    """

    kind: Literal["set"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]
    assignments: tuple[SetItem, ...]


@dataclass(frozen=True, slots=True)
class NormalizedDeleteNode:
    """Normalized generated-parser representation of one MATCH ... DETACH DELETE.

    Attributes:
        kind: Statement family discriminator for DELETE normalization.
        pattern_kind: Pattern discriminator indicating a node pattern.
        node: Matched node pattern to delete.
        predicates: Normalized predicates applied before deletion.
        detach: Whether the delete should detach relationships first.
    """

    kind: Literal["delete"]
    pattern_kind: Literal["node"]
    node: NodePattern
    predicates: tuple[Predicate, ...]
    detach: bool = True


@dataclass(frozen=True, slots=True)
class NormalizedDeleteRelationship:
    """Normalized generated-parser representation of one MATCH ... DELETE.

    Attributes:
        kind: Statement family discriminator for DELETE normalization.
        pattern_kind: Pattern discriminator indicating a relationship pattern.
        left: Left endpoint node pattern.
        relationship: Matched relationship pattern to delete.
        right: Right endpoint node pattern.
        predicates: Normalized predicates applied before deletion.
    """

    kind: Literal["delete"]
    pattern_kind: Literal["relationship"]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern
    predicates: tuple[Predicate, ...]


@dataclass(frozen=True, slots=True)
class NormalizedMatchCreateRelationship:
    """Normalized generated-parser representation of one MATCH ... CREATE.

    Attributes:
        kind: Statement family discriminator for MATCH ... CREATE normalization.
        pattern_kind: Pattern discriminator indicating a relationship pattern.
        match_node: Matched node pattern that scopes the CREATE.
        predicates: Normalized predicates applied to the MATCH portion.
        left: Left endpoint node pattern for the created relationship.
        relationship: Relationship pattern to create.
        right: Right endpoint node pattern for the created relationship.
    """

    kind: Literal["match_create"]
    pattern_kind: Literal["relationship"]
    match_node: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


@dataclass(frozen=True, slots=True)
class NormalizedMatchCreateRelationshipBetweenNodes:
    """Normalized generated-parser representation of two-node MATCH ... CREATE.

    Attributes:
        kind: Statement family discriminator for MATCH ... CREATE normalization.
        pattern_kind: Pattern discriminator indicating a relationship pattern.
        left_match: Left MATCH node pattern used to bind the create endpoints.
        right_match: Right MATCH node pattern used to bind the create endpoints.
        predicates: Normalized predicates applied to the MATCH portion.
        left: Left endpoint node pattern for the created relationship.
        relationship: Relationship pattern to create.
        right: Right endpoint node pattern for the created relationship.
    """

    kind: Literal["match_create"]
    pattern_kind: Literal["relationship"]
    left_match: NodePattern
    right_match: NodePattern
    predicates: tuple[Predicate, ...]
    left: NodePattern
    relationship: RelationshipPattern
    right: NodePattern


NormalizedCypherStatement = (
    NormalizedCreateNode
    | NormalizedCreateRelationship
    | NormalizedCreateRelationshipFromSeparatePatterns
    | NormalizedMatchNode
    | NormalizedMatchRelationship
    | NormalizedSetNode
    | NormalizedSetRelationship
    | NormalizedDeleteNode
    | NormalizedDeleteRelationship
    | NormalizedMatchCreateRelationship
    | NormalizedMatchCreateRelationshipBetweenNodes
)


def _validate_normalized_match_set_assignments(
    assignments: tuple[SetItem, ...],
    *,
    target_alias: str | None,
    target_kind: Literal["node", "relationship"],
) -> None:
    """Reject normalized MATCH ... SET assignments that target another alias."""

    for assignment in assignments:
        if assignment.alias != target_alias:
            raise ValueError(
                "HumemCypher v0 MATCH ... SET assignments must target the "
                f"matched {target_kind} alias."
            )


def _validate_normalized_match_predicates(
    predicates: tuple[Predicate, ...],
    *,
    alias_kinds: dict[str, Literal["node", "relationship"]],
) -> None:
    """Reject unsupported normalized MATCH predicate aliases and operators."""

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


def normalize_cypher_text(text: str) -> NormalizedCypherStatement:
    """Parse and normalize one admitted Cypher statement."""

    return normalize_cypher_parse_result(parse_cypher_text(text))


def normalize_cypher_parse_result(
    result: CypherParseResult,
) -> NormalizedCypherStatement:
    """Normalize one validated parse result into the current admitted subset."""

    single_part_query = validate_cypher_parse_result(result)
    updating_clauses = single_part_query.oC_UpdatingClause()
    reading_clauses = single_part_query.oC_ReadingClause()

    if updating_clauses:
        if reading_clauses:
            match_ctx = reading_clauses[0].oC_Match()
            assert match_ctx is not None
            pattern_text = _context_text(result, match_ctx.oC_Pattern())

            predicates: tuple[Predicate, ...] = ()
            where_ctx = match_ctx.oC_Where()
            if where_ctx is not None:
                predicates = _parse_predicates(
                    _context_text(result, where_ctx.oC_Expression())
                )

            create_ctx = updating_clauses[0].oC_Create()
            if create_ctx is not None:
                match_patterns = _split_comma_separated(pattern_text)
                if any(
                    _looks_like_relationship_pattern(pattern)
                    for pattern in match_patterns
                ):
                    raise ValueError(
                        "HumemCypher v0 MATCH ... CREATE currently supports only "
                        "matched node patterns before CREATE."
                    )

                create_pattern_text = _context_text(result, create_ctx.oC_Pattern())
                if not _looks_like_relationship_pattern(create_pattern_text):
                    raise ValueError(
                        "HumemCypher v0 MATCH ... CREATE currently supports only one "
                        "directed relationship pattern in the CREATE clause."
                    )

                left_text, relationship_text, right_text, direction = (
                    _split_relationship_pattern(create_pattern_text)
                )
                left = _parse_node_pattern(
                    left_text,
                    default_alias="__humem_match_create_left_node",
                )
                relationship = _parse_relationship_pattern(
                    relationship_text,
                    direction,
                )
                right = _parse_node_pattern(
                    right_text,
                    default_alias="__humem_match_create_right_node",
                )
                if len(match_patterns) == 1:
                    match_node = _parse_node_pattern(
                        _unwrap_node_pattern(match_patterns[0]),
                        default_alias="__humem_match_create_node",
                    )
                    _validate_normalized_match_predicates(
                        predicates,
                        alias_kinds={match_node.alias: "node"},
                    )
                    _validate_match_create_relationship_endpoints(
                        match_node,
                        left,
                        right,
                    )
                    return NormalizedMatchCreateRelationship(
                        kind="match_create",
                        pattern_kind="relationship",
                        match_node=match_node,
                        predicates=predicates,
                        left=left,
                        relationship=relationship,
                        right=right,
                    )

                if len(match_patterns) == 2:
                    left_match = _parse_node_pattern(
                        _unwrap_node_pattern(match_patterns[0]),
                        default_alias="__humem_match_create_left_match_node",
                    )
                    right_match = _parse_node_pattern(
                        _unwrap_node_pattern(match_patterns[1]),
                        default_alias="__humem_match_create_right_match_node",
                    )
                    _validate_normalized_match_predicates(
                        predicates,
                        alias_kinds={
                            left_match.alias: "node",
                            right_match.alias: "node",
                        },
                    )
                    _validate_match_create_relationship_between_nodes_endpoints(
                        left_match,
                        right_match,
                        left,
                        right,
                    )
                    return NormalizedMatchCreateRelationshipBetweenNodes(
                        kind="match_create",
                        pattern_kind="relationship",
                        left_match=left_match,
                        right_match=right_match,
                        predicates=predicates,
                        left=left,
                        relationship=relationship,
                        right=right,
                    )

                raise ValueError(
                    "HumemCypher v0 MATCH ... CREATE currently supports one matched "
                    "node pattern, or two disconnected matched node patterns, before "
                    "CREATE."
                )

            set_ctx = updating_clauses[0].oC_Set()
            if set_ctx is not None:
                assignments = _parse_set_items(
                    ", ".join(
                        _context_text(result, item_ctx)
                        for item_ctx in set_ctx.oC_SetItem()
                    )
                )

                if _looks_like_relationship_pattern(pattern_text):
                    left_text, relationship_text, right_text, direction = (
                        _split_relationship_pattern(pattern_text)
                    )
                    left = _parse_node_pattern(
                        left_text,
                        default_alias="__humem_set_left_node",
                    )
                    relationship = _parse_relationship_pattern(
                        relationship_text,
                        direction,
                    )
                    right = _parse_node_pattern(
                        right_text,
                        default_alias="__humem_set_right_node",
                    )
                    _validate_normalized_match_predicates(
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
                    _validate_normalized_match_set_assignments(
                        assignments,
                        target_alias=relationship.alias,
                        target_kind="relationship",
                    )
                    return NormalizedSetRelationship(
                        kind="set",
                        pattern_kind="relationship",
                        left=left,
                        relationship=relationship,
                        right=right,
                        predicates=predicates,
                        assignments=assignments,
                    )

                node = _parse_node_pattern(
                    _unwrap_node_pattern(pattern_text),
                    default_alias="__humem_set_node",
                )
                _validate_normalized_match_predicates(
                    predicates,
                    alias_kinds={node.alias: "node"},
                )
                _validate_normalized_match_set_assignments(
                    assignments,
                    target_alias=node.alias,
                    target_kind="node",
                )
                return NormalizedSetNode(
                    kind="set",
                    pattern_kind="node",
                    node=node,
                    predicates=predicates,
                    assignments=assignments,
                )

            delete_ctx = updating_clauses[0].oC_Delete()
            assert delete_ctx is not None
            delete_text = _context_text(result, delete_ctx).strip()
            detach_match = re.fullmatch(
                r"(?is)detach\s+delete\s+(?P<target>[A-Za-z_][A-Za-z0-9_]*)",
                delete_text,
            )
            delete_match = re.fullmatch(
                r"(?is)delete\s+(?P<target>[A-Za-z_][A-Za-z0-9_]*)",
                delete_text,
            )
            detach = detach_match is not None
            target_alias = (
                detach_match.group("target")
                if detach_match is not None
                else delete_match.group("target")
                if delete_match is not None
                else None
            )
            if target_alias is None:
                raise ValueError(
                    "Generated Cypher frontend currently validates only narrow "
                    "MATCH ... DELETE alias and MATCH ... DETACH DELETE alias "
                    "statements."
                )

            if _looks_like_relationship_pattern(pattern_text):
                if detach:
                    raise ValueError(
                        "HumemCypher v0 currently supports DETACH DELETE only "
                        "for matched node aliases."
                    )
                left_text, relationship_text, right_text, direction = (
                    _split_relationship_pattern(pattern_text)
                )
                left = _parse_node_pattern(
                    left_text,
                    default_alias="__humem_delete_left_node",
                )
                relationship = _parse_relationship_pattern(
                    relationship_text,
                    direction,
                )
                right = _parse_node_pattern(
                    right_text,
                    default_alias="__humem_delete_right_node",
                )
                _validate_normalized_match_predicates(
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
                        "HumemCypher v0 MATCH ... DELETE relationship "
                        "statements must delete the matched relationship alias."
                    )
                return NormalizedDeleteRelationship(
                    kind="delete",
                    pattern_kind="relationship",
                    left=left,
                    relationship=relationship,
                    right=right,
                    predicates=predicates,
                )

            node = _parse_node_pattern(
                _unwrap_node_pattern(pattern_text),
                default_alias="__humem_delete_node",
            )
            _validate_normalized_match_predicates(
                predicates,
                alias_kinds={node.alias: "node"},
            )
            if target_alias != node.alias:
                raise ValueError(
                    "HumemCypher v0 MATCH ... DELETE node statements must delete "
                    "the matched node alias."
                )
            if not detach:
                raise ValueError(
                    "HumemCypher v0 currently supports node deletion only through "
                    "DETACH DELETE."
                )
            return NormalizedDeleteNode(
                kind="delete",
                pattern_kind="node",
                node=node,
                predicates=predicates,
                detach=True,
            )

        create_ctx = updating_clauses[0].oC_Create()
        assert create_ctx is not None
        pattern_text = _context_text(result, create_ctx.oC_Pattern())
        create_patterns = _split_comma_separated(pattern_text)
        if len(create_patterns) == 3:
            if any(
                _looks_like_relationship_pattern(pattern)
                for pattern in create_patterns[:2]
            ) or not _looks_like_relationship_pattern(create_patterns[2]):
                raise ValueError(
                    "HumemCypher v0 CREATE currently supports either one node "
                    "pattern, one directed relationship pattern, or the narrow "
                    "three-pattern form with two node patterns followed by one "
                    "relationship pattern."
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
            return NormalizedCreateRelationshipFromSeparatePatterns(
                kind="create",
                pattern_kind="relationship",
                first_node=first_node,
                second_node=second_node,
                left=left,
                relationship=relationship,
                right=right,
            )

        if _looks_like_relationship_pattern(pattern_text):
            left_text, relationship_text, right_text, direction = (
                _split_relationship_pattern(pattern_text)
            )
            return NormalizedCreateRelationship(
                kind="create",
                pattern_kind="relationship",
                left=_parse_node_pattern(
                    left_text,
                    require_label=True,
                    default_alias="__humem_create_left_node",
                ),
                relationship=_parse_relationship_pattern(relationship_text, direction),
                right=_parse_node_pattern(
                    right_text,
                    require_label=True,
                    default_alias="__humem_create_right_node",
                ),
            )

        return NormalizedCreateNode(
            kind="create",
            pattern_kind="node",
            node=_parse_node_pattern(
                _unwrap_node_pattern(pattern_text),
                require_label=True,
                default_alias="__humem_create_node",
            ),
        )

    match_ctx = reading_clauses[0].oC_Match()
    assert match_ctx is not None
    pattern_text = _context_text(result, match_ctx.oC_Pattern())
    predicates: tuple[Predicate, ...] = ()
    where_ctx = match_ctx.oC_Where()
    if where_ctx is not None:
        predicates = _parse_predicates(
            _context_text(result, where_ctx.oC_Expression())
        )

    return_ctx = single_part_query.oC_Return()
    assert return_ctx is not None
    projection_text = _context_text(result, return_ctx.oC_ProjectionBody())
    return_text, order_by, limit, distinct, skip = _split_return_clause(
        projection_text
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
        _validate_normalized_match_predicates(
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
        return NormalizedMatchRelationship(
            kind="match",
            pattern_kind="relationship",
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
    _validate_normalized_match_predicates(
        predicates,
        alias_kinds={node.alias: "node"},
    )
    return NormalizedMatchNode(
        kind="match",
        pattern_kind="node",
        node=node,
        predicates=predicates,
        returns=returns,
        order_by=order_by,
        limit=limit,
        distinct=distinct,
        skip=skip,
    )


def _context_text(result: CypherParseResult, ctx: object) -> str:
    """Return original source text for one ANTLR context."""

    start_index = ctx.start.tokenIndex
    stop_index = ctx.stop.tokenIndex
    return result.token_stream.getText(start=start_index, stop=stop_index)
