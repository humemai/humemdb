"""Validate the admitted HumemCypher subset against parsed structures."""

from __future__ import annotations

from .parser import CypherParseResult, parse_cypher_text


def validate_cypher_text(text: str):
    """Parse and validate one Cypher statement for the current frontend subset."""

    return validate_cypher_parse_result(parse_cypher_text(text))


def validate_cypher_parse_result(result: CypherParseResult):
    """Validate that one parse result fits the current generated-frontend subset."""

    if result.has_errors:
        first_error = result.syntax_errors[0]
        raise ValueError(
            "Generated Cypher frontend reported syntax errors: "
            + (
                f"line {first_error.line}, column {first_error.column}: "
                f"{first_error.message}"
            )
        )

    statement_ctx = result.tree.oC_Statement()
    query_ctx = statement_ctx.oC_Query()
    regular_query_ctx = query_ctx.oC_RegularQuery()
    if regular_query_ctx is None:
        raise ValueError(
            "Generated Cypher frontend currently validates only regular "
            "CREATE and MATCH queries."
        )

    single_query_ctx = regular_query_ctx.oC_SingleQuery()
    single_part_query_ctx = single_query_ctx.oC_SinglePartQuery()
    if single_part_query_ctx is None:
        raise ValueError(
            "Generated Cypher frontend currently validates only single-part "
            "CREATE and MATCH queries."
        )

    updating_clauses = single_part_query_ctx.oC_UpdatingClause()
    reading_clauses = single_part_query_ctx.oC_ReadingClause()
    return_ctx = single_part_query_ctx.oC_Return()

    if len(updating_clauses) > 1 or len(reading_clauses) > 1:
        raise ValueError(
            "Generated Cypher frontend currently validates only one CREATE, "
            "MATCH, or SET clause per statement."
        )

    if updating_clauses and not reading_clauses:
        update_ctx = updating_clauses[0]
        if update_ctx.oC_Create() is None or return_ctx is not None:
            raise ValueError(
                "Generated Cypher frontend currently validates only CREATE "
                "statements in the write subset."
            )
        return single_part_query_ctx

    if reading_clauses and not updating_clauses:
        read_ctx = reading_clauses[0]
        if read_ctx.oC_Match() is None or return_ctx is None:
            raise ValueError(
                "Generated Cypher frontend currently validates only MATCH ... "
                "RETURN statements in the read subset."
            )
        return single_part_query_ctx

    if reading_clauses and updating_clauses:
        read_ctx = reading_clauses[0]
        update_ctx = updating_clauses[0]
        if read_ctx.oC_Match() is None or return_ctx is not None:
            raise ValueError(
                "Generated Cypher frontend currently validates only MATCH ... "
                "SET, MATCH ... CREATE, and narrow MATCH ... DELETE statements "
                "in the mixed read-write subset."
            )

        if (
            update_ctx.oC_Set() is None
            and update_ctx.oC_Create() is None
            and update_ctx.oC_Delete() is None
        ):
            raise ValueError(
                "Generated Cypher frontend currently validates only MATCH ... "
                "SET, MATCH ... CREATE, and narrow MATCH ... DELETE statements "
                "in the mixed read-write subset."
            )
        return single_part_query_ctx

    raise ValueError(
        "Generated Cypher frontend currently validates only CREATE, MATCH ... "
        "RETURN, MATCH ... SET, narrow MATCH ... CREATE, and narrow MATCH ... "
        "DELETE statements."
    )
