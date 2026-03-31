"""Parsing entrypoints for the next HumemDB Cypher frontend."""

from __future__ import annotations

from dataclasses import dataclass

from antlr4 import CommonTokenStream, InputStream
from antlr4.error.ErrorListener import ErrorListener

from .generated.CypherLexer import CypherLexer
from .generated.CypherParser import CypherParser


@dataclass(frozen=True, slots=True)
class CypherSyntaxError:
    """One syntax error reported by the generated Cypher frontend.

    Attributes:
        line: 1-based line number where the parser reported the error.
        column: 0-based column offset where the parser reported the error.
        message: Human-readable parser error text.
    """

    line: int
    column: int
    message: str


@dataclass(frozen=True, slots=True)
class CypherParseResult:
    """Raw parse result from the generated Cypher frontend.

    Attributes:
        source_text: Original Cypher text passed to the parser.
        tree: Raw ANTLR parse tree.
        token_stream: Token stream produced by the generated lexer.
        syntax_errors: Collected syntax errors emitted during parsing.
    """

    source_text: str
    tree: object
    token_stream: object
    syntax_errors: tuple[CypherSyntaxError, ...]

    @property
    def has_errors(self) -> bool:
        """Return whether the parse encountered syntax errors."""

        return bool(self.syntax_errors)


class _CollectingErrorListener(ErrorListener):
    """Collect ANTLR syntax errors into a stable HumemDB-facing structure."""

    def __init__(self) -> None:
        """Initialize the mutable error list used during parsing."""

        self.errors: list[CypherSyntaxError] = []

    def syntaxError(
        self,
        _recognizer,
        _offending_symbol,
        line: int,
        column: int,
        msg: str,
        _exc,
    ) -> None:
        """Record one ANTLR syntax error in HumemDB's error format."""

        self.errors.append(
            CypherSyntaxError(
                line=line,
                column=column,
                message=msg,
            )
        )


def parse_cypher_text(text: str) -> CypherParseResult:
    """Parse one Cypher statement through the generated ANTLR frontend.

    The current result is intentionally raw: a parse tree plus collected syntax
    errors. Later parser work will layer normalization, subset validation, and
    lowering on top of this boundary.
    """

    input_stream = InputStream(text)
    lexer = CypherLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = CypherParser(token_stream)

    error_listener = _CollectingErrorListener()
    lexer.removeErrorListeners()
    parser.removeErrorListeners()
    lexer.addErrorListener(error_listener)
    parser.addErrorListener(error_listener)

    tree = parser.oC_Cypher()
    return CypherParseResult(
        source_text=text,
        tree=tree,
        token_stream=token_stream,
        syntax_errors=tuple(error_listener.errors),
    )
