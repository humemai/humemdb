#!/usr/bin/env python3
"""Normalize Markdown formatting in the docs tree.

Rules:
1. Headings should start at column 0 when not inside a code fence.
2. A line ending with ':' should be followed by a blank line when the next
   non-empty line is a list item or code fence.
3. List item indentation should be normalized to 4-space nesting.
4. In Python fenced code blocks, obvious accidental over-indentation after a
   block opener is clamped to one additional indentation level.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable

FENCE_RE = re.compile(r"^\s*([`~]{3,})(.*)$")
HEADING_RE = re.compile(r"^\s*(#{1,6})\s+\S")
LIST_RE = re.compile(r"^(\s*)(?:[-+*]|\d+\.)\s+")


def iter_md_files(root: Path) -> Iterable[Path]:
    return root.rglob("*.md")


def normalize_indent(indent_len: int) -> int:
    if indent_len <= 0:
        return 0
    return ((indent_len + 3) // 4) * 4


def _format_line_list(lines: list[int], max_items: int = 20) -> str:
    if not lines:
        return ""
    if len(lines) <= max_items:
        return ", ".join(str(number) for number in lines)
    head = ", ".join(str(number) for number in lines[:max_items])
    return f"{head}, ... (+{len(lines) - max_items} more)"


def _parse_fence(line: str) -> tuple[str, int] | None:
    match = FENCE_RE.match(line)
    if not match:
        return None
    fence = match.group(1)
    if fence[0] not in ("`", "~"):
        return None
    return fence[0], len(fence)


def _is_python_fence(line: str) -> bool:
    match = FENCE_RE.match(line.lstrip())
    if not match:
        return False
    info = match.group(2).strip().lower()
    return info.startswith("python")


def _is_fence_close(line: str, fence_char: str, fence_len: int) -> bool:
    stripped = line.lstrip()
    if not stripped.startswith(fence_char * fence_len):
        return False

    run = 0
    for char in stripped:
        if char == fence_char:
            run += 1
        else:
            break
    return run >= fence_len


def process_file(
    path: Path,
) -> tuple[
    int,
    int,
    int,
    int,
    list[int],
    list[int],
    list[int],
    list[int],
    int,
    list[int],
] | None:
    original = path.read_text(encoding="utf-8")
    lines = original.splitlines()
    new_lines: list[str] = []

    in_fence = False
    in_python_fence = False
    fence_char: str | None = None
    fence_len: int | None = None
    changed = False

    header_fixes = 0
    blank_lines = 0
    heading_blank_lines = 0
    list_indent_fixes = 0
    code_indent_fixes = 0

    header_lines: list[int] = []
    blank_lines_at: list[int] = []
    heading_blank_lines_at: list[int] = []
    list_indent_lines: list[int] = []
    code_indent_lines: list[int] = []

    last_list_indent_len: int | None = None
    last_list_fixed_len: int | None = None
    last_code_indent_len: int | None = None
    last_code_line: str | None = None

    index = 0
    while index < len(lines):
        line = lines[index]

        if in_fence:
            if fence_char is not None and fence_len is not None:
                if _is_fence_close(line, fence_char, fence_len):
                    in_fence = False
                    in_python_fence = False
                    fence_char = None
                    fence_len = None
                    last_code_indent_len = None
                    last_code_line = None
                    new_lines.append(line)
                    index += 1
                    continue

            if in_python_fence and line.strip():
                indent_len = len(line) - len(line.lstrip(" "))
                if (
                    last_code_indent_len is not None
                    and last_code_line is not None
                    and last_code_line.rstrip().endswith(":")
                    and indent_len > last_code_indent_len + 4
                ):
                    line = " " * (last_code_indent_len + 4) + line.lstrip(" ")
                    changed = True
                    code_indent_fixes += 1
                    code_indent_lines.append(index + 1)

                last_code_indent_len = len(line) - len(line.lstrip(" "))
                last_code_line = line
            elif in_python_fence:
                last_code_indent_len = None
                last_code_line = None

            new_lines.append(line)
            index += 1
            continue

        fence = _parse_fence(line)
        if fence:
            in_fence = True
            in_python_fence = _is_python_fence(line)
            fence_char, fence_len = fence
            last_code_indent_len = None
            last_code_line = None
            new_lines.append(line)
            index += 1
            continue

        if HEADING_RE.match(line):
            stripped = line.lstrip()
            if stripped != line:
                line = stripped
                changed = True
                header_fixes += 1
                header_lines.append(index + 1)

            if index + 1 < len(lines) and lines[index + 1] != "":
                new_lines.append(line)
                new_lines.append("")
                changed = True
                heading_blank_lines += 1
                heading_blank_lines_at.append(index + 1)
                index += 1
                continue

        if line.rstrip().endswith(":"):
            lookahead = index + 1
            while lookahead < len(lines) and lines[lookahead] == "":
                lookahead += 1

            if lookahead < len(lines):
                list_match = LIST_RE.match(lines[lookahead])
                is_fence_next = _parse_fence(lines[lookahead]) is not None
                is_current_list = LIST_RE.match(line) is not None
                if (
                    (list_match or is_fence_next)
                    and not is_current_list
                    and index + 1 < len(lines)
                    and lines[index + 1] != ""
                ):
                    new_lines.append(line)
                    new_lines.append("")
                    changed = True
                    blank_lines += 1
                    blank_lines_at.append(index + 1)
                    index += 1
                    continue

        list_match = LIST_RE.match(line)
        if list_match:
            indent = list_match.group(1)
            indent_len = len(indent.replace("\t", "    "))
            fixed_len = normalize_indent(indent_len)

            if (
                last_list_indent_len is not None
                and last_list_fixed_len is not None
                and indent_len > last_list_indent_len
                and fixed_len <= last_list_fixed_len
            ):
                fixed_len = last_list_fixed_len + 4

            if fixed_len != indent_len:
                line = " " * fixed_len + line[len(indent):]
                changed = True
                list_indent_fixes += 1
                list_indent_lines.append(index + 1)

            last_list_indent_len = indent_len
            last_list_fixed_len = fixed_len

        new_lines.append(line)
        index += 1

    new_content = "\n".join(new_lines)
    if original.endswith("\n"):
        new_content += "\n"

    if not changed:
        return None

    path.write_text(new_content, encoding="utf-8")
    return (
        header_fixes,
        blank_lines,
        list_indent_fixes,
        code_indent_fixes,
        header_lines,
        blank_lines_at,
        list_indent_lines,
        code_indent_lines,
        heading_blank_lines,
        heading_blank_lines_at,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Normalize Markdown formatting in docs.",
    )
    parser.add_argument(
        "--docs",
        default="/mnt/ssd2/repos/humemdb/docs",
        help="Path to docs directory",
    )
    args = parser.parse_args()

    docs_root = Path(args.docs)
    if not docs_root.exists():
        raise SystemExit(f"Docs path does not exist: {docs_root}")

    changed_files: list[
        tuple[
            Path,
            int,
            int,
            int,
            int,
            list[int],
            list[int],
            list[int],
            list[int],
            int,
            list[int],
        ]
    ] = []
    total_header = 0
    total_blank = 0
    total_indent = 0
    total_code_indent = 0

    for md_file in iter_md_files(docs_root):
        result = process_file(md_file)
        if result is None:
            continue

        (
            header_fixes,
            blank_lines,
            list_indent_fixes,
            code_indent_fixes,
            header_lines,
            blank_lines_at,
            list_indent_lines,
            code_indent_lines,
            heading_blank_lines,
            heading_blank_lines_at,
        ) = result
        changed_files.append(
            (
                md_file,
                header_fixes,
                blank_lines,
                list_indent_fixes,
                code_indent_fixes,
                header_lines,
                blank_lines_at,
                list_indent_lines,
                code_indent_lines,
                heading_blank_lines,
                heading_blank_lines_at,
            )
        )
        total_header += header_fixes
        total_blank += blank_lines + heading_blank_lines
        total_indent += list_indent_fixes
        total_code_indent += code_indent_fixes

    print(f"Updated {len(changed_files)} files")
    print(
        "Totals: "
        f"headers fixed={total_header}, "
        f"blank lines inserted={total_blank}, "
        f"list indents normalized={total_indent}, "
        f"python code indents normalized={total_code_indent}",
    )
    for (
        md_file,
        header_fixes,
        blank_lines,
        list_indent_fixes,
        code_indent_fixes,
        header_lines,
        blank_lines_at,
        list_indent_lines,
        code_indent_lines,
        heading_blank_lines,
        heading_blank_lines_at,
    ) in changed_files:
        detail_parts: list[str] = []
        if header_fixes:
            detail_parts.append(
                f"headers({header_fixes}) at {_format_line_list(header_lines)}"
            )
        if blank_lines:
            detail_parts.append(
                f"colon spacing({blank_lines}) at {_format_line_list(blank_lines_at)}"
            )
        if heading_blank_lines:
            detail_parts.append(
                "heading spacing"
                f"({heading_blank_lines}) at "
                f"{_format_line_list(heading_blank_lines_at)}"
            )
        if list_indent_fixes:
            detail_parts.append(
                "list indents"
                f"({list_indent_fixes}) at {_format_line_list(list_indent_lines)}"
            )
        if code_indent_fixes:
            detail_parts.append(
                "python code indents"
                f"({code_indent_fixes}) at {_format_line_list(code_indent_lines)}"
            )
        details = "; ".join(detail_parts) if detail_parts else "changed"
        print(f"- {md_file}: {details}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
