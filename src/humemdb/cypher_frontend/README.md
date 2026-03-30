# Cypher Frontend Plan

This package is the in-repo home for HumemDB's next Cypher parser expansion.

## Purpose

HumemDB is moving away from broadening the current handwritten Cypher parser as the
main strategy for grammar expansion. The goal here is a Python-first, HumemDB-owned
frontend pipeline with clear boundaries between:

1. grammar inputs
2. generated parser artifacts
3. parse-tree normalization
4. subset validation
5. lowering to HumemDB plans

## First Grammar Source

The first parser-ready grammar source is now the published openCypher 9 ANTLR grammar
artifact, vendored under `grammar/Cypher.g4` and used to generate the checked-in parser
artifacts under `generated/`.

The cloned openCypher main repository remains useful for:

1. the current ISO WG3 BNF reference in `grammar/openCypher.bnf`
2. the clause-organized TCK scenarios in `tck/features/**`

The cloned main repo should not be treated as the runtime parser package by itself.

## Initial Layout

- `grammar/`: grammar sources or references that HumemDB owns or vendors
- `generated/`: generated parser artifacts checked in or regenerated explicitly
- `parser.py`: parsing entrypoint layer
- `normalize.py`: parse-tree to HumemDB-normalized structures
- `validate.py`: admitted HumemCypher subset checks
- `lower.py`: lowering into internal execution plans
- `tck/`: subset mapping notes or TCK integration helpers

## Near-Term Work

1. expand the normalize, validate, and lowering layers beyond the current admitted
  `CREATE`, `MATCH ... RETURN`, and `MATCH ... SET` shapes so handwritten fallback
  can shrink further as grammar breadth grows
2. begin clause-by-clause TCK adoption for `create`, `match`, and `match-where`

## Regenerating Artifacts

HumemDB keeps the generated parser artifacts checked in under `generated/`.
The shipped Python wheel should include those generated Python files directly.
Java is only a containerized development-time prerequisite for regenerating or
verifying the artifacts, not a runtime dependency for using the installed package.

Use the repository helper to refresh or verify them.

`scripts/dev/regenerate_cypher_frontend_docker.sh`

This wrapper builds a small dev-only container, mounts the repository, and runs the
internal Python helper inside it. The container currently pins
`eclipse-temurin:25-jdk`, which is the newest stable Java image tag verified from this
environment. This keeps Java out of the host machine while keeping the wheel runtime
Python-only.

Prerequisites:

1. Docker must be available.

Useful modes:

1. regenerate artifacts in place with Docker:
  `scripts/dev/regenerate_cypher_frontend_docker.sh`
2. verify that checked-in artifacts are current with Docker:
  `scripts/dev/regenerate_cypher_frontend_docker.sh --check`

After regeneration, rerun the focused frontend and planning tests before keeping the
updated artifacts.
