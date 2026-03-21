# Installation

## Requirements

- Python 3.10 or newer.
- A normal local Python environment managed with `uv`.

## Install from source

```bash
uv pip install .
```

## Install in editable mode

```bash
uv pip install -e .
```

## Contributor setup

```bash
uv sync
```

This installs the locked project environment defined by `pyproject.toml` and
`uv.lock`.

When dependencies change:

```bash
uv lock
uv sync
```

## Optional docs dependencies

To build the docs site locally:

```bash
uv sync --group docs
```

## Installed runtime dependencies

- `sqlite3` from the Python standard library for the canonical local write path.
- `duckdb` for analytical reads.
- `numpy` for the exact vector search baseline.
- `sqlglot[c]` for PostgreSQL-like SQL translation.
- `lancedb` for benchmark work and future indexed ANN paths.
- `threadpoolctl` for thread-pool coordination.

LanceDB is present for benchmark and later accelerated vector work, but the shipped
default vector runtime today is still the exact SQLite plus NumPy path.

## Licensing note

HumemDB's own code is MIT-licensed, but installed dependencies keep their own licenses.
See the project [LICENSE]({{ config.repo_url }}/blob/{{ config.extra.version_tag
}}/LICENSE) for HumemDB itself and the lockfile plus package metadata for the concrete
third-party dependency set.
