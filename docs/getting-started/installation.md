# Installation

## Requirements

- Python 3.12 or newer.
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

- `duckdb`
- `lancedb`
- `numpy`
- `sqlglot[c]`
- `threadpoolctl`

LanceDB is present for benchmark and later accelerated vector work, but the shipped
default vector runtime today is still the exact SQLite plus NumPy path.